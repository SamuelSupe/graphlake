use std::{collections::BTreeMap, ops::Range, path::Path, sync::Arc};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::{Session, TableProvider},
    common::DFSchema,
    datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
    },
    execution::{
        memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
        object_store::ObjectStoreUrl,
    },
    logical_expr::{Expr, TableProviderFilterPushDown, TableType, utils::conjunction},
    physical_plan::{ExecutionPlan, metrics::ExecutionPlanMetricsSet},
};
use datafusion_datasource::{PartitionedFile, source::DataSourceExec};
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture};
use object_store::{
    ClientOptions, GetOptions, GetRange, ObjectMeta, ObjectStore, ObjectStoreExt,
    aws::AmazonS3Builder, http::HttpBuilder, path::Path as ObjectPath,
};
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    },
    errors::{ParquetError, Result as ParquetResult},
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        statistics::Statistics,
    },
};
use url::Url;

use super::{ReadStats, SourceVersion};

#[cfg(feature = "admin")]
pub(crate) async fn object_version(
    location: &str,
    pool: Arc<dyn MemoryPool>,
    stats: Arc<ReadStats>,
) -> Result<SourceVersion> {
    Ok(PinnedFile::open(location, pool, stats)
        .await?
        .version(location))
}

#[derive(Clone, Debug)]
struct PinnedFile {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    local: bool,
    parallel_ranges: bool,
    pool: Arc<dyn MemoryPool>,
    stats: Arc<ReadStats>,
}

impl PinnedFile {
    fn version(&self, location: &str) -> SourceVersion {
        SourceVersion {
            source: location.to_owned(),
            version: self
                .meta
                .version
                .clone()
                .or_else(|| self.meta.e_tag.clone())
                .unwrap_or_default(),
            consistency: "pinned object",
        }
    }

    async fn open(file: &str, pool: Arc<dyn MemoryPool>, stats: Arc<ReadStats>) -> Result<Self> {
        let url = if file.contains("://") {
            Url::parse(file).context("invalid Parquet object URL")?
        } else {
            Url::from_file_path(std::path::absolute(Path::new(file))?)
                .map_err(|_| anyhow::anyhow!("invalid Parquet file path"))?
        };
        ensure!(
            matches!(url.scheme(), "file" | "http" | "https" | "s3"),
            "unsupported Parquet URL scheme {}",
            url.scheme()
        );
        ensure!(
            url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "Parquet URLs must be object locations without credentials, query parameters or fragments"
        );
        let (store, path): (Arc<dyn ObjectStore>, ObjectPath) = if url.scheme() == "s3" {
            let store = AmazonS3Builder::from_env()
                .with_bucket_name(url.host_str().context("S3 URL requires a bucket")?)
                .build()?;
            (Arc::new(store), ObjectPath::from_url_path(url.path())?)
        } else if matches!(url.scheme(), "http" | "https") {
            let store = HttpBuilder::new()
                .with_url(&url[..url::Position::BeforePath])
                .with_client_options(ClientOptions::new().with_allow_http(url.scheme() == "http"))
                .build()?;
            (Arc::new(store), ObjectPath::from_url_path(url.path())?)
        } else {
            let (store, path) = object_store::parse_url(&url)?;
            (Arc::from(store), path)
        };
        stats.request();
        let meta = store
            .head(&path)
            .await
            .with_context(|| format!("pinning Parquet object {url}"))?;
        ensure!(
            meta.version.is_some()
                || meta
                    .e_tag
                    .as_ref()
                    .is_some_and(|tag| !tag.starts_with("W/")),
            "Parquet object {url} must provide an object version or strong ETag"
        );
        Ok(Self {
            store,
            meta,
            local: url.scheme() == "file",
            parallel_ranges: false,
            pool,
            stats,
        })
    }
    #[cfg(unix)]
    async fn read_local_ranges(&self, ranges: Vec<Range<u64>>) -> ParquetResult<Vec<Bytes>> {
        use object_store::GetResultPayload;
        use std::os::unix::fs::FileExt;

        let external = |error| ParquetError::External(Box::new(error));
        let mut buffers = Vec::with_capacity(ranges.len());
        for range in ranges {
            let len = range
                .end
                .checked_sub(range.start)
                .filter(|_| range.end <= self.meta.size)
                .and_then(|len| usize::try_from(len).ok())
                .ok_or_else(|| ParquetError::General("invalid Parquet object range".into()))?;
            let reservation = MemoryConsumer::new("GraphLake Parquet range").register(&self.pool);
            reservation
                .try_grow(len)
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            buffers.push((range.start, len, reservation));
        }
        self.stats.request();
        let result = self
            .store
            .get_opts(
                &self.meta.location,
                GetOptions {
                    if_match: self.meta.e_tag.clone(),
                    ..Default::default()
                },
            )
            .await
            .map_err(external)?;
        let GetResultPayload::File(file, _) = result.payload else {
            return Err(ParquetError::General(
                "expected a local Parquet file".into(),
            ));
        };
        let values = tokio::task::spawn_blocking(move || {
            buffers
                .into_iter()
                .map(|(offset, len, reservation)| {
                    let mut bytes = vec![0; len];
                    file.read_exact_at(&mut bytes, offset)?;
                    Ok(Bytes::from_owner(ReservedBytes {
                        bytes: Bytes::from(bytes),
                        _reservation: reservation,
                    }))
                })
                .collect::<std::io::Result<Vec<_>>>()
        })
        .await
        .map_err(|error| ParquetError::External(Box::new(error)))??;
        self.stats.request();
        let current = self
            .store
            .head(&self.meta.location)
            .await
            .map_err(external)?;
        if current.e_tag != self.meta.e_tag || current.size != self.meta.size {
            return Err(ParquetError::General(
                "local Parquet file changed during range read".into(),
            ));
        }
        self.stats.object_bytes(values.iter().map(Bytes::len).sum());
        Ok(values)
    }
}

impl AsyncFileReader for PinnedFile {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        async move {
            if range.start > range.end || range.end > self.meta.size {
                return Err(ParquetError::General("invalid Parquet object range".into()));
            }
            let len = usize::try_from(range.end - range.start).map_err(|_| {
                ParquetError::General("Parquet range exceeds platform capacity".into())
            })?;
            if len == 0 {
                return Ok(Bytes::new());
            }
            let reservation = MemoryConsumer::new("GraphLake Parquet range").register(&self.pool);
            reservation
                .try_grow(len)
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            self.stats.request();
            let result = self
                .store
                .get_opts(
                    &self.meta.location,
                    GetOptions {
                        version: self.meta.version.clone(),
                        if_match: self.meta.e_tag.clone(),
                        range: Some(GetRange::Bounded(range.clone())),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            if result.range != range
                || result.meta.size != self.meta.size
                || self
                    .meta
                    .e_tag
                    .as_ref()
                    .is_some_and(|tag| result.meta.e_tag.as_ref() != Some(tag))
                || self
                    .meta
                    .version
                    .as_ref()
                    .is_some_and(|version| result.meta.version.as_ref() != Some(version))
            {
                return Err(ParquetError::General(format!(
                    "Parquet object changed during query: {}",
                    self.meta.location
                )));
            }
            let bytes = result
                .bytes()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            if bytes.len() != len {
                return Err(ParquetError::General(
                    "incomplete Parquet object range".into(),
                ));
            }
            if self.local {
                self.stats.request();
                let current = self
                    .store
                    .head(&self.meta.location)
                    .await
                    .map_err(|e| ParquetError::External(Box::new(e)))?;
                if current.e_tag != self.meta.e_tag {
                    return Err(ParquetError::General(
                        "local Parquet file changed during range read".into(),
                    ));
                }
            }
            self.stats.object_bytes(len);
            Ok(Bytes::from_owner(ReservedBytes {
                bytes,
                _reservation: reservation,
            }))
        }
        .boxed()
    }

    fn get_byte_ranges(
        &mut self,
        ranges: Vec<Range<u64>>,
    ) -> BoxFuture<'_, ParquetResult<Vec<Bytes>>> {
        async move {
            #[cfg(unix)]
            if self.parallel_ranges && self.local && ranges.len() > 1 {
                return self.read_local_ranges(ranges).await;
            }
            // Bound concurrent object reads and keep the decoder's range order.
            let parallelism = if self.parallel_ranges { 8 } else { 1 };
            futures::stream::iter(ranges.into_iter().map(|range| {
                let mut reader = self.clone();
                async move { reader.get_bytes(range).await }
            }))
            .buffered(parallelism)
            .try_collect()
            .await
        }
        .boxed()
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        async move {
            let size = self.meta.size;
            let metadata = ParquetMetaDataReader::new()
                .with_prefetch_hint(Some(64 * 1024))
                .with_arrow_reader_options(options)
                .load_and_finish(self, size)
                .await?;
            Ok(Arc::new(metadata))
        }
        .boxed()
    }
}

struct ReservedBytes {
    bytes: Bytes,
    _reservation: MemoryReservation,
}

impl AsRef<[u8]> for ReservedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

#[derive(Debug)]
struct PinnedReaderFactory(BTreeMap<ObjectPath, PinnedFile>);

#[cfg(feature = "admin")]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(crate) struct ScanObject {
    pub path: String,
    pub location: String,
    pub version: String,
    pub size: u64,
    pub row_groups: Vec<i64>,
}

impl ParquetFileReaderFactory for PinnedReaderFactory {
    fn create_reader(
        &self,
        _: usize,
        file: PartitionedFile,
        _: Option<usize>,
        _: &ExecutionPlanMetricsSet,
    ) -> datafusion::common::Result<Box<dyn AsyncFileReader + Send>> {
        let reader = self.0.get(&file.object_meta.location).ok_or_else(|| {
            datafusion::common::DataFusionError::Internal("missing pinned Parquet file".into())
        })?;
        Ok(Box::new(reader.clone()))
    }
}

#[derive(Debug)]
pub(crate) struct ParquetTable {
    schema: SchemaRef,
    readers: Arc<PinnedReaderFactory>,
    files: Vec<PartitionedFile>,
    metadata: Vec<Arc<ParquetMetaData>>,
    #[cfg(feature = "admin")]
    scan_objects: Vec<ScanObject>,
}

impl ParquetTable {
    #[cfg(feature = "admin")]
    pub(crate) async fn pinned_scan(
        objects: &[ScanObject],
        scope: &crate::engine::QueryScope,
    ) -> Result<Arc<dyn ParquetFileReaderFactory>> {
        let mut readers = BTreeMap::new();
        for expected in objects {
            let file = PinnedFile::open(
                &expected.location,
                Arc::clone(&scope.runtime.memory_pool),
                Arc::clone(&scope.reads),
            )
            .await?;
            ensure!(
                file.version(&expected.location).version == expected.version
                    && file.meta.size == expected.size,
                "distributed Parquet snapshot changed: {}",
                expected.location
            );
            readers.insert(ObjectPath::parse(&expected.path)?, file);
        }
        Ok(Arc::new(PinnedReaderFactory(readers)))
    }
    #[cfg(feature = "admin")]
    pub(crate) fn for_exploration(mut self) -> Self {
        for file in Arc::get_mut(&mut self.readers)
            .expect("new Parquet table")
            .0
            .values_mut()
        {
            file.parallel_ranges = true;
        }
        self
    }

    pub(crate) fn integer_groups(&self, column: &str) -> Option<Vec<(i64, i64, usize)>> {
        self.metadata
            .iter()
            .flat_map(|file| file.row_groups())
            .map(|group| {
                let column = group
                    .columns()
                    .iter()
                    .find(|field| field.column_path().parts() == [column])?;
                let (min, max) = match column.statistics()? {
                    Statistics::Int32(stats) => {
                        (i64::from(*stats.min_opt()?), i64::from(*stats.max_opt()?))
                    }
                    Statistics::Int64(stats) => (*stats.min_opt()?, *stats.max_opt()?),
                    _ => return None,
                };
                Some((min, max, usize::try_from(group.num_rows()).ok()?))
            })
            .collect()
    }

    pub(crate) async fn open(
        locations: &[String],
        columns: &[String],
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<(Self, Vec<SourceVersion>)> {
        let mut versions = Vec::new();
        let mut schema = None;
        let mut readers = BTreeMap::new();
        let mut files = Vec::new();
        let mut metadata = Vec::new();
        #[cfg(feature = "admin")]
        let mut scan_objects = Vec::new();
        for (index, location) in locations.iter().enumerate() {
            let file = PinnedFile::open(location, Arc::clone(&pool), Arc::clone(&stats)).await?;
            let builder = ParquetRecordBatchStreamBuilder::new(file.clone())
                .await
                .with_context(|| format!("reading Parquet schema for {location}"))?;
            metadata.push(Arc::clone(builder.metadata()));
            #[cfg(feature = "admin")]
            scan_objects.push(ScanObject {
                path: format!("object-{index}.parquet"),
                location: location.clone(),
                version: file.version(location).version,
                size: file.meta.size,
                row_groups: builder
                    .metadata()
                    .row_groups()
                    .iter()
                    .map(|group| {
                        let column = group.column(0);
                        column
                            .dictionary_page_offset()
                            .unwrap_or_else(|| column.data_page_offset())
                    })
                    .collect(),
            });
            let arrow_schema = builder.schema();
            let fields = if columns.is_empty() {
                arrow_schema
                    .fields()
                    .iter()
                    .map(|field| field.as_ref().clone())
                    .collect()
            } else {
                columns
                    .iter()
                    .map(|name| {
                        arrow_schema
                            .field_with_name(name)
                            .cloned()
                            .with_context(|| crate::catalog::ValidationSite::Column {
                                column: name.clone(),
                            })
                            .with_context(|| format!("missing column {name} in {location}"))
                    })
                    .collect::<Result<Vec<_>>>()?
            };
            if let Some(expected) = &schema {
                let expected: &SchemaRef = expected;
                ensure!(
                    fields.len() == expected.fields().len(),
                    "Parquet files have different field counts"
                );
                for (field, required) in fields.iter().zip(expected.fields()) {
                    ensure!(
                        field.name() == required.name()
                            && field.data_type() == required.data_type(),
                        "Parquet column {} has inconsistent types across files",
                        field.name()
                    );
                }
            } else {
                schema = Some(Arc::new(Schema::new(
                    fields
                        .into_iter()
                        .map(|field| {
                            // Nullability may differ across files with the same logical field types.
                            field.with_nullable(true)
                        })
                        .collect::<Vec<_>>(),
                )));
            }
            versions.push(file.version(location));
            let mut meta = file.meta.clone();
            // The same object path can occur in different buckets or on different hosts.
            meta.location = ObjectPath::from(format!("object-{index}.parquet"));
            readers.insert(meta.location.clone(), file);
            files.push(PartitionedFile::new_from_meta(meta));
        }
        Ok((
            Self {
                schema: schema.context("no Parquet files")?,
                readers: Arc::new(PinnedReaderFactory(readers)),
                files,
                metadata,
                #[cfg(feature = "admin")]
                scan_objects,
            },
            versions,
        ))
    }
}

#[async_trait]
impl TableProvider for ParquetTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> datafusion::common::Result<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let mut source = ParquetSource::new(self.schema())
            .with_parquet_file_reader_factory(self.readers.clone());
        if let Some(predicate) = conjunction(filters.iter().cloned()) {
            let schema = DFSchema::try_from(self.schema())?;
            source = source.with_predicate(state.create_physical_expr(predicate, &schema)?);
        }
        #[cfg(feature = "admin")]
        let files = self
            .files
            .iter()
            .zip(&self.scan_objects)
            .map(|(file, object)| file.clone().with_extension(object.clone()))
            .collect();
        #[cfg(not(feature = "admin"))]
        let files = self.files.clone();
        let config =
            FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), Arc::new(source))
                .with_file_group(FileGroup::new(files))
                .with_batch_size(Some(state.config().batch_size()))
                .with_projection_indices(projection.cloned())?
                .with_limit(limit)
                .build();
        Ok(DataSourceExec::from_data_source(config))
    }
}
