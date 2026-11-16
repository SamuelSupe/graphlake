use std::{collections::BTreeMap, ops::Range, path::Path, sync::Arc};

use anyhow::{Context, Result, ensure};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::{
    arrow::datatypes::{Schema, SchemaRef},
    catalog::{Session, TableProvider},
    common::{DFSchema, DataFusionError, ScalarValue},
    datasource::physical_plan::{
        FileGroup, FileScanConfigBuilder, ParquetFileReaderFactory, ParquetSource,
    },
    execution::{
        memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation},
        object_store::ObjectStoreUrl,
    },
    logical_expr::{
        Expr, Operator, TableProviderFilterPushDown, TableType,
        utils::{conjunction, split_conjunction},
    },
    physical_plan::{ExecutionPlan, empty::EmptyExec, metrics::ExecutionPlanMetricsSet},
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
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        statistics::Statistics,
    },
};
use url::Url;

use super::parquet_snapshot::{self, Part, RemoteFile, RemoteSnapshot, Snapshot};
use super::{ReadStats, SourceVersion};

pub(crate) async fn object_version(
    location: &str,
    store: Option<&Arc<dyn ObjectStore>>,
    pool: Arc<dyn MemoryPool>,
    stats: Arc<ReadStats>,
) -> Result<SourceVersion> {
    if store.is_none()
        && let Some(path) = parquet_snapshot::managed_location(location)
        && let Some(snapshot) = Snapshot::read(&path)?
    {
        return Ok(snapshot_version(location, &snapshot));
    }
    Ok(PinnedFile::open_storage(location, store, pool, stats)
        .await?
        .version(location))
}

fn snapshot_version(location: &str, snapshot: &Snapshot) -> SourceVersion {
    SourceVersion {
        source: location.to_owned(),
        version: snapshot.generation.to_string(),
        consistency: "pinned object",
    }
}

#[derive(Clone, Debug)]
pub(super) struct PinnedFile {
    store: Arc<dyn ObjectStore>,
    meta: ObjectMeta,
    local: bool,
    parallel_ranges: bool,
    pool: Arc<dyn MemoryPool>,
    stats: Arc<ReadStats>,
    footer: Option<Arc<ParquetMetaData>>,
    _snapshot_lease: Option<Arc<std::fs::File>>,
}

impl PinnedFile {
    pub(super) fn version(&self, location: &str) -> SourceVersion {
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

    pub(super) fn len(&self) -> u64 {
        self.meta.size
    }

    pub(super) async fn open(
        file: &str,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        Self::open_storage(file, None, pool, stats).await
    }

    pub(super) async fn open_storage(
        file: &str,
        store: Option<&Arc<dyn ObjectStore>>,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        if let Some(store) = store {
            let path = super::storage::object_path(file)?;
            return Self::pin(Arc::clone(store), path, false, pool, stats).await;
        }
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
        Self::pin(store, path, url.scheme() == "file", pool, stats).await
    }

    async fn pin(
        store: Arc<dyn ObjectStore>,
        path: ObjectPath,
        local: bool,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        stats.request();
        let meta = store
            .head(&path)
            .await
            .with_context(|| format!("pinning object {path}"))?;
        ensure!(
            meta.version.is_some()
                || meta
                    .e_tag
                    .as_ref()
                    .is_some_and(|tag| !tag.starts_with("W/")),
            "object {path} must provide a version or strong ETag"
        );
        Ok(Self {
            store,
            meta,
            local,
            parallel_ranges: false,
            pool,
            stats,
            footer: None,
            _snapshot_lease: None,
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
            if !self.local {
                // Merge nearby chunks without dropping object version checks or
                // changing the order expected by the Parquet decoder.
                let count = ranges.len();
                let mut ordered = ranges.into_iter().enumerate().collect::<Vec<_>>();
                ordered.sort_unstable_by_key(|(_, range)| (range.start, range.end));
                let mut requests: Vec<(Range<u64>, Vec<(usize, Range<u64>)>)> = Vec::new();
                for (index, range) in ordered {
                    if range.start > range.end || range.end > self.meta.size {
                        return Err(ParquetError::General("invalid Parquet object range".into()));
                    }
                    if let Some((merged, parts)) = requests.last_mut()
                        && range.start <= merged.end.saturating_add(4096)
                        && range.end.saturating_sub(merged.start) <= 1024 * 1024
                    {
                        merged.end = merged.end.max(range.end);
                        parts.push((index, range));
                    } else {
                        requests.push((range.clone(), vec![(index, range)]));
                    }
                }
                let mut fetched =
                    futures::stream::iter(requests.into_iter().map(|(range, parts)| {
                        let mut reader = self.clone();
                        async move {
                            let start = range.start;
                            Ok::<_, ParquetError>((start, parts, reader.get_bytes(range).await?))
                        }
                    }))
                    .buffer_unordered(8);
                let mut result = vec![Bytes::new(); count];
                while let Some((start, parts, bytes)) = fetched.try_next().await? {
                    for (index, range) in parts {
                        result[index] = bytes
                            .slice((range.start - start) as usize..(range.end - start) as usize);
                    }
                }
                return Ok(result);
            }
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
            // Managed parts keep their UUID for the lifetime of their contents.
            // Reuse the pinned footer read while resolving the table schema.
            if let Some(footer) = &self.footer
                && options.is_none_or(|options| {
                    options.column_index_policy() == PageIndexPolicy::Skip
                        && options.offset_index_policy() == PageIndexPolicy::Skip
                })
            {
                return Ok(Arc::clone(footer));
            }
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mask: Option<Part>,
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
    input: ParquetInput,
    parallel_ranges: bool,
}

type PartInput = (
    String,
    Option<Arc<Part>>,
    Option<Arc<std::fs::File>>,
    Option<RemoteFile>,
);

#[derive(Debug)]
enum ParquetInput {
    Open(OpenedParquet),
    Snapshot {
        key: Option<String>,
        parts: Vec<PartInput>,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    },
}

#[derive(Debug)]
struct OpenedParquet {
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
        self.parallel_ranges = true;
        if let ParquetInput::Open(open) = &mut self.input {
            for file in Arc::get_mut(&mut open.readers)
                .expect("new Parquet table")
                .0
                .values_mut()
            {
                file.parallel_ranges = true;
            }
        }
        self
    }

    pub(crate) fn integer_groups(&self, column: &str) -> Option<Vec<(i64, i64, usize)>> {
        let ParquetInput::Open(open) = &self.input else {
            return None;
        };
        if open.files.iter().any(|file| {
            file.extension::<datafusion_datasource_parquet::ParquetRowSelection>()
                .is_some()
        }) {
            return None;
        }
        open.metadata
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
        prune_snapshot: bool,
    ) -> Result<(Self, Vec<SourceVersion>)> {
        Self::open_storage(locations, columns, None, pool, stats, prune_snapshot).await
    }

    pub(crate) async fn open_with_managed(
        locations: &[String],
        snapshots: &BTreeMap<uuid::Uuid, RemoteSnapshot>,
        columns: &[String],
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
        prune_snapshot: bool,
    ) -> Result<(Self, Vec<SourceVersion>)> {
        Self::open_inner(
            locations,
            columns,
            None,
            pool,
            stats,
            prune_snapshot,
            snapshots,
        )
        .await
    }

    pub(crate) async fn open_storage(
        locations: &[String],
        columns: &[String],
        storage: Option<&crate::StorageConfig>,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
        prune_snapshot: bool,
    ) -> Result<(Self, Vec<SourceVersion>)> {
        Self::open_inner(
            locations,
            columns,
            storage,
            pool,
            stats,
            prune_snapshot,
            &BTreeMap::new(),
        )
        .await
    }

    async fn open_inner(
        locations: &[String],
        columns: &[String],
        storage: Option<&crate::StorageConfig>,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
        prune_snapshot: bool,
        snapshots: &BTreeMap<uuid::Uuid, RemoteSnapshot>,
    ) -> Result<(Self, Vec<SourceVersion>)> {
        let store = storage.map(super::storage::open).transpose()?;
        let mut versions = Vec::new();
        let mut inputs = Vec::new();
        for location in locations {
            if let Some(id) = parquet_snapshot::logical_id(location) {
                let frozen = snapshots
                    .get(&id)
                    .context("managed local table has no pinned generation")?;
                versions.push(SourceVersion {
                    source: location.clone(),
                    version: frozen.generation.to_string(),
                    consistency: "pinned object",
                });
                if let Some(snapshot) = &frozen.snapshot {
                    for part in &snapshot.parts {
                        let file = frozen
                            .files
                            .get(&part.id)
                            .context("managed snapshot is missing a part")?;
                        inputs.push((
                            file.location.clone(),
                            Some(Arc::new(part.clone())),
                            None,
                            Some(file.clone()),
                        ));
                    }
                } else {
                    let file = frozen
                        .files
                        .get(&uuid::Uuid::nil())
                        .context("managed snapshot is missing its Parquet file")?;
                    inputs.push((file.location.clone(), None, None, Some(file.clone())));
                }
                continue;
            }
            let managed = if store.is_none() {
                parquet_snapshot::managed_location(location)
            } else {
                None
            };
            let lease = managed
                .as_deref()
                .map(parquet_snapshot::pin)
                .transpose()?
                .flatten();
            if let Some(path) = managed
                && let Some(snapshot) = Snapshot::read(&path)?
            {
                // A publisher may have created the lock on the first conversion
                // from an ordinary Parquet file after our initial open.
                let (snapshot, lease) = if lease.is_none()
                    && let Some(lease) = parquet_snapshot::pin(&path)?
                {
                    (
                        Snapshot::read(&path)?.context("local snapshot changed while pinning")?,
                        Some(lease),
                    )
                } else {
                    (snapshot, lease)
                };

                versions.push(snapshot_version(location, &snapshot));
                // A singleton managed snapshot carries a validated schema and can
                // postpone pinning its parts until DataFusion passes scan filters.
                let logical = snapshot
                    .pruning
                    .as_ref()
                    .map(|index| (Some(index.key.clone()), &index.schema))
                    .or_else(|| snapshot.indexed.as_ref().map(|index| (None, &index.schema)));
                if prune_snapshot
                    && locations.len() == 1
                    && let Some((key, schema)) = logical
                {
                    return Ok((
                        Self {
                            schema: selected_schema(schema, columns, location)?,
                            input: ParquetInput::Snapshot {
                                key,
                                parts: snapshot_inputs(&path, snapshot, lease),
                                pool,
                                stats,
                            },
                            parallel_ranges: false,
                        },
                        versions,
                    ));
                }
                inputs.extend(snapshot_inputs(&path, snapshot, lease));
            } else {
                inputs.push((location.clone(), None, None, None));
            }
        }
        let (schema, open, physical_versions) =
            OpenedParquet::open(inputs, columns, store.as_ref(), &pool, &stats, false).await?;
        versions.extend(physical_versions);
        Ok((
            Self {
                schema,
                input: ParquetInput::Open(open),
                parallel_ranges: false,
            },
            versions,
        ))
    }

    #[cfg(feature = "admin")]
    pub(crate) async fn open_snapshot(
        path: &Path,
        snapshot: Snapshot,
        pool: Arc<dyn MemoryPool>,
        stats: Arc<ReadStats>,
    ) -> Result<Self> {
        let lease = parquet_snapshot::pin(path)?;
        if let Some(index) = &snapshot.indexed {
            let schema = selected_schema(&index.schema, &[], &path.display().to_string())?;
            return Ok(Self {
                schema,
                input: ParquetInput::Snapshot {
                    key: None,
                    parts: snapshot_inputs(path, snapshot, lease),
                    pool,
                    stats,
                },
                parallel_ranges: false,
            });
        }
        let inputs = snapshot_inputs(path, snapshot, lease);
        let (schema, open, _) =
            OpenedParquet::open(inputs, &[], None, &pool, &stats, false).await?;
        Ok(Self {
            schema,
            input: ParquetInput::Open(open),
            parallel_ranges: false,
        })
    }
}

impl OpenedParquet {
    async fn open(
        inputs: Vec<PartInput>,
        columns: &[String],
        store: Option<&Arc<dyn ObjectStore>>,
        pool: &Arc<dyn MemoryPool>,
        stats: &Arc<ReadStats>,
        parallel_ranges: bool,
    ) -> Result<(SchemaRef, Self, Vec<SourceVersion>)> {
        let mut versions = Vec::new();
        let mut schema = None;
        let mut readers = BTreeMap::new();
        let mut files = Vec::new();
        let mut metadata = Vec::new();
        #[cfg(feature = "admin")]
        let mut scan_objects = Vec::new();
        let parallelism = if store.is_some()
            || inputs.iter().all(|(location, part, _, _)| {
                part.is_some() || location.contains("://") && !location.starts_with("file://")
            }) {
            8
        } else {
            1
        };
        let mut inputs = futures::stream::iter(inputs.into_iter().enumerate().map(
            |(index, (location, mask, lease, expected))| {
                let pool = Arc::clone(pool);
                let stats = Arc::clone(stats);
                async move {
                    let mut file = PinnedFile::open_storage(&location, store, pool, stats).await?;
                    if let Some(expected) = &expected {
                        ensure!(
                            file.version(&location).version == expected.version
                                && file.len() == expected.size,
                            "managed Parquet object changed"
                        );
                    }
                    file.parallel_ranges = parallel_ranges;
                    file._snapshot_lease = lease;
                    let builder = ParquetRecordBatchStreamBuilder::new(file.clone())
                        .await
                        .with_context(|| format!("reading Parquet schema for {location}"))?;
                    Ok::<_, anyhow::Error>((
                        index,
                        location,
                        mask,
                        file,
                        builder,
                        expected.is_some(),
                    ))
                }
            },
        ))
        .buffered(parallelism);
        while let Some((index, location, mask, mut file, builder, managed)) =
            inputs.try_next().await?
        {
            let footer = builder.metadata();
            metadata.push(Arc::clone(footer));
            file.footer = Some(Arc::clone(footer));
            if let Some(mask) = &mask {
                ensure!(
                    usize::try_from(footer.file_metadata().num_rows())? == mask.rows,
                    "local Parquet snapshot part row count changed"
                );
            } else if !managed {
                versions.push(file.version(&location));
            }
            #[cfg(feature = "admin")]
            scan_objects.push(ScanObject {
                path: format!("object-{index}.parquet"),
                location: location.clone(),
                version: file.version(&location).version,
                size: file.meta.size,
                mask: mask
                    .as_deref()
                    .filter(|part| !part.deleted.is_empty())
                    .map(|part| Part {
                        key_range: None,
                        ..part.clone()
                    }),
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
            let projected = selected_schema(builder.schema(), columns, &location)?;
            if let Some(expected) = &schema {
                check_schema(expected, &projected)?;
            } else {
                schema = Some(projected);
            }
            let mut meta = file.meta.clone();
            // The same object path can occur in different buckets or on different hosts.
            meta.location = ObjectPath::from(format!("object-{index}.parquet"));
            readers.insert(meta.location.clone(), file);
            let mut file = PartitionedFile::new_from_meta(meta);
            if let Some(mask) = mask {
                file = parquet_snapshot::select_rows(file, mask.rows, &mask.deleted);
            }
            files.push(file);
        }
        Ok((
            schema.context("no Parquet files")?,
            Self {
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

fn selected_schema(schema: &Schema, columns: &[String], location: &str) -> Result<SchemaRef> {
    let fields = if columns.is_empty() {
        schema.fields().iter().map(AsRef::as_ref).collect()
    } else {
        columns
            .iter()
            .map(|name| {
                schema
                    .field_with_name(name)
                    .with_context(|| crate::catalog::ValidationSite::Column {
                        column: name.clone(),
                    })
                    .with_context(|| format!("missing column {name} in {location}"))
            })
            .collect::<Result<Vec<_>>>()?
    };
    // Files can differ in nullability while sharing the same logical field types.
    Ok(Arc::new(Schema::new(
        fields
            .into_iter()
            .map(|field| field.clone().with_nullable(true))
            .collect::<Vec<_>>(),
    )))
}

fn check_schema(expected: &SchemaRef, actual: &SchemaRef) -> Result<()> {
    ensure!(
        expected.fields().len() == actual.fields().len(),
        "Parquet files have different field counts"
    );
    for (field, required) in actual.fields().iter().zip(expected.fields()) {
        ensure!(
            field.name() == required.name() && field.data_type() == required.data_type(),
            "Parquet column {} has inconsistent types across files",
            field.name()
        );
    }
    Ok(())
}

fn snapshot_inputs(
    path: &Path,
    snapshot: Snapshot,
    lease: Option<Arc<std::fs::File>>,
) -> Vec<PartInput> {
    snapshot
        .parts
        .into_iter()
        .map(|part| {
            (
                part.path(path).to_string_lossy().into_owned(),
                Some(Arc::new(part)),
                lease.clone(),
                None,
            )
        })
        .collect()
}

fn column_equality(expr: &Expr) -> Option<(&str, &ScalarValue)> {
    let Expr::BinaryExpr(expr) = expr else {
        return None;
    };
    if expr.op != Operator::Eq {
        return None;
    }
    match (expr.left.as_ref(), expr.right.as_ref()) {
        (Expr::Column(column), Expr::Literal(value, ..))
        | (Expr::Literal(value, ..), Expr::Column(column)) => Some((&column.name, value)),
        _ => None,
    }
}

fn snapshot_point(filters: &[Expr], key: &str) -> Option<i64> {
    filters
        .iter()
        .flat_map(split_conjunction)
        .filter_map(column_equality)
        .filter(|(column, _)| *column == key)
        .find_map(|(_, value)| match value {
            ScalarValue::Int8(value) => value.map(i64::from),
            ScalarValue::Int16(value) => value.map(i64::from),
            ScalarValue::Int32(value) => value.map(i64::from),
            ScalarValue::Int64(value) => *value,
            _ => None,
        })
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
        let opened;
        let open = match &self.input {
            ParquetInput::Open(open) => open,
            ParquetInput::Snapshot {
                key,
                parts,
                pool,
                stats,
            } => {
                let point = key.as_deref().and_then(|key| snapshot_point(filters, key));
                let candidates = parts
                    .iter()
                    .filter(|(_, part, _, _)| {
                        let bounds = part.as_ref().and_then(|part| part.key_range);
                        match (point, bounds) {
                            (Some(key), Some([min, max])) => (min..=max).contains(&key),
                            _ => true,
                        }
                    })
                    .cloned()
                    .collect::<Vec<_>>();
                if candidates.is_empty() {
                    let schema = match projection {
                        Some(indices) => Arc::new(self.schema.project(indices)?),
                        None => self.schema(),
                    };
                    return Ok(Arc::new(EmptyExec::new(schema)));
                }
                let columns = self
                    .schema
                    .fields()
                    .iter()
                    .map(|field| field.name().clone())
                    .collect::<Vec<_>>();
                let (schema, files, _) = OpenedParquet::open(
                    candidates,
                    &columns,
                    None,
                    pool,
                    stats,
                    self.parallel_ranges,
                )
                .await
                .map_err(|error| DataFusionError::External(error.into()))?;
                check_schema(&self.schema, &schema)
                    .map_err(|error| DataFusionError::External(error.into()))?;
                opened = files;
                &opened
            }
        };
        let mut source = ParquetSource::new(self.schema())
            .with_parquet_file_reader_factory(open.readers.clone());
        if let Some(predicate) = conjunction(filters.iter().cloned()) {
            let schema = DFSchema::try_from(self.schema())?;
            let point_filter = split_conjunction(&predicate)
                .into_iter()
                .any(|filter| column_equality(filter).is_some());
            // Full scans also carry the model's key null checks. A row filter
            // costs memory on those scans without avoiding payload pages.
            if point_filter && open.readers.0.values().all(|file| file.footer.is_some()) {
                source = source.with_pushdown_filters(true);
            }
            source = source.with_predicate(state.create_physical_expr(predicate, &schema)?);
        }
        #[cfg(feature = "admin")]
        let files = open
            .files
            .iter()
            .zip(&open.scan_objects)
            .map(|(file, object)| file.clone().with_extension(object.clone()))
            .collect();
        #[cfg(not(feature = "admin"))]
        let files = open.files.clone();
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

#[cfg(test)]
mod range_tests {
    use super::*;

    #[tokio::test]
    async fn remote_ranges_are_coalesced_without_changing_decoder_order() -> Result<()> {
        use datafusion::execution::memory_pool::UnboundedMemoryPool;
        use object_store::ObjectStoreExt;

        let store = Arc::new(object_store::memory::InMemory::new());
        let path = ObjectPath::parse("file.parquet")?;
        let data = Bytes::from(Vec::from_iter((0..16000).map(|i| (i % 251) as u8)));
        store.put(&path, data.clone().into()).await?;
        let stats = Arc::new(ReadStats::default());
        let mut reader = PinnedFile::pin(
            store,
            path,
            false,
            Arc::new(UnboundedMemoryPool::default()),
            Arc::clone(&stats),
        )
        .await?;
        let chunks = reader
            .get_byte_ranges(vec![40..55, 0..20, 15000..15008, 19..32])
            .await?;
        assert_eq!(
            chunks,
            [
                data.slice(40..55),
                data.slice(0..20),
                data.slice(15000..15008),
                data.slice(19..32)
            ]
        );
        assert_eq!(
            stats.metrics().requests,
            3,
            "one HEAD and two merged range requests"
        );
        assert_eq!(stats.metrics().object_bytes, 63);
        Ok(())
    }
}
