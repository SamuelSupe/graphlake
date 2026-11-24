use std::{
    collections::{BTreeMap, BTreeSet},
    io::{BufWriter, Write},
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::{
        array::{RecordBatch, StringArray, UInt64Array},
        datatypes::{DataType, Field, Schema, SchemaRef},
        json::writer::LineDelimited,
    },
    common::DataFusionError,
    execution::{SendableRecordBatchStream, memory_pool::MemoryConsumer},
    physical_plan::stream::RecordBatchStreamAdapter,
};
use futures::TryStreamExt;
use parquet::arrow::ArrowWriter;

use super::QueryScope;

mod s3;

enum Target {
    File(PathBuf),
    S3(s3::Target),
}

pub(crate) struct Destination {
    name: String,
    target: Target,
    parquet: bool,
}

pub(super) fn parse<'a>(
    query: &'a str,
    locations: &BTreeMap<String, String>,
) -> Result<(Option<Destination>, &'a str)> {
    let query = query.trim_start();
    let Some((keyword, rest)) = query.split_once(char::is_whitespace) else {
        return Ok((None, query));
    };
    if !keyword.eq_ignore_ascii_case("export") {
        return Ok((None, query));
    }
    let (to, rest) = rest
        .trim_start()
        .split_once(char::is_whitespace)
        .context("expected EXPORT TO 'location:path'")?;
    ensure!(
        to.eq_ignore_ascii_case("to"),
        "expected EXPORT TO 'location:path'"
    );
    let rest = rest.trim_start();
    ensure!(
        rest.starts_with(['\'', '"']),
        "EXPORT destination must be a quoted string"
    );
    let end = crate::cypher::paths::skip_literal(rest.as_bytes(), 0)?
        .context("missing export destination")?;
    let opencypher::ExprKind::String(name) = opencypher::parse_expr(&rest[..end])?.kind else {
        anyhow::bail!("invalid export destination")
    };
    let (location, path) = name
        .split_once(':')
        .context("export destination requires a registered location and a relative path")?;
    let directory = locations
        .get(location)
        .with_context(|| format!("unknown export location {location}"))?;
    let path = Path::new(path);
    ensure!(
        !path.as_os_str().is_empty()
            && path
                .components()
                .all(|component| matches!(component, Component::Normal(_))),
        "export path must be relative to its registered directory"
    );
    let parquet = match path.extension().and_then(|ext| ext.to_str()) {
        Some("parquet") => true,
        Some("csv") => false,
        _ => anyhow::bail!("export filename must end in .csv or .parquet"),
    };
    let target = if directory.starts_with("s3://") {
        Target::S3(s3::Target::new(directory, path)?)
    } else {
        let root = crate::blocking(|| {
            std::fs::canonicalize(directory).context("opening registered export directory")
        })?;
        let parent_rel = path
            .parent()
            .context("export path has no parent")?
            .to_owned();
        let parent = crate::blocking(|| {
            root.join(&parent_rel)
                .canonicalize()
                .context("opening export parent directory")
        })?;
        ensure!(
            parent.is_dir() && parent.starts_with(&root),
            "export path leaves its registered directory"
        );
        Target::File(parent.join(path.file_name().context("missing export filename")?))
    };
    Ok((
        Some(Destination {
            name,
            target,
            parquet,
        }),
        rest[end..].trim_start(),
    ))
}

impl Destination {
    pub(super) fn write(
        self,
        mut batches: SendableRecordBatchStream,
        scope: Arc<QueryScope>,
    ) -> SendableRecordBatchStream {
        let schema = Arc::new(Schema::new(vec![
            Field::new("destination", DataType::Utf8, false),
            Field::new("rows", DataType::UInt64, false),
        ]));
        let output = Arc::clone(&schema);
        let stream = async_stream::try_stream! {
            let count = self.save(&mut batches, &scope).await.map_err(|error| DataFusionError::External(error.into()))?;
            yield RecordBatch::try_new(output, vec![Arc::new(StringArray::from(vec![self.name])), Arc::new(UInt64Array::from(vec![count]))])?;
        };
        Box::pin(RecordBatchStreamAdapter::new(schema, stream))
    }

    async fn save(
        &self,
        batches: &mut SendableRecordBatchStream,
        scope: &Arc<QueryScope>,
    ) -> Result<u64> {
        scope.check()?;
        match &self.target {
            Target::File(path) => {
                let path = path.clone();
                let parent = path
                    .parent()
                    .expect("validated destination parent")
                    .to_owned();
                let (stage, reopened) = tokio::task::spawn_blocking(move || {
                    let stage = tempfile::NamedTempFile::new_in(parent)?;
                    let reopened = stage.reopen()?;
                    Ok::<_, anyhow::Error>((stage, reopened))
                })
                .await??;
                let (_, count) = self.serialize(reopened, batches, scope).await?;
                let scope = scope.clone();
                let name = self.name.clone();
                tokio::task::spawn_blocking(move || {
                    stage.as_file().sync_all()?;
                    scope.check()?;
                    stage
                        .persist_noclobber(path)
                        .with_context(|| format!("publishing export {name}"))?;
                    Ok(count)
                })
                .await?
            }
            Target::S3(target) => {
                let disk = scope.runtime.disk_manager.clone();
                let (stage, writer) = tokio::task::spawn_blocking(move || {
                    let stage = disk.create_tmp_file("staging a GraphLake S3 export")?;
                    let writer = stage.open_writer()?;
                    Ok::<_, anyhow::Error>((stage, writer))
                })
                .await??;
                let (mut writer, count) = self.serialize(writer, batches, scope).await?;
                tokio::task::spawn_blocking(move || writer.finish()).await??;
                let path = stage
                    .path()
                    .context("S3 export requires a local temporary file")?;
                target.publish(path, scope).await?;
                Ok(count)
            }
        }
    }

    async fn serialize<W: Write + Send + 'static>(
        &self,
        writer: W,
        batches: &mut SendableRecordBatchStream,
        scope: &Arc<QueryScope>,
    ) -> Result<(W, u64)> {
        let (send, receive) = tokio::sync::mpsc::channel(1);
        let parquet = self.parquet;
        let schema = batches.schema();
        let worker_scope = scope.clone();
        let worker = tokio::task::spawn_blocking(move || {
            Self::encode(
                BufWriter::new(writer),
                schema,
                receive,
                &worker_scope,
                parquet,
            )
        });
        while let Some(batch) = batches.try_next().await? {
            scope.check()?;
            if send.send(batch).await.is_err() {
                break;
            }
        }
        drop(send);
        worker.await?
    }

    fn encode<W: Write + Send>(
        writer: BufWriter<W>,
        schema: SchemaRef,
        mut batches: tokio::sync::mpsc::Receiver<RecordBatch>,
        scope: &QueryScope,
        parquet: bool,
    ) -> Result<(W, u64)> {
        let mut count = 0;
        let budget = MemoryConsumer::new("GraphLake export").register(&scope.runtime.memory_pool);
        let writer = if parquet {
            let mut writer = ArrowWriter::try_new(writer, schema, None)?;
            while let Some(batch) = batches.blocking_recv() {
                scope.check()?;
                let batch_memory = batch.get_array_memory_size().saturating_mul(2);
                if budget.try_grow(batch_memory).is_err() {
                    writer.flush()?;
                    budget.free();
                    budget.try_grow(batch_memory)?;
                }
                writer.write(&batch)?;
                if writer.memory_size() >= (scope.options.memory_bytes / 8).min(8 * 1024 * 1024)
                    || budget.try_resize(writer.memory_size()).is_err()
                {
                    writer.flush()?;
                    budget.free();
                }
                count += batch.num_rows() as u64;
            }
            writer.into_inner()?
        } else {
            let mut writer = writer;
            let columns: Vec<_> = schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect();
            ensure!(
                columns.iter().collect::<BTreeSet<_>>().len() == columns.len(),
                "CSV export requires unique column names"
            );
            csv_row(&mut writer, columns.iter().copied().map(Some))?;
            let simple_scalar = schema.fields().iter().all(|field| {
                matches!(
                    field.data_type(),
                    DataType::Boolean
                        | DataType::Int8
                        | DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::UInt8
                        | DataType::UInt16
                        | DataType::UInt32
                        | DataType::UInt64
                        | DataType::Utf8
                        | DataType::LargeUtf8
                        | DataType::Utf8View
                )
            });
            while let Some(batch) = batches.blocking_recv() {
                scope.check()?;
                if simple_scalar {
                    use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
                    let options = FormatOptions::default();
                    let formatters = batch
                        .columns()
                        .iter()
                        .map(|col| ArrayFormatter::try_new(col.as_ref(), &options))
                        .collect::<std::result::Result<Vec<_>, _>>()?;
                    let mut cell = String::new();
                    for row in 0..batch.num_rows() {
                        if row.is_multiple_of(256) {
                            scope.check()?;
                        }
                        for (index, (col, fmt)) in
                            batch.columns().iter().zip(&formatters).enumerate()
                        {
                            if index > 0 {
                                writer.write_all(b",")?;
                            }
                            if col.is_null(row) {
                                continue;
                            }
                            cell.clear();
                            std::fmt::Write::write_fmt(
                                &mut cell,
                                format_args!("{}", fmt.value(row)),
                            )
                            .map_err(std::io::Error::other)?;
                            if cell.capacity() > 64 * 1024 {
                                budget.try_resize(cell.capacity())?;
                            }
                            write_csv_cell(&mut writer, &cell)?;
                            if cell.capacity() > 64 * 1024 {
                                cell = String::new();
                                budget.free();
                            }
                        }
                        writer.write_all(b"\n")?;
                    }
                    count += batch.num_rows() as u64;
                    continue;
                }
                // Slice complex/dictionary batches into small chunks so dictionary
                // expansion stays bounded and accurately charged against MemoryPool.
                const CSV_CHUNK: usize = 64;
                for offset in (0..batch.num_rows()).step_by(CSV_CHUNK) {
                    scope.check()?;
                    let slice = batch.slice(offset, CSV_CHUNK.min(batch.num_rows() - offset));
                    let mut estimated = 0usize;
                    for col in slice.columns() {
                        estimated = estimated.saturating_add(
                            crate::engine::query::allocation::slice_bytes(col.as_ref())?,
                        );
                    }
                    budget.try_grow(estimated.saturating_mul(2))?;
                    let mut json = crate::json_writer().build::<_, LineDelimited>(Vec::new());
                    json.write(&slice)?;
                    json.finish()?;
                    let bytes = json.into_inner();
                    budget.try_resize(bytes.len().saturating_mul(2))?;
                    for line in bytes
                        .split(|byte| *byte == b'\n')
                        .filter(|line| !line.is_empty())
                    {
                        let row: BTreeMap<String, &serde_json::value::RawValue> =
                            serde_json::from_slice(line)?;
                        let cells = columns
                            .iter()
                            .map(|column| match row.get(*column).map(|value| value.get()) {
                                None | Some("null") => Ok(None),
                                Some(value) if value.starts_with('"') => {
                                    serde_json::from_str::<String>(value).map(Some)
                                }
                                Some(value) => Ok(Some(value.to_owned())),
                            })
                            .collect::<std::result::Result<Vec<_>, _>>()?;
                        csv_row(&mut writer, cells.iter().map(Option::as_deref))?;
                    }
                    budget.free();
                }
                count += batch.num_rows() as u64;
            }
            writer
        };
        let writer = writer.into_inner().map_err(|error| error.into_error())?;
        Ok((writer, count))
    }
}

fn write_csv_cell(writer: &mut impl Write, cell: &str) -> std::io::Result<()> {
    if cell.is_empty() || cell.contains([',', '"', '\r', '\n']) {
        writer.write_all(b"\"")?;
        let mut first = true;
        for part in cell.split('"') {
            if !first {
                writer.write_all(b"\"\"")?;
            }
            writer.write_all(part.as_bytes())?;
            first = false;
        }
        writer.write_all(b"\"")
    } else {
        writer.write_all(cell.as_bytes())
    }
}

fn csv_row(
    writer: &mut impl Write,
    cells: impl Iterator<Item = Option<impl AsRef<str>>>,
) -> std::io::Result<()> {
    for (index, cell) in cells.enumerate() {
        if index > 0 {
            writer.write_all(b",")?;
        }
        let Some(cell) = cell else { continue };
        write_csv_cell(writer, cell.as_ref())?;
    }
    writer.write_all(b"\n")
}
