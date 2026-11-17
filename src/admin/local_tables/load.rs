use std::{collections::BTreeMap, fs::File, path::Path, sync::Arc};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::datatypes::DataType,
    common::{DataFusionError, ScalarValue},
    dataframe::DataFrame,
    execution::{
        memory_pool::{FairSpillPool, MemoryConsumer, MemoryReservation},
        runtime_env::RuntimeEnv,
        session_state::SessionStateBuilder,
    },
    prelude::{ParquetReadOptions, SessionContext, col, lit},
};
use futures::TryStreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use super::{
    key_index, overlay, row_groups,
    store::{Mode, Table},
};
use crate::{
    Catalog, QueryOptions,
    catalog::ident,
    engine::{Admission, QueryScope},
    source::{parquet::ParquetTable, parquet_snapshot::Snapshot},
};

pub(super) async fn run(
    table: &Table,
    path: &Path,
    mode: Mode,
    options: QueryOptions,
    admission: &Arc<Admission>,
) -> Result<(u64, u64, crate::source::ReadMetrics, Option<uuid::Uuid>)> {
    let scope = Arc::new(QueryScope::new(options, admission).await?);
    let (loaded, total) = scope.run(load(table, path, mode, &scope)).await?;
    clean_parts(path);
    let generation = Snapshot::read(path)?
        .filter(|snapshot| {
            snapshot
                .indexed
                .as_ref()
                .is_some_and(|index| index.run_id == table.history[0].id)
        })
        .map(|snapshot| snapshot.generation);
    Ok((loaded, total, scope.reads.metrics(), generation))
}

async fn load(
    table: &Table,
    path: &Path,
    mode: Mode,
    scope: &Arc<QueryScope>,
) -> Result<(u64, u64)> {
    let config = &table.config;
    let run_id = table.history[0].id;
    let context = scope.session();
    let snapshot = Snapshot::read(path)?;
    // A full refresh can repair missing parts using the committed logical
    // schema. Incremental refresh still needs the previous generation intact.
    let full_schema = if mode == Mode::Full {
        snapshot.as_ref().and_then(|snapshot| {
            snapshot
                .indexed
                .as_ref()
                .map(|index| &index.schema)
                .or_else(|| snapshot.pruning.as_ref().map(|index| &index.schema))
        })
    } else {
        None
    };
    let previous = if full_schema.is_some() {
        None
    } else if let Some(snapshot) = &snapshot {
        let table = ParquetTable::open_snapshot(
            path,
            snapshot.clone(),
            Arc::clone(&scope.runtime.memory_pool),
            Arc::clone(&scope.reads),
        )
        .await?;
        Some(context.read_table(Arc::new(table))?)
    } else if path.is_file() {
        Some(
            context
                .read_parquet(
                    path.to_str().context("invalid local path")?,
                    ParquetReadOptions::default(),
                )
                .await?,
        )
    } else {
        None
    };
    let checkpoint = match (&previous, mode) {
        (Some(previous), Mode::Incremental) => {
            context.register_table("previous", previous.clone().into_view())?;
            let name = config
                .watermark_column
                .as_deref()
                .context("missing watermark column")?;
            if let Some(snapshot) = &snapshot
                && let Some(index) = &snapshot.indexed
            {
                ensure!(
                    index.keys == config.key_columns && index.watermark_column == name,
                    "local snapshot key or watermark does not match its definition"
                );
                for part in &snapshot.parts {
                    ensure!(
                        part.path(path).metadata()?.is_file(),
                        "missing local snapshot part"
                    );
                }
                Some((key_index::checkpoint(index)?, snapshot.rows()))
            } else if let Some(snapshot) = &snapshot {
                // The newest delta was filtered at the previous watermark. It
                // therefore contains the maximum of the committed snapshot.
                let latest = snapshot
                    .parts
                    .last()
                    .expect("manifest has parts")
                    .path(path);
                context
                    .register_parquet(
                        "checkpoint",
                        latest.to_str().context("invalid local path")?,
                        ParquetReadOptions::default(),
                    )
                    .await?;
                let batches = context
                    .sql(&format!("SELECT MAX({}) FROM checkpoint", ident(name)))
                    .await?
                    .collect()
                    .await?;
                let watermark = ScalarValue::try_from_array(batches[0].column(0), 0)?;
                Some((watermark, snapshot.rows()))
            } else {
                let batches = context
                    .sql(&format!(
                        "SELECT MAX({}), COUNT(*) FROM previous",
                        ident(name)
                    ))
                    .await?
                    .collect()
                    .await?;
                let watermark = ScalarValue::try_from_array(batches[0].column(0), 0)?;
                let rows = ScalarValue::try_from_array(batches[0].column(1), 0)?;
                let ScalarValue::Int64(Some(rows)) = rows else {
                    anyhow::bail!("invalid local row count")
                };
                Some((watermark, u64::try_from(rows)?))
            }
        }
        _ => None,
    };
    let watermark = checkpoint
        .as_ref()
        .map(|(value, _)| value)
        .filter(|value| !value.is_null());
    let lower = watermark
        .map(|value| lookback(value, config.lookback))
        .transpose()?;
    let lower = lower.as_ref();
    let (source_name, _) = config
        .source_table
        .split_once('.')
        .context("invalid source table")?;
    let catalog = Catalog {
        graph: "local_table_load".into(),
        semantics: None,
        tasks: Default::default(),
        sources: BTreeMap::from([(source_name.into(), table.source.clone())]),
        nodes: BTreeMap::new(),
        edges: BTreeMap::new(),
        export_locations: BTreeMap::new(),
        freshness: Default::default(),
    };
    let predicates = lower
        .and_then(|value| {
            // Keep the existing integer path for connectors without typed watermarks.
            let value = match value {
                ScalarValue::Int16(Some(v)) => serde_json::json!(v),
                ScalarValue::Int32(Some(v)) => serde_json::json!(v),
                ScalarValue::Int64(Some(v)) => serde_json::json!(v),
                ScalarValue::UInt16(Some(v)) => serde_json::json!(v),
                ScalarValue::UInt32(Some(v)) => serde_json::json!(v),
                ScalarValue::UInt64(Some(v)) => serde_json::json!(v),
                _ => return None,
            };
            Some(crate::Predicate {
                column: config.watermark_column.clone().unwrap(),
                op: crate::Comparison::Ge,
                value,
            })
        })
        .into_iter()
        .collect();
    let (mut incoming, _prepared) = crate::engine::sources::table(
        &catalog,
        &config.source_table,
        &config.columns,
        predicates,
        config.watermark_column.as_deref().zip(lower),
        scope,
    )
    .await?;
    // Preserve the configured order even when a connector sorts its projection.
    incoming = incoming.select(
        config
            .columns
            .iter()
            .map(|name| col(ident(name)))
            .collect::<Vec<_>>(),
    )?;
    if let Some(name) = &config.watermark_column {
        let field = incoming.schema().field_with_unqualified_name(name)?;
        ensure!(
            matches!(
                field.data_type(),
                DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Date32
                    | DataType::Date64
                    | DataType::Timestamp(_, _)
            ),
            "增量字段必须是整数、日期或时间戳"
        );
        if let Some(value) = lower {
            ensure!(
                *field.data_type() == value.data_type(),
                "增量字段类型已变化，请新建本地表"
            );
            incoming = incoming.filter(col(ident(name)).gt_eq(lit(value.clone())))?;
        }
    }
    let root = path.parent().context("missing local table directory")?;
    let delta = staging_file(root)?;
    let loaded = write(incoming, delta.as_file().try_clone()?, scope).await?;
    let delta_bytes = delta.as_file().metadata()?.len();
    scope.reads.local_parquet_write(delta_bytes);
    if snapshot.is_some() {
        let locations = [delta.path().to_string_lossy().into_owned()];
        let (table, _) = ParquetTable::open(
            &locations,
            &[],
            Arc::clone(&scope.runtime.memory_pool),
            Arc::clone(&scope.reads),
            false,
        )
        .await?;
        context.register_table("incoming", Arc::new(table))?;
    } else {
        context
            .register_parquet(
                "incoming",
                delta.path().to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
    }
    let incoming = context.table("incoming").await?;
    if let Some(expected) = full_schema
        .map(|schema| schema.fields())
        .or_else(|| previous.as_ref().map(|frame| frame.schema().fields()))
    {
        let same = expected
            .iter()
            .zip(incoming.schema().fields())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type());
        ensure!(
            same && expected.len() == incoming.schema().fields().len(),
            "来源字段类型已变化，请新建本地表"
        );
    }
    if !config.key_columns.is_empty() {
        let keys = config
            .key_columns
            .iter()
            .map(|name| ident(name))
            .collect::<Vec<_>>()
            .join(", ");
        let nulls = config
            .key_columns
            .iter()
            .map(|name| format!("{} IS NULL", ident(name)))
            .collect::<Vec<_>>()
            .join(" OR ");
        ensure!(
            !any(
                &context,
                &format!("SELECT 1 FROM incoming WHERE {nulls} LIMIT 1")
            )
            .await?,
            "主键不能包含空值"
        );
        ensure!(
            !any(
                &context,
                &format!("SELECT 1 FROM incoming GROUP BY {keys} HAVING COUNT(*) > 1 LIMIT 1")
            )
            .await?,
            "来源包含重复主键；请修正来源后重试"
        );
    }
    if let Some(name) = &config.watermark_column {
        ensure!(
            !any(
                &context,
                &format!(
                    "SELECT 1 FROM incoming WHERE {} IS NULL LIMIT 1",
                    ident(name)
                )
            )
            .await?,
            "增量字段不能包含空值"
        );
    }
    let deleted = if let Some(column) = &config.deleted_column {
        let field = incoming.schema().field_with_unqualified_name(column)?;
        let column = ident(column);
        let condition = match field.data_type() {
            DataType::Boolean => column.clone(),
            kind if kind.is_integer() => format!("{column} <> 0"),
            _ => anyhow::bail!("删除标记必须是布尔值或整数"),
        };
        ensure!(
            !any(
                &context,
                &format!("SELECT 1 FROM incoming WHERE {column} IS NULL LIMIT 1")
            )
            .await?,
            "删除标记不能包含空值"
        );
        any(
            &context,
            &format!("SELECT 1 FROM incoming WHERE {condition} LIMIT 1"),
        )
        .await?
        .then_some(condition)
    } else {
        None
    };
    let candidate = if let Some(column) = config.watermark_column.as_deref()
        && key_index::supports(
            incoming.schema().as_arrow(),
            &config.key_columns,
            config.deleted_column.is_some() || config.lookback != 0,
        ) {
        let result = context
            .sql(&format!("SELECT MAX({}) FROM incoming", ident(column)))
            .await?
            .collect()
            .await?;
        let highest = ScalarValue::try_from_array(result[0].column(0), 0)?;
        let highest = match watermark {
            Some(old) if highest.is_null() || highest < *old => old.clone(),
            _ => highest,
        };
        Some((column, highest))
    } else {
        ensure!(
            config.deleted_column.is_none() && config.lookback == 0,
            "重读窗口与删除标记需要字符串、整数或二进制主键"
        );
        None
    };
    if let Some((column, next)) = &candidate
        && checkpoint.is_some()
        && deleted.is_none()
        && loaded > 0
        && delta_bytes <= overlay::MAX_DELTA_BYTES
    {
        match tokio::task::block_in_place(|| {
            key_index::incremental(
                path,
                snapshot.as_ref(),
                &delta,
                &config.key_columns,
                column,
                next,
                run_id,
                scope,
            )
        }) {
            Ok(Some(total)) => return Ok((loaded, total)),
            Ok(None) => {}
            Err(error) if out_of_memory(&error) => {}
            Err(error) => return Err(error),
        }
    }
    let (output, total) = if let Some((_, total)) = checkpoint {
        let same_boundary = if deleted.is_none()
            && config.lookback == 0
            && loaded > 0
            && loaded <= scope.options.batch_size as u64
            && let (Some(column), Some(watermark)) = (config.watermark_column.as_deref(), watermark)
        {
            unchanged(&context, &incoming, column, watermark, loaded as usize).await?
        } else {
            false
        };
        if loaded == 0 || same_boundary {
            scope.check()?;
            return Ok((loaded, total));
        }
        if let Some(mark) = &deleted
            && candidate
                .as_ref()
                .is_some_and(|(_, high)| Some(high) == watermark)
        {
            let keys = config
                .key_columns
                .iter()
                .map(|name| ident(name))
                .collect::<Vec<_>>()
                .join(", ");
            let changed = async {
                if any(&context, &format!("SELECT 1 FROM (SELECT {keys} FROM incoming WHERE {mark}) d JOIN previous p USING ({keys}) LIMIT 1")).await? {
                    return Ok(true);
                }
                any(&context, &format!("(SELECT * FROM incoming WHERE NOT ({mark}) EXCEPT SELECT * FROM previous) LIMIT 1")).await
            }
            .await;
            match changed {
                Ok(false) => return Ok((loaded, total)),
                Ok(true) => {}
                Err(error) if out_of_memory(&error) => {}
                Err(error) => return Err(error),
            }
        }
        let selective = if candidate.is_none()
            && delta_bytes <= overlay::MAX_DELTA_BYTES
            && let [key] = config.key_columns.as_slice()
            && incoming
                .schema()
                .field_with_unqualified_name(key)?
                .data_type()
                .is_signed_integer()
        {
            if delta.as_file().metadata()?.len() <= overlay::MAX_DELTA_BYTES {
                let anchor = path.to_owned();
                let previous = snapshot.clone();
                let staged = delta.reopen()?;
                let key = key.clone();
                let worker_scope = Arc::clone(scope);
                if let Some(next) = tokio::task::spawn_blocking(move || {
                    overlay::prepare(&anchor, previous, staged, &key, &worker_scope)
                })
                .await??
                    && overlay::publish(path, &delta, snapshot.is_none(), &next, scope, || Ok(()))?
                {
                    return Ok((loaded, next.snapshot.rows()));
                }
            }
            if snapshot.is_none() {
                let previous = File::open(path)?;
                let incoming = delta.reopen()?;
                let root = root.to_owned();
                let key = key.clone();
                let scope = Arc::clone(scope);
                tokio::task::spawn_blocking(move || {
                    row_groups::merge(previous, incoming, &root, &key, &scope)
                })
                .await??
            } else {
                None
            }
        } else {
            None
        };
        if let Some(result) = selective {
            result
        } else if key_index::supports(incoming.schema().as_arrow(), &config.key_columns, true) {
            let output = staging_file(root)?;
            let total = tokio::task::block_in_place(|| {
                key_index::compact(
                    path,
                    snapshot.as_ref(),
                    &delta,
                    output.reopen()?,
                    &config.key_columns,
                    config.deleted_column.as_deref(),
                    scope,
                )
            })?;
            scope
                .reads
                .local_parquet_write(output.as_file().metadata()?.len());
            (output, total)
        } else {
            let keys = config
                .key_columns
                .iter()
                .map(|name| format!("p.{} = i.{}", ident(name), ident(name)))
                .collect::<Vec<_>>()
                .join(" AND ");
            let (compaction, _sort_budget) = compaction_session(&context, scope)?;
            let visible = deleted
                .as_ref()
                .map_or(String::new(), |mark| format!(" WHERE NOT ({mark})"));
            let merged = compaction
                .sql(&format!(
                    "SELECT p.* FROM previous p LEFT ANTI JOIN incoming i ON {keys} UNION ALL SELECT * FROM incoming{visible}"
                ))
                .await?;
            let output = staging_file(root)?;
            let total = write(merged, output.as_file().try_clone()?, scope).await?;
            scope
                .reads
                .local_parquet_write(output.as_file().metadata()?.len());
            (output, total)
        }
    } else if let Some(mark) = &deleted {
        let output = staging_file(root)?;
        let frame = context
            .sql(&format!("SELECT * FROM incoming WHERE NOT ({mark})"))
            .await?;
        let rows = write(frame, output.as_file().try_clone()?, scope).await?;
        scope
            .reads
            .local_parquet_write(output.as_file().metadata()?.len());
        (output, rows)
    } else {
        (delta, loaded)
    };
    scope.check()?;
    if let Some((column, value)) = &candidate {
        tokio::task::block_in_place(|| {
            key_index::full(
                path,
                &output,
                &config.key_columns,
                column,
                value,
                run_id,
                scope,
            )
        })?;
        return Ok((loaded, total));
    }
    output.as_file().sync_all()?;
    scope.check()?;
    // The checkpoint is MAX(watermark) in this same file. A restart can never
    // advance a separate checkpoint past the data that was atomically installed.
    output.persist(path)?;
    File::open(root)?.sync_all()?;
    Ok((loaded, total))
}

fn compaction_session(
    context: &SessionContext,
    scope: &QueryScope,
) -> Result<(SessionContext, MemoryReservation)> {
    // Sorts can spill, but the Parquet readers cannot evict their input pages.
    // Cap the planner's pool while keeping both pools within the query budget.
    let sort_bytes = scope.options.memory_bytes / 2;
    let budget = MemoryConsumer::new("local table compaction").register(&scope.runtime.memory_pool);
    budget.try_grow(sort_bytes)?;
    let runtime = Arc::new(RuntimeEnv {
        memory_pool: Arc::new(FairSpillPool::new(sort_bytes)),
        disk_manager: scope.runtime.disk_manager.clone(),
        cache_manager: scope.runtime.cache_manager.clone(),
        object_store_registry: scope.runtime.object_store_registry.clone(),
    });
    let mut state = context.state();
    let options = state.config_mut().options_mut();
    options.optimizer.prefer_hash_join = false;
    options.execution.sort_spill_reservation_bytes = 256 * 1024;
    options.execution.target_partitions = options.execution.target_partitions.max(2);
    let state = SessionStateBuilder::new_from_existing(state)
        .with_runtime_env(runtime)
        .build();
    Ok((SessionContext::new_with_state(state), budget))
}

fn out_of_memory(error: &anyhow::Error) -> bool {
    error.chain().any(|error| {
        error
            .downcast_ref::<DataFusionError>()
            .is_some_and(|error| {
                matches!(error.find_root(), DataFusionError::ResourcesExhausted(_))
            })
    })
}

fn lookback(value: &ScalarValue, distance: u64) -> Result<ScalarValue> {
    if distance == 0 {
        return Ok(value.clone());
    }
    use ScalarValue::*;
    let signed = |n: i64| (i128::from(n) - i128::from(distance)).max(i128::from(i64::MIN)) as i64;
    Ok(match value {
        Int8(Some(v)) => Int8(Some(signed((*v).into()).max(i8::MIN.into()) as i8)),
        Int16(Some(v)) => Int16(Some(signed((*v).into()).max(i16::MIN.into()) as i16)),
        Int32(Some(v)) => Int32(Some(signed((*v).into()).max(i32::MIN.into()) as i32)),
        Int64(Some(v)) => Int64(Some(signed(*v))),
        UInt8(Some(v)) => UInt8(Some(u64::from(*v).saturating_sub(distance) as u8)),
        UInt16(Some(v)) => UInt16(Some(u64::from(*v).saturating_sub(distance) as u16)),
        UInt32(Some(v)) => UInt32(Some(u64::from(*v).saturating_sub(distance) as u32)),
        UInt64(Some(v)) => UInt64(Some(v.saturating_sub(distance))),
        Date32(Some(v)) => Date32(Some(signed((*v).into()).max(i32::MIN.into()) as i32)),
        Date64(Some(v)) => Date64(Some(signed(*v))),
        TimestampSecond(Some(v), tz) => TimestampSecond(Some(signed(*v)), tz.clone()),
        TimestampMillisecond(Some(v), tz) => TimestampMillisecond(Some(signed(*v)), tz.clone()),
        TimestampMicrosecond(Some(v), tz) => TimestampMicrosecond(Some(signed(*v)), tz.clone()),
        TimestampNanosecond(Some(v), tz) => TimestampNanosecond(Some(signed(*v)), tz.clone()),
        _ => anyhow::bail!("增量字段不支持重读窗口"),
    })
}

fn clean_parts(path: &Path) {
    if let Err(error) = overlay::clean(path) {
        eprintln!("GraphLake local-table part cleanup failed: {error}");
    }
}

pub(super) fn staging_file(root: &Path) -> std::io::Result<tempfile::NamedTempFile> {
    tempfile::Builder::new()
        .prefix(".sync-")
        .suffix(".parquet")
        .tempfile_in(root)
}

async fn unchanged(
    context: &SessionContext,
    incoming: &DataFrame,
    column: &str,
    watermark: &ScalarValue,
    loaded: usize,
) -> Result<bool> {
    // Only use SQL equality for types whose equal values are interchangeable.
    // Other types (e.g. floats and nested values) use the normal merge path.
    if !incoming.schema().fields().iter().all(|field| {
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
                | DataType::Date32
                | DataType::Date64
                | DataType::Timestamp(_, _)
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Utf8View
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::BinaryView
        )
    }) {
        return Ok(false);
    }
    let boundary = context
        .table("previous")
        .await?
        .filter(col(ident(column)).eq(lit(watermark.clone())))?
        .limit(0, Some(loaded))?;
    let difference = incoming.clone().except(boundary)?.limit(0, Some(1))?;
    match difference.collect().await {
        Ok(batches) => Ok(batches.iter().all(|batch| batch.num_rows() == 0)),
        // Comparing every field can take more memory than the key-only merge.
        Err(error) if matches!(error.find_root(), DataFusionError::ResourcesExhausted(_)) => {
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

async fn any(context: &SessionContext, sql: &str) -> Result<bool> {
    Ok(context
        .sql(sql)
        .await?
        .collect()
        .await?
        .iter()
        .any(|b| b.num_rows() > 0))
}

async fn write(frame: DataFrame, file: File, scope: &QueryScope) -> Result<u64> {
    let mut stream = frame.execute_stream().await?;
    let mut writer = ArrowWriter::try_new(file, stream.schema(), Some(writer_properties()))?;
    let mut count = 0;
    while let Some(batch) = stream.try_next().await? {
        scope.check()?;
        count += batch.num_rows() as u64;
        writer.write(&batch)?;
        if writer.memory_size() > scope.options.memory_bytes / 8 {
            writer.flush()?;
        }
    }
    writer.close()?;
    Ok(count)
}

pub(super) fn writer_properties() -> WriterProperties {
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(8192))
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn large_delta_compaction_preserves_rows_and_tombstones_under_a_small_budget()
    -> Result<()> {
        let scratch = tempfile::tempdir()?;
        let source = scratch.path().join("input.db");
        {
            let db = rusqlite::Connection::open(&source)?;
            db.execute_batch(
                "CREATE TABLE items(id INTEGER PRIMARY KEY, ver INTEGER NOT NULL,
                                    removed INTEGER NOT NULL, body BLOB NOT NULL) STRICT;
                 WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i<4800)
                 INSERT INTO items SELECT i, i=1, 0, randomblob(4096) FROM n;",
            )?;
        }
        let mut table: Table = serde_json::from_value(serde_json::json!({
            "model": "model", "data_id": uuid::Uuid::new_v4(), "revision": uuid::Uuid::new_v4(),
            "source": {"type":"sqlite", "path":source},
            "config": {"name":"items","source_table":"db.items","columns":["id","ver","removed","body"],
                "key_columns":["id"],"watermark_column":"ver","deleted_column":"removed","mode":"incremental"},
            "history":[]
        }))?;
        let path = scratch.path().join("items.parquet");
        let admission = Arc::new(Admission::new(crate::EngineLimits::default())?);
        super::super::prepare_run(&mut table, Some(Mode::Full), false, false)
            .map_err(|error| anyhow::anyhow!(error.message))?;
        let (_, before, _, _) = run(
            &table,
            &path,
            Mode::Full,
            QueryOptions::default(),
            &admission,
        )
        .await?;
        assert_eq!(before, 4800);
        rusqlite::Connection::open(source)?.execute_batch(
            "UPDATE items SET body=randomblob(4096), ver=2 WHERE id BETWEEN 101 AND 4400;
             UPDATE items SET removed=1, ver=2 WHERE id<=100;
             INSERT INTO items VALUES (5000, 2, 0, x'0102');",
        )?;
        // Simulate the next load with a fresh run identifier and the old watermark intact.
        table.history[0].id = uuid::Uuid::new_v4();
        let (_, total, _, committed) = run(
            &table,
            &path,
            Mode::Incremental,
            QueryOptions {
                memory_bytes: 24 * 1024 * 1024,
                batch_size: 128,
                ..QueryOptions::default()
            },
            &admission,
        )
        .await?;
        assert_eq!(total, 4701);
        assert!(committed.is_some());
        let next = Snapshot::read(&path)?.expect("indexed local snapshot");
        assert_eq!(
            key_index::checkpoint(next.indexed.as_ref().unwrap())?,
            ScalarValue::Int64(Some(2))
        );
        let part = next.parts.first().expect("compacted base");
        let batches = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
            File::open(part.path(&path))?,
        )?
        .build()?;
        let mut items = Vec::new();
        for batch in batches {
            use crate::arrow::array::Int64Array;
            let batch = batch?;
            let keys = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("id column")?;
            let versions = batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("version column")?;
            items.extend(
                keys.values()
                    .iter()
                    .copied()
                    .zip(versions.values().iter().copied()),
            );
        }
        items.sort_unstable();
        assert_eq!(
            items,
            (101..=4800)
                .chain([5000])
                .map(|id| (id, if (4401..=4800).contains(&id) { 0 } else { 2 }))
                .collect::<Vec<_>>()
        );
        Ok(())
    }
}
