use std::{collections::BTreeMap, fs::File, path::Path};

use anyhow::{Context, Result, ensure};
use datafusion::{
    arrow::datatypes::DataType,
    common::{DataFusionError, ScalarValue},
    dataframe::DataFrame,
    prelude::{ParquetReadOptions, SessionContext, col, lit},
};
use futures::TryStreamExt;
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};

use super::store::{Mode, Table};
use crate::{
    Catalog, QueryOptions,
    catalog::ident,
    engine::{QueryScope, stage_scope},
};

pub(super) async fn run(
    table: &Table,
    path: &Path,
    mode: Mode,
    options: QueryOptions,
) -> Result<(u64, u64)> {
    let scope = stage_scope(options).await?;
    scope.run(load(table, path, mode, &scope)).await
}

async fn load(table: &Table, path: &Path, mode: Mode, scope: &QueryScope) -> Result<(u64, u64)> {
    let config = &table.config;
    let context = scope.session();
    let previous = if path.is_file() {
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
        _ => None,
    };
    let watermark = checkpoint
        .as_ref()
        .map(|(value, _)| value)
        .filter(|value| !value.is_null());
    let (source_name, _) = config
        .source_table
        .split_once('.')
        .context("invalid source table")?;
    let catalog = Catalog {
        graph: "local_table_load".into(),
        sources: BTreeMap::from([(source_name.into(), table.source.clone())]),
        nodes: BTreeMap::new(),
        edges: BTreeMap::new(),
        export_locations: BTreeMap::new(),
    };
    let predicates = watermark
        .and_then(|value| {
            // Integer predicates can be expressed losslessly by every connector. Other
            // watermark types are filtered by the typed Arrow expression below.
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
        if let Some(value) = watermark {
            ensure!(
                *field.data_type() == value.data_type(),
                "增量字段类型已变化，请新建本地表"
            );
            incoming = incoming.filter(col(ident(name)).gt_eq(lit(value.clone())))?;
        }
    }
    let root = path.parent().context("missing local table directory")?;
    let delta = tempfile::Builder::new()
        .prefix(".sync-")
        .suffix(".parquet")
        .tempfile_in(root)?;
    let loaded = write(incoming, delta.as_file().try_clone()?, scope).await?;
    context
        .register_parquet(
            "incoming",
            delta.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await?;
    let incoming = context.table("incoming").await?;
    if let Some(previous) = &previous {
        let same = previous
            .schema()
            .fields()
            .iter()
            .zip(incoming.schema().fields())
            .all(|(a, b)| a.name() == b.name() && a.data_type() == b.data_type());
        ensure!(
            same && previous.schema().fields().len() == incoming.schema().fields().len(),
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
    let (output, total) = if let Some((_, total)) = checkpoint {
        let same_boundary = if loaded > 0
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
        let keys = config
            .key_columns
            .iter()
            .map(|name| format!("p.{} = i.{}", ident(name), ident(name)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let merged = context.sql(&format!("SELECT p.* FROM previous p LEFT ANTI JOIN incoming i ON {keys} UNION ALL SELECT * FROM incoming")).await?;
        let output = tempfile::Builder::new()
            .prefix(".sync-")
            .suffix(".parquet")
            .tempfile_in(root)?;
        let total = write(merged, output.as_file().try_clone()?, scope).await?;
        (output, total)
    } else {
        (delta, loaded)
    };
    scope.check()?;
    output.as_file().sync_all()?;
    // The checkpoint is MAX(watermark) in this same file. A restart can never
    // advance a separate checkpoint past the data that was atomically installed.
    output.persist(path)?;
    File::open(root)?.sync_all()?;
    Ok((loaded, total))
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
    let properties = WriterProperties::builder()
        .set_max_row_group_row_count(Some(8192))
        .build();
    let mut writer = ArrowWriter::try_new(file, stream.schema(), Some(properties))?;
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
