use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};

use datafusion::{
    arrow::{
        array::{Array, BooleanArray, Int64Array},
        compute::cast,
        datatypes::DataType,
    },
    common::{Column, ScalarValue},
    datasource::source_as_provider,
    logical_expr::{ColumnarValue, Expr, LogicalPlan, Volatility, create_udf},
    prelude::{col, lit},
};

use super::{Keys, integer_ranges};
use crate::source::parquet::ParquetTable;

pub(super) fn predicate(
    plan: &LogicalPlan,
    columns: &[String],
    keys: &Keys,
    memory: usize,
) -> Option<Expr> {
    let [column] = columns else { return None };
    if keys.len() < 1024 {
        return None;
    }
    let groups = groups(plan, column)?;
    let ranges = integer_ranges(keys)?;
    if ranges.len() <= keys.len() / 4 {
        return None;
    }
    let sorted = keys
        .iter()
        .map(|key| integer(&key[0]))
        .collect::<Option<Vec<_>>>()?;
    let mut sorted = sorted;
    sorted.sort_unstable();
    if !prefer_range(&groups, &sorted, memory) {
        return None;
    }

    // Keep exact membership local to this scan. Passing a literal per key
    // through DataFusion's optimizer is expensive when most row groups survive.
    let set = sorted.into_iter().collect::<HashSet<_>>();
    let ty = keys.iter().next()?.first()?.data_type();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let udf = create_udf(
        &format!(
            "__graphlake_parquet_keys_{}",
            NEXT.fetch_add(1, Ordering::Relaxed)
        ),
        vec![ty],
        DataType::Boolean,
        Volatility::Immutable,
        Arc::new(move |args| {
            Ok(match &args[0] {
                ColumnarValue::Scalar(value) => ColumnarValue::Scalar(ScalarValue::Boolean(
                    integer(value).map(|key| set.contains(&key)),
                )),
                ColumnarValue::Array(array) => {
                    let keys = cast(array, &DataType::Int64)?;
                    let keys = keys
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("cast to Int64");
                    let matched = keys
                        .iter()
                        .map(|key| key.map(|key| set.contains(&key)))
                        .collect::<BooleanArray>();
                    ColumnarValue::Array(Arc::new(matched))
                }
            })
        }),
    );
    let column = col(Column::from_name(column));
    let bounds = column
        .clone()
        .gt_eq(lit(ranges[0].0.clone()))
        .and(column.clone().lt_eq(lit(ranges.last()?.1.clone())));
    Some(bounds.and(udf.call(vec![column])))
}

fn prefer_range(groups: &[(i64, i64, usize)], sorted: &[i64], memory: usize) -> bool {
    let first = sorted[0];
    let last = sorted[sorted.len() - 1];
    let mut range_groups = 0usize;
    let mut hits = 0usize;
    let mut scanned_rows = 0usize;
    for (min, max, rows) in groups {
        if *min <= last && first <= *max {
            range_groups += 1;
            scanned_rows = scanned_rows.saturating_add(*rows);
            let index = sorted.partition_point(|key| key < min);
            hits += usize::from(sorted.get(index).is_some_and(|key| key <= max));
        }
    }
    hits * 2 >= groups.len()
        && hits * 4 >= range_groups * 3
        && scanned_rows >= sorted.len().saturating_mul(8)
        // Keep headroom for decoded key batches and the consuming join.
        && scanned_rows.saturating_mul(32) <= memory / 4
}

fn integer(value: &ScalarValue) -> Option<i64> {
    match value {
        ScalarValue::Int16(Some(n)) => Some(i64::from(*n)),
        ScalarValue::Int32(Some(n)) => Some(i64::from(*n)),
        ScalarValue::Int64(Some(n)) => Some(*n),
        _ => None,
    }
}

fn groups(plan: &LogicalPlan, column: &str) -> Option<Vec<(i64, i64, usize)>> {
    match plan {
        LogicalPlan::TableScan(scan) => {
            let provider = source_as_provider(&scan.source).ok()?;
            if let Some(table) = provider.downcast_ref::<ParquetTable>() {
                table.integer_groups(column)
            } else {
                groups(provider.get_logical_plan()?.as_ref(), column)
            }
        }
        LogicalPlan::Projection(p) => {
            let index = p.schema.index_of_column_by_name(None, column)?;
            match &p.expr[index] {
                Expr::Column(source) => groups(&p.input, &source.name),
                Expr::Alias(alias) if let Expr::Column(source) = alias.expr.as_ref() => {
                    groups(&p.input, &source.name)
                }
                _ => None,
            }
        }
        LogicalPlan::Filter(filter) => groups(&filter.input, column),
        LogicalPlan::Window(window) => groups(&window.input, column),
        LogicalPlan::SubqueryAlias(alias) => groups(&alias.input, column),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::prefer_range;

    #[test]
    fn broad_sparse_batches_need_memory_for_range_pruning() {
        let groups = (0..16)
            .map(|i| (i * 1000, i * 1000 + 999, 1000))
            .collect::<Vec<_>>();
        let sparse = (0..16000).step_by(64).collect::<Vec<_>>();
        assert!(prefer_range(&groups, &sparse, 16 * 1024 * 1024));
        assert!(!prefer_range(&groups, &sparse, 1024 * 1024));
        assert!(!prefer_range(&groups, &[0, 1, 2, 15999], 16 * 1024 * 1024));
    }
}
