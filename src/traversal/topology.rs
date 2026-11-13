use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Result, ensure};
use datafusion::{
    arrow::{
        array::{ListArray, RecordBatch, StructArray, UInt32Array, UInt64Array},
        buffer::{OffsetBuffer, ScalarBuffer},
        compute::{concat_batches, take},
        datatypes::{DataType, Field, FieldRef, Schema, SchemaRef},
        row::{RowConverter, SortField},
    },
    common::{DataFusionError, ScalarValue},
    execution::{SendableRecordBatchStream, memory_pool::MemoryReservation},
};
use futures::TryStreamExt;

use super::{COOPERATIVE_INTERVAL, Hit};

#[derive(Clone, Copy, Debug)]
pub(super) struct Neighbor {
    pub(super) vertex: usize,
    pub(super) edge: usize,
}

#[derive(Debug)]
pub(super) struct Topology {
    pub(super) keys: RecordBatch,
    pub(super) identities: RecordBatch,
    pub(super) offsets: Vec<usize>,
    pub(super) neighbors: Vec<Neighbor>,
    pub(super) index: HashMap<Box<[u8]>, usize>,
}

impl Topology {
    pub(super) async fn load(
        mut nodes: SendableRecordBatchStream,
        mut edges: SendableRecordBatchStream,
        identity: SchemaRef,
        undirected: bool,
        reservation: &MemoryReservation,
    ) -> Result<Self> {
        let node_schema = nodes.schema();
        let width = node_schema.fields().len();
        let converter = row_converter(&node_schema)?;
        let identities = row_converter(&identity)?;
        let mut index = HashMap::new();
        let mut key_batches = Vec::new();
        while let Some(batch) = nodes.try_next().await? {
            reservation.try_grow(batch.get_array_memory_size().saturating_mul(2))?;
            let rows = converter.convert_columns(batch.columns())?;
            for row in rows.iter() {
                reservation.try_grow(row.data().len().saturating_add(160))?;
                let id = index.len();
                ensure!(
                    index.insert(Box::<[u8]>::from(row.data()), id).is_none(),
                    "duplicate node business key"
                );
            }
            key_batches.push(batch);
            tokio::task::yield_now().await;
        }
        let mut arcs = Vec::new();
        let mut unique = HashSet::new();
        let unique_budget = reservation.new_empty();
        let mut edge_batches = Vec::new();
        let mut row_offset = 0;
        while let Some(batch) = edges.try_next().await? {
            reservation.try_grow(batch.get_array_memory_size().saturating_mul(2))?;
            let columns = batch.columns();
            let from = converter.convert_columns(&columns[..width])?;
            let to = converter.convert_columns(&columns[width..2 * width])?;
            let keys = identities.convert_columns(&columns[2 * width..])?;
            for (i, ((from, to), key)) in from.iter().zip(to.iter()).zip(keys.iter()).enumerate() {
                unique_budget.try_grow(key.data().len().saturating_add(128))?;
                ensure!(
                    unique.insert(Box::<[u8]>::from(key.data())),
                    "duplicate relationship business key"
                );
                let (Some(&from), Some(&to)) = (index.get(from.data()), index.get(to.data()))
                else {
                    continue;
                };
                if undirected && from != to {
                    reservation.try_grow(4 * std::mem::size_of::<Neighbor>())?;
                    arcs.push((
                        to,
                        Neighbor {
                            vertex: from,
                            edge: row_offset + i,
                        },
                    ));
                }
                arcs.push((
                    from,
                    Neighbor {
                        vertex: to,
                        edge: row_offset + i,
                    },
                ));
            }
            row_offset += batch.num_rows();
            edge_batches.push(RecordBatch::try_new(
                Arc::clone(&identity),
                columns[2 * width..].to_vec(),
            )?);
            tokio::task::yield_now().await;
        }
        let n = index.len();
        drop(unique);
        drop(unique_budget);
        let mut offsets = vec![0; n + 1];
        for chunk in arcs.chunks(COOPERATIVE_INTERVAL) {
            for (source, _) in chunk {
                offsets[source + 1] += 1;
            }
            tokio::task::yield_now().await;
        }
        for i in 1..=n {
            offsets[i] += offsets[i - 1];
            if i % COOPERATIVE_INTERVAL == 0 {
                tokio::task::yield_now().await;
            }
        }
        let mut positions = offsets[..n].to_vec();
        let mut neighbors = vec![Neighbor { vertex: 0, edge: 0 }; arcs.len()];
        for chunk in arcs.chunks(COOPERATIVE_INTERVAL) {
            for &(from, neighbor) in chunk {
                neighbors[positions[from]] = neighbor;
                positions[from] += 1;
            }
            tokio::task::yield_now().await;
        }
        let keys = concat_batches(&node_schema, &key_batches)?;
        let identities = concat_batches(&identity, &edge_batches)?;
        Ok(Self {
            keys,
            identities,
            offsets,
            neighbors,
            index,
        })
    }

    pub(super) fn node_id(&self, key: &[ScalarValue]) -> Result<Option<usize>> {
        let schema = self.keys.schema();
        let converter = row_converter(&schema)?;
        let key = encode_key(key, &schema, &converter)?;
        Ok(self.index.get(key.as_slice()).copied())
    }

    pub(super) fn batch(
        &self,
        hits: &[(usize, Hit)],
        schema: SchemaRef,
        include_start: bool,
    ) -> datafusion::common::Result<RecordBatch> {
        let mut columns = Vec::new();
        if include_start {
            let starts = UInt64Array::from_iter_values(hits.iter().map(|(start, _)| *start as u64));
            for col in self.keys.columns() {
                columns.push(take(col, &starts, None)?);
            }
        }
        let nodes = UInt64Array::from_iter_values(hits.iter().map(|(_, hit)| hit.vertex as u64));
        for col in self.keys.columns() {
            columns.push(take(col, &nodes, None)?);
        }
        columns.push(Arc::new(UInt32Array::from_iter_values(
            hits.iter().map(|(_, hit)| hit.depth as u32),
        )));
        if schema.fields().len() > columns.len() {
            columns.push(Arc::new(path_list(&self.identities, hits, |hit| {
                &hit.edges
            })?));
        }
        if schema.fields().len() > columns.len() {
            columns.push(Arc::new(path_list(&self.keys, hits, |hit| &hit.vertices)?));
        }
        Ok(RecordBatch::try_new(schema, columns)?)
    }
}

fn path_list(
    batch: &RecordBatch,
    hits: &[(usize, Hit)],
    indices: impl Fn(&Hit) -> &[usize],
) -> datafusion::common::Result<ListArray> {
    let mut offsets = Vec::with_capacity(hits.len() + 1);
    let mut rows = Vec::new();
    offsets.push(0_i32);
    for (_, hit) in hits {
        rows.extend(indices(hit).iter().map(|&index| index as u64));
        offsets.push(i32::try_from(rows.len()).map_err(|_| {
            DataFusionError::Execution("path batch exceeds Arrow List capacity".into())
        })?);
    }
    let rows = UInt64Array::from(rows);
    let values = batch
        .columns()
        .iter()
        .map(|column| take(column, &rows, None))
        .collect::<Result<Vec<_>, _>>()?;
    let values = StructArray::from(RecordBatch::try_new(batch.schema(), values)?);
    Ok(ListArray::try_new(
        path_item(&batch.schema()),
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        Arc::new(values),
        None,
    )?)
}

pub(super) fn path_item(identity: &Schema) -> FieldRef {
    Arc::new(Field::new(
        "edge",
        DataType::Struct(identity.fields().clone()),
        false,
    ))
}

pub(super) fn row_converter(schema: &Schema) -> Result<RowConverter> {
    Ok(RowConverter::new(
        schema
            .fields()
            .iter()
            .map(|field| SortField::new(field.data_type().clone()))
            .collect(),
    )?)
}

pub(super) fn encode_key(
    key: &[ScalarValue],
    schema: &Schema,
    converter: &RowConverter,
) -> Result<Vec<u8>> {
    ensure!(
        key.len() == schema.fields().len(),
        "wrong number of business key values"
    );
    let arrays = coerce_key(key, schema)?
        .iter()
        .map(ScalarValue::to_array)
        .collect::<Result<Vec<_>, _>>()?;
    Ok(converter.convert_columns(&arrays)?.row(0).data().to_vec())
}

pub(super) fn coerce_key(key: &[ScalarValue], schema: &Schema) -> Result<Vec<ScalarValue>> {
    ensure!(
        key.len() == schema.fields().len(),
        "wrong number of business key values"
    );
    key.iter()
        .zip(schema.fields())
        .map(|(value, field)| {
            let ty = field.data_type();
            let value = if value.data_type() == *ty {
                value.clone()
            } else if let ScalarValue::Utf8(Some(text)) = value {
                ScalarValue::try_from_string(text.clone(), ty)?
            } else {
                anyhow::bail!(
                    "business key {} expects {ty}, got {}",
                    field.name(),
                    value.data_type()
                );
            };
            ensure!(
                !value.is_null(),
                "business key {} cannot be null",
                field.name()
            );
            Ok(value)
        })
        .collect()
}
