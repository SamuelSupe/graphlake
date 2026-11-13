use std::{collections::HashMap, sync::Arc};

use datafusion::{
    arrow::{
        array::{Array, ArrayRef, BinaryArray, ListArray, new_empty_array},
        datatypes::DataType,
        row::RowConverter,
    },
    common::{Result, ScalarValue, exec_err, utils::SingleRowListArrayBuilder},
    logical_expr::Accumulator,
};

use super::super::encode_equivalence;

#[derive(Debug)]
struct Entry {
    value: ScalarValue,
    order: Vec<u8>,
    sequence: usize,
}

impl Entry {
    fn bytes(&self) -> usize {
        self.value.size() - size_of::<ScalarValue>() + self.order.capacity()
    }
}

/// Each run keeps the first value per Cypher equivalence key, including the
/// sort key that established its position. Merging runs applies the same rule.
#[derive(Debug)]
pub(super) struct Distinct {
    value_type: DataType,
    order: Option<Arc<RowConverter>>,
    entries: HashMap<Vec<u8>, Entry>,
    sequence: usize,
    heap_bytes: usize,
}

impl Distinct {
    pub(super) fn new(value_type: DataType, order: Option<Arc<RowConverter>>) -> Self {
        Self {
            value_type,
            order,
            entries: HashMap::new(),
            sequence: 0,
            heap_bytes: 0,
        }
    }

    fn insert(&mut self, value: ScalarValue, order: &[u8]) -> Result<()> {
        let mut key = Vec::new();
        encode_equivalence(value.clone(), &mut key)?;
        if let Some(previous) = self.entries.get_mut(&key) {
            if order < previous.order.as_slice() {
                self.heap_bytes -= previous.bytes();
                previous.value = value.compacted();
                previous.order = order.to_vec();
                self.heap_bytes += previous.bytes();
            }
        } else {
            let entry = Entry {
                value: value.compacted(),
                order: order.to_vec(),
                sequence: self.sequence,
            };
            self.sequence += 1;
            self.heap_bytes += key.capacity() + entry.bytes();
            self.entries.insert(key, entry);
        }
        Ok(())
    }

    fn list(&self, values: impl ExactSizeIterator<Item = ScalarValue>) -> Result<ScalarValue> {
        let values = if values.len() == 0 {
            new_empty_array(&self.value_type)
        } else {
            ScalarValue::iter_to_array(values)?
        };
        Ok(ScalarValue::List(Arc::new(
            SingleRowListArrayBuilder::new(values).build_list_array(),
        )))
    }
}

impl Accumulator for Distinct {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let Some((values, sort)) = values.split_first() else {
            return Ok(());
        };
        let order = self
            .order
            .as_ref()
            .map(|converter| converter.convert_columns(sort))
            .transpose()?;
        for index in 0..values.len() {
            let value = ScalarValue::try_from_array(values, index)?;
            if !value.is_null() {
                let row = order.as_ref().map(|rows| rows.row(index));
                self.insert(value, row.as_ref().map_or(&[], |row| row.as_ref()))?;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let [values, orders] = states else {
            return exec_err!("invalid distinct collect state");
        };
        let (Some(values), Some(orders)) = (
            values.as_any().downcast_ref::<ListArray>(),
            orders.as_any().downcast_ref::<ListArray>(),
        ) else {
            return exec_err!("invalid distinct collect state arrays");
        };
        for index in 0..values.len() {
            let values = values.value(index);
            let orders = orders.value(index);
            let Some(orders) = orders.as_any().downcast_ref::<BinaryArray>() else {
                return exec_err!("invalid distinct collect sort keys");
            };
            for row in 0..values.len() {
                self.insert(
                    ScalarValue::try_from_array(&values, row)?,
                    orders.value(row),
                )?;
            }
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut entries = self.entries.values().collect::<Vec<_>>();
        entries.sort_unstable_by_key(|entry| entry.sequence);
        let order: ArrayRef = Arc::new(BinaryArray::from_iter_values(
            entries.iter().map(|entry| &entry.order),
        ));
        Ok(vec![
            self.list(entries.into_iter().map(|entry| entry.value.clone()))?,
            ScalarValue::List(Arc::new(
                SingleRowListArrayBuilder::new(order).build_list_array(),
            )),
        ])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut entries = std::mem::take(&mut self.entries)
            .into_values()
            .collect::<Vec<_>>();
        self.heap_bytes = 0;
        entries.sort_unstable_by(|a, b| a.order.cmp(&b.order).then(a.sequence.cmp(&b.sequence)));
        self.list(entries.into_iter().map(|entry| entry.value))
    }

    fn size(&self) -> usize {
        size_of::<Self>()
            + self.entries.capacity() * size_of::<(Vec<u8>, Entry)>()
            + self.heap_bytes
    }
}
