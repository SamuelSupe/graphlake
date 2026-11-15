use super::*;

pub(super) struct OrderProjection {
    pub(super) full_block: Vec<usize>,
    pages: Option<Vec<(usize, String)>>,
    offsets: Vec<u64>,
}

impl Read<'_> {
    pub(super) fn order_projection(&self, label: &str, ty: &DataType) -> Result<OrderProjection> {
        let DataType::Struct(entity) = ty else {
            anyhow::bail!("invalid CSR entity layout")
        };
        let DataType::Struct(fields) = entity[3].data_type() else {
            anyhow::bail!("invalid CSR order layout")
        };
        let properties = if entity[3].name() == "lanes" {
            match fields.find(label).map(|(_, field)| field.data_type()) {
                Some(DataType::Struct(fields)) => fields,
                _ => anyhow::bail!("unknown CSR order label"),
            }
        } else {
            fields
        };
        let schema = decode_schema(
            self.manifest
                .table_schemas
                .get(label)
                .context("missing CSR property schema")?,
        )?;
        let requested = std::iter::once(LOOKUP_ID)
            .chain(properties.iter().filter_map(|field| {
                schema
                    .field_with_name(field.name())
                    .is_ok()
                    .then_some(field.name().as_str())
            }))
            .collect::<Vec<_>>();
        let full_block = std::iter::once(0)
            .chain(
                requested
                    .iter()
                    .skip(1)
                    .map(|name| schema.index_of(name).map(|index| index + 1))
                    .collect::<Result<Vec<_>, _>>()?,
            )
            .collect();
        let pages = self.manifest.filter_columns.get(label).and_then(|columns| {
            requested
                .iter()
                .map(|name| {
                    columns
                        .iter()
                        .position(|column| column == name)
                        .map(|index| (index, (*name).to_owned()))
                })
                .collect::<Option<Vec<_>>>()
        });
        let offsets = if pages.is_some() {
            self.manifest.tables[label]
                .iter()
                .scan(0, |offset, block| {
                    let first = *offset;
                    *offset += block
                        .columns
                        .iter()
                        .map(|page| u64::from(page.bytes))
                        .sum::<u64>();
                    Some(first)
                })
                .collect()
        } else {
            Vec::new()
        };
        Ok(OrderProjection {
            full_block,
            pages,
            offsets,
        })
    }

    pub(super) async fn order_pages(
        &self,
        label: &str,
        index: usize,
        block: &Block,
        ids: &[&String],
        selected: &OrderProjection,
        order_only: bool,
    ) -> Result<Option<(PropertyReader, u64)>> {
        let (Some(columns), Some(cache)) = (&selected.pages, self.cache) else {
            return Ok(None);
        };
        let cells = self
            .prefix
            .clone()
            .join("tables")
            .join(label)
            .join("filter-columns");
        let offset = selected.offsets[index];
        let mut requests = Vec::new();
        let mut start = offset;
        for (i, page) in block.columns.iter().enumerate() {
            if let Some((_, name)) = columns.iter().find(|(index, _)| *index == i) {
                requests.push((i, name, start, page));
            }
            start += u64::from(page.bytes);
        }
        ensure!(requests.len() == columns.len(), "missing CSR sort column");
        if !order_only
            && requests
                .iter()
                .map(|(_, _, _, page)| u64::from(page.bytes))
                .sum::<u64>()
                > block.bytes
        {
            return Ok(None);
        }
        // Resolve lookup IDs first so the other columns retain only the requested rows.
        requests.sort_by_key(|&(_, name, _, _)| name != LOOKUP_ID);
        let memory = MemoryConsumer::new("CSR decoded sort columns")
            .register(&self.scope.runtime.memory_pool);
        let mut reads = futures::stream::FuturesOrdered::new();
        for (_, name, start, page) in requests {
            let size = u64::from(page.bytes);
            let descriptor = Block {
                bytes: size,
                decoded_bytes: None,
                sha256: page.sha256,
                first: block.first,
                last: block.last,
                ids: None,
                numbers: BTreeMap::new(),
                columns: Vec::new(),
            };
            let key = cells.clone().join(start.to_string());
            let request = cells.clone();
            let fetch = async move {
                let mut result = self
                    .objects
                    .get_ranges(&request, &[start..start + size])
                    .await?;
                let bytes = result.pop().context("missing CSR sort column response")?;
                ensure!(
                    bytes.len() as u64 == size && Sha256::digest(&bytes).as_slice() == page.sha256,
                    "CSR sort column checksum mismatch"
                );
                self.scope.reads.object_bytes(bytes.len());
                self.scope.reads.request();
                Ok(bytes)
            };
            reads.push_back(self.order_page(cache, key, descriptor, fetch, name));
        }
        let mut fields = Vec::new();
        let mut arrays = Vec::new();
        let mut bytes_read = 0;
        let mut rows = None;
        while let Some((data, name, read_bytes)) = reads.try_next().await? {
            bytes_read += read_bytes;
            let mut reader = StreamReader::try_new(std::io::Cursor::new(data.bytes.clone()), None)?;
            let batch = reader.next().context("empty CSR sort column")??;
            ensure!(reader.next().is_none(), "invalid CSR sort column batches");
            let decoded = memory.new_empty();
            decoded.try_grow(batch.get_array_memory_size())?;
            ensure!(
                batch.num_columns() == 1 && batch.num_rows() as u64 == block.last - block.first + 1,
                "invalid CSR sort column size"
            );
            ensure!(
                batch.schema_ref().field(0).name() == name,
                "invalid CSR sort column name"
            );
            fields.push(batch.schema_ref().fields()[0].clone());
            let offsets = if let Some(rows) = &rows {
                rows
            } else {
                let lookup = batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .context("missing CSR lookup IDs")?;
                rows.insert(UInt32Array::from_iter_values(ids.iter().filter_map(|id| {
                    let i = lower_bound(lookup.len(), |i| lookup.value(i) < id.as_str());
                    (i < lookup.len() && lookup.value(i) == id.as_str()).then_some(i as u32)
                })))
            };
            let array = compact_view(take(batch.column(0), offsets, None)?);
            memory.try_grow(array.get_array_memory_size())?;
            arrays.push(array);
        }
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?;
        Ok(Some((
            (Box::new(std::iter::once(Ok(batch))), memory, Vec::new()),
            bytes_read,
        )))
    }

    async fn order_page<'a>(
        &self,
        cache: &BlockCache,
        key: Key,
        descriptor: Block,
        fetch: impl std::future::Future<Output = Result<bytes::Bytes>>,
        name: &'a str,
    ) -> Result<(Arc<CachedBlock>, &'a str, u64)> {
        let mut read_bytes = 0;
        let fetch = async {
            let data = fetch.await?;
            read_bytes = descriptor.bytes;
            Ok(data)
        };
        let data = cache.read_with(key, &descriptor, self.scope, fetch).await?;
        Ok((data, name, read_bytes))
    }
}
