use std::collections::BTreeSet;

use anyhow::{Context, Result, ensure};
use base64::{Engine as _, engine::general_purpose::STANDARD as B64};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use uuid::Uuid;

use super::Managed;
use crate::admin::{cluster::control::Leadership, local_tables::store::Table};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
pub(in crate::admin) struct Head {
    pub generation: Uuid,
    pub upload: Uuid,
    pub sha256: String,
    pub watermark: Option<Value>,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Historical {
    pub head: Head,
    pub retired_ms: u64,
    pub marked_ms: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize)]
#[serde(untagged)]
pub(super) enum Retirements {
    Inline(Vec<Historical>),
    // An object makes old Coordinators fail closed instead of sweeping objects
    // referenced by retirement keys they do not understand.
    Separate {
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        remaining: Vec<Historical>,
    },
}

impl Default for Retirements {
    fn default() -> Self {
        Self::Separate {
            remaining: Vec::new(),
        }
    }
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Pending {
    pub run: Uuid,
    pub owner: String,
}

#[derive(Clone, Deserialize, Serialize)]
pub(super) struct Record {
    pub table: Table,
    pub head: Option<Head>,
    #[serde(default)]
    pub retired: Retirements,
    pub pending: Option<Pending>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(in crate::admin) struct Pin {
    pub table: Uuid,
    pub ticket: Uuid,
    pub head: Head,
}

fn revision(value: &Value) -> Result<i64> {
    Ok(value["mod_revision"]
        .as_str()
        .context("missing local table revision")?
        .parse()?)
}

fn decode<T: DeserializeOwned>(value: &Value) -> Result<T> {
    Ok(serde_json::from_slice(&B64.decode(
        value.as_str().context("missing local table value")?,
    )?)?)
}

fn encode(value: &impl Serialize) -> Result<String> {
    let json = serde_json::to_vec(value)?;
    ensure!(
        json.len() <= 768 * 1024,
        "managed local table metadata exceeds size limit"
    );
    Ok(B64.encode(json))
}

// Leave room for rewriting an old state record in the same etcd transaction.
fn retirement_batch(changes: impl Iterator<Item = Result<Value>>) -> Result<Vec<Value>> {
    const MAX_BYTES: usize = 96 * 1024;
    let mut bytes = 0usize;
    let mut batch = Vec::new();
    for change in changes.take(64) {
        let change = change?;
        bytes += change.to_string().len();
        if bytes > MAX_BYTES && !batch.is_empty() {
            break;
        }
        batch.push(change);
    }
    Ok(batch)
}

impl Managed {
    fn table_key(&self, table: Uuid) -> String {
        B64.encode(format!("{}/local-table/state/{table}", self.meta.prefix))
    }

    fn retired_prefix(&self, table: Uuid) -> String {
        format!("{}/local-table/retired/{table}/", self.meta.prefix)
    }

    fn retired_key(&self, table: Uuid, generation: Uuid) -> String {
        B64.encode(format!("{}{generation}", self.retired_prefix(table)))
    }

    fn retired_put(&self, table: Uuid, item: &Historical) -> Result<Value> {
        Ok(json!({"request_put":{
            "key": self.retired_key(table, item.head.generation),
            "value": encode(item)?
        }}))
    }

    fn pins_prefix(&self, table: Uuid) -> String {
        format!("{}/local-table/pin/{table}/", self.meta.prefix)
    }

    fn pin_key(&self, table: Uuid, ticket: Uuid) -> String {
        B64.encode(format!("{}{ticket}", self.pins_prefix(table)))
    }

    async fn list(&self, prefix: &str) -> Result<Vec<Value>> {
        let mut end = prefix.as_bytes().to_vec();
        // All prefixes are ASCII and end in a slash.
        *end.last_mut().unwrap() += 1;
        let end = B64.encode(end);
        let mut key = B64.encode(prefix);
        let mut at_revision = None;
        let mut entries = Vec::new();
        loop {
            let result = self
                .meta
                .call(
                    "/v3/kv/range",
                    json!({
                        "key":key, "range_end":end, "limit":"128",
                        "sort_order":"ASCEND", "revision":at_revision.as_deref().unwrap_or("0")
                    }),
                )
                .await?;
            let page = result["kvs"].as_array();
            entries.extend(page.into_iter().flatten().cloned());
            if result["more"].as_bool() != Some(true) {
                return Ok(entries);
            }
            let last = entries.last().context("empty managed metadata page")?;
            let mut after = B64.decode(last["key"].as_str().context("missing metadata key")?)?;
            after.push(0);
            key = B64.encode(after);
            if at_revision.is_none() {
                at_revision = Some(
                    result["header"]["revision"]
                        .as_str()
                        .context("missing metadata list revision")?
                        .parse::<i64>()?
                        .to_string(),
                );
            }
        }
    }

    async fn read(&self, key: String) -> Result<Option<Value>> {
        let result = self.meta.call("/v3/kv/range", json!({"key":key})).await?;
        Ok(result["kvs"].as_array().and_then(|r| r.first()).cloned())
    }

    pub(super) async fn records(&self) -> Result<Vec<(Record, i64)>> {
        self.list(&format!("{}/local-table/state/", self.meta.prefix))
            .await?
            .iter()
            .map(|v| Ok((decode(&v["value"])?, revision(v)?)))
            .collect()
    }

    pub(super) async fn record(&self, id: Uuid) -> Result<Option<(Record, i64)>> {
        self.read(self.table_key(id))
            .await?
            .map(|v| Ok((decode(&v["value"])?, revision(&v)?)))
            .transpose()
    }

    pub(super) async fn save(
        &self,
        record: &Record,
        rev: i64,
        leader: &Leadership,
    ) -> Result<bool> {
        self.save_retiring(record, rev, leader, None).await
    }

    pub(super) async fn save_retiring(
        &self,
        record: &Record,
        rev: i64,
        leader: &Leadership,
        retired: Option<&Historical>,
    ) -> Result<bool> {
        ensure!(
            retired.is_none() || matches!(record.retired, Retirements::Separate { .. }),
            "migrate inline snapshot history before retiring a generation"
        );
        let changes = retired
            .map(|item| self.retired_put(record.table.data_id, item))
            .transpose()?
            .into_iter()
            .collect();
        self.save_record(record, rev, leader, changes).await
    }

    async fn save_record(
        &self,
        record: &Record,
        rev: i64,
        leader: &Leadership,
        mut changes: Vec<Value>,
    ) -> Result<bool> {
        leader.admission().map_err(super::api_error)?;
        let key = self.table_key(record.table.data_id);
        changes.push(json!({"request_put":{"key":key,"value":encode(record)?}}));
        let result = self.meta.call("/v3/kv/txn", json!({
            "compare":[leader.owner(),{"key":key,"target":"MOD","result":"EQUAL","mod_revision":rev.to_string()}],
            "success":changes
        })).await?;
        Ok(result["succeeded"].as_bool() == Some(true))
    }

    pub(super) async fn migrate_retirements(&self, table: Uuid, leader: &Leadership) -> Result<()> {
        let mut conflicts = 0;
        while let Some((mut state, rev)) = self.record(table).await? {
            let mut remaining = match std::mem::take(&mut state.retired) {
                Retirements::Separate { remaining } if remaining.is_empty() => return Ok(()),
                Retirements::Inline(items) | Retirements::Separate { remaining: items } => items,
            };
            let changes = retirement_batch(
                remaining
                    .iter()
                    .rev()
                    .map(|item| self.retired_put(table, item)),
            )?;
            remaining.truncate(remaining.len() - changes.len());
            state.retired = Retirements::Separate { remaining };
            if self.save_record(&state, rev, leader, changes).await? {
                conflicts = 0;
            } else {
                conflicts += 1;
                ensure!(
                    conflicts < 8,
                    "managed table changed too often to migrate snapshot history"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn retirements(&self, table: Uuid) -> Result<Vec<Historical>> {
        self.list(&self.retired_prefix(table))
            .await?
            .iter()
            .map(|entry| decode(&entry["value"]))
            .collect()
    }

    pub(super) async fn update_retirements(
        &self,
        table: Uuid,
        leader: &Leadership,
        items: &[Historical],
        mark: Option<u64>,
    ) -> Result<()> {
        let mut offset = 0;
        while offset < items.len() {
            let changes = retirement_batch(items[offset..].iter().map(|item| {
                if let Some(now) = mark {
                    let mut item = item.clone();
                    item.marked_ms = Some(now);
                    self.retired_put(table, &item)
                } else {
                    Ok(json!({"request_delete_range":{
                        "key":self.retired_key(table, item.head.generation)
                    }}))
                }
            }))?;
            offset += changes.len();
            leader.admission().map_err(super::api_error)?;
            let result = self
                .meta
                .call(
                    "/v3/kv/txn",
                    json!({
                        "compare":[leader.owner()], "success":changes
                    }),
                )
                .await?;
            ensure!(
                result["succeeded"].as_bool() == Some(true),
                "lost leadership during snapshot GC"
            );
        }
        Ok(())
    }

    pub(super) async fn grant(&self, ttl: u64) -> Result<String> {
        let response = self
            .meta
            .call("/v3/lease/grant", json!({"TTL":ttl.to_string()}))
            .await?;
        Ok(response["ID"]
            .as_str()
            .context("missing managed table lease ID")?
            .to_owned())
    }

    pub(super) async fn release(&self, lease: &str) {
        let _ = self
            .meta
            .call("/v3/lease/revoke", json!({"ID":lease}))
            .await;
    }

    pub(super) async fn pin_current(
        &self,
        id: Uuid,
        ticket: Uuid,
        lease: &str,
    ) -> Result<(Table, Option<Pin>)> {
        for _ in 0..8 {
            let (state, rev) = self
                .record(id)
                .await?
                .context("unknown managed local table")?;
            let Some(head) = &state.head else {
                return Ok((state.table, None));
            };
            let key = self.table_key(id);
            let pin = Pin {
                table: id,
                ticket,
                head: head.clone(),
            };
            // The comparison orders pin admission against a head change. A pin
            // addresses only the current head, which GC always retains.
            let result = self.meta.call("/v3/kv/txn", json!({
                "compare":[{"key":key,"target":"MOD","result":"EQUAL","mod_revision":rev.to_string()}],
                "success":[{"request_put":{"key":self.pin_key(id,ticket),"value":encode(&pin)?,"lease":lease}}]
            })).await?;
            if result["succeeded"].as_bool() == Some(true) {
                return Ok((state.table, Some(pin)));
            }
        }
        anyhow::bail!("local table changed too often to pin; retry the operation")
    }

    pub(super) async fn check_pin(&self, pin: &Pin) -> Result<()> {
        let value = self
            .read(self.pin_key(pin.table, pin.ticket))
            .await?
            .context("managed snapshot lease expired")?;
        let saved: Pin = decode(&value["value"])?;
        ensure!(
            saved.table == pin.table && saved.ticket == pin.ticket && saved.head == pin.head,
            "managed snapshot pin differs from the dispatched generation"
        );
        Ok(())
    }

    pub(in crate::admin) async fn remaining(&self, pin: &Pin) -> Result<std::time::Duration> {
        let entry = self
            .read(self.pin_key(pin.table, pin.ticket))
            .await?
            .context("managed snapshot lease expired before execution")?;
        let id = entry["lease"]
            .as_str()
            .context("managed snapshot is missing its lease")?;
        let result = self
            .meta
            .call("/v3/lease/timetolive", json!({"ID":id}))
            .await?;
        let seconds: u64 = result["TTL"]
            .as_str()
            .context("managed snapshot lease has no TTL")?
            .parse()?;
        Ok(std::time::Duration::from_secs(seconds))
    }

    pub(super) async fn active_pins(&self, table: Uuid) -> Result<BTreeSet<Uuid>> {
        self.list(&self.pins_prefix(table))
            .await?
            .iter()
            .map(|v| {
                let pin: Pin = decode(&v["value"])?;
                Ok(pin.head.generation)
            })
            .collect()
    }
}
