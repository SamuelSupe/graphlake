use std::{
    collections::{BTreeMap, VecDeque},
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use anyhow::{Result, ensure};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use hmac::{Hmac, KeyInit, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::sync::watch;
use uuid::Uuid;

use super::{Collector, Element, ExploreResult, NEIGHBOR_LIMIT, SourceVersion, invalid_cursor};

const MAX_SESSIONS: usize = 8;
const MAX_BYTES: u64 = 64 * 1024 * 1024;
const TTL: Duration = Duration::from_secs(15 * 60);
const CHECKPOINT_TTL: Duration = Duration::from_secs(60 * 60);
const CHECKPOINT_BYTES: u64 = 256 * 1024 * 1024;
const MAX_CHECKPOINTS: usize = 32;
const CURSOR_HISTORY: usize = 8;
const SEARCH_BYTES: usize = 8 * 1024 * 1024;

pub(in crate::admin) struct Sessions {
    entries: VecDeque<(Uuid, Arc<Pages>)>,
    checkpoints: VecDeque<(Uuid, Arc<Pages>, u64)>,
    searches: VecDeque<(Uuid, Arc<SearchIndex>)>,
    pub(super) cursors: Cursors,
}

impl Default for Sessions {
    fn default() -> Self {
        Self {
            entries: VecDeque::new(),
            checkpoints: VecDeque::new(),
            searches: VecDeque::new(),
            cursors: Cursors(rand::random()),
        }
    }
}

impl Sessions {
    pub(in crate::admin) fn with_cursor_key(key: [u8; 32]) -> Self {
        Self {
            cursors: Cursors(key),
            ..Self::default()
        }
    }

    pub(super) fn search_index(&mut self, id: Uuid) -> Option<Arc<SearchIndex>> {
        self.searches
            .retain(|(_, index)| index.created.elapsed() < CHECKPOINT_TTL);
        let entry = self
            .searches
            .remove(self.searches.iter().position(|(key, _)| *key == id)?)?;
        let index = Arc::clone(&entry.1);
        self.searches.push_back(entry);
        Some(index)
    }

    pub(super) fn remember_search(&mut self, id: Uuid, index: SearchIndex) {
        self.searches.retain(|(key, _)| *key != id);
        if index.bytes > SEARCH_BYTES {
            return;
        }
        let mut bytes = index.bytes
            + self
                .searches
                .iter()
                .map(|(_, index)| index.bytes)
                .sum::<usize>();
        while self.searches.len() >= 8 || bytes > SEARCH_BYTES {
            let Some((_, old)) = self.searches.pop_front() else {
                break;
            };
            bytes -= old.bytes;
        }
        self.searches.push_back((id, Arc::new(index)));
    }

    pub(super) fn find(&mut self, signature: &str) -> Option<(Uuid, Arc<Pages>)> {
        self.expire();
        let id = self
            .entries
            .iter()
            .rev()
            .find(|(_, pages)| pages.signature == signature && pages.can_serve(0))?
            .0;
        self.get(id).map(|pages| (id, pages))
    }

    pub(super) fn get(&mut self, id: Uuid) -> Option<Arc<Pages>> {
        self.expire();
        if let Some(index) = self.entries.iter().position(|(key, _)| *key == id) {
            let entry = self.entries.remove(index)?;
            let result = Arc::clone(&entry.1);
            self.entries.push_back(entry);
            return Some(result);
        }
        let index = self.checkpoints.iter().position(|(key, _, _)| *key == id)?;
        let entry = self.checkpoints.remove(index)?;
        let result = Arc::clone(&entry.1);
        self.checkpoints.push_back(entry);
        Some(result)
    }

    pub(super) fn remove(&mut self, id: Uuid) {
        self.entries.retain(|(key, _)| *key != id);
        self.checkpoints.retain(|(key, _, _)| *key != id);
    }

    pub(super) fn insert(&mut self, id: Uuid, pages: Arc<Pages>) {
        self.expire();
        let start = pages.first_page();
        let covers_start = |cached: &Pages| cached.first_page() < start && cached.can_serve(start);
        if self
            .entries
            .iter()
            .any(|(key, cached)| *key == id && covers_start(cached))
            || self
                .checkpoints
                .iter()
                .any(|(key, cached, _)| *key == id && covers_start(cached))
        {
            return;
        }
        self.remove(id);
        if self.entries.len() == MAX_SESSIONS
            && let Some(entry) = self.entries.pop_front()
        {
            self.checkpoint(entry);
        }
        self.entries.push_back((id, pages));
    }

    fn expire(&mut self) {
        self.checkpoints
            .retain(|(_, pages, _)| pages.created.elapsed() < CHECKPOINT_TTL);
        while let Some(index) = self
            .entries
            .iter()
            .position(|(_, pages)| pages.created.elapsed() >= TTL)
        {
            if let Some(entry) = self.entries.remove(index) {
                self.checkpoint(entry);
            }
        }
    }

    fn checkpoint(&mut self, (id, pages): (Uuid, Arc<Pages>)) {
        let Some(bytes) = pages.completed_bytes() else {
            return;
        };
        if pages.created.elapsed() >= CHECKPOINT_TTL {
            return;
        }
        self.checkpoints.push_back((id, pages, bytes));
        let mut total = self
            .checkpoints
            .iter()
            .map(|(_, _, bytes)| bytes)
            .sum::<u64>();
        while total > CHECKPOINT_BYTES || self.checkpoints.len() > MAX_CHECKPOINTS {
            if let Some((_, _, bytes)) = self.checkpoints.pop_front() {
                total -= bytes;
            }
        }
    }
}

#[derive(Clone, Copy)]
pub(super) struct Cursors([u8; 32]);

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(super) struct Anchor {
    pub label: String,
    pub key: Vec<String>,
    pub ordinal: u64,
}

#[derive(Serialize, Deserialize)]
pub(super) struct Cursor {
    pub session: Uuid,
    pub page: usize,
    pub versions: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temporal_at: Option<chrono::DateTime<chrono::Utc>>,
    pub anchor: Option<Anchor>,
    pub history: Vec<Option<Anchor>>,
}

impl Cursors {
    pub fn decode(&self, encoded: &str, signature: &str) -> Option<Cursor> {
        if encoded.len() > 24 * 1024 {
            return None;
        }
        let bytes = URL_SAFE_NO_PAD.decode(encoded).ok()?;
        let (payload, tag) = bytes.split_at_checked(bytes.len().checked_sub(32)?)?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).ok()?;
        mac.update(signature.as_bytes());
        mac.update(payload);
        mac.verify_slice(tag).ok()?;
        serde_json::from_slice(payload).ok()
    }

    fn encode(&self, signature: &str, mut cursor: Cursor) -> String {
        let mut bytes = serde_json::to_vec(&cursor).expect("serialize exploration cursor");
        while bytes.len() > 16 * 1024 && !cursor.history.is_empty() {
            cursor.history.remove(0);
            bytes = serde_json::to_vec(&cursor).expect("serialize exploration cursor");
        }
        if bytes.len() > 16 * 1024 {
            if let Some(anchor) = &mut cursor.anchor {
                anchor.key.clear();
            }
            bytes = serde_json::to_vec(&cursor).expect("serialize exploration cursor");
        }
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.0).expect("fixed HMAC key");
        mac.update(signature.as_bytes());
        mac.update(&bytes);
        bytes.extend_from_slice(&mac.finalize().into_bytes());
        URL_SAFE_NO_PAD.encode(bytes)
    }
}

pub(super) struct Pages {
    pub signature: String,
    pub sources: Vec<SourceVersion>,
    pub versions: [u8; 32],
    temporal_at: Option<chrono::DateTime<chrono::Utc>>,
    pub postgres: Mutex<BTreeMap<String, String>>,
    selected: Option<Element>,
    storage: Mutex<Storage>,
    progress: watch::Sender<()>,
    created: Instant,
}

struct Storage {
    file: File,
    first_page: usize,
    history: Vec<Option<Anchor>>,
    offsets: Vec<Page>,
    next_anchor: Option<Anchor>,
    done: Option<super::ApiResult<()>>,
}

struct Page {
    offset: u64,
    len: usize,
    more: bool,
    anchor: Option<Anchor>,
}

impl Pages {
    pub fn new(
        signature: String,
        temporal_at: Option<chrono::DateTime<chrono::Utc>>,
        sources: Vec<SourceVersion>,
        postgres: BTreeMap<String, String>,
        selected: Option<Element>,
        first_page: usize,
        anchor: Option<Anchor>,
        history: Vec<Option<Anchor>>,
    ) -> Result<Arc<Self>> {
        Ok(Arc::new(Self {
            signature,
            temporal_at,
            versions: super::versions_digest(&sources),
            sources,
            postgres: Mutex::new(postgres),
            selected,
            storage: Mutex::new(Storage {
                file: crate::blocking(tempfile::tempfile)?,
                first_page,
                history,
                offsets: Vec::new(),
                next_anchor: anchor,
                done: None,
            }),
            progress: watch::channel(()).0,
            created: Instant::now(),
        }))
    }

    fn first_page(&self) -> usize {
        self.storage.lock().unwrap().first_page
    }

    pub fn can_serve(&self, page: usize) -> bool {
        let storage = self.storage.lock().unwrap();
        page >= storage.first_page
            && (page - storage.first_page < storage.offsets.len() || storage.done.is_none())
    }

    fn completed_bytes(&self) -> Option<u64> {
        let storage = self.storage.lock().unwrap();
        storage.done.as_ref()?.as_ref().ok()?;
        storage
            .offsets
            .last()
            .map(|page| page.offset + page.len as u64)
    }

    pub fn complete(&self, result: super::ApiResult<()>) {
        self.storage.lock().unwrap().done = Some(result);
        self.progress.send_replace(());
    }

    fn publish(&self, offset: u64, bytes: &[u8], next: Option<Anchor>) -> Result<()> {
        crate::blocking(|| {
            let mut storage = self.storage.lock().unwrap();
            storage.file.seek(SeekFrom::Start(offset))?;
            storage.file.write_all(bytes)?;
            let more = next.is_some();
            let anchor = std::mem::replace(&mut storage.next_anchor, next);
            storage.offsets.push(Page {
                offset,
                len: bytes.len(),
                more,
                anchor,
            });
            self.progress.send_replace(());
            Ok(())
        })
    }

    fn anchor(&self, page: usize) -> Option<Anchor> {
        let storage = self.storage.lock().unwrap();
        if page < storage.first_page {
            let index = page.checked_sub(storage.first_page - storage.history.len())?;
            return storage.history.get(index)?.clone();
        }
        let index = page.checked_sub(storage.first_page)?;
        if index == storage.offsets.len() {
            storage.next_anchor.clone()
        } else {
            storage.offsets.get(index)?.anchor.clone()
        }
    }

    fn read_page(&self, page: usize) -> super::ApiResult<Option<(Vec<u8>, bool)>> {
        crate::blocking(|| {
            let mut storage = self.storage.lock().unwrap();
            let index = page
                .checked_sub(storage.first_page)
                .ok_or_else(invalid_cursor)?;
            let Some(&Page {
                offset, len, more, ..
            }) = storage.offsets.get(index)
            else {
                return match &storage.done {
                    Some(result) => Err(result.clone().err().unwrap_or_else(invalid_cursor)),
                    None => Ok(None),
                };
            };
            let mut bytes = vec![0; len];
            storage
                .file
                .seek(SeekFrom::Start(offset))
                .and_then(|_| storage.file.read_exact(&mut bytes))
                .map_err(|_| super::Error::storage())?;
            Ok(Some((bytes, more)))
        })
    }

    pub async fn response(
        &self,
        cursors: &Cursors,
        id: Uuid,
        page: usize,
        timeout: Duration,
    ) -> super::ApiResult<ExploreResult> {
        let (bytes, more) = tokio::time::timeout(timeout, self.wait_for_page(page))
            .await
            .map_err(|_| super::query_error(anyhow::anyhow!("query deadline exceeded")))??;
        let graph = String::from_utf8(bytes).map_err(|_| super::Error::storage())?;
        let graph =
            serde_json::value::RawValue::from_string(graph).map_err(|_| super::Error::storage())?;
        let encode = |page| self.cursor(cursors, id, page);
        Ok(ExploreResult {
            selected: self.selected.clone(),
            graph,
            sources: self.sources.clone(),
            next_cursor: more.then(|| encode(page + 1)),
            page_cursor: Some(encode(page)),
            previous_cursor: page.checked_sub(1).map(encode),
            page_number: page + 1,
        })
    }

    async fn wait_for_page(&self, page: usize) -> super::ApiResult<(Vec<u8>, bool)> {
        let mut progress = self.progress.subscribe();
        loop {
            if let Some(result) = self.read_page(page)? {
                return Ok(result);
            }
            progress
                .changed()
                .await
                .map_err(|_| super::Error::storage())?;
        }
    }

    fn cursor(&self, cursors: &Cursors, id: Uuid, page: usize) -> String {
        let history: Vec<_> = (page.saturating_sub(CURSOR_HISTORY)..page)
            .map(|previous| self.anchor(previous))
            .skip_while(Option::is_none)
            .collect();
        cursors.encode(
            &self.signature,
            Cursor {
                session: id,
                page,
                versions: self.versions,
                temporal_at: self.temporal_at,
                anchor: self.anchor(page),
                history,
            },
        )
    }

    pub async fn build_search_index(
        &self,
        cursors: &Cursors,
        id: Uuid,
        search: &super::ExploreSearch,
    ) -> super::ApiResult<(super::ExploreSearchResult, Option<SearchIndex>)> {
        if self.first_page() != 0 {
            return Err(invalid_cursor());
        }
        let mut page = 0;
        let mut indexed = Some(Vec::new());
        let mut size = 0;
        let mut matches = SearchMatches::new(search);
        loop {
            let (bytes, more) = self.wait_for_page(page).await?;
            let cursor = self.cursor(cursors, id, page);
            let graph = crate::blocking(|| {
                let graph: SearchFragment =
                    serde_json::from_slice(&bytes).map_err(|_| super::Error::storage())?;
                matches.add(&graph.elements, page, &cursor);
                Ok::<_, super::Error>(graph)
            })?;
            size += bytes.len() + cursor.len();
            if size > SEARCH_BYTES {
                indexed = None;
            }
            if let Some(indexed) = &mut indexed {
                indexed.push(SearchPage {
                    items: graph.elements,
                    cursor,
                });
            }
            if !more {
                break;
            }
            page += 1;
            tokio::task::yield_now().await;
        }
        let index = indexed.map(|indexed| SearchIndex {
            pages: indexed,
            bytes: size,
            created: Instant::now(),
            versions: self.versions,
            sources: self.sources.clone(),
            postgres: Mutex::new(self.postgres.lock().unwrap().clone()),
        });
        Ok((matches.result, index))
    }
}

pub(super) struct SearchIndex {
    pages: Vec<SearchPage>,
    bytes: usize,
    created: Instant,
    pub versions: [u8; 32],
    pub sources: Vec<SourceVersion>,
    pub postgres: Mutex<BTreeMap<String, String>>,
}

struct SearchPage {
    items: Vec<SearchElement>,
    cursor: String,
}

impl SearchIndex {
    pub fn search(&self, search: &super::ExploreSearch) -> super::ExploreSearchResult {
        let mut matches = SearchMatches::new(search);
        for (page, data) in self.pages.iter().enumerate() {
            matches.add(&data.items, page, &data.cursor);
        }
        matches.result
    }
}

struct SearchMatches<'a> {
    search: &'a super::ExploreSearch,
    query: String,
    seen: std::collections::HashSet<String>,
    result: super::ExploreSearchResult,
}

impl<'a> SearchMatches<'a> {
    fn new(search: &'a super::ExploreSearch) -> Self {
        Self {
            search,
            query: search.query.trim().to_lowercase(),
            seen: Default::default(),
            result: super::ExploreSearchResult {
                matches: Vec::new(),
                total: 0,
            },
        }
    }

    fn add(&mut self, items: &[SearchElement], page: usize, cursor: &str) {
        for item in items {
            if self.seen.contains(&item.id)
                || !self.search.visible(item)
                || !super::search_match(item, &self.query)
            {
                continue;
            }
            self.seen.insert(item.id.clone());
            if self.result.total >= self.search.offset
                && self.result.matches.len() < super::SEARCH_PAGE_SIZE
            {
                self.result.matches.push(super::ExploreMatch {
                    id: item.id.clone(),
                    identity: super::Identity {
                        kind: item.kind,
                        label: item.label.clone(),
                        key: item.key.clone(),
                    },
                    page: page + 1,
                    cursor: cursor.to_owned(),
                });
            }
            self.result.total += 1;
        }
    }
}

#[derive(Deserialize)]
struct SearchFragment {
    elements: Vec<SearchElement>,
}

#[derive(Deserialize)]
pub(super) struct SearchElement {
    id: String,
    pub kind: super::Kind,
    pub label: String,
    pub key: Vec<String>,
    pub properties: Option<BTreeMap<String, Option<String>>>,
    pub from: Option<String>,
    pub to: Option<String>,
}

pub(super) struct Production(pub Arc<Pages>);

impl Drop for Production {
    fn drop(&mut self) {
        let mut storage = self
            .0
            .storage
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if storage.done.is_none() {
            storage.done = Some(Err(super::Error::storage()));
            self.0.progress.send_replace(());
        }
    }
}

pub(super) struct Writer {
    pages: Arc<Pages>,
    bytes: u64,
    output: Collector,
    edge_order: Vec<String>,
    limit: u64,
}

impl Writer {
    pub fn new(pages: Arc<Pages>, spill_bytes: u64) -> Self {
        let mut writer = Self {
            pages,
            bytes: 0,
            output: Collector::default(),
            edge_order: Vec::new(),
            limit: MAX_BYTES.min(spill_bytes),
        };
        writer.reset_page();
        writer
    }

    pub fn push(
        &mut self,
        from: Element,
        to: Element,
        edge: Element,
        ordinal: u64,
    ) -> Result<bool> {
        let published = self.edge_order.len() == NEIGHBOR_LIMIT
            || !self
                .output
                .insert_group([from.clone(), to.clone(), edge.clone()]);
        if published {
            ensure!(!self.edge_order.is_empty(), "neighbor page budget exceeded");
            self.flush_page(Some(Anchor {
                label: edge.identity.label.clone(),
                key: if edge.transient {
                    Vec::new()
                } else {
                    edge.identity.key.clone()
                },
                ordinal,
            }))?;
            self.reset_page();
            ensure!(
                self.output.insert_group([from, to, edge.clone()]),
                "neighbor page budget exceeded"
            );
        }
        self.edge_order.push(edge.id);
        Ok(published)
    }

    pub fn mark_truncated(&mut self) {
        self.output.mark_truncated();
    }

    fn reset_page(&mut self) {
        if let Some(item) = &self.pages.selected {
            self.output.insert(item.clone());
        }
    }

    fn flush_page(&mut self, next: Option<Anchor>) -> Result<()> {
        let mut graph = std::mem::take(&mut self.output).finish();
        graph.truncated |= next.is_some();
        let order: BTreeMap<_, _> = self
            .edge_order
            .iter()
            .enumerate()
            .map(|(index, id)| (id, index + 1))
            .collect();
        graph
            .elements
            .sort_by_key(|item| order.get(&item.id).copied().unwrap_or_default());
        let bytes = serde_json::to_vec(&graph)?;
        ensure!(
            self.bytes + bytes.len() as u64 <= self.limit,
            "neighbor page budget exceeded"
        );
        self.pages.publish(self.bytes, &bytes, next)?;
        self.bytes += bytes.len() as u64;
        self.edge_order.clear();
        Ok(())
    }

    pub fn finish(mut self) -> Result<()> {
        self.flush_page(None)
    }
}
