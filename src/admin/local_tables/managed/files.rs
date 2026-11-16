use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, ensure};
use futures::StreamExt;
use object_store::path::Path as Key;
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use uuid::Uuid;

use super::{Head, Managed};
use crate::source::parquet_snapshot::{self, RemoteFile, RemoteSnapshot, Snapshot};

fn hex(hash: impl AsRef<[u8]>) -> String {
    hash.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}

async fn hash(path: &Path) -> Result<(u64, String)> {
    let mut file = tokio::fs::File::open(path).await?;
    let size = file.metadata().await?.len();
    let mut hash = Sha256::new();
    let mut bytes = vec![0; 256 * 1024];
    loop {
        let count = file.read(&mut bytes).await?;
        if count == 0 {
            break;
        }
        hash.update(&bytes[..count]);
    }
    Ok((size, hex(hash.finalize())))
}

impl Managed {
    pub(super) fn upload_prefix(&self, table: Uuid, upload: Uuid) -> Key {
        self.objects.local_table(table).join(upload.to_string())
    }

    pub(super) fn manifest_key(&self, table: Uuid, head: &Head) -> Key {
        self.upload_prefix(table, head.upload).join("manifest.json")
    }

    pub(super) async fn snapshot(&self, table: Uuid, head: &Head) -> Result<RemoteSnapshot> {
        let result = self.objects.get(&self.manifest_key(table, head)).await?;
        ensure!(
            result.meta.size <= 8 * 1024 * 1024,
            "managed snapshot manifest exceeds size limit"
        );
        let bytes = result.bytes().await?;
        ensure!(
            hex(Sha256::digest(&bytes)) == head.sha256,
            "managed snapshot manifest checksum mismatch"
        );
        let frozen: RemoteSnapshot = serde_json::from_slice(&bytes)?;
        ensure!(
            frozen.generation == head.generation,
            "managed snapshot generation mismatch"
        );
        if let Some(snapshot) = &frozen.snapshot {
            // Validate the existing GLPQ mask, checkpoint and part constraints.
            snapshot.encode()?;
            ensure!(
                snapshot.generation == head.generation
                    && frozen.files.len() == snapshot.parts.len()
                    && snapshot
                        .parts
                        .iter()
                        .all(|p| frozen.files.contains_key(&p.id)),
                "managed snapshot parts mismatch"
            );
        } else {
            ensure!(
                frozen.files.len() == 1 && frozen.files.contains_key(&Uuid::nil()),
                "invalid managed Parquet anchor"
            );
        }
        for file in frozen.files.values() {
            self.objects.local_key(table, &file.location)?;
        }
        Ok(frozen)
    }

    pub(super) async fn upload(
        &self,
        table: Uuid,
        upload: Uuid,
        path: &Path,
        previous: Option<&RemoteSnapshot>,
        restored_generation: Option<Uuid>,
    ) -> Result<Option<Head>> {
        let snapshot = Snapshot::read(path)?;
        let generation = snapshot
            .as_ref()
            .map_or(restored_generation.unwrap_or(upload), |s| s.generation);
        if previous.is_some_and(|p| p.generation == generation) {
            return Ok(None);
        }
        let entries: Vec<(Uuid, PathBuf)> = match &snapshot {
            Some(snapshot) => snapshot
                .parts
                .iter()
                .map(|part| (part.id, part.path(path)))
                .collect(),
            None => vec![(Uuid::nil(), path.to_owned())],
        };
        let prefix = self.upload_prefix(table, upload);
        let mut files = BTreeMap::new();
        for (id, path) in entries {
            // Indexed sync reuses UUIDs only for immutable input parts.
            let old = previous.and_then(|old| old.files.get(&id));
            if id != Uuid::nil()
                && let Some(old) = old
            {
                files.insert(id, old.clone());
                continue;
            }
            let (size, sha256) = hash(&path).await?;
            if id == Uuid::nil() && old.is_some_and(|old| old.sha256 == sha256) {
                return Ok(None);
            }
            let key = prefix.clone().join(format!("{id}.parquet"));
            self.objects.put_file(&key, &path, size).await?;
            let meta = self.objects.object_meta(&key).await?;
            ensure!(meta.size == size, "managed upload length mismatch");
            let version = meta
                .version
                .or(meta.e_tag)
                .context("S3 object has no version or ETag")?;
            files.insert(
                id,
                RemoteFile {
                    location: self.objects.object_url(&key)?,
                    size,
                    version,
                    sha256,
                },
            );
        }
        let watermark = snapshot
            .as_ref()
            .and_then(|s| s.indexed.as_ref())
            .map(crate::admin::local_tables::key_index::checkpoint)
            .transpose()?
            .map(|value| {
                serde_json::json!({"type":value.data_type().to_string(),
                "value":(!value.is_null()).then(|| value.to_string())})
            });
        let frozen = RemoteSnapshot {
            generation,
            snapshot,
            files,
        };
        let bytes = serde_json::to_vec(&frozen)?;
        let head = Head {
            generation,
            upload,
            sha256: hex(Sha256::digest(&bytes)),
            watermark,
        };
        self.objects
            .put_immutable(&self.manifest_key(table, &head), bytes)
            .await?;
        self.snapshot(table, &head).await?;
        Ok(Some(head))
    }

    pub(super) async fn download(
        &self,
        table: Uuid,
        snapshot: &RemoteSnapshot,
        path: &Path,
    ) -> Result<()> {
        let directory = path.parent().context("missing managed table directory")?;
        tokio::fs::create_dir_all(directory).await?;
        if let Some(index) = &snapshot.snapshot {
            tokio::fs::create_dir_all(parquet_snapshot::parts_dir(path)).await?;
            tokio::fs::File::create(parquet_snapshot::reader_lock(path)).await?;
            for part in &index.parts {
                self.download_file(table, &snapshot.files[&part.id], &part.path(path))
                    .await?;
            }
            tokio::fs::write(path, index.encode()?).await?;
        } else {
            self.download_file(table, &snapshot.files[&Uuid::nil()], path)
                .await?;
        }
        Ok(())
    }

    async fn download_file(&self, table: Uuid, expected: &RemoteFile, path: &Path) -> Result<()> {
        let key = self.objects.local_key(table, &expected.location)?;
        let result = self.objects.get(&key).await?;
        ensure!(
            result.meta.size == expected.size
                && result.meta.version.as_ref().or(result.meta.e_tag.as_ref())
                    == Some(&expected.version),
            "managed snapshot file version changed"
        );
        let mut stream = result.into_stream();
        let mut out = tokio::fs::File::create(path).await?;
        let mut sum = Sha256::new();
        let mut size = 0;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            sum.update(&chunk);
            size += chunk.len() as u64;
            ensure!(
                size <= expected.size,
                "managed download exceeds expected length"
            );
            out.write_all(&chunk).await?;
        }
        ensure!(
            size == expected.size && hex(sum.finalize()) == expected.sha256,
            "managed snapshot file checksum mismatch"
        );
        out.sync_all().await?;
        Ok(())
    }
}
