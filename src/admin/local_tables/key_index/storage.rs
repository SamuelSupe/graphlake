use std::{
    fs, io,
    path::{Path, PathBuf},
};

use anyhow::{Result, ensure};
use rusqlite::{Connection, Transaction};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::engine::QueryScope;

// The Arrow row encoding is internal to Arrow. Rebuild the cache when upgrading it.
const ENCODING: &str = "arrow-row-59-v1";

pub(super) fn generation(id: Uuid) -> String {
    format!("{ENCODING}:{id}")
}

pub(super) fn set_generation(tx: &Transaction, id: Uuid) -> Result<()> {
    tx.execute("DELETE FROM generation", [])?;
    tx.execute(
        "INSERT INTO generation (value) VALUES (?)",
        [generation(id)],
    )?;
    Ok(())
}

fn cache_path(anchor: &Path) -> Result<PathBuf> {
    // The index belongs to the active writer. Never put a SQLite WAL on the
    // shared catalog filesystem used by the HA coordinator.
    let dir = std::env::temp_dir().join(cache_name());
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    if let Err(error) = builder.create(&dir) {
        if error.kind() != io::ErrorKind::AlreadyExists {
            return Err(error.into());
        }
    }
    let meta = fs::symlink_metadata(&dir)?;
    ensure!(meta.is_dir(), "unsafe local key index directory");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(
            meta.uid() == unsafe { libc::geteuid() } && meta.mode() & 0o077 == 0,
            "unsafe local key index directory permissions"
        );
    }
    let anchor = std::path::absolute(anchor)?;
    use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
    let name = URL_SAFE_NO_PAD.encode(Sha256::digest(anchor.as_os_str().as_encoded_bytes()));
    Ok(dir.join(format!("{name}.sqlite")))
}

fn cache_name() -> String {
    #[cfg(unix)]
    {
        format!("graphlake-local-keys-{}", unsafe { libc::geteuid() })
    }
    #[cfg(not(unix))]
    {
        "graphlake-local-keys".into()
    }
}

pub(super) fn open(anchor: &Path, scope: &QueryScope) -> Result<Connection> {
    let path = cache_path(anchor)?;
    match open_database(&path, scope) {
        Err(error)
            if error
                .downcast_ref::<rusqlite::Error>()
                .and_then(|e| e.sqlite_error_code())
                .is_some_and(|code| {
                    matches!(
                        code,
                        rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase
                    )
                }) =>
        {
            for name in [
                path.clone(),
                path.with_extension("sqlite-wal"),
                path.with_extension("sqlite-shm"),
            ] {
                match fs::remove_file(name) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                    Err(error) => return Err(error.into()),
                }
            }
            open_database(&path, scope)
        }
        result => result,
    }
}

fn open_database(path: &Path, scope: &QueryScope) -> Result<Connection> {
    let connection = Connection::open(path)?;
    let cache_kib = (scope.options.memory_bytes / 8192).clamp(256, 8192);
    connection.pragma_update(None, "cache_size", -(cache_kib as i64))?;
    // UUID batches touch scattered B-tree leaves. Smaller pages bound how
    // much of each leaf goes through the WAL and back into the cache file.
    connection.execute_batch(
        "PRAGMA page_size=1024; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;
         CREATE TABLE IF NOT EXISTS generation (value TEXT NOT NULL);
         CREATE TABLE IF NOT EXISTS position (
           key BLOB PRIMARY KEY NOT NULL, part BLOB NOT NULL, row INTEGER NOT NULL
         ) WITHOUT ROWID;
         CREATE INDEX IF NOT EXISTS position_part ON position(part);",
    )?;
    Ok(connection)
}
