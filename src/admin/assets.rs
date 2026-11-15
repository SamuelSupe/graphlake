use std::{
    collections::HashMap,
    fs::{self, OpenOptions},
    io::{self, Read},
    path::Path,
};

use anyhow::{Context, Result, ensure};
use axum::{
    body::Bytes,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};

const ASSET_BUDGET: usize = 32 * 1024 * 1024;

pub(super) struct Assets {
    index: Bytes,
    files: HashMap<String, (&'static str, Bytes)>,
}

impl Assets {
    pub(super) fn load(root: &Path, catalog: &Path) -> Result<Self> {
        ensure!(
            !catalog.starts_with(root),
            "assets directory must not contain the catalog"
        );
        ensure!(
            !root.starts_with(catalog.join(".graphlake-auth")),
            "assets directory must be outside the credential directory"
        );
        let index =
            read(&root.join("index.html"), ASSET_BUDGET).context("opening assets index.html")?;
        let mut remaining = ASSET_BUDGET - index.len();
        let mut files = HashMap::new();
        let dir = root.join("assets");
        match fs::symlink_metadata(&dir) {
            Ok(meta) => {
                ensure!(
                    meta.is_dir(),
                    "assets subdirectory must be a real directory"
                );
                for entry in fs::read_dir(dir)? {
                    let entry = entry?;
                    let name = entry.file_name();
                    let Some(name) = name.to_str() else { continue };
                    if name.starts_with('.')
                        || !entry.file_type()?.is_file()
                        || !name
                            .bytes()
                            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
                    {
                        continue;
                    }
                    let mime = match name.rsplit_once('.').map(|(_, extension)| extension) {
                        Some("js") => "text/javascript; charset=utf-8",
                        Some("css") => "text/css; charset=utf-8",
                        Some("svg") => "image/svg+xml",
                        Some("png") => "image/png",
                        Some("woff2") => "font/woff2",
                        _ => continue,
                    };
                    let data = read(&entry.path(), remaining)?;
                    remaining -= data.len();
                    files.insert(name.to_owned(), (mime, data));
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        Ok(Self { index, files })
    }

    pub(super) fn page(&self, path: &str) -> Response {
        let parts = path
            .strip_suffix('/')
            .unwrap_or(path)
            .split('/')
            .collect::<Vec<_>>();
        let valid = match parts.as_slice() {
            [""]
            | [
                "",
                "new" | "catalogs" | "query" | "access" | "settings" | "recovery",
            ] => true,
            ["", "models", id] | ["", "models", id, "catalogs" | "query"] => {
                super::models::valid_id(id)
            }
            _ => false,
        };
        if !valid {
            return StatusCode::NOT_FOUND.into_response();
        }
        (
            [
                (header::CONTENT_TYPE, "text/html; charset=utf-8"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            self.index.clone(),
        )
            .into_response()
    }

    pub(super) fn file(&self, name: &str) -> Response {
        match self.files.get(name) {
            Some((mime, contents)) => {
                ([(header::CONTENT_TYPE, *mime)], contents.clone()).into_response()
            }
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }
}

fn read(path: &Path, limit: usize) -> Result<Bytes> {
    ensure!(
        fs::symlink_metadata(path)?.is_file(),
        "web asset must be a regular file"
    );
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let meta = file.metadata()?;
    ensure!(meta.is_file(), "web asset must be a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        ensure!(meta.nlink() == 1, "web asset must not be a hard link");
    }
    ensure!(meta.len() <= limit as u64, "web assets exceed 32 MiB");
    let mut data = Vec::new();
    file.take((limit + 1) as u64).read_to_end(&mut data)?;
    ensure!(data.len() <= limit, "web assets exceed 32 MiB");
    Ok(data.into())
}
