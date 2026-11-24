use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};

use anyhow::{Context, Result, ensure};
use object_store::{
    ObjectStore,
    aws::{AmazonS3Builder, AmazonS3ConfigKey},
    azure::MicrosoftAzureBuilder,
    gcp::{GoogleCloudStorageBuilder, GoogleConfigKey},
    path::Path,
};

use crate::StorageConfig;
use sha2::{Digest, Sha256};

type Connection = (
    [u8; 32],
    Option<tokio::runtime::Id>,
    Instant,
    Arc<dyn ObjectStore>,
);

fn cached(
    config: &[u8],
    include_env: impl Fn(&std::ffi::OsStr) -> bool,
    connect: impl FnOnce() -> Result<Arc<dyn ObjectStore>>,
) -> Result<Arc<dyn ObjectStore>> {
    static CONNECTIONS: OnceLock<Mutex<VecDeque<Connection>>> = OnceLock::new();
    let mut settings = std::env::vars_os()
        .filter(|(key, _)| include_env(key))
        .collect::<Vec<_>>();
    settings.sort_unstable();
    let mut fingerprint = Sha256::new();
    fingerprint.update(config.len().to_le_bytes());
    fingerprint.update(config);
    for (key, value) in settings {
        for part in [key.as_encoded_bytes(), value.as_encoded_bytes()] {
            fingerprint.update(part.len().to_le_bytes());
            fingerprint.update(part);
        }
    }
    let fingerprint: [u8; 32] = fingerprint.finalize().into();
    let runtime = tokio::runtime::Handle::try_current().ok().map(|h| h.id());
    {
        let mut connections = CONNECTIONS.get_or_init(Mutex::default).lock().unwrap();
        connections.retain(|(_, _, opened, _)| opened.elapsed() < Duration::from_secs(300));
        if let Some(index) = connections
            .iter()
            .position(|(key, rt, _, _)| key == &fingerprint && *rt == runtime)
        {
            let entry = connections.remove(index).expect("located S3 connection");
            let store = entry.3.clone();
            connections.push_back(entry);
            return Ok(store);
        }
    }
    let store = connect()?;
    let mut connections = CONNECTIONS.get_or_init(Mutex::default).lock().unwrap();
    connections.retain(|(_, _, opened, _)| opened.elapsed() < Duration::from_secs(300));
    if let Some(index) = connections
        .iter()
        .position(|(key, rt, _, _)| key == &fingerprint && *rt == runtime)
    {
        let entry = connections.remove(index).expect("located S3 connection");
        let existing = entry.3.clone();
        connections.push_back(entry);
        return Ok(existing);
    }
    if connections.len() == 32 {
        connections.pop_front();
    }
    connections.push_back((fingerprint, runtime, Instant::now(), store.clone()));
    Ok(store)
}

pub(super) fn s3_from_env(bucket: &str) -> Result<Arc<dyn ObjectStore>> {
    cached(
        bucket.as_bytes(),
        |key| key.as_encoded_bytes().starts_with(b"AWS_"),
        || {
            Ok(Arc::new(
                AmazonS3Builder::from_env()
                    .with_bucket_name(bucket)
                    .build()?,
            ))
        },
    )
}

pub(crate) fn validate(config: &StorageConfig) -> Result<()> {
    ensure!(
        matches!(
            config.provider.as_str(),
            "s3" | "minio" | "oss" | "tos" | "cos" | "azure" | "gcs"
        ),
        "unsupported storage provider"
    );
    ensure!(
        !config.bucket.trim().is_empty() && !config.bucket.contains(['/', '\\', ':']),
        "storage requires a bucket or container name"
    );
    if let Some(endpoint) = &config.endpoint {
        let url = crate::catalog::warehouse_endpoint(endpoint)?;
        ensure!(
            url.path() == "/" || url.path().is_empty(),
            "storage endpoint must be an origin without a path"
        );
    }
    ensure!(
        config.access_key_env.is_some() == config.secret_key_env.is_some(),
        "access_key_env and secret_key_env must be configured together"
    );
    if matches!(config.provider.as_str(), "minio" | "oss" | "tos" | "cos") {
        ensure!(
            config.endpoint.is_some(),
            "this storage provider requires an endpoint"
        );
    }
    if matches!(config.provider.as_str(), "oss" | "tos" | "cos") {
        ensure!(
            config.region.as_ref().is_some_and(|r| !r.trim().is_empty()),
            "this storage provider requires its region"
        );
    }
    if config.provider == "azure" {
        ensure!(
            config
                .account
                .as_ref()
                .is_some_and(|s| !s.trim().is_empty()),
            "Azure storage requires account"
        );
    }
    if config.provider == "gcs" {
        ensure!(
            config.endpoint.is_none(),
            "GCS uses its service-account or default storage endpoint"
        );
    }
    Ok(())
}

pub(crate) fn version_prefix(config: &StorageConfig) -> String {
    format!(
        "{}:{}:{}:{}/",
        config.provider,
        config.endpoint.as_deref().unwrap_or("default"),
        config.account.as_deref().unwrap_or("default"),
        config.bucket,
    )
}

pub(crate) fn object_path(value: &str) -> Result<Path> {
    ensure!(
        !value.trim().is_empty()
            && !value.contains("://")
            && !value.starts_with('/')
            && !value.split('/').any(|part| matches!(part, "." | "..")),
        "object paths must be relative keys inside the configured bucket"
    );
    Ok(Path::parse(value)?)
}

fn secret(name: &str) -> Result<String> {
    let value =
        std::env::var(name).with_context(|| format!("missing storage credential in {name}"))?;
    ensure!(!value.is_empty(), "empty storage credential in {name}");
    Ok(value)
}

pub(crate) fn open(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    validate(config)?;
    let credentials = [
        &config.access_key_env,
        &config.secret_key_env,
        &config.session_token_env,
        &config.account_key_env,
        &config.service_account_env,
    ];
    let file_credentials = if config.provider == "gcs" && config.service_account_env.is_none() {
        let builder = GoogleCloudStorageBuilder::from_env();
        match builder.get_config_value(&GoogleConfigKey::ServiceAccountKey) {
            Some(_) => None,
            None => {
                let file = builder
                    .get_config_value(&GoogleConfigKey::ServiceAccount)
                    .or_else(|| builder.get_config_value(&GoogleConfigKey::ApplicationCredentials))
                    .map(std::path::PathBuf::from)
                    .or_else(|| {
                        let (env, file) = if cfg!(windows) {
                            ("APPDATA", "gcloud/application_default_credentials.json")
                        } else {
                            (
                                "HOME",
                                ".config/gcloud/application_default_credentials.json",
                            )
                        };
                        std::env::var_os(env).map(|home| std::path::PathBuf::from(home).join(file))
                    });
                file.as_deref()
                    .map(credential_digest)
                    .transpose()?
                    .flatten()
            }
        }
    } else {
        None
    };
    let settings = serde_json::to_vec(&(config, file_credentials))?;
    cached(
        &settings,
        |key| {
            let key_bytes = key.as_encoded_bytes();
            [b"AWS_".as_slice(), b"AZURE_", b"GOOGLE_"]
                .iter()
                .any(|prefix| key_bytes.starts_with(prefix))
                || (config.provider == "gcs" && key == "SERVICE_ACCOUNT")
                || credentials
                    .iter()
                    .filter_map(|name| name.as_deref())
                    .any(|name| key == name)
        },
        || connect(config),
    )
}

fn credential_digest(path: &std::path::Path) -> Result<Option<[u8; 32]>> {
    // GCS loads a credential file once when building a client. Rotating at the
    // same path must change the key even if the old client has a live token.
    match crate::blocking(|| std::fs::read(path)) {
        Ok(contents) => Ok(Some(Sha256::digest(&contents).into())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn connect(config: &StorageConfig) -> Result<Arc<dyn ObjectStore>> {
    Ok(match config.provider.as_str() {
        "azure" => {
            let mut builder = if config.account_key_env.is_some() {
                MicrosoftAzureBuilder::new()
            } else {
                MicrosoftAzureBuilder::from_env()
            }
            .with_account(config.account.as_ref().unwrap())
            .with_container_name(&config.bucket);
            if let Some(name) = &config.account_key_env {
                builder = builder.with_access_key(secret(name)?);
            }
            if let Some(endpoint) = &config.endpoint {
                builder = builder
                    .with_endpoint(endpoint.clone())
                    .with_allow_http(endpoint.starts_with("http://"));
            }
            Arc::new(builder.build()?)
        }
        "gcs" => {
            let mut builder = if config.service_account_env.is_some() {
                GoogleCloudStorageBuilder::new()
            } else {
                GoogleCloudStorageBuilder::from_env()
            }
            .with_bucket_name(&config.bucket);
            if let Some(name) = &config.service_account_env {
                builder = builder.with_service_account_key(secret(name)?);
            }
            Arc::new(builder.build()?)
        }
        _ => {
            let mut builder = if config.access_key_env.is_some() {
                AmazonS3Builder::new()
            } else {
                AmazonS3Builder::from_env()
            }
            .with_bucket_name(&config.bucket);
            if let Some(region) = &config.region {
                builder = builder.with_region(region);
            } else {
                builder = builder.with_region(
                    std::env::var("AWS_REGION")
                        .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                        .unwrap_or_else(|_| "us-east-1".into()),
                );
            }
            if let Some(endpoint) = &config.endpoint {
                // These providers require bucket-prefixed endpoints for virtual hosting.
                let hosted = matches!(config.provider.as_str(), "oss" | "tos" | "cos");
                let mut url = url::Url::parse(endpoint)?;
                if hosted {
                    let host = url.host_str().context("missing storage endpoint host")?;
                    if !host.starts_with(&format!("{}.", config.bucket)) {
                        url.set_host(Some(&format!("{}.{host}", config.bucket)))?;
                    }
                }
                builder = builder
                    .with_config(
                        AmazonS3ConfigKey::S3Endpoint,
                        url.as_str().trim_end_matches('/'),
                    )
                    .with_virtual_hosted_style_request(hosted)
                    .with_allow_http(url.scheme() == "http");
            }
            if let Some(name) = &config.access_key_env {
                builder = builder.with_access_key_id(secret(name)?);
            }
            if let Some(name) = &config.secret_key_env {
                builder = builder.with_secret_access_key(secret(name)?);
            }
            if let Some(name) = &config.session_token_env {
                builder = builder.with_token(secret(name)?);
            }
            Arc::new(builder.build()?)
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn credential_file_rotation_replaces_the_cached_client() -> Result<()> {
        let file = tempfile::NamedTempFile::new()?;
        let open = || {
            let key =
                serde_json::to_vec(&("credential_file_fixture", credential_digest(file.path())?))?;
            cached(
                &key,
                |_| false,
                || Ok(Arc::new(object_store::memory::InMemory::new())),
            )
        };
        std::fs::write(file.path(), b"first")?;
        let old = open()?;
        assert!(Arc::ptr_eq(&old, &open()?));
        std::fs::write(file.path(), b"other")?;
        assert!(!Arc::ptr_eq(&old, &open()?));
        Ok(())
    }

    #[test]
    fn cached_clients_are_isolated_by_tokio_runtime_id() -> Result<()> {
        let key = b"runtime_isolation_fixture";
        let open = || {
            cached(
                key,
                |_| false,
                || Ok(Arc::new(object_store::memory::InMemory::new())),
            )
        };
        let rt1 = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let first = rt1.block_on(async { open() })?;
        let first_again = rt1.block_on(async { open() })?;
        assert!(Arc::ptr_eq(&first, &first_again));
        drop(rt1);

        let rt2 = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let second = rt2.block_on(async { open() })?;
        assert!(!Arc::ptr_eq(&first, &second));
        Ok(())
    }
}
