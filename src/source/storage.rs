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
    gcp::GoogleCloudStorageBuilder,
    path::Path,
};

use crate::StorageConfig;
use sha2::{Digest, Sha256};

type Connection = ([u8; 32], Instant, Arc<dyn ObjectStore>);

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
    let mut connections = CONNECTIONS.get_or_init(Mutex::default).lock().unwrap();
    connections.retain(|(_, opened, _)| opened.elapsed() < Duration::from_secs(300));
    if let Some(index) = connections
        .iter()
        .position(|(key, _, _)| key == &fingerprint)
    {
        let entry = connections.remove(index).expect("located S3 connection");
        let store = entry.2.clone();
        connections.push_back(entry);
        return Ok(store);
    }
    let store = connect()?;
    if connections.len() == 32 {
        connections.pop_front();
    }
    connections.push_back((fingerprint, Instant::now(), store.clone()));
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
    let settings = serde_json::to_vec(config)?;
    cached(
        &settings,
        |key| {
            let key_bytes = key.as_encoded_bytes();
            [b"AWS_".as_slice(), b"AZURE_", b"GOOGLE_"]
                .iter()
                .any(|prefix| key_bytes.starts_with(prefix))
                || credentials
                    .iter()
                    .filter_map(|name| name.as_deref())
                    .any(|name| key == name)
        },
        || connect(config),
    )
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
