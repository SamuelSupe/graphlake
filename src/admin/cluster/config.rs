use std::{
    path::{Path as FsPath, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::Deserialize;

use super::{Metadata, Objects, models};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    pub name: String,
    #[serde(deserialize_with = "metadata_endpoints")]
    metadata: Vec<url::Url>,
    storage: String,
    token_env: String,
    #[serde(default)]
    pub workers: Vec<url::Url>,
    #[serde(default)]
    pub distributed_partitions: usize,
    #[serde(default = "default_memory")]
    pub memory_mib: usize,
    #[serde(default = "default_process_memory")]
    pub process_memory_mib: usize,
    #[serde(default)]
    pub distributed_min_rows: usize,
    #[serde(default)]
    pub(super) managed_local_tables: bool,
    #[serde(default = "default_local_retention")]
    pub(super) local_snapshot_retention_seconds: u64,
    #[serde(default = "default_local_gc_grace")]
    pub(super) local_snapshot_gc_grace_seconds: u64,
    ca_file: Option<PathBuf>,
    identity_file: Option<PathBuf>,
}

#[derive(Clone)]
pub(super) struct Shared {
    pub client: reqwest::Client,
    pub metadata: Metadata,
    pub objects: Objects,
    pub token: String,
}

impl Config {
    fn read(path: &FsPath) -> Result<Self> {
        let text = models::read_file(path, 64 * 1024)
            .map_err(|_| anyhow::anyhow!("reading cluster configuration"))?
            .context("cluster configuration not found")?;
        let config: Self = serde_saphyr::from_str(&text)?;
        Ok(config)
    }

    pub fn ingress(path: &FsPath) -> Result<Metadata> {
        let config = Self::read(path)?;
        ensure!(models::valid_id(&config.name), "invalid cluster name");
        config.validate_endpoints()?;
        Ok(Metadata::new(
            config.client(path, true)?,
            config.metadata,
            &config.name,
        ))
    }

    pub fn load(path: &FsPath) -> Result<(Self, Shared)> {
        let config = Self::read(path)?;
        ensure!(models::valid_id(&config.name), "invalid cluster name");
        ensure!(
            config.distributed_partitions == 0
                || (2..=8).contains(&config.distributed_partitions)
                    && (1..=32).contains(&config.workers.len()),
            "distributed execution requires 2–8 partitions and 1–32 workers"
        );
        ensure!(
            (16..=2048).contains(&config.memory_mib)
                && (16..=2048).contains(&config.process_memory_mib)
                && config.memory_mib * (1 + usize::from(config.distributed_partitions > 0))
                    <= config.process_memory_mib,
            "cluster memory_mib must fit the 16–2048 MiB process_memory_mib budget, reserving one remote stage when distributed execution is enabled"
        );
        config.validate_endpoints()?;
        let token = std::env::var(&config.token_env).context("reading cluster token")?;
        ensure!(
            (32..=256).contains(&token.len()) && token.bytes().all(|b| b.is_ascii_graphic()),
            "cluster token must have 32–256 printable ASCII characters"
        );
        let client = config.client(path, false)?;
        let metadata = Metadata::new(
            if config.metadata.len() == 1 {
                client.clone()
            } else {
                config.client(path, true)?
            },
            config.metadata.clone(),
            &config.name,
        );
        let objects = Objects::open(&config.storage, &config.name)?;
        ensure!(
            !config.managed_local_tables || objects.remote(),
            "managed local tables require S3-compatible storage"
        );
        ensure!(
            (1..=2_592_000).contains(&config.local_snapshot_retention_seconds)
                && (1..=86_400).contains(&config.local_snapshot_gc_grace_seconds),
            "invalid local snapshot retention or GC grace"
        );
        Ok((
            config,
            Shared {
                client,
                metadata,
                objects,
                token,
            },
        ))
    }
    fn validate_endpoints(&self) -> Result<()> {
        ensure!(
            (1..=3).contains(&self.metadata.len()),
            "configure 1–3 etcd endpoints"
        );
        for url in self.metadata.iter().chain(&self.workers) {
            let loopback = match url.host() {
                Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
                Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
                _ => false,
            };
            ensure!(
                (url.scheme() == "https" || url.scheme() == "http" && loopback)
                    && url.host().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.path() == "/"
                    && url.query().is_none()
                    && url.fragment().is_none(),
                "cluster endpoints require HTTPS or a numeric loopback HTTP address"
            );
        }
        Ok(())
    }

    fn client(&self, path: &FsPath, metadata: bool) -> Result<reqwest::Client> {
        let mut client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_millis(
                if metadata && self.metadata.len() > 1 {
                    150
                } else {
                    600
                },
            ))
            .no_proxy();
        let relative = path
            .parent()
            .context("missing cluster configuration directory")?;
        if let Some(ca) = &self.ca_file {
            client = client.tls_certs_only([reqwest::Certificate::from_pem(&std::fs::read(
                relative.join(ca),
            )?)?]);
        }
        if let Some(identity) = &self.identity_file {
            client = client.identity(reqwest::Identity::from_pem(&std::fs::read(
                relative.join(identity),
            )?)?);
        }
        ensure!(
            self.workers.iter().all(|url| url.scheme() != "https")
                || self.identity_file.is_some() && self.ca_file.is_some(),
            "HTTPS worker endpoints require a cluster CA and client identity"
        );
        Ok(client.build()?)
    }
}

fn default_process_memory() -> usize {
    2048
}

fn metadata_endpoints<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<Vec<url::Url>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Endpoints {
        Single(url::Url),
        Multiple(Vec<url::Url>),
    }
    Ok(match Endpoints::deserialize(decoder)? {
        Endpoints::Single(endpoint) => vec![endpoint],
        Endpoints::Multiple(endpoints) => endpoints,
    })
}

fn default_memory() -> usize {
    256
}

fn default_local_retention() -> u64 {
    86_400
}
fn default_local_gc_grace() -> u64 {
    300
}
