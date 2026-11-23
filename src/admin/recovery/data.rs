use std::{
    path::{Component, Path},
    process::Stdio,
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use tokio::{
    fs::{self, File},
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    process::Command,
};
use tokio_postgres::{
    Config,
    config::{Host, SslMode},
};

use super::{Manifest, Postgres, TIMEOUT, fingerprint_postgres};
use crate::source::{ReadStats, postgres::Snapshot};

pub(super) async fn open_file(path: &Path) -> Result<File> {
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    let file = options.open(path).await?;
    ensure!(file.metadata().await?.is_file(), "不是普通文件");
    Ok(file)
}

async fn transfer(input: &mut File, mut output: Option<&mut File>) -> Result<String> {
    let mut hash = Sha256::new();
    let mut buffer = vec![0; 128 * 1024];
    loop {
        let size = input.read(&mut buffer).await?;
        if size == 0 {
            break;
        }
        let bytes = &buffer[..size];
        hash.update(bytes);
        if let Some(output) = output.as_deref_mut() {
            output.write_all(bytes).await?;
        }
    }
    Ok(hash
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

pub(super) async fn archive(dir: &Path, expected: &str) -> Result<File> {
    let mut file = open_file(&dir.join("postgres.dump"))
        .await
        .context("无法读取 PostgreSQL 备份文件")?;
    let mut magic = [0; 5];
    file.read_exact(&mut magic)
        .await
        .context("PostgreSQL 备份文件不完整")?;
    ensure!(&magic == b"PGDMP", "PostgreSQL 备份需要 pg_dump 自定义格式");
    file.rewind().await?;
    ensure!(
        transfer(&mut file, None)
            .await
            .context("无法读取 PostgreSQL 备份文件")?
            == expected,
        "PostgreSQL 备份摘要不匹配"
    );
    file.rewind().await?;
    // Let pg_restore consume the same open file whose bytes were checked.
    Ok(file)
}

fn parquet_path(path: &Path) -> bool {
    path.components().all(|part| {
        matches!(part, Component::Normal(name)
        if name.to_str().is_some_and(|name| !name.starts_with('.')))
    }) && path
        .extension()
        .and_then(|suffix| suffix.to_str())
        .is_some_and(|suffix| {
            suffix.eq_ignore_ascii_case("parquet") || suffix.eq_ignore_ascii_case("pq")
        })
}

async fn parquet_dir(root: &Path, relative: &Path, create: bool) -> Result<std::path::PathBuf> {
    let mut dir = root.to_owned();
    for part in relative.parent().unwrap_or(Path::new("")).components() {
        let parent = dir.clone();
        dir.push(part.as_os_str());
        if create {
            match fs::create_dir(&dir).await {
                Ok(()) => {
                    File::open(&parent).await?.sync_all().await?;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        ensure!(
            fs::symlink_metadata(&dir).await?.is_dir(),
            "目录必须是实际目录"
        );
    }
    Ok(dir)
}

pub(super) async fn parquet(bundle: &Path, target: &Path, manifest: &Manifest) -> Result<()> {
    for (name, expected) in &manifest.parquet {
        let path = Path::new(name);
        ensure!(parquet_path(path), "Parquet 恢复路径无效 · {name}");
        let from = parquet_dir(bundle, path, false)
            .await
            .with_context(|| format!("无法读取 Parquet 备份 · {name}"))?;
        let to = parquet_dir(target, path, true)
            .await
            .with_context(|| format!("无法打开 Parquet 目标目录 · {name}"))?;
        let name_part = path.file_name().expect("validated Parquet path");
        let destination = to.join(name_part);
        let mut input = open_file(&from.join(name_part))
            .await
            .with_context(|| format!("无法读取 Parquet 备份 · {name}"))?;
        if let Ok(mut existing) = open_file(&destination).await {
            ensure!(
                transfer(&mut existing, None)
                    .await
                    .with_context(|| format!("无法读取目标 Parquet · {name}"))?
                    == *expected,
                "目标已有不同的 Parquet 文件 · {name}"
            );
            ensure!(
                transfer(&mut input, None)
                    .await
                    .with_context(|| format!("无法读取 Parquet 备份 · {name}"))?
                    == *expected,
                "Parquet 备份摘要不匹配 · {name}"
            );
            continue;
        }
        let temp = tempfile::NamedTempFile::new_in(&to)
            .with_context(|| format!("无法写入 Parquet 目标目录 · {name}"))?;
        let mut output = File::from_std(temp.reopen()?);
        let actual = transfer(&mut input, Some(&mut output))
            .await
            .with_context(|| format!("复制 Parquet 失败 · {name}"))?;
        ensure!(actual == *expected, "Parquet 备份摘要不匹配 · {name}");
        output
            .sync_all()
            .await
            .with_context(|| format!("无法持久化 Parquet · {name}"))?;
        drop(output);
        temp.persist_noclobber(&destination)
            .with_context(|| format!("无法写入目标 Parquet · {name}"))?;
        File::open(&to)
            .await?
            .sync_all()
            .await
            .with_context(|| format!("无法持久化 Parquet 目标目录 · {name}"))?;
    }
    Ok(())
}

pub(super) struct Destination {
    config: Config,
}

impl Destination {
    pub(super) fn load(env: &str) -> Result<Self> {
        let config = std::env::var(env)
            .ok()
            .and_then(|dsn| dsn.parse::<Config>().ok())
            .context("配置的目标 PostgreSQL DSN 无效")?;
        ensure!(
            matches!(config.get_hosts(), [Host::Tcp(host)] if !host.is_empty() && !host.contains(','))
                && config.get_hostaddrs().is_empty()
                && config.get_ports().len() <= 1
                && config.get_dbname().is_some_and(|name| !name.is_empty())
                && config.get_user().is_some_and(|user| !user.is_empty()),
            "目标 PostgreSQL DSN 需要单一 TCP 主机、显式数据库及用户"
        );
        Ok(Self { config })
    }

    pub(super) async fn empty(env: &str) -> Result<bool> {
        let snap = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            Snapshot::open(env, TIMEOUT, Arc::new(ReadStats::default())),
        )
        .await
        .context("连接目标 PostgreSQL 超时")?
        .context("无法连接目标 PostgreSQL")?;
        snap.recovery_target_empty()
            .await
            .context("无法检查目标 PostgreSQL 是否为空")
    }

    pub(super) async fn verify(postgres: &Postgres) -> Result<()> {
        let relations: Vec<_> = postgres.relations.keys().cloned().collect();
        let hashes = fingerprint_postgres(&postgres.dsn_env, &relations)
            .await
            .context("无法读取已恢复的 PostgreSQL 表，请检查目标库与恢复权限")?;
        ensure!(
            hashes == postgres.relations,
            "PostgreSQL 已有数据与清单恢复点不匹配"
        );
        Ok(())
    }

    pub(super) async fn check_archive(archive: &mut File) -> Result<()> {
        let input = archive.try_clone().await?.into_std().await;
        let result = pg_restore_command()
            .arg("--list")
            .stdin(Stdio::from(input))
            .output()
            .await
            .context("无法启动 pg_restore，请安装兼容版本的 PostgreSQL 客户端")?;
        ensure!(
            result.status.success(),
            "pg_restore 无法读取归档，请检查归档完整性与 PostgreSQL 客户端版本"
        );
        archive.rewind().await?;
        Ok(())
    }

    pub(super) async fn restore(&self, archive: File) -> Result<()> {
        let config = &self.config;
        let [Host::Tcp(host)] = config.get_hosts() else {
            unreachable!("validated target host")
        };
        let database = config
            .get_dbname()
            .unwrap()
            .replace('\\', "\\\\")
            .replace('\'', "\\'");
        let mut command = pg_restore_command();
        // Explicitly set libpq's target and exclude inherited connection settings.
        command
            .env("PGHOST", host)
            .env(
                "PGPORT",
                config
                    .get_ports()
                    .first()
                    .copied()
                    .unwrap_or(5432)
                    .to_string(),
            )
            .env("PGUSER", config.get_user().unwrap())
            .env("PGCONNECT_TIMEOUT", "10")
            .args([
                "--single-transaction",
                "--exit-on-error",
                "--no-owner",
                "--no-acl",
                "--no-tablespaces",
                "--dbname",
            ])
            .arg(format!("dbname='{database}'"))
            .stdin(Stdio::from(archive.into_std().await));
        if let Some(password) = config.get_password() {
            command.env(
                "PGPASSWORD",
                std::str::from_utf8(password).context("目标 PostgreSQL 密码需要 UTF-8 编码")?,
            );
        }
        if config.get_ssl_mode() == SslMode::Disable {
            command.env("PGSSLMODE", "disable");
        } else {
            let ca = std::env::var_os("GRAPHLAKE_RESTORE_PG_CA_FILE")
                .map(std::path::PathBuf::from)
                .or_else(|| {
                    ["/etc/ssl/certs/ca-certificates.crt", "/etc/ssl/cert.pem"]
                        .into_iter()
                        .map(std::path::PathBuf::from)
                        .find(|file| file.is_file())
                })
                .context("缺少用于验证目标 PostgreSQL 的 CA 文件")?;
            command
                .env("PGSSLMODE", "verify-full")
                .env("PGSSLROOTCERT", ca);
        }
        let mut child = command
            .spawn()
            .context("无法启动 pg_restore，请安装兼容版本的 PostgreSQL 客户端")?;
        let mut stderr = child.stderr.take().unwrap();
        let (status, details) = tokio::try_join!(child.wait(), async move {
            let mut bytes = Vec::new();
            (&mut stderr).take(4096).read_to_end(&mut bytes).await?;
            tokio::io::copy(&mut stderr, &mut tokio::io::sink()).await?;
            Ok(bytes)
        })
        .context("读取 pg_restore 结果失败")?;
        if status.success() {
            return Ok(());
        }
        // PostgreSQL diagnostics can include SQL text and restored values.
        let details = String::from_utf8_lossy(&details);
        let message = if details.contains("unsupported version") {
            "pg_restore 版本与归档不兼容"
        } else if details.contains("permission denied") || details.contains("must be superuser") {
            "目标 PostgreSQL 用户权限不足"
        } else if details.contains("connection") || details.contains("authentication") {
            "pg_restore 无法连接目标 PostgreSQL，请检查连接、认证和 TLS"
        } else if details.contains("already exists") {
            "目标 PostgreSQL 已存在归档中的对象，请检查目标库"
        } else {
            "pg_restore 执行失败，事务已回滚，请检查归档、扩展和目标数据库版本"
        };
        bail!("{message}")
    }
}

fn pg_restore_command() -> Command {
    let binary =
        std::env::var_os("GRAPHLAKE_RESTORE_PG_RESTORE").unwrap_or_else(|| "pg_restore".into());
    let mut command = Command::new(binary);
    command
        .env_clear()
        .env("LC_ALL", "C")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true);
    if let Some(path) = std::env::var_os("PATH") {
        command.env("PATH", path);
    }
    command
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn restore_parquet_verify_before_publish_and_never_replace() -> Result<()> {
        let bundle = tempfile::tempdir()?;
        let target = tempfile::tempdir()?;
        let name = "tables/graph.parquet";
        fs::create_dir(bundle.path().join("tables")).await?;
        let source = bundle.path().join(name);
        let output = target.path().join(name);
        let manifest = serde_json::from_value(serde_json::json!({
            "format": 1, "control_sha256": "",
            "postgres": {"dump_sha256":"", "dsn_env":"PG", "relations":{}},
            "parquet": {(name): super::super::digest(b"archive")}, "canaries": [],
        }))?;
        fs::write(&source, b"corrupt").await?;
        assert!(
            parquet(bundle.path(), target.path(), &manifest)
                .await
                .is_err()
        );
        assert!(!output.exists());
        fs::write(&source, b"archive").await?;
        parquet(bundle.path(), target.path(), &manifest).await?;
        parquet(bundle.path(), target.path(), &manifest).await?;
        fs::write(&output, b"existing").await?;
        assert!(
            parquet(bundle.path(), target.path(), &manifest)
                .await
                .is_err()
        );
        assert_eq!(fs::read(&output).await?, b"existing");
        #[cfg(unix)]
        {
            fs::remove_file(&output).await?;
            std::os::unix::fs::symlink(&source, &output)?;
            assert!(
                parquet(bundle.path(), target.path(), &manifest)
                    .await
                    .is_err()
            );
            assert!(fs::symlink_metadata(&output).await?.is_symlink());
        }
        Ok(())
    }
}
