use std::{fmt, io, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt, stream::BoxStream};
use object_store::{
    Attributes, ClientConfigKey, ClientOptions, CopyOptions, Error, GetOptions, GetRange,
    GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result, UploadPart,
    aws::{AmazonS3Builder, AmazonS3ConfigKey as S3Key},
    client::{HttpClient, HttpConnector, ReqwestConnector},
    path::Path,
};
use opendal::{
    Buffer, BytesRange, ErrorKind, HttpTransporter, Metadata, Operator,
    options::{ListOptions, ReaderOptions, StatOptions, WriteOptions},
    raw::{OpDelete, OpWrite, Service, oio},
    services::S3,
};
use tokio::sync::Mutex;

#[cfg(test)]
mod tests;

pub(super) fn open(bucket: &str) -> anyhow::Result<(Arc<dyn ObjectStore>, Arc<dyn ObjectStore>)> {
    let config = AmazonS3Builder::from_env().with_bucket_name(bucket);
    let backend = match std::env::var("GRAPHLAKE_CLUSTER_S3_BACKEND") {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => "object_store".into(),
        Err(error) => return Err(error).context("reading GRAPHLAKE_CLUSTER_S3_BACKEND"),
    };
    let (store, arrow_store): (Arc<dyn ObjectStore>, Arc<dyn ObjectStore>) = match backend.as_str()
    {
        "object_store" => {
            let store = config.clone().build()?;
            let arrow = config
                .with_credentials(store.credentials().clone())
                .with_http_connector(ArrowDownloadConnector)
                .build()?;
            (Arc::new(store), Arc::new(arrow))
        }
        "opendal" => {
            let store = OpenDalS3::open(bucket, &config)?;
            let arrow = OpenDalS3::with_request_timeout(store.0.clone(), None)?;
            (Arc::new(store), Arc::new(arrow))
        }
        _ => anyhow::bail!("GRAPHLAKE_CLUSTER_S3_BACKEND must be object_store or opendal"),
    };
    crate::admin::telemetry::event(
        opentelemetry::logs::Severity::Info,
        "cluster_s3_backend",
        serde_json::json!({"backend": backend}),
    );
    Ok((store, arrow_store))
}

#[derive(Debug)]
struct ArrowDownloadConnector;

impl HttpConnector for ArrowDownloadConnector {
    fn connect(&self, options: &ClientOptions) -> object_store::Result<HttpClient> {
        // Arrow downloads use idle deadlines and can stay active under backpressure.
        ReqwestConnector::default().connect(&options.clone().with_timeout_disabled())
    }
}

#[derive(Debug)]
struct OpenDalS3(Operator);

impl OpenDalS3 {
    fn open(bucket: &str, config: &AmazonS3Builder) -> anyhow::Result<Self> {
        let get = |key| config.get_config_value(&key);
        let enabled = |key: S3Key| -> anyhow::Result<bool> {
            match get(key).as_deref().map(str::to_ascii_lowercase).as_deref() {
                Some("1" | "true" | "on" | "yes" | "y") => Ok(true),
                None | Some("0" | "false" | "off" | "no" | "n") => Ok(false),
                Some(_) => anyhow::bail!("invalid boolean for {}", key.as_ref()),
            }
        };
        let setting = |name: &str| name.parse().expect("known S3 configuration key");
        let region = get(S3Key::Region).unwrap_or_else(|| "us-east-1".into());
        let configured_endpoint = get(S3Key::S3Endpoint).or_else(|| get(S3Key::Endpoint));
        let default_endpoint = format!("https://s3.{region}.amazonaws.com");
        let endpoint = configured_endpoint.as_deref().unwrap_or(&default_endpoint);
        let virtual_host = enabled(S3Key::VirtualHostedStyleRequest)?;
        let url = url::Url::parse(&endpoint).context("invalid cluster S3 endpoint")?;
        ensure!(
            url.scheme() == "https"
                || (url.scheme() == "http" && enabled(S3Key::Client(ClientConfigKey::AllowHttp))?),
            "cluster S3 endpoint requires HTTPS unless AWS_ALLOW_HTTP=true"
        );
        ensure!(
            url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
                && url.query().is_none()
                && url.fragment().is_none(),
            "invalid cluster S3 endpoint"
        );
        if configured_endpoint.is_some() {
            let bucket_in_host = url
                .host_str()
                .is_some_and(|host| host.starts_with(&format!("{bucket}.")));
            ensure!(
                bucket_in_host == virtual_host,
                "OpenDAL custom virtual-hosted endpoints must start with the bucket name"
            );
        }
        for key in [
            S3Key::MetadataEndpoint,
            S3Key::StsEndpoint,
            setting("aws_sse_bucket_key_enabled"),
            setting("aws_sse_customer_key_base64"),
        ] {
            ensure!(
                get(key).is_none(),
                "OpenDAL cluster S3 does not support {}",
                key.as_ref()
            );
        }
        for key in [S3Key::ImdsV1Fallback, S3Key::S3Express] {
            ensure!(
                !enabled(key)?,
                "OpenDAL cluster S3 does not support {}",
                key.as_ref()
            );
        }
        ensure!(
            get(S3Key::ConditionalPut).as_deref().map(str::trim) == Some("etag"),
            "OpenDAL cluster S3 requires standard conditional writes (AWS_CONDITIONAL_PUT=etag)"
        );
        for (name, _) in std::env::vars_os() {
            if let Some(name) = name.to_str()
                && name.starts_with("AWS_")
                && let Ok(S3Key::Client(key)) = name.to_ascii_lowercase().parse()
                && key.as_ref() != "allow_http"
            {
                anyhow::bail!("OpenDAL cluster S3 does not support {name}");
            }
        }
        let mut s3 = S3::default()
            .bucket(bucket)
            .region(&region)
            .endpoint(endpoint);
        match (get(S3Key::AccessKeyId), get(S3Key::SecretAccessKey)) {
            (Some(id), Some(secret)) => s3 = s3.access_key_id(&id).secret_access_key(&secret),
            (None, None) => {}
            _ => anyhow::bail!("cluster S3 requires both AWS access key and secret key"),
        }
        if let Some(token) = get(S3Key::Token) {
            s3 = s3.session_token(&token);
        }
        if virtual_host {
            s3 = s3.enable_virtual_host_style();
        }
        if enabled(S3Key::SkipSignature)? {
            s3 = s3.skip_signature();
        }
        if enabled(S3Key::RequestPayer)? {
            s3 = s3.enable_request_payer();
        }
        if let Some(algorithm) = get(S3Key::Checksum) {
            s3 = s3.checksum_algorithm(&algorithm);
        }
        if let Some(algorithm) = get(setting("aws_server_side_encryption")) {
            s3 = s3.server_side_encryption(&algorithm);
        }
        if let Some(key) = get(setting("aws_sse_kms_key_id")) {
            s3 = s3.server_side_encryption_aws_kms_key_id(&key);
        }
        Self::with_request_timeout(Operator::new(s3)?, Some(Duration::from_secs(30)))
    }

    fn with_request_timeout(op: Operator, timeout: Option<Duration>) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .no_gzip();
        let client = match timeout {
            Some(timeout) => client.timeout(timeout),
            None => client,
        };
        let transport = opendal_http_transport_reqwest::ReqwestTransport::new(client.build()?);
        let ctx = op
            .context()
            .with_http_transport(HttpTransporter::new(transport));
        Ok(Self(op.with_context(ctx)))
    }

    fn writer(&self, key: &Path, if_not_exists: bool) -> Result<oio::Writer> {
        let options = WriteOptions {
            if_not_exists,
            concurrent: 1,
            ..Default::default()
        };
        let service = self.0.service();
        let (args, _) = OpWrite::from_options(&service.capability(), options)
            .map_err(|error| store_error(key, error, false))?;
        // ObjectStore paths are already S3 keys. Operator path normalization
        // would trim edge whitespace, and percent decoding would change literal keys.
        service
            .write(self.0.context(), key.as_ref(), args)
            .map_err(|error| store_error(key, error, false))
    }

    fn lister(&self, prefix: &Path, recursive: bool) -> Result<oio::Lister> {
        let directory = format!("{prefix}/");
        let options = ListOptions {
            recursive,
            ..Default::default()
        };
        self.0
            .service()
            .list(self.0.context(), &directory, options.into())
            .map_err(|error| store_error(prefix, error, false))
    }
}

impl fmt::Display for OpenDalS3 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("OpenDAL S3")
    }
}

#[async_trait]
impl ObjectStore for OpenDalS3 {
    async fn put_opts(
        &self,
        key: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        let create = match options.mode {
            PutMode::Create => true,
            PutMode::Overwrite => false,
            PutMode::Update(_) => return Err(unsupported("conditional update")),
        };
        let mut writer = self.writer(key, create)?;
        oio::Write::write(&mut writer, Buffer::from_iter(payload))
            .await
            .map_err(|error| store_error(key, error, create))?;
        let meta = oio::Write::close(&mut writer)
            .await
            .map_err(|error| store_error(key, error, create))?;
        Ok(put_result(&meta))
    }

    async fn put_multipart_opts(
        &self,
        key: &Path,
        _: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        Ok(Box::new(Upload {
            key: key.clone(),
            writer: Arc::new(Mutex::new(self.writer(key, false)?)),
        }))
    }

    async fn get_opts(&self, key: &Path, options: GetOptions) -> Result<GetResult> {
        let ctx = self.0.context();
        let service = self.0.service();
        let if_modified_since = options
            .if_modified_since
            .and_then(|date| date.to_rfc3339().parse().ok());
        let if_unmodified_since = options
            .if_unmodified_since
            .and_then(|date| date.to_rfc3339().parse().ok());
        let (metadata, payload) = if options.head {
            let stat = StatOptions {
                version: options.version,
                if_match: options.if_match,
                if_none_match: options.if_none_match,
                if_modified_since,
                if_unmodified_since,
                ..Default::default()
            };
            let meta = service
                .stat(ctx, key.as_ref(), stat.into())
                .await
                .map_err(|error| store_error(key, error, false))?
                .into_metadata();
            (
                meta,
                GetResultPayload::Stream(futures::stream::empty().boxed()),
            )
        } else {
            let range = match &options.range {
                Some(range) => {
                    range.is_valid().map_err(generic)?;
                    match range {
                        GetRange::Bounded(range) => range.clone().into(),
                        GetRange::Offset(offset) => BytesRange::new(*offset, None),
                        GetRange::Suffix(size) => BytesRange::suffix(*size),
                    }
                }
                None => BytesRange::default(),
            };
            let (args, _) = ReaderOptions {
                version: options.version,
                if_match: options.if_match,
                if_none_match: options.if_none_match,
                if_modified_since,
                if_unmodified_since,
                ..Default::default()
            }
            .into();
            let reader = service
                .read(ctx, key.as_ref(), args)
                .map_err(|error| store_error(key, error, false))?;
            let (reply, stream) = oio::Read::open(&reader, range)
                .await
                .map_err(|error| store_error(key, error, false))?;
            let meta = reply.into_metadata().ok_or_else(|| {
                generic(io::Error::other("S3 GET did not return object metadata"))
            })?;
            let key = key.clone();
            let stream =
                futures::stream::try_unfold((stream, key), |(mut stream, key)| async move {
                    let chunk = oio::ReadStream::read(&mut stream)
                        .await
                        .map_err(|error| store_error(&key, error, false))?;
                    Ok((!chunk.is_empty()).then(|| (chunk.to_bytes(), (stream, key))))
                })
                .boxed();
            (meta, GetResultPayload::Stream(stream))
        };
        let meta = object_meta(key.clone(), &metadata);
        let range = if options.head {
            0..0
        } else if let Some(range) = options.range {
            range.as_range(meta.size).map_err(generic)?
        } else {
            0..meta.size
        };
        Ok(GetResult {
            payload,
            meta,
            range,
            attributes: Attributes::default(),
        })
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let prefix = prefix.cloned().unwrap_or_default();
        let lister = self.lister(&prefix, true);
        async_stream::try_stream! {
            let mut lister = lister?;
            while let Some(entry) = oio::List::next(&mut lister)
                .await.map_err(|error| store_error(&prefix, error, false))?
            {
                let (path, meta) = entry.into_parts();
                if !meta.is_dir() {
                    yield object_meta(Path::parse(path)?, &meta);
                }
            }
        }
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let prefix = prefix.cloned().unwrap_or_default();
        let mut lister = self.lister(&prefix, false)?;
        let mut common_prefixes = Vec::new();
        let mut objects = Vec::new();
        while let Some(entry) = oio::List::next(&mut lister)
            .await
            .map_err(|error| store_error(&prefix, error, false))?
        {
            let (path, meta) = entry.into_parts();
            let key = Path::parse(path)?;
            if meta.is_dir() {
                common_prefixes.push(key);
            } else {
                objects.push(object_meta(key, &meta));
            }
        }
        Ok(ListResult {
            common_prefixes,
            objects,
        })
    }

    fn delete_stream(
        &self,
        keys: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let op = self.0.clone();
        keys.and_then(move |key| {
            let op = op.clone();
            async move {
                let mut deleter = op
                    .service()
                    .delete(op.context())
                    .map_err(|error| store_error(&key, error, false))?;
                oio::Delete::delete(&mut deleter, key.as_ref(), OpDelete::default())
                    .await
                    .map_err(|error| store_error(&key, error, false))?;
                oio::Delete::close(&mut deleter)
                    .await
                    .map_err(|error| store_error(&key, error, false))?;
                Ok(key)
            }
        })
        .boxed()
    }

    async fn copy_opts(&self, _: &Path, _: &Path, _: CopyOptions) -> Result<()> {
        Err(unsupported("copy"))
    }
}

struct Upload {
    key: Path,
    writer: Arc<Mutex<oio::Writer>>,
}

impl fmt::Debug for Upload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenDAL upload")
            .field("key", &self.key)
            .finish()
    }
}

#[async_trait]
impl MultipartUpload for Upload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let writer = Arc::clone(&self.writer);
        let key = self.key.clone();
        // Objects::put_file awaits each part before it feeds the next one.
        Box::pin(async move {
            oio::Write::write(&mut *writer.lock().await, Buffer::from_iter(data))
                .await
                .map_err(|error| store_error(&key, error, false))
        })
    }

    async fn complete(&mut self) -> Result<PutResult> {
        let meta = oio::Write::close(&mut *self.writer.lock().await)
            .await
            .map_err(|error| store_error(&self.key, error, false))?;
        Ok(put_result(&meta))
    }

    async fn abort(&mut self) -> Result<()> {
        oio::Write::abort(&mut *self.writer.lock().await)
            .await
            .map_err(|error| store_error(&self.key, error, false))
    }
}

fn object_meta(location: Path, meta: &Metadata) -> ObjectMeta {
    ObjectMeta {
        location,
        last_modified: meta
            .last_modified()
            .map(|time| chrono::DateTime::from(std::time::SystemTime::from(time)))
            .unwrap_or_default(),
        size: meta.content_length(),
        e_tag: meta.etag().map(str::to_owned),
        version: meta.version().map(str::to_owned),
    }
}

fn put_result(meta: &Metadata) -> PutResult {
    PutResult {
        e_tag: meta.etag().map(str::to_owned),
        version: meta.version().map(str::to_owned),
    }
}

fn store_error(key: &Path, error: opendal::Error, create: bool) -> Error {
    let kind = error.kind();
    let path = key.to_string();
    let source = Box::new(error);
    match kind {
        ErrorKind::NotFound => Error::NotFound { path, source },
        ErrorKind::PermissionDenied => Error::PermissionDenied { path, source },
        ErrorKind::AlreadyExists => Error::AlreadyExists { path, source },
        ErrorKind::ConditionNotMatch if create => Error::AlreadyExists { path, source },
        ErrorKind::ConditionNotMatch => Error::Precondition { path, source },
        ErrorKind::Unsupported => Error::NotSupported { source },
        // OpenDAL distinguishes 409 Conflict from a failed condition (412).
        // Propagate it so an incomplete create can never authorize a commit.
        _ => Error::Generic {
            store: "OpenDAL S3",
            source,
        },
    }
}

fn generic(source: impl std::error::Error + Send + Sync + 'static) -> Error {
    Error::Generic {
        store: "OpenDAL S3",
        source: Box::new(source),
    }
}

fn unsupported(operation: &str) -> Error {
    Error::NotImplemented {
        operation: operation.into(),
        implementer: "GraphLake OpenDAL S3".into(),
    }
}
