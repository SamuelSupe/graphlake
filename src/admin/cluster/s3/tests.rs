use anyhow::Result;
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, StatusCode},
    routing::{get, put},
};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use uuid::Uuid;

use super::*;

#[tokio::test]
async fn arrow_storage_response_can_outlast_the_regular_http_deadline() -> Result<()> {
    use axum::http::{Request, header::CONTENT_LENGTH};
    use object_store::client::{HttpErrorKind, HttpRequestBody};

    let router = Router::new().route(
        "/test/result.arrow",
        get(|| async {
            let body = Body::from_stream(futures::stream::once(async {
                tokio::time::sleep(Duration::from_millis(150)).await;
                Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"arrow result"))
            }));
            ([(CONTENT_LENGTH, "12")], body)
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let endpoint = format!("http://{}", listener.local_addr()?);
    let server = tokio::spawn(async move { axum::serve(listener, router).await });
    let request_timeout = Duration::from_millis(45);
    let test_timeout = Duration::from_secs(3);

    let options = ClientOptions::new()
        .with_allow_http(true)
        .with_timeout(request_timeout);
    let uri = format!("{endpoint}/test/result.arrow");
    let request = || Request::builder().uri(&uri).body(HttpRequestBody::empty());
    let response = ReqwestConnector::default()
        .connect(&options)?
        .execute(request()?)
        .await;
    match response {
        Ok(response) => assert!(response.into_body().bytes().await.is_err()),
        Err(error) => assert_eq!(error.kind(), HttpErrorKind::Timeout),
    }
    let arrow_client = ArrowDownloadConnector.connect(&options)?;
    let result = tokio::time::timeout(test_timeout, async {
        arrow_client
            .execute(request().unwrap())
            .await?
            .into_body()
            .bytes()
            .await
    })
    .await??;
    assert_eq!(result, "arrow result");

    let config = AmazonS3Builder::new()
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_access_key_id("test")
        .with_secret_access_key("test");
    let store = OpenDalS3::open("test", &config)?;
    let regular_store = OpenDalS3::with_request_timeout(store.0.clone(), Some(request_timeout))?;
    let arrow_store = OpenDalS3::with_request_timeout(store.0, None)?;
    let key = Path::from("result.arrow");
    let result = async { regular_store.get(&key).await?.bytes().await }.await;
    assert!(result.is_err());
    let result = tokio::time::timeout(test_timeout, async {
        arrow_store.get(&key).await?.bytes().await
    })
    .await??;
    server.abort();
    assert_eq!(result, "arrow result");
    Ok(())
}

#[tokio::test]
async fn only_failed_create_preconditions_are_reported_as_already_exists() -> Result<()> {
    async fn conditional_put(
        axum::extract::Path((_, name)): axum::extract::Path<(String, String)>,
        headers: HeaderMap,
    ) -> (StatusCode, &'static str) {
        if headers
            .get("if-none-match")
            .and_then(|value| value.to_str().ok())
            != Some("*")
        {
            return (StatusCode::BAD_REQUEST, "missing create condition");
        }
        match name.as_str() {
            "exists" => (
                StatusCode::PRECONDITION_FAILED,
                "<Error><Code>PreconditionFailed</Code></Error>",
            ),
            "conflict" => (
                StatusCode::CONFLICT,
                "<Error><Code>ConditionalRequestConflict</Code></Error>",
            ),
            _ => (StatusCode::CONFLICT, ""),
        }
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let config = AmazonS3Builder::new()
        .with_endpoint(format!("http://{}", listener.local_addr()?))
        .with_allow_http(true)
        .with_access_key_id("test")
        .with_secret_access_key("test");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new().route("/{bucket}/{key}", put(conditional_put)),
        )
        .await
    });
    let store = OpenDalS3::open("test", &config)?;
    for key in ["exists", "conflict", "empty"] {
        let error = store
            .put_opts(&Path::from(key), "candidate".into(), PutMode::Create.into())
            .await
            .unwrap_err();
        match (key, &error) {
            ("exists", Error::AlreadyExists { .. }) => {}
            ("conflict", Error::Generic { source, .. }) => {
                assert_eq!(
                    source.downcast_ref::<opendal::Error>().unwrap().kind(),
                    ErrorKind::Conflict
                );
            }
            ("empty", Error::Generic { .. }) => {}
            _ => panic!("{key}: {error}"),
        }
    }
    server.abort();
    Ok(())
}

#[tokio::test]
#[ignore = "requires a disposable versioned S3 bucket in GRAPHLAKE_TEST_VERSIONED_BUCKET"]
async fn s3_backends_share_versioned_objects_and_literal_keys() -> Result<()> {
    let bucket = std::env::var("GRAPHLAKE_TEST_VERSIONED_BUCKET")?;
    let config = AmazonS3Builder::from_env().with_bucket_name(&bucket);
    let legacy = config.clone().build()?;
    let opendal = OpenDalS3::open(&bucket, &config)?;
    let prefix = Path::parse(format!(" graphlake-s3-test/{}/snap%2F雪 ", Uuid::new_v4()))?;
    let key = Path::parse(format!("{prefix}/data +%25 "))?;
    let neighbor = Path::parse(format!("{prefix}-neighbor/data"))?;
    let old = legacy.put(&key, "legacy".into()).await?;
    let old_version = old
        .version
        .context("test bucket must have versioning enabled")?;
    let first = opendal.get(&key).await?;
    assert_eq!(first.meta.version.as_deref(), Some(old_version.as_str()));
    assert_eq!(first.bytes().await?, "legacy");
    assert_eq!(
        opendal.head(&key).await?.version.as_deref(),
        Some(old_version.as_str())
    );
    legacy.put(&neighbor, "other".into()).await?;

    let update = opendal.put(&key, "opendal".into()).await?;
    let current = legacy.get(&key).await?;
    assert!(current.meta.version.is_some());
    assert_ne!(current.meta.version.as_deref(), Some(old_version.as_str()));
    assert_eq!(current.meta.version, update.version);
    assert_eq!(current.bytes().await?, "opendal");
    assert_eq!(
        opendal
            .get_opts(
                &key,
                GetOptions {
                    version: Some(old_version.clone()),
                    if_match: old.e_tag.clone(),
                    range: Some(GetRange::Bounded(1..4)),
                    ..Default::default()
                }
            )
            .await?
            .bytes()
            .await?,
        "ega"
    );
    assert!(matches!(
        opendal
            .get_opts(
                &key,
                GetOptions {
                    if_match: old.e_tag,
                    ..Default::default()
                }
            )
            .await,
        Err(Error::Precondition { .. })
    ));

    let objects = opendal.list(Some(&prefix)).try_collect::<Vec<_>>().await?;
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0].location, key);
    let root = Path::parse(prefix.as_ref().rsplit_once('/').unwrap().0)?;
    assert!(
        opendal
            .list_with_delimiter(Some(&root))
            .await?
            .common_prefixes
            .contains(&prefix)
    );
    let other = Path::parse(format!("{prefix}/delete %2F second"))?;
    legacy.put(&other, "other literal key".into()).await?;
    let requested = [key.clone(), other.clone()];
    assert_eq!(
        opendal
            .delete_stream(futures::stream::iter(requested.clone().map(Ok)).boxed())
            .try_collect::<Vec<_>>()
            .await?,
        requested
    );
    assert!(matches!(
        legacy.head(&key).await,
        Err(Error::NotFound { .. })
    ));
    assert!(matches!(
        legacy.head(&other).await,
        Err(Error::NotFound { .. })
    ));
    assert_eq!(legacy.get(&neighbor).await?.bytes().await?, "other");
    legacy.delete(&neighbor).await?;

    let multipart = Path::parse(format!("{prefix}/multipart"))?;
    let mut upload = opendal.put_multipart(&multipart).await?;
    let part_size = 5 * 1024 * 1024;
    upload.put_part(vec![b'a'; part_size].into()).await?;
    upload.put_part("tail".into()).await?;
    let completed = upload.complete().await?;
    assert_eq!(legacy.head(&multipart).await?.version, completed.version);
    assert_eq!(
        legacy
            .get_range(&multipart, part_size as u64 - 2..part_size as u64 + 4)
            .await?,
        "aatail"
    );
    legacy.delete(&multipart).await?;
    Ok(())
}
