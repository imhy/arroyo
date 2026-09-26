use arroyo_rpc::errors::StorageError;
use arroyo_rpc::retry;
use aws::ArroyoCredentialProvider;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use object_store::aws::{AmazonS3, AmazonS3ConfigKey};
use object_store::azure::{MicrosoftAzure, MicrosoftAzureBuilder};
use object_store::buffered::BufWriter;
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder};
use object_store::multipart::{MultipartStore, PartId};
use object_store::path::Path;
use object_store::{Error, ObjectMeta};
use object_store::{
    MultipartId, ObjectStore, PutMode, PutOptions, PutPayload, RetryConfig, aws::AmazonS3Builder,
    local::LocalFileSystem,
};
use regex::{Captures, Regex};
use std::borrow::Cow;
use std::fmt::{Debug, Formatter};
use std::future::ready;
use std::path::PathBuf;
use std::str::FromStr;
use std::{
    collections::HashMap,
    sync::{Arc, OnceLock},
};
use tracing::{debug, error};

mod aws;
mod endpoint;

/// A reference-counted reference to a [StorageProvider].
pub type StorageProviderRef = Arc<StorageProvider>;

/// Identifies the role / context a [`StorageProvider`] is being requested for.
///
/// Worker processes have a single effective checkpoint URL — the per-pipeline
/// `state_url` is injected via the `ARROYO__CHECKPOINT_URL` env var at worker
/// startup, which feeds `config().checkpoint_url`. Workers therefore use the
/// [`Worker`](StorageProviderFor::Worker) variant.
///
/// Controller processes (may) manage many pipelines simultaneously, each potentially
/// backed by a different storage URL. Controllers should use
/// [`Controller`](StorageProviderFor::Controller) and supply the pipeline's
/// `state_url`. When `storage_url` is `None`, the controller falls back to
/// `config().checkpoint_url`, matching the worker default.
#[derive(Debug, Clone)]
pub enum StorageProviderFor {
    /// Use the process-wide checkpoint URL from `config().checkpoint_url`.
    Worker,
    /// Use a per-pipeline `state_url`, falling back to `config().checkpoint_url`
    /// when `None`.
    Controller { storage_url: Option<String> },
}

/// The concrete `object_store` implementation behind a [`StorageProvider`], with its
/// type still intact.
///
/// [`StorageProvider::get_backing_store`] hands out an `Arc<dyn ObjectStore>`, and a
/// consumer that has to establish what the store can *physically* do — native ranged
/// GET, whole-object PUT that publishes atomically, multipart whose completion
/// publishes atomically — cannot recover that from an erased handle. Nor can it take
/// the store's word for it: a store supplies its own `Display`, so anything it says
/// about itself is forgeable. The concrete type is the one witness that is not, so
/// this enum carries it out of the constructor that built the store.
///
/// The `Arc` in each variant is the **same allocation** the provider serves its own
/// requests through. [`StorageProvider::get_backing_store`] is this handle cloned and
/// coerced, never a second store rebuilt at the same location, so the typed handle and
/// the erased one always describe one store — see
/// [`as_object_store`](Self::as_object_store).
///
/// # Why R2 has no variant of its own
///
/// [`BackendConfig::R2`] is served by an `AmazonS3`: `construct_r2` builds one with
/// `AmazonS3Builder` pointed at a Cloudflare endpoint. An R2 provider's backing store
/// therefore *is* an `AmazonS3`, and a separate variant would name a type that does
/// not exist. The one way R2 differs operationally — it rejects a multipart upload
/// whose non-final parts differ in size — is a property of the configured backend
/// rather than of the client type, and it is already published by
/// [`StorageProvider::requires_same_part_sizes`]. Spelling it here too would give that
/// fact a second source free to drift from the first, and would invite a consumer to
/// read capabilities off a variant name instead of off the type.
#[derive(Debug, Clone)]
pub enum BackingStoreHandle {
    /// Amazon S3 — and Cloudflare R2, which `construct_r2` also builds as an
    /// `AmazonS3`.
    AmazonS3(Arc<AmazonS3>),
    /// Google Cloud Storage.
    GoogleCloudStorage(Arc<GoogleCloudStorage>),
    /// Azure Blob Storage.
    MicrosoftAzure(Arc<MicrosoftAzure>),
    /// A local directory, rooted at the provider's configured path.
    LocalFileSystem(Arc<LocalFileSystem>),
}

impl BackingStoreHandle {
    /// The same store, erased: exactly the value
    /// [`StorageProvider::get_backing_store`] returns.
    ///
    /// "The same" is literal rather than equivalent — this clones the `Arc` and
    /// coerces it, so both views share one allocation and
    /// `Arc::ptr_eq(&handle.as_object_store(), &provider.get_backing_store())` holds
    /// for every provider.
    pub fn as_object_store(&self) -> Arc<dyn ObjectStore> {
        match self {
            Self::AmazonS3(store) => store.clone(),
            Self::GoogleCloudStorage(store) => store.clone(),
            Self::MicrosoftAzure(store) => store.clone(),
            Self::LocalFileSystem(store) => store.clone(),
        }
    }

    /// The same store as a [`MultipartStore`], for the implementations that provide
    /// one.
    ///
    /// `None` for [`LocalFileSystem`], which does not implement the trait — the same
    /// store [`StorageProvider::as_multipart`] already returns `None` for.
    fn as_multipart_store(&self) -> Option<Arc<dyn MultipartStore>> {
        match self {
            Self::AmazonS3(store) => Some(store.clone()),
            Self::GoogleCloudStorage(store) => Some(store.clone()),
            Self::MicrosoftAzure(store) => Some(store.clone()),
            Self::LocalFileSystem(_) => None,
        }
    }
}

#[derive(Clone)]
pub struct StorageProvider {
    config: BackendConfig,
    /// The backing store with its concrete type intact. `object_store` and
    /// `multipart_store` below are derived from this one value by
    /// [`StorageProvider::with_backing`], so all three are views of one allocation.
    backing: BackingStoreHandle,
    object_store: Arc<dyn ObjectStore>,
    multipart_store: Option<Arc<dyn MultipartStore>>,
    canonical_url: String,
    storage_options: HashMap<String, String>,
    /// The endpoint the backing client was built to talk to, read off its final builder
    /// (the `endpoint` module); `None` when this crate cannot tell. See
    /// [`StorageProvider::effective_endpoint`].
    effective_endpoint: Option<String>,
}

impl Debug for StorageProvider {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "StorageProvider<{}>", self.canonical_url)
    }
}

// https://s3.us-west-2.amazonaws.com/DOC-EXAMPLE-BUCKET1/puppy.jpg
const S3_PATH: &str =
    r"^https://s3\.(?P<region>[\w\-]+)\.amazonaws\.com/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
// https://DOC-EXAMPLE-BUCKET1.s3.us-west-2.amazonaws.com/puppy.png
const S3_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z0-9\-\.]+)\.s3\.(?P<region>[\w\-]+)\.amazonaws\.com(/(?P<key>.+))?$";
// S3://mybucket/puppy.jpg
const S3_URL: &str = r"^[sS]3[aA]?://(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
// unofficial, but convenient -- s3::https://my-endpoint.com:1234/mybucket/puppy.jpg
const S3_ENDPOINT_URL: &str = r"^[sS]3[aA]?::(?<protocol>https?)://(?P<endpoint>[^:/]+):(?<port>\d+)/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";

// Cloudflare R2
const R2_CONFIGURED_VIA_ENDPOINT: &str =
    r"^https://(?P<account_id>[a-zA-Z0-9]+)(\.(?P<jurisdiction>\w+))?\.[rR]2.cloudflarestorage.com";
const R2_URL: &str =
    r"^[rR]2://((?P<account_id>[a-zA-Z0-9]+)@)?(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const R2_ENDPOINT: &str = r"^https://(?P<account_id>[a-zA-Z0-9]+)(\.(?P<jurisdiction>\w+))?\.[rR]2.cloudflarestorage.com/(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";
const R2_VIRTUAL: &str = r"^https://(?P<bucket>[a-z0-9\-]+)\.(?P<account_id>[a-zA-Z0-9]+)(\.(?P<jurisdiction>\w+))?\.[rR]2.cloudflarestorage.com(/(?P<key>.+))?$";

// file:///my/path/directory
const FILE_URI: &str = r"^file://(?P<path>.*)$";
// file:/my/path/directory
const FILE_URL: &str = r"^file:(?P<path>.*)$";
// /my/path/directory
const FILE_PATH: &str = r"^/(?P<path>.*)$";

// https://BUCKET_NAME.storage.googleapis.com/OBJECT_NAME
const GCS_VIRTUAL: &str =
    r"^https://(?P<bucket>[a-z\d\-_\.]+)\.storage\.googleapis\.com(/(?P<key>.+))?$";
// https://storage.googleapis.com/BUCKET_NAME/OBJECT_NAME
const GCS_PATH: &str =
    r"^https://storage\.googleapis\.com/(?P<bucket>[a-z\d\-_\.]+)(/(?P<key>.+))?$";
const GCS_URL: &str = r"^[gG][sS]://(?P<bucket>[a-z0-9\-\.]+)(/(?P<key>.+))?$";

// abfs[s]://CONTAINER_NAME@STORAGE_ACCOUNT_NAME.dfs.core.windows.net/OBJECT_NAME
const ABFS_URL: &str = r"^abfss?://(?P<container>[a-z0-9\-]+)@(?P<account>[a-z0-9]+)\.dfs\.core\.windows\.net(/(?P<key>.+))?$";
// https://STORAGE_ACCOUNT_NAME.dfs.core.windows.net/CONTAINER_NAME/OBJECT_NAME
const AZURE_HTTPS: &str = r"^https://(?P<account>[a-z0-9]+)\.(blob|dfs)\.core\.windows\.net/(?P<container>[a-z0-9\-]+)(/(?P<key>.+))?$";

#[derive(Debug, Clone, Hash, PartialEq, Eq, Copy)]
enum Backend {
    S3,
    R2,
    #[allow(clippy::upper_case_acronyms)]
    GCS,
    Azure,
    Local,
}

fn matchers() -> &'static HashMap<Backend, Vec<Regex>> {
    static MATCHERS: OnceLock<HashMap<Backend, Vec<Regex>>> = OnceLock::new();
    MATCHERS.get_or_init(|| {
        let mut m = HashMap::new();

        m.insert(
            Backend::S3,
            vec![
                Regex::new(S3_PATH).unwrap(),
                Regex::new(S3_VIRTUAL).unwrap(),
                Regex::new(S3_ENDPOINT_URL).unwrap(),
                Regex::new(S3_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::R2,
            vec![
                Regex::new(R2_URL).unwrap(),
                Regex::new(R2_ENDPOINT).unwrap(),
                Regex::new(R2_VIRTUAL).unwrap(),
            ],
        );

        m.insert(
            Backend::GCS,
            vec![
                Regex::new(GCS_PATH).unwrap(),
                Regex::new(GCS_VIRTUAL).unwrap(),
                Regex::new(GCS_URL).unwrap(),
            ],
        );

        m.insert(
            Backend::Azure,
            vec![
                Regex::new(ABFS_URL).unwrap(),
                Regex::new(AZURE_HTTPS).unwrap(),
            ],
        );

        m.insert(
            Backend::Local,
            vec![
                Regex::new(FILE_URI).unwrap(),
                Regex::new(FILE_URL).unwrap(),
                Regex::new(FILE_PATH).unwrap(),
            ],
        );

        m
    })
}

fn should_retry(e: &object_store::Error) -> bool {
    match e {
        Error::Generic { source, .. } => {
            // some operations (like CompleteMultipartUpload)
            !source.to_string().contains("status 404 Not Found")
        }
        // 409s with "error code: 1018" are spurious upstream errors, not real conflicts.
        Error::AlreadyExists { source, .. } => source.to_string().contains("error code: 1018"),
        _ => false,
    }
}

macro_rules! storage_retry {
    ($e: expr) => {
        retry!(
            $e,
            10,
            Duration::from_millis(100),
            Duration::from_secs(10),
            |e| error!("Error: {}. Retrying...", e),
            should_retry
        )
    };
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct S3Config {
    endpoint: Option<String>,
    region: Option<String>,
    pub bucket: String,
    key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct R2Config {
    account_id: String,
    pub bucket: String,
    jurisdiction: Option<String>, // e.g., "eu"
    key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GCSConfig {
    pub bucket: String,
    key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AzureConfig {
    pub account: String,
    pub container: String,
    key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalConfig {
    pub path: String,
    pub key: Option<Path>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BackendConfig {
    S3(S3Config),
    R2(R2Config),
    GCS(GCSConfig),
    Azure(AzureConfig),
    Local(LocalConfig),
}

impl BackendConfig {
    pub fn parse_url(url: &str, with_key: bool) -> Result<Self, StorageError> {
        for (k, v) in matchers() {
            if let Some(matches) = v.iter().filter_map(|r| r.captures(url)).next() {
                return match k {
                    Backend::S3 => Self::parse_s3(matches),
                    Backend::R2 => Ok(Self::R2(Self::parse_r2(url, matches, None, None)?)),
                    Backend::GCS => Self::parse_gcs(matches),
                    Backend::Azure => Self::parse_azure(matches),
                    Backend::Local => Self::parse_local(matches, with_key),
                };
            }
        }

        Err(StorageError::InvalidUrl)
    }

    fn parse_s3(matches: Captures) -> Result<Self, StorageError> {
        // fill in env vars
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();
        let region = last([
            std::env::var("AWS_DEFAULT_REGION").ok(),
            matches.name("region").map(|m| m.as_str().to_string()),
        ]);

        let endpoint = last([
            std::env::var("AWS_ENDPOINT").ok(),
            matches
                .name("endpoint")
                .map(|endpoint| -> Result<String, StorageError> {
                    let port = if let Some(port) = matches.name("port") {
                        u16::from_str(port.as_str()).map_err(|_| {
                            StorageError::PathError(format!("invalid port: {}", port.as_str()))
                        })?
                    } else {
                        443
                    };

                    let protocol = if let Some(protocol) = matches.name("protocol") {
                        protocol.as_str().to_string()
                    } else {
                        "https".to_string()
                    };

                    Ok(format!("{}://{}:{}", protocol, endpoint.as_str(), port))
                })
                .transpose()?,
        ]);

        let key = matches.name("key").map(|m| m.as_str().into());

        Ok(BackendConfig::S3(S3Config {
            endpoint,
            region,
            bucket,
            key,
        }))
    }

    fn parse_r2(
        url: &str,
        matches: Captures,
        bucket: Option<String>,
        key: Option<Path>,
    ) -> Result<R2Config, StorageError> {
        let account_id = last([
            std::env::var("CLOUDFLARE_ACCOUNT_ID").ok(),
            matches
                .name("account_id")
                .map(|m| m.as_str().to_lowercase()),
        ])
        .ok_or_else(|| {
            StorageError::PathError(format!(
                "Could not determine Cloudflare Account ID from '{url}'; \
                    must be specified either as part of the URL or via the \
                    CLOUDFLARE_ACCOUNT_ID environment variable"
            ))
        })?;

        let jurisdiction = matches
            .name("jurisdiction")
            .map(|s| s.as_str().to_lowercase());

        let bucket = matches
            .name("bucket")
            .map(|s| s.as_str().to_string())
            .or(bucket)
            .expect("bucket should always be available");

        let key = matches.name("key").map(|m| m.as_str().into()).or(key);

        Ok(R2Config {
            account_id,
            bucket,
            jurisdiction,
            key,
        })
    }

    fn parse_gcs(matches: Captures) -> Result<Self, StorageError> {
        let bucket = matches
            .name("bucket")
            .expect("bucket should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().into());

        Ok(BackendConfig::GCS(GCSConfig { bucket, key }))
    }

    fn parse_azure(matches: Captures) -> Result<Self, StorageError> {
        let container = matches
            .name("container")
            .expect("container should always be available")
            .as_str()
            .to_string();

        let account = matches
            .name("account")
            .expect("account should always be available")
            .as_str()
            .to_string();

        let key = matches.name("key").map(|r| r.as_str().into());

        Ok(BackendConfig::Azure(AzureConfig {
            account,
            container,
            key,
        }))
    }

    fn parse_local(matches: Captures, with_key: bool) -> Result<Self, StorageError> {
        let path = matches
            .name("path")
            .expect("path regex must contain a path group")
            .as_str();

        let mut path = if !path.starts_with('/') {
            PathBuf::from(format!("/{path}"))
        } else {
            PathBuf::from(path)
        };

        let key = if with_key {
            let key = path
                .file_name()
                .map(|k| k.to_str().unwrap().to_string().into());
            path.pop();
            key
        } else {
            None
        };

        Ok(BackendConfig::Local(LocalConfig {
            path: path.to_str().unwrap().to_string(),
            key,
        }))
    }

    fn key(&self) -> Option<&Path> {
        match self {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::R2(r2) => r2.key.as_ref(),
            BackendConfig::GCS(gcs) => gcs.key.as_ref(),
            BackendConfig::Azure(azure) => azure.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
    }

    pub fn is_local(&self) -> bool {
        matches!(self, BackendConfig::Local { .. })
    }
}

fn last<I: Sized, const COUNT: usize>(opts: [Option<I>; COUNT]) -> Option<I> {
    opts.into_iter().flatten().last()
}

impl StorageProvider {
    /// Assemble a provider around the store that has just been built.
    ///
    /// The **only** place a `StorageProvider` value is constructed, which is what
    /// makes "the typed handle and the erased handle are one allocation" a property
    /// of the type rather than of each constructor remembering to clone instead of
    /// rebuild. Every `construct_*` below hands its concrete client here, and the
    /// erased `object_store` and `multipart_store` views are derived from it.
    fn with_backing(
        config: BackendConfig,
        backing: BackingStoreHandle,
        canonical_url: String,
        storage_options: HashMap<String, String>,
        effective_endpoint: Option<String>,
    ) -> Self {
        Self {
            config,
            object_store: backing.as_object_store(),
            multipart_store: backing.as_multipart_store(),
            backing,
            canonical_url,
            storage_options,
            effective_endpoint,
        }
    }

    pub async fn for_url(url: &str) -> Result<Self, StorageError> {
        Self::for_url_with_options(url, HashMap::new()).await
    }

    pub async fn for_url_with_options(
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let config: BackendConfig = BackendConfig::parse_url(url, false)?;

        match config {
            BackendConfig::R2(config) => Self::construct_r2(config, options).await,
            BackendConfig::S3(config) => Self::construct_s3(config, options).await,
            BackendConfig::GCS(config) => Self::construct_gcs(config).await,
            BackendConfig::Azure(config) => Self::construct_azure(config).await,
            BackendConfig::Local(config) => Self::construct_local(config).await,
        }
    }

    pub async fn get_url(url: &str) -> Result<Bytes, StorageError> {
        Self::get_url_with_options(url, HashMap::new()).await
    }

    pub async fn get_url_with_options(
        url: &str,
        options: HashMap<String, String>,
    ) -> Result<Bytes, StorageError> {
        let config: BackendConfig = BackendConfig::parse_url(url, true)?;

        let provider = match config {
            BackendConfig::S3(config) => Self::construct_s3(config, options).await,
            BackendConfig::R2(config) => Self::construct_r2(config, options).await,
            BackendConfig::GCS(config) => Self::construct_gcs(config).await,
            BackendConfig::Azure(config) => Self::construct_azure(config).await,
            BackendConfig::Local(config) => Self::construct_local(config).await,
        }?;

        provider.get("").await
    }

    pub fn get_key(url: &str) -> Result<Path, StorageError> {
        let config = BackendConfig::parse_url(url, true)?;
        let key = match &config {
            BackendConfig::S3(s3) => s3.key.as_ref(),
            BackendConfig::R2(r2) => r2.key.as_ref(),
            BackendConfig::GCS(gcs) => gcs.key.as_ref(),
            BackendConfig::Azure(azure) => azure.key.as_ref(),
            BackendConfig::Local(local) => local.key.as_ref(),
        }
        .ok_or_else(|| StorageError::NoKeyInUrl)?;
        Ok(key.to_owned())
    }

    async fn construct_s3(
        mut config: S3Config,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);
        let mut aws_key_manually_set = false;
        let mut s3_options = HashMap::new();

        for (key, value) in options.clone() {
            let s3_config_key = key.parse().map_err(|_| {
                StorageError::CredentialsError(format!("invalid S3 config key: {key}"))
            })?;
            if AmazonS3ConfigKey::AccessKeyId == s3_config_key {
                aws_key_manually_set = true;
            }
            s3_options.insert(s3_config_key, value.clone());
            builder = builder.with_config(s3_config_key, value);
        }

        // disable retries; we do our own
        let retry_config = RetryConfig {
            max_retries: 0,
            ..Default::default()
        };

        builder = builder.with_retry(retry_config);

        let endpoint = config
            .endpoint
            .as_ref()
            .or(s3_options.get(&AmazonS3ConfigKey::Endpoint));

        // is this actually r2 configured via AWS_ENDPOINT_URL?
        let r2_endpoint_regex = Regex::new(R2_CONFIGURED_VIA_ENDPOINT).unwrap();
        if let Some((url, Some(captures))) = endpoint.map(|e| (e, r2_endpoint_regex.captures(e))) {
            let config = BackendConfig::parse_r2(url, captures, Some(config.bucket), config.key)?;

            return Self::construct_r2(config, options).await;
        }

        if !aws_key_manually_set {
            let credentials: Arc<ArroyoCredentialProvider> =
                Arc::new(ArroyoCredentialProvider::try_new().await?);
            builder = builder.with_credentials(credentials);
        }

        let default_region = ArroyoCredentialProvider::default_region().await;
        config.region = config.region.or(default_region);
        if let Some(region) = &config.region {
            builder = builder.with_region(region);
            s3_options.insert(AmazonS3ConfigKey::Region, region.clone());
        }

        if let Some(endpoint) = config
            .endpoint
            .as_ref()
            .or(s3_options.get(&AmazonS3ConfigKey::Endpoint))
        {
            builder = builder
                .with_endpoint(endpoint)
                .with_virtual_hosted_style_request(false)
                .with_allow_http(true);
            s3_options.insert(AmazonS3ConfigKey::Endpoint, endpoint.clone());
            s3_options.insert(
                AmazonS3ConfigKey::VirtualHostedStyleRequest,
                "false".to_string(),
            );
            s3_options.insert(
                AmazonS3ConfigKey::Client(object_store::ClientConfigKey::AllowHttp),
                "true".to_string(),
            );
        }

        let mut canonical_url = match (&config.region, &config.endpoint) {
            (_, Some(endpoint)) => {
                format!("s3::{}/{}", endpoint, config.bucket)
            }
            (Some(region), _) => {
                format!("https://s3.{}.amazonaws.com/{}", region, config.bucket)
            }
            _ => {
                format!("https://s3.amazonaws.com/{}", config.bucket)
            }
        };
        if let Some(key) = &config.key {
            canonical_url = format!("{canonical_url}/{key}");
        }

        let effective_endpoint = endpoint::s3(&builder);
        Ok(Self::with_backing(
            BackendConfig::S3(config),
            BackingStoreHandle::AmazonS3(Arc::new(builder.build()?)),
            canonical_url,
            s3_options
                .into_iter()
                .map(|(k, v)| (k.as_ref().to_string(), v))
                .collect(),
            effective_endpoint,
        ))
    }

    async fn construct_r2(
        config: R2Config,
        options: HashMap<String, String>,
    ) -> Result<Self, StorageError> {
        let mut builder = AmazonS3Builder::from_env().with_bucket_name(&config.bucket);

        builder = builder.with_access_key_id(
            last([
                std::env::var("AWS_ACCESS_KEY_ID").ok(),
                std::env::var("R2_ACCESS_KEY_ID").ok(),
                options.get("aws_access_key_id").cloned(),
                options.get("r2_access_key_id").cloned(),
            ])
            .ok_or_else(|| {
                StorageError::CredentialsError(
                    "access_key_id not provided for R2 storage backend".to_string(),
                )
            })?,
        );

        builder = builder.with_secret_access_key(
            last([
                std::env::var("AWS_SECRET_ACCESS_KEY").ok(),
                std::env::var("R2_SECRET_ACCESS_KEY").ok(),
                options.get("aws_secret_access_key").cloned(),
                options.get("r2_secret_access_key").cloned(),
            ])
            .ok_or_else(|| {
                StorageError::CredentialsError(
                    "secret_access_key not provided for R2 storage backend".to_string(),
                )
            })?,
        );

        // disable retries; we do our own
        let retry_config = RetryConfig {
            max_retries: 0,
            ..Default::default()
        };

        builder = builder.with_retry(retry_config);

        let mut endpoint = "https://".to_string();
        endpoint.push_str(&config.account_id);
        if let Some(jurisdiction) = config.jurisdiction.as_ref() {
            endpoint.push('.');
            endpoint.push_str(jurisdiction);
        }
        endpoint.push_str(".r2.cloudflarestorage.com");

        let mut canonical_url = format!("{endpoint}/{}", config.bucket);
        if let Some(key) = &config.key {
            canonical_url.push('/');
            canonical_url.push_str(key.as_ref());
        }

        builder = builder
            .with_endpoint(endpoint)
            .with_virtual_hosted_style_request(false);

        let effective_endpoint = endpoint::s3(&builder);
        Ok(Self::with_backing(
            BackendConfig::R2(config),
            // R2 is served by an `AmazonS3` against a Cloudflare endpoint, so the
            // handle names the client type that was actually built.
            BackingStoreHandle::AmazonS3(Arc::new(builder.build()?)),
            canonical_url,
            HashMap::new(),
            effective_endpoint,
        ))
    }

    async fn construct_gcs(config: GCSConfig) -> Result<Self, StorageError> {
        let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(&config.bucket);

        // disable retries; we do our own
        let retry_config = RetryConfig {
            max_retries: 0,
            ..Default::default()
        };

        builder = builder.with_retry(retry_config);

        if let Ok(service_account_key) = std::env::var("GOOGLE_SERVICE_ACCOUNT_KEY") {
            debug!("Constructing GCS builder with service account key");
            builder = builder.with_service_account_key(&service_account_key);
        }

        let mut canonical_url = format!("https://{}.storage.googleapis.com", config.bucket);
        if let Some(key) = &config.key {
            canonical_url = format!("{canonical_url}/{key}");
        }

        let effective_endpoint = endpoint::gcs(&builder);
        Ok(Self::with_backing(
            BackendConfig::GCS(config),
            BackingStoreHandle::GoogleCloudStorage(Arc::new(builder.build()?)),
            canonical_url,
            HashMap::new(),
            effective_endpoint,
        ))
    }

    async fn construct_azure(config: AzureConfig) -> Result<Self, StorageError> {
        let builder: MicrosoftAzureBuilder =
            MicrosoftAzureBuilder::from_env().with_container_name(&config.container);

        let canonical_url = format!(
            "https://{}.blob.core.windows.net/{}",
            config.account, config.container
        );

        let effective_endpoint = endpoint::azure(&builder);
        Ok(Self::with_backing(
            BackendConfig::Azure(config),
            BackingStoreHandle::MicrosoftAzure(Arc::new(builder.build()?)),
            canonical_url,
            HashMap::new(),
            effective_endpoint,
        ))
    }

    async fn construct_local(config: LocalConfig) -> Result<Self, StorageError> {
        tokio::fs::create_dir_all(&config.path).await.map_err(|e| {
            StorageError::PathError(format!(
                "failed to create directory {}: {:?}",
                config.path, e
            ))
        })?;

        let backing = BackingStoreHandle::LocalFileSystem(Arc::new(
            LocalFileSystem::new_with_prefix(&config.path).map_err(Into::<StorageError>::into)?,
        ));

        let canonical_url = format!("file://{}", config.path);
        Ok(Self::with_backing(
            BackendConfig::Local(config),
            backing,
            canonical_url,
            HashMap::new(),
            // A local directory has no network endpoint to vouch for.
            None,
        ))
    }

    pub fn requires_same_part_sizes(&self) -> bool {
        matches!(self.config, BackendConfig::R2(_))
    }

    pub async fn list(
        &self,
        include_subdirectories: bool,
    ) -> Result<impl Stream<Item = Result<Path, object_store::Error>> + '_, StorageError> {
        let key_path: Option<Path> = self.config.key().map(|key| key.to_string().into());
        let key_part_count = key_path
            .as_ref()
            .map(|key| key.parts().count())
            .unwrap_or_default();
        let list = self
            .object_store
            .list(key_path.as_ref())
            .filter_map(move |meta| {
                let result = {
                    match meta {
                        Ok(metadata) => {
                            let path = metadata.location;
                            if !include_subdirectories && path.parts().count() != key_part_count + 1
                            {
                                None
                            } else {
                                Some(Ok(path))
                            }
                        }
                        Err(err) => Some(Err(err)),
                    }
                };
                ready(result)
            });

        Ok(list)
    }

    pub async fn get(&self, path: impl Into<Path>) -> Result<Bytes, StorageError> {
        let path = path.into();
        let bytes = self
            .object_store
            .get(&self.qualify_path(&path))
            .await
            .map_err(Into::<StorageError>::into)?
            .bytes()
            .await?;

        Ok(bytes)
    }

    pub async fn get_if_present(
        &self,
        path: impl Into<Path>,
    ) -> Result<Option<Bytes>, StorageError> {
        let path: Path = path.into();
        match self.object_store.get(&self.qualify_path(&path)).await {
            Ok(obj) => {
                let bytes = obj.bytes().await?;
                Ok(Some(bytes))
            }
            Err(err) => {
                if let object_store::Error::NotFound { .. } = &err {
                    return Ok(None);
                }
                Err(err.into())
            }
        }
    }

    pub async fn exists<P: Into<Path>>(&self, path: P) -> Result<bool, StorageError> {
        let path: Path = path.into();
        let exists = self.object_store.head(&self.qualify_path(&path)).await;

        match exists {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    pub async fn get_as_stream(
        &self,
        path: impl Into<Path>,
    ) -> Result<impl tokio::io::AsyncRead, StorageError> {
        let path = path.into();
        let path = self.qualify_path(&path);
        let bytes = storage_retry!(self.object_store.get(&path).await)
            .map_err(Into::<StorageError>::into)?
            .into_stream();

        Ok(tokio_util::io::StreamReader::new(bytes))
    }

    pub async fn put(&self, path: impl Into<Path>, bytes: Vec<u8>) -> Result<(), StorageError> {
        let bytes = PutPayload::from(Bytes::from(bytes));
        let path = path.into();
        let path = self.qualify_path(&path);
        storage_retry!(self.object_store.put(&path, bytes.clone()).await)?;

        Ok(())
    }

    pub async fn put_bytes(&self, path: &Path, bytes: Bytes) -> Result<(), StorageError> {
        let bytes = PutPayload::from(bytes);
        let path = self.qualify_path(path);
        storage_retry!(self.object_store.put(&path, bytes.clone()).await)?;

        Ok(())
    }

    /// Atomically creates a new object at `path`, failing with
    /// [`StorageError::AlreadyExists`] if the object already exists.
    ///
    /// This uses the underlying object store's conditional-create semantics
    /// (S3 `If-None-Match`, GCS `ifGenerationMatch=0`, etc.) and therefore
    /// provides a true race-free check-and-put.
    pub async fn put_if_not_exists(
        &self,
        path: impl Into<Path>,
        bytes: Vec<u8>,
    ) -> Result<(), StorageError> {
        let payload = PutPayload::from(Bytes::from(bytes));
        let path = path.into();
        let qualified = self.qualify_path(&path);
        let opts = PutOptions {
            mode: PutMode::Create,
            ..Default::default()
        };

        match storage_retry!(
            self.object_store
                .put_opts(&qualified, payload.clone(), opts.clone())
                .await
        ) {
            Ok(_) => Ok(Ok(())),
            Err(object_store::Error::AlreadyExists { path, .. }) => {
                Ok(Err(StorageError::AlreadyExists { path }))
            }
            Err(e) => Err(StorageError::from(e)),
        }?
    }

    /// The immediate child directories of `prefix`, as a store-side delimiter listing.
    ///
    /// A delimiter listing returns the common prefixes under `prefix` without enumerating what
    /// is inside them, which is what makes "which generations exist" cheap on a job whose
    /// generations hold thousands of checkpoint objects.
    pub async fn list_directories(
        &self,
        prefix: impl Into<Path>,
    ) -> Result<Vec<Path>, StorageError> {
        let prefix: Path = prefix.into();
        let qualified = self.qualify_path(&prefix).into_owned();
        let listing = self
            .object_store
            .list_with_delimiter(Some(&qualified))
            .await?;
        Ok(listing.common_prefixes)
    }

    pub fn qualify_path<'a>(&self, path: &'a Path) -> Cow<'a, Path> {
        match self.config.key() {
            Some(prefix) => Cow::Owned(prefix.parts().chain(path.parts()).collect()),
            None => Cow::Borrowed(path),
        }
    }

    /// The key prefix this provider namespaces every object under, owned.
    ///
    /// **The** supported way to hand the prefix to a consumer that will talk to the
    /// backing store directly — see [`get_backing_store`](Self::get_backing_store),
    /// whose caller owes exactly this. It is *defined* as
    /// `qualify_path(&Path::default())` rather than re-derived, so it cannot disagree
    /// with the qualification the provider's own operations apply: the `Some` arm of
    /// [`qualify_path`](Self::qualify_path) returns the configured key by
    /// construction, and its `None` arm returns the empty path, which is the
    /// "no prefix configured" case.
    ///
    /// [`get_key`](Self::get_key) is **not** an alternative spelling of this. It
    /// re-parses a URL with `with_key = true`, which for a `file://` URL pops the
    /// last path segment off as a key while [`for_url`](Self::for_url) keeps that
    /// segment in the filesystem root — so the two disagree for local providers — and
    /// it fails outright for a URL with no key, where the answer here is the empty
    /// path.
    pub fn configured_prefix(&self) -> Path {
        self.qualify_path(&Path::default()).into_owned()
    }

    pub async fn delete_if_present(&self, path: impl Into<Path>) -> Result<(), StorageError> {
        let path = path.into();
        let path = self.qualify_path(&path);
        match self.object_store.delete(&path).await {
            Ok(_) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Deletes an empty directory, only on local backends
    pub async fn delete_directory(&self, path: &str) -> Result<(), StorageError> {
        // only applies to local -- object stores don't have directories
        let BackendConfig::Local(config) = &self.config else {
            return Ok(());
        };

        let path = format!("{}/{}", config.path, path);
        tokio::fs::remove_dir(&path).await.map_err(|e| {
            StorageError::ObjectStore(Error::Generic {
                store: "local",
                source: e.into(),
            })
        })?;

        Ok(())
    }

    pub fn as_multipart(&self) -> Option<Arc<dyn MultipartStore>> {
        self.multipart_store.clone()
    }

    fn get_multipart(&self) -> &Arc<dyn MultipartStore> {
        self.multipart_store
            .as_ref()
            .unwrap_or_else(|| panic!("Not a multipart store: {self:?}"))
    }

    /// Produces a URL representation of this path that can be read by other systems,
    /// in particular Nomad's artifact fetcher and Arroyo's artifact fetcher.
    pub fn canonical_url(&self) -> &str {
        &self.canonical_url
    }

    pub fn canonical_url_for(&self, path: &str) -> String {
        format!("{}/{}", self.canonical_url, path)
    }

    pub fn storage_options(&self) -> &HashMap<String, String> {
        &self.storage_options
    }

    pub fn config(&self) -> &BackendConfig {
        &self.config
    }

    /// Provides an Arc'd copy of the underlying ObjectStore implementation. Be *very* careful
    /// using this. An objectstore URL is made up of two components: a bucket and a key, like
    /// s3://my-bucket/my/key.
    ///
    /// The backing store only knows about buckets, so it is *your* responsibility when using
    /// raw object store methods to prepend the key as well (for example, by using
    /// `ArroyoStorage::qualify_path`, or by taking the prefix from
    /// [`configured_prefix`](Self::configured_prefix), which is that call with the
    /// empty path).
    ///
    /// The concrete implementation is erased here. A consumer that needs to know what
    /// the store can physically do must take [`backing_handle`](Self::backing_handle)
    /// instead — it is the same allocation with its type intact.
    pub fn get_backing_store(&self) -> Arc<dyn ObjectStore> {
        self.object_store.clone()
    }

    /// The backing store with its concrete type intact — the same allocation
    /// [`get_backing_store`](Self::get_backing_store) hands out erased.
    ///
    /// This is the handle a capability-checking consumer needs. What a store can
    /// physically do is a property of its implementation, and an `Arc<dyn
    /// ObjectStore>` has already thrown that away; see [`BackingStoreHandle`] for why
    /// the type, rather than anything the store reports about itself, is the witness.
    ///
    /// It is half of a handoff: pair it with
    /// [`configured_prefix`](Self::configured_prefix), which supplies the key
    /// everything under this provider is namespaced by. Both come from this one
    /// provider, so the store and the prefix a consumer ends up holding describe the
    /// same location by construction.
    pub fn backing_handle(&self) -> BackingStoreHandle {
        self.backing.clone()
    }

    /// The endpoint the backing client was built to talk to, read off the final builder
    /// the client was built from — the configured S3/R2 endpoint or AWS S3 in the
    /// configured region, the Azure account URL, the GCS base URL — or `None` when this
    /// crate cannot tell (an S3 Express bucket, the Azure emulator, an unreadable
    /// service-account key, a local directory). See the `endpoint` module for each rule.
    ///
    /// A client's type does not fix its endpoint — an `AmazonS3` is also R2 and every
    /// S3-compatible service — so a consumer that trusts one endpoint's listing and
    /// durability compares its declaration with this value, from the same provider as
    /// [`backing_handle`](Self::backing_handle), and trusts nothing when it is `None`.
    pub fn effective_endpoint(&self) -> Option<&str> {
        self.effective_endpoint.as_deref()
    }

    pub async fn head(&self, path: impl Into<Path>) -> Result<ObjectMeta, StorageError> {
        let path = path.into();
        storage_retry!(self.object_store.head(&self.qualify_path(&path)).await)
            .map_err(Into::<StorageError>::into)
    }

    pub fn buf_writer(&self, path: impl Into<Path>) -> BufWriter {
        let path = path.into();
        BufWriter::new(
            self.object_store.clone(),
            self.qualify_path(&path).into_owned(),
        )
    }

    pub async fn start_multipart(&self, path: &Path) -> Result<MultipartId, StorageError> {
        let id = storage_retry!(
            self.get_multipart()
                .create_multipart(&self.qualify_path(path))
                .await
        )
        .map_err(Into::<StorageError>::into)?;

        debug!(
            message = "started multipart upload",
            path = path.as_ref(),
            id = id.as_str()
        );

        Ok(id)
    }

    pub async fn add_multipart(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        part_number: usize,
        bytes: Bytes,
    ) -> Result<PartId, StorageError> {
        let part_id = storage_retry!(
            self.get_multipart()
                .put_part(
                    &self.qualify_path(path),
                    multipart_id,
                    part_number,
                    bytes.clone().into()
                )
                .await
        )
        .map_err(Into::<StorageError>::into)?;

        debug!(
            message = "added part",
            path = path.as_ref(),
            id = multipart_id.as_str(),
            part_number,
            size = bytes.len()
        );

        Ok(part_id)
    }

    pub async fn close_multipart(
        &self,
        path: &Path,
        multipart_id: &MultipartId,
        parts: Vec<PartId>,
    ) -> Result<(), StorageError> {
        debug!(
            message = "closing multipart",
            path = path.as_ref(),
            id = multipart_id.as_str(),
            parts = parts.len()
        );

        storage_retry!(
            self.get_multipart()
                .complete_multipart(&self.qualify_path(path), multipart_id, parts.clone())
                .await
        )
        .map_err(Into::<StorageError>::into)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use arroyo_types::to_nanos;
    use object_store::aws::AmazonS3Builder;
    use object_store::azure::MicrosoftAzureBuilder;
    use object_store::gcp::GoogleCloudStorageBuilder;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::SystemTime;

    use crate::{
        AzureConfig, BackendConfig, BackingStoreHandle, GCSConfig, LocalConfig, R2Config, S3Config,
        StorageProvider, matchers,
    };

    #[test]
    fn test_regex_compilation() {
        matchers();
    }

    #[test]
    fn test_s3_configs() {
        assert_eq!(
            BackendConfig::parse_url("s3://mybucket/puppy.jpg", false).unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: None,
                bucket: "mybucket".to_string(),
                key: Some("puppy.jpg".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "https://s3.us-west-2.amazonaws.com/my-bucket1/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: Some("us-west-2".to_string()),
                bucket: "my-bucket1".to_string(),
                key: Some("puppy.jpg".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url("https://s3.us-east-1.amazonaws.com/my-bucket", false)
                .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: Some("us-east-1".to_string()),
                bucket: "my-bucket".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "https://my-bucket.s3.us-west-2.amazonaws.com/my/path/test.pdf",
                false
            )
            .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: None,
                region: Some("us-west-2".to_string()),
                bucket: "my-bucket".to_string(),
                key: Some("my/path/test.pdf".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "s3::https://my-custom-endpoint.com:1234/my-bucket/path/test.pdf",
                false
            )
            .unwrap(),
            BackendConfig::S3(crate::S3Config {
                endpoint: Some("https://my-custom-endpoint.com:1234".to_string()),
                region: None,
                bucket: "my-bucket".to_string(),
                key: Some("path/test.pdf".into()),
            })
        );
    }

    #[test]
    fn test_azure_configs() {
        assert_eq!(
            BackendConfig::parse_url(
                "https://mystorageaccount.blob.core.windows.net/my-container/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::Azure(crate::AzureConfig {
                account: "mystorageaccount".to_string(),
                container: "my-container".to_string(),
                key: Some("puppy.jpg".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "https://mystorageaccount.dfs.core.windows.net/my-container/some/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::Azure(crate::AzureConfig {
                account: "mystorageaccount".to_string(),
                container: "my-container".to_string(),
                key: Some("some/puppy.jpg".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "abfs://my-container@mystorageaccount.dfs.core.windows.net/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::Azure(crate::AzureConfig {
                account: "mystorageaccount".to_string(),
                container: "my-container".to_string(),
                key: Some("puppy.jpg".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url(
                "abfss://my-container@mystorageaccount.dfs.core.windows.net/some/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::Azure(crate::AzureConfig {
                account: "mystorageaccount".to_string(),
                container: "my-container".to_string(),
                key: Some("some/puppy.jpg".into()),
            })
        );
    }

    #[test]
    fn test_r2_configs() {
        // Fully qualified R2 scheme without path
        assert_eq!(
            BackendConfig::parse_url("r2://97493190cfb48832a99ee97a7637ff6a@my-bucket1", false)
                .unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "97493190cfb48832a99ee97a7637ff6a".to_string(),
                bucket: "my-bucket1".to_string(),
                jurisdiction: None,
                key: None,
            })
        );

        // Fully qualified R2 scheme with path
        assert_eq!(
            BackendConfig::parse_url(
                "r2://97493190cfb48832a99ee97a7637ff6a@my-bucket1/puppy.jpg",
                false
            )
            .unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "97493190cfb48832a99ee97a7637ff6a".to_string(),
                bucket: "my-bucket1".to_string(),
                jurisdiction: None,
                key: Some("puppy.jpg".into()),
            })
        );

        unsafe {
            std::env::set_var("CLOUDFLARE_ACCOUNT_ID", "4bed8261ff878a81208da2fface71221");
        }

        // R2 scheme with account ID from env
        assert_eq!(
            BackendConfig::parse_url("r2://mybucket/puppy.jpg", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "4bed8261ff878a81208da2fface71221".to_string(),
                bucket: "mybucket".to_string(),
                jurisdiction: None,
                key: Some("puppy.jpg".into()),
            })
        );

        // Path-style without prefix
        assert_eq!(
            BackendConfig::parse_url(
                "https://266035a37ceb5d774c51af1272485f1f.r2.cloudflarestorage.com/mybucket",
                false
            )
            .unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "mybucket".to_string(),
                jurisdiction: None,
                key: None,
            })
        );

        // Path-style with prefix
        assert_eq!(
            BackendConfig::parse_url("https://266035a37ceb5d774c51af1272485f1f.r2.cloudflarestorage.com/mybucket/my/key/path", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "mybucket".to_string(),
                jurisdiction: None,
                key: Some("my/key/path".into()),
            })
        );

        assert_eq!(
            BackendConfig::parse_url("https://266035a37ceb5d774c51af1272485f1f.eu.r2.cloudflarestorage.com/mybucket/my/key/path", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "mybucket".to_string(),
                jurisdiction: Some("eu".to_string()),
                key: Some("my/key/path".into()),
            })
        );

        // Virtual style
        assert_eq!(
            BackendConfig::parse_url(
                "https://my-bucket.266035a37ceb5d774c51af1272485f1f.eu.r2.cloudflarestorage.com",
                false
            )
            .unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "my-bucket".to_string(),
                jurisdiction: Some("eu".to_string()),
                key: None,
            })
        );

        // Virtual style with path
        assert_eq!(
            BackendConfig::parse_url("https://my-bucket.266035a37ceb5d774c51af1272485f1f.eu.r2.cloudflarestorage.com/my/key/path", false).unwrap(),
            BackendConfig::R2(crate::R2Config {
                account_id: "266035a37ceb5d774c51af1272485f1f".to_string(),
                bucket: "my-bucket".to_string(),
                jurisdiction: Some("eu".to_string()),
                key: Some("my/key/path".into()),
            })
        );
    }

    #[test]
    fn test_local_configs() {
        assert_eq!(
            BackendConfig::parse_url("file:///my/path/directory", false).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url("file:/my/path/directory", false).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url("/my/path/directory", false).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: None,
            })
        );

        assert_eq!(
            BackendConfig::parse_url("/my/path/directory/my-file.pdf", true).unwrap(),
            BackendConfig::Local(crate::LocalConfig {
                path: "/my/path/directory".to_string(),
                key: Some("my-file.pdf".into()),
            })
        );
    }

    /// The address of the allocation an `Arc` owns, with any trait-object metadata
    /// discarded.
    ///
    /// Two `Arc`s of *different* trait-object types cannot be compared with
    /// `Arc::ptr_eq` at all, and that is exactly the comparison the handoff needs: the
    /// erased `Arc<dyn ObjectStore>` and the `Arc<dyn MultipartStore>` must be two
    /// views of one store, not two stores.
    fn address<T: ?Sized>(arc: &Arc<T>) -> *const () {
        Arc::as_ptr(arc).cast::<()>()
    }

    /// One provider shape per [`BackendConfig`] variant, each paired with the concrete
    /// `object_store` client its own `construct_*` builds, and with whether that client
    /// implements [`MultipartStore`](object_store::multipart::MultipartStore).
    ///
    /// Every client here is built offline. `AmazonS3Builder`, `GoogleCloudStorageBuilder`
    /// and `MicrosoftAzureBuilder` resolve credentials lazily — `build()` only assembles
    /// a credential *provider* — so construction issues no request and reads no
    /// credential file. `key` is threaded through so the prefix matrix below can vary it
    /// independently of the backend.
    fn backends(
        root: &std::path::Path,
        key: Option<Path>,
    ) -> Vec<(&'static str, BackendConfig, BackingStoreHandle, bool)> {
        let amazon_s3 = || {
            Arc::new(
                AmazonS3Builder::new()
                    .with_bucket_name("bucket")
                    .build()
                    .expect("an S3 client builds without credentials or network"),
            )
        };
        vec![
            (
                "S3",
                BackendConfig::S3(S3Config {
                    endpoint: None,
                    region: Some("us-east-1".to_string()),
                    bucket: "bucket".to_string(),
                    key: key.clone(),
                }),
                BackingStoreHandle::AmazonS3(amazon_s3()),
                true,
            ),
            (
                // R2 is an `AmazonS3` against a Cloudflare endpoint, which is why it
                // shares the handle variant and not the config variant.
                "R2",
                BackendConfig::R2(R2Config {
                    account_id: "0123456789abcdef".to_string(),
                    bucket: "bucket".to_string(),
                    jurisdiction: None,
                    key: key.clone(),
                }),
                BackingStoreHandle::AmazonS3(amazon_s3()),
                true,
            ),
            (
                "GCS",
                BackendConfig::GCS(GCSConfig {
                    bucket: "bucket".to_string(),
                    key: key.clone(),
                }),
                BackingStoreHandle::GoogleCloudStorage(Arc::new(
                    GoogleCloudStorageBuilder::new()
                        .with_bucket_name("bucket")
                        .build()
                        .expect("a GCS client builds without credentials or network"),
                )),
                true,
            ),
            (
                "Azure",
                BackendConfig::Azure(AzureConfig {
                    account: "account".to_string(),
                    container: "container".to_string(),
                    key: key.clone(),
                }),
                BackingStoreHandle::MicrosoftAzure(Arc::new(
                    MicrosoftAzureBuilder::new()
                        .with_account("account")
                        .with_container_name("container")
                        .build()
                        .expect("an Azure client builds without credentials or network"),
                )),
                true,
            ),
            (
                "Local",
                BackendConfig::Local(LocalConfig {
                    path: root.display().to_string(),
                    key,
                }),
                BackingStoreHandle::LocalFileSystem(Arc::new(
                    LocalFileSystem::new_with_prefix(root).expect("a local store at a real root"),
                )),
                false,
            ),
        ]
    }

    /// The typed handle, the erased handle and the multipart handle are three views of
    /// **one allocation**, for every backend shape.
    ///
    /// This is the property the capability handoff rests on. A consumer takes
    /// `backing_handle()` to learn what the store is and `get_backing_store()` to talk
    /// to it; if those were two stores built at the same location the first would be
    /// describing something the second is not, and nothing downstream could tell. It
    /// is asserted by address rather than by behaviour because "the same location"
    /// would pass a behavioural check just as well — which is precisely the mistake
    /// this replaces.
    #[test]
    fn every_backend_hands_all_three_views_one_allocation() {
        let root = std::env::temp_dir().join(format!("arroyo-backing-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("a scratch root");

        let mut seen = Vec::new();
        for (name, config, handle, has_multipart) in backends(&root, None) {
            let typed = handle.as_object_store();
            let provider = StorageProvider::with_backing(
                config,
                handle,
                "test://provider".to_string(),
                HashMap::new(),
                None,
            );

            let erased = provider.get_backing_store();
            assert!(
                Arc::ptr_eq(&typed, &erased),
                "{name}: get_backing_store must hand out the store the provider was \
                 built on, not a second one"
            );
            assert!(
                Arc::ptr_eq(&provider.backing_handle().as_object_store(), &erased),
                "{name}: backing_handle and get_backing_store must describe one store"
            );

            match (provider.as_multipart(), has_multipart) {
                (Some(multipart), true) => assert_eq!(
                    address(&multipart),
                    address(&erased),
                    "{name}: the multipart view must be the same allocation"
                ),
                (None, false) => {}
                (found, _) => panic!(
                    "{name}: multipart support is {}, expected {has_multipart}",
                    found.is_some()
                ),
            }
            seen.push(name);
        }

        assert_eq!(
            seen,
            vec!["S3", "R2", "GCS", "Azure", "Local"],
            "every BackendConfig variant must be exercised"
        );
        std::fs::remove_dir_all(&root).ok();
    }

    /// `configured_prefix()` is `qualify_path(&Path::default())`, for every backend and
    /// every key shape — including the no-key case, where it is the empty path.
    ///
    /// The two are crossed rather than both re-derived from the config: `qualify_path`
    /// is what every provider operation already applies, so a consumer that takes the
    /// prefix from here and qualifies keys itself lands where the provider lands. The
    /// closed-form expectation is asserted too, so a `qualify_path` that started
    /// returning something else would fail here rather than agreeing with itself.
    #[test]
    fn the_configured_prefix_is_qualification_of_the_empty_path() {
        let root = std::env::temp_dir().join(format!("arroyo-prefix-{}", std::process::id()));
        std::fs::create_dir_all(&root).expect("a scratch root");

        // No key, one segment, several — the three shapes a parsed URL can produce.
        let keys: [(Option<Path>, &str); 3] = [
            (None, ""),
            (Some("job-1".into()), "job-1"),
            (
                Some("job-1/operator-7/ckpt".into()),
                "job-1/operator-7/ckpt",
            ),
        ];

        for (key, expected) in keys {
            for (name, config, handle, _) in backends(&root, key.clone()) {
                let provider = StorageProvider::with_backing(
                    config,
                    handle,
                    "test://provider".to_string(),
                    HashMap::new(),
                    None,
                );

                assert_eq!(
                    provider.configured_prefix().as_ref(),
                    expected,
                    "{name}: the configured prefix must be the key the config carries"
                );
                assert_eq!(
                    provider.configured_prefix(),
                    provider.qualify_path(&Path::default()).into_owned(),
                    "{name}: the configured prefix must be qualification of the empty path"
                );
                // And it really is the prefix the provider applies to a real key.
                // Bound to a local: `qualify_path` returns a `Cow` that may borrow its
                // argument, so a temporary would not outlive the comparison.
                let key = Path::from("manifest/cp1");
                let qualified = provider.qualify_path(&key);
                let expected_qualified: Path = provider
                    .configured_prefix()
                    .parts()
                    .chain(key.parts())
                    .collect();
                assert_eq!(
                    qualified.as_ref(),
                    &expected_qualified,
                    "{name}: qualifying under the published prefix must reach the \
                     provider's own path"
                );
            }
        }
        std::fs::remove_dir_all(&root).ok();
    }

    /// The endpoint the provider `a_provider_built_from_a_url_serves_one_allocation` builds
    /// for `url` must report, spelled independently of the `endpoint` module: none for a
    /// local directory, the R2 account's Cloudflare endpoint, and Google's base URL.
    /// `from_env` would replace the last with a service-account key's `gcs_base_url`, so on a
    /// host whose environment configures a key that row is not asserted (outer `None`).
    fn expected_endpoint(url: &str) -> Option<Option<&'static str>> {
        if url.starts_with("file://") {
            return Some(None);
        }
        if url.starts_with("r2://") {
            return Some(Some("https://0123456789abcdef.r2.cloudflarestorage.com"));
        }
        let keyed = [
            "GOOGLE_SERVICE_ACCOUNT",
            "GOOGLE_SERVICE_ACCOUNT_PATH",
            "GOOGLE_SERVICE_ACCOUNT_KEY",
        ]
        .iter()
        .any(|var| std::env::var_os(var).is_some());
        (!keyed).then_some(Some("https://storage.googleapis.com"))
    }

    /// The end-to-end half: a provider built from a URL by its real constructor hands
    /// both views one allocation, publishes the prefix its own URL carries, and reports
    /// the endpoint its client was built for ([`expected_endpoint`]).
    ///
    /// `file://` and `gs://` build with no credentials at all; `r2://` needs an access
    /// key and secret, which are passed explicitly as storage options rather than read
    /// from the environment so the test does not depend on the machine it runs on.
    /// `s3://` is deliberately absent: `construct_s3` loads the AWS SDK's default
    /// config to resolve a region, which reaches for instance metadata on a machine
    /// that has none. Its client type is `AmazonS3`, the same variant `r2://` exercises
    /// here and `backends` covers above.
    #[tokio::test]
    async fn a_provider_built_from_a_url_serves_one_allocation() {
        let root = std::env::temp_dir().join(format!("arroyo-url-backing-{}", std::process::id()));
        let cases: Vec<(String, HashMap<String, String>, &str)> = vec![
            (format!("file://{}", root.display()), HashMap::new(), ""),
            ("gs://bucket/job-1".to_string(), HashMap::new(), "job-1"),
            (
                "r2://0123456789abcdef@bucket/job-1/ckpt".to_string(),
                HashMap::from([
                    ("r2_access_key_id".to_string(), "key".to_string()),
                    ("r2_secret_access_key".to_string(), "secret".to_string()),
                ]),
                "job-1/ckpt",
            ),
        ];

        for (url, options, expected_prefix) in cases {
            let provider = StorageProvider::for_url_with_options(&url, options)
                .await
                .unwrap_or_else(|e| panic!("{url} builds a provider: {e}"));
            if let Some(expected) = expected_endpoint(&url) {
                assert_eq!(
                    provider.effective_endpoint(),
                    expected,
                    "{url}: the endpoint its client was built for"
                );
            }

            assert!(
                Arc::ptr_eq(
                    &provider.backing_handle().as_object_store(),
                    &provider.get_backing_store()
                ),
                "{url}: the typed and erased handles must be one allocation"
            );
            assert_eq!(
                provider.configured_prefix().as_ref(),
                expected_prefix,
                "{url}: the published prefix must be the URL's own key"
            );
        }

        std::fs::remove_dir_all(&root).ok();
    }

    #[tokio::test]
    async fn test_local_fs() {
        let storage = StorageProvider::for_url("file:///tmp/arroyo-testing/storage-tests")
            .await
            .unwrap();

        let now = to_nanos(SystemTime::now());
        let data = now.to_le_bytes().to_vec();
        let key = Path::parse(format!("my-test/{now}")).unwrap();

        assert!(storage.put(key.clone(), data.clone()).await.is_ok());
        assert_eq!(storage.get(key.clone()).await.unwrap(), data.clone());

        let full_url = storage.canonical_url_for(key.as_ref());

        assert_eq!(
            StorageProvider::get_url(&full_url).await.unwrap(),
            data.clone()
        );

        storage.delete_if_present(key.clone()).await.unwrap();

        assert!(
            !tokio::fs::try_exists(format!("/tmp/arroyo-testing/storage-tests/{key}"))
                .await
                .unwrap()
        );
    }
}
