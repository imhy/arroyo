//! The endpoint a [`StorageProvider`](crate::StorageProvider)'s client was built to talk to
//! (statebackend M11.T10, PR #200 review comment 5846291555).
//!
//! A client's type does not fix its endpoint. An `AmazonS3` is also Cloudflare R2 and any
//! S3-compatible service, and `AmazonS3Builder::from_env` honours `AWS_ENDPOINT_URL`; an
//! Azure client can target the Azurite emulator or a Fabric endpoint; a GCS base URL can come
//! from the service-account key. A consumer that trusts an endpoint's listing consistency and
//! durability — stateengine's durable list-after-write declaration — must compare what it
//! trusts with the endpoint the provider was really built for.
//!
//! Each function here reads that endpoint off the **final builder**, the one `build()` then
//! consumes, through `get_config_value`, and reproduces the default `build()` falls back to
//! when nothing is configured (`object_store` 0.12.5: `src/aws/builder.rs:1129-1135`,
//! `src/azure/builder.rs:918-947`, `src/gcp/builder.rs:494-497`). Every setting that moves
//! the endpoint in a way not reproduced here — S3 Express zonal endpoints, the Azure emulator,
//! a flag whose raw text is not exactly `true` or `false` — yields `None`: unknown, never
//! guessed. Arroyo never calls a builder's `with_url`, which `get_config_value` cannot see;
//! these functions rely on that.

use object_store::aws::{AmazonS3Builder, AmazonS3ConfigKey};
use object_store::azure::{AzureConfigKey, MicrosoftAzureBuilder};
use object_store::gcp::{GoogleCloudStorageBuilder, GoogleConfigKey};

/// `object_store`'s default GCS base URL (`src/gcp/credential.rs:48`).
const GCS_DEFAULT_BASE_URL: &str = "https://storage.googleapis.com";

/// The region `AmazonS3Builder::build` assumes when none is configured
/// (`src/aws/builder.rs:1003`).
const S3_DEFAULT_REGION: &str = "us-east-1";

/// A boolean builder flag, read as `build()` would only when its raw text is unambiguous.
///
/// `get_config_value` renders a flag's raw configured text (`ConfigValue::Deferred`), which
/// `build()` parses later with rules this crate does not reproduce; anything but the two
/// canonical spellings is therefore unknown.
fn flag(raw: Option<String>) -> Option<bool> {
    match raw.as_deref() {
        Some("true") => Some(true),
        Some("false") => Some(false),
        _ => None,
    }
}

/// The endpoint an `AmazonS3` built from `builder` talks to: the configured endpoint (R2's,
/// or an S3-compatible service's) verbatim, else AWS S3 in the configured region.
///
/// `None` for an S3 Express bucket, whose zonal endpoint `build()` derives from the bucket
/// name, and for an S3 Express flag this crate cannot read.
pub(crate) fn s3(builder: &AmazonS3Builder) -> Option<String> {
    if flag(builder.get_config_value(&AmazonS3ConfigKey::S3Express))? {
        return None;
    }
    Some(
        match builder.get_config_value(&AmazonS3ConfigKey::Endpoint) {
            Some(endpoint) => endpoint,
            None => format!(
                "https://s3.{}.amazonaws.com",
                builder
                    .get_config_value(&AmazonS3ConfigKey::Region)
                    .unwrap_or_else(|| S3_DEFAULT_REGION.to_string())
            ),
        },
    )
}

/// The endpoint a `MicrosoftAzure` built from `builder` talks to: the configured account URL
/// verbatim, else the account's Blob or Fabric endpoint.
///
/// `None` for the emulator (whose URL `build()` reads from `AZURITE_BLOB_STORAGE_URL`), for a
/// flag this crate cannot read, and for a missing account name, which `build()` refuses.
pub(crate) fn azure(builder: &MicrosoftAzureBuilder) -> Option<String> {
    if flag(builder.get_config_value(&AzureConfigKey::UseEmulator))? {
        return None;
    }
    if let Some(endpoint) = builder.get_config_value(&AzureConfigKey::Endpoint) {
        return Some(endpoint);
    }
    let account = builder.get_config_value(&AzureConfigKey::AccountName)?;
    let host = if flag(builder.get_config_value(&AzureConfigKey::UseFabricEndpoint))? {
        "blob.fabric.microsoft.com"
    } else {
        "blob.core.windows.net"
    };
    Some(format!("https://{account}.{host}"))
}

/// The base URL a `GoogleCloudStorage` built from `builder` talks to: the service-account
/// key's `gcs_base_url` when it has one, else Google's.
///
/// The key is the one `build()` reads — the configured key text, or the file at the
/// configured path. `None` when both are configured (`build()` refuses that), when the file
/// cannot be read, or when the key is not a JSON object whose `gcs_base_url` is absent,
/// `null` or a string.
pub(crate) fn gcs(builder: &GoogleCloudStorageBuilder) -> Option<String> {
    let key = match (
        builder.get_config_value(&GoogleConfigKey::ServiceAccount),
        builder.get_config_value(&GoogleConfigKey::ServiceAccountKey),
    ) {
        (None, None) => return Some(GCS_DEFAULT_BASE_URL.to_string()),
        (None, Some(key)) => key,
        (Some(path), None) => std::fs::read_to_string(path).ok()?,
        (Some(_), Some(_)) => return None,
    };
    let key: serde_json::Value = serde_json::from_str(&key).ok()?;
    match key.as_object()?.get("gcs_base_url") {
        None | Some(serde_json::Value::Null) => Some(GCS_DEFAULT_BASE_URL.to_string()),
        Some(serde_json::Value::String(url)) => Some(url.clone()),
        Some(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A builder that is not `from_env`, so the host's environment cannot move a row.
    fn s3_builder() -> AmazonS3Builder {
        AmazonS3Builder::new().with_bucket_name("bucket")
    }

    /// S3: the region varies the default, a configured endpoint wins over the region, and
    /// every S3 Express setting other than a readable `false` is unknown.
    #[test]
    fn s3_is_the_configured_endpoint_or_aws_in_the_region() {
        assert_eq!(
            s3(&s3_builder()).as_deref(),
            Some("https://s3.us-east-1.amazonaws.com")
        );
        assert_eq!(
            s3(&s3_builder().with_region("eu-west-1")).as_deref(),
            Some("https://s3.eu-west-1.amazonaws.com")
        );
        for region in [None, Some("eu-west-1")] {
            let mut builder = s3_builder().with_endpoint("https://acct.r2.cloudflarestorage.com");
            if let Some(region) = region {
                builder = builder.with_region(region);
            }
            assert_eq!(
                s3(&builder).as_deref(),
                Some("https://acct.r2.cloudflarestorage.com"),
                "{region:?}"
            );
            // Virtual-hosted addressing changes the host `build()` derives for a request, not
            // the service a configured endpoint names.
            assert_eq!(
                s3(&builder.with_virtual_hosted_style_request(true)).as_deref(),
                Some("https://acct.r2.cloudflarestorage.com")
            );
        }
        assert_eq!(s3(&s3_builder().with_s3_express(true)), None);
        assert_eq!(
            s3(&s3_builder().with_config(AmazonS3ConfigKey::S3Express, "TRUE")),
            None,
            "a flag spelled otherwise than build() is known to read it is unknown"
        );
        assert_eq!(
            s3(&s3_builder().with_config(AmazonS3ConfigKey::S3Express, "false")).as_deref(),
            Some("https://s3.us-east-1.amazonaws.com")
        );
    }

    fn azure_builder() -> MicrosoftAzureBuilder {
        MicrosoftAzureBuilder::new().with_container_name("container")
    }

    /// Azure: the account's Blob endpoint, its Fabric endpoint, a configured account URL, and
    /// the three unknowns — the emulator, an unreadable flag, no account.
    #[test]
    fn azure_is_the_account_url_or_its_default_host() {
        let account = || azure_builder().with_account("acct");
        assert_eq!(
            azure(&account()).as_deref(),
            Some("https://acct.blob.core.windows.net")
        );
        assert_eq!(
            azure(&account().with_use_fabric_endpoint(true)).as_deref(),
            Some("https://acct.blob.fabric.microsoft.com")
        );
        assert_eq!(
            azure(&account().with_endpoint("https://blob.example.net".to_string())).as_deref(),
            Some("https://blob.example.net")
        );
        assert_eq!(azure(&account().with_use_emulator(true)), None);
        assert_eq!(
            azure(&account().with_config(AzureConfigKey::UseFabricEndpoint, "1")),
            None
        );
        assert_eq!(azure(&azure_builder()), None, "no account: build() refuses");
    }

    fn gcs_builder() -> GoogleCloudStorageBuilder {
        GoogleCloudStorageBuilder::new().with_bucket_name("bucket")
    }

    /// GCS: Google's base URL with no key, the key's `gcs_base_url` when it names one — given
    /// inline or as a file — and unknown for a key `build()` could not read the same way.
    #[test]
    fn gcs_is_the_service_account_base_url_or_googles() {
        assert_eq!(gcs(&gcs_builder()).as_deref(), Some(GCS_DEFAULT_BASE_URL));
        let keyed = |key: &str| gcs(&gcs_builder().with_service_account_key(key));
        assert_eq!(
            keyed(r#"{"gcs_base_url":"https://localhost:4443"}"#).as_deref(),
            Some("https://localhost:4443")
        );
        assert_eq!(
            keyed(r#"{"private_key":"k"}"#).as_deref(),
            Some(GCS_DEFAULT_BASE_URL)
        );
        assert_eq!(
            keyed(r#"{"gcs_base_url":null}"#).as_deref(),
            Some(GCS_DEFAULT_BASE_URL)
        );
        for unreadable in [r#"{"gcs_base_url":7}"#, "[]", "not json"] {
            assert_eq!(keyed(unreadable), None, "{unreadable}");
        }

        let file = std::env::temp_dir().join(format!("arroyo-gcs-key-{}.json", std::process::id()));
        std::fs::write(&file, r#"{"gcs_base_url":"https://gcs.example.net"}"#).expect("key file");
        let path = file.to_str().expect("a UTF-8 temp path");
        assert_eq!(
            gcs(&gcs_builder().with_service_account_path(path)).as_deref(),
            Some("https://gcs.example.net")
        );
        assert_eq!(
            gcs(&gcs_builder()
                .with_service_account_path(path)
                .with_service_account_key("{}")),
            None,
            "a path and a key together: build() refuses"
        );
        std::fs::remove_file(&file).expect("remove the key file");
        assert_eq!(gcs(&gcs_builder().with_service_account_path(path)), None);
    }
}
