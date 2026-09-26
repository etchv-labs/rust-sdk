use crate::{Client, Result, check_id, with_query};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::fmt;

/// Object visibility for a storage destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StorageVisibility {
    /// Objects are private (default).
    Private,
    /// Objects are publicly readable; deliveries report a `public_url`.
    Public,
}

/// A customer storage destination (S3, GCS or Azure Blob).
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct StorageDestination {
    /// Destination ID (`dst_…`).
    pub id: String,
    /// Display name.
    pub name: String,
    /// `s3`, `gcs` or `azure`.
    pub provider: String,
    /// Bucket or container name.
    pub bucket: String,
    /// Object key prefix.
    #[serde(default)]
    pub prefix: Option<String>,
    /// `private` or `public`.
    #[serde(default)]
    pub visibility: Option<String>,
    /// AWS region (S3 only).
    #[serde(default)]
    pub region: Option<String>,
    /// IAM role assumed by Etchv (S3 only).
    #[serde(default)]
    pub role_arn: Option<String>,
    /// Storage account (Azure only).
    #[serde(default)]
    pub account: Option<String>,
    /// External ID to require in the S3 role trust policy.
    #[serde(default)]
    pub external_id: Option<String>,
    /// Etchv principal to trust in your AWS role policy.
    #[serde(default)]
    pub aws_principal_arn: Option<String>,
    /// `service_account_key` or `workload_identity` (GCS only).
    #[serde(default)]
    pub gcs_auth: Option<String>,
    /// Workload identity provider resource (keyless GCS).
    #[serde(default)]
    pub gcs_workload_identity_provider: Option<String>,
    /// Google service account email (keyless GCS).
    #[serde(default)]
    pub gcs_service_account: Option<String>,
    /// Federated subject to grant in Google Cloud (keyless GCS).
    #[serde(default)]
    pub gcs_subject: Option<String>,
    /// Whether the destination can be selected.
    pub enabled: bool,
    /// When the connection was last verified; `None` until verified.
    #[serde(default)]
    pub verified_at: Option<String>,
    /// Creation time (ISO 8601).
    pub created_at: String,
    /// Last verification error code.
    #[serde(default)]
    pub last_error: Option<String>,
    /// Expiry of the stored credential (Azure SAS).
    #[serde(default)]
    pub credential_expires_at: Option<String>,
}

/// Parameters for [`Client::create_storage_destination`].
///
/// Build with the provider-specific constructor, then optionally set the
/// prefix and visibility. Credentials are never shown by `Debug`.
///
/// ```
/// use etchv::{NewStorageDestination, StorageVisibility};
/// let s3 = NewStorageDestination::s3(
///     "Exports",
///     "my-bucket",
///     "us-east-1",
///     "arn:aws:iam::123456789012:role/etchv-storage-exports",
/// )
/// .prefix("etchv/exports")
/// .visibility(StorageVisibility::Private);
/// ```
#[derive(Clone)]
#[non_exhaustive]
pub struct NewStorageDestination {
    /// Display name.
    pub name: String,
    /// `s3`, `gcs` or `azure`.
    pub provider: String,
    /// Bucket or container name.
    pub bucket: String,
    /// Object key prefix (server default `etchv`).
    pub prefix: Option<String>,
    /// Object visibility (server default private).
    pub visibility: Option<StorageVisibility>,
    /// AWS region (S3).
    pub region: Option<String>,
    /// IAM role ARN named `etchv-storage-…` (S3).
    pub role_arn: Option<String>,
    /// Storage account (Azure).
    pub account: Option<String>,
    /// Service account JSON key (GCS) or container SAS token (Azure).
    pub credentials: Option<String>,
    /// `service_account_key` or `workload_identity` (GCS).
    pub gcs_auth: Option<String>,
    /// Workload identity provider resource (keyless GCS).
    pub gcs_workload_identity_provider: Option<String>,
    /// Google service account email (keyless GCS).
    pub gcs_service_account: Option<String>,
}

impl fmt::Debug for NewStorageDestination {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NewStorageDestination")
            .field("name", &self.name)
            .field("provider", &self.provider)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("visibility", &self.visibility)
            .field("region", &self.region)
            .field("role_arn", &self.role_arn)
            .field("account", &self.account)
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "[redacted]"),
            )
            .field("gcs_auth", &self.gcs_auth)
            .field(
                "gcs_workload_identity_provider",
                &self.gcs_workload_identity_provider,
            )
            .field("gcs_service_account", &self.gcs_service_account)
            .finish()
    }
}

impl NewStorageDestination {
    fn base(name: &str, provider: &str, bucket: &str) -> Self {
        Self {
            name: name.into(),
            provider: provider.into(),
            bucket: bucket.into(),
            prefix: None,
            visibility: None,
            region: None,
            role_arn: None,
            account: None,
            credentials: None,
            gcs_auth: None,
            gcs_workload_identity_provider: None,
            gcs_service_account: None,
        }
    }
    /// Amazon S3 using an IAM role that trusts Etchv.
    pub fn s3(name: &str, bucket: &str, region: &str, role_arn: &str) -> Self {
        let mut d = Self::base(name, "s3", bucket);
        d.region = Some(region.into());
        d.role_arn = Some(role_arn.into());
        d
    }
    /// Google Cloud Storage using a service account JSON key.
    pub fn gcs(name: &str, bucket: &str, service_account_key_json: &str) -> Self {
        let mut d = Self::base(name, "gcs", bucket);
        d.gcs_auth = Some("service_account_key".into());
        d.credentials = Some(service_account_key_json.into());
        d
    }
    /// Google Cloud Storage using keyless workload identity federation.
    pub fn gcs_workload_identity(
        name: &str,
        bucket: &str,
        workload_identity_provider: &str,
        service_account: &str,
    ) -> Self {
        let mut d = Self::base(name, "gcs", bucket);
        d.gcs_auth = Some("workload_identity".into());
        d.gcs_workload_identity_provider = Some(workload_identity_provider.into());
        d.gcs_service_account = Some(service_account.into());
        d
    }
    /// Azure Blob Storage using a container SAS token.
    pub fn azure(name: &str, account: &str, container: &str, sas_token: &str) -> Self {
        let mut d = Self::base(name, "azure", container);
        d.account = Some(account.into());
        d.credentials = Some(sas_token.into());
        d
    }
    /// Set the object key prefix.
    pub fn prefix(mut self, prefix: impl Into<String>) -> Self {
        self.prefix = Some(prefix.into());
        self
    }
    /// Set object visibility.
    pub fn visibility(mut self, visibility: StorageVisibility) -> Self {
        self.visibility = Some(visibility);
        self
    }

    fn to_json(&self) -> Value {
        let mut body = Map::new();
        body.insert("name".into(), json!(self.name));
        body.insert("provider".into(), json!(self.provider));
        body.insert("bucket".into(), json!(self.bucket));
        let visibility = self.visibility.map(|v| match v {
            StorageVisibility::Private => "private".to_owned(),
            StorageVisibility::Public => "public".to_owned(),
        });
        for (k, v) in [
            ("prefix", &self.prefix),
            ("visibility", &visibility),
            ("region", &self.region),
            ("role_arn", &self.role_arn),
            ("account", &self.account),
            ("credentials", &self.credentials),
            ("gcs_auth", &self.gcs_auth),
            (
                "gcs_workload_identity_provider",
                &self.gcs_workload_identity_provider,
            ),
            ("gcs_service_account", &self.gcs_service_account),
        ] {
            if let Some(v) = v {
                body.insert(k.into(), json!(v));
            }
        }
        Value::Object(body)
    }
}

/// Changes for [`Client::update_storage_destination`]. Credentials are never
/// shown by `Debug`.
///
/// ```
/// let disable = etchv::StorageDestinationUpdate::new().enabled(false);
/// ```
#[derive(Default, Clone)]
#[non_exhaustive]
pub struct StorageDestinationUpdate {
    /// Enable or disable the destination.
    pub enabled: Option<bool>,
    /// Replacement credentials (GCS key or Azure SAS). Requires re-verification.
    pub credentials: Option<String>,
}

impl fmt::Debug for StorageDestinationUpdate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageDestinationUpdate")
            .field("enabled", &self.enabled)
            .field(
                "credentials",
                &self.credentials.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

impl StorageDestinationUpdate {
    /// No changes; equivalent to `StorageDestinationUpdate::default()`.
    pub fn new() -> Self {
        Self::default()
    }
    /// Enable or disable the destination.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = Some(enabled);
        self
    }
    /// Replace the stored credentials.
    pub fn credentials(mut self, credentials: impl Into<String>) -> Self {
        self.credentials = Some(credentials.into());
        self
    }
}

/// One upload attempt of a storage delivery.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct StorageAttempt {
    /// Attempt time (ISO 8601).
    pub at: String,
    /// Resulting delivery status.
    #[serde(default)]
    pub status: Option<String>,
    /// Failure code, if any.
    #[serde(default)]
    pub error_code: Option<String>,
}

/// Delivery of a watermarked asset to a customer storage destination.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct StorageDelivery {
    /// Delivery ID (`std_…`).
    pub id: String,
    /// `queued`, `uploading`, `retrying`, `stored`, `failed` or `cancelled`.
    pub status: String,
    /// Delivered asset (`ast_…`).
    #[serde(default)]
    pub asset_id: Option<String>,
    /// Job that produced the asset.
    #[serde(default)]
    pub request_id: Option<String>,
    /// Destination (`dst_…`).
    #[serde(default)]
    pub destination_id: Option<String>,
    /// `s3`, `gcs` or `azure`.
    #[serde(default)]
    pub provider: Option<String>,
    /// Full object key.
    #[serde(default)]
    pub key: Option<String>,
    /// Provider URI of the object.
    #[serde(default)]
    pub uri: Option<String>,
    /// Public URL for public destinations.
    #[serde(default)]
    pub public_url: Option<String>,
    /// Upload attempts so far.
    #[serde(default)]
    pub attempts: u32,
    /// Creation time (ISO 8601).
    #[serde(default)]
    pub created_at: Option<String>,
    /// Next scheduled attempt.
    #[serde(default)]
    pub next_attempt_at: Option<String>,
    /// Terminal time.
    #[serde(default)]
    pub completed_at: Option<String>,
    /// Failure code.
    #[serde(default)]
    pub error_code: Option<String>,
    /// Up to the last 30 attempts.
    #[serde(default)]
    pub history: Vec<StorageAttempt>,
    /// When the staged Etchv copy expires and retries stop.
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// One page of [`Client::list_storage_deliveries`] results.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct StorageDeliveryPage {
    /// Deliveries on this page, newest first (at most 50).
    pub items: Vec<StorageDelivery>,
    /// Pass as `after` to fetch the next page.
    pub next_cursor: Option<String>,
}

fn destination(id: &str) -> Result<String> {
    check_id(id, "dst_", 32, "storage destination")?;
    Ok(format!("storage/destinations/{id}"))
}
fn delivery(id: &str) -> Result<String> {
    check_id(id, "std_", 64, "storage delivery")?;
    Ok(format!("storage/deliveries/{id}"))
}

impl Client {
    /// List storage destinations (`GET /storage/destinations`). Requires `storage:read`.
    pub fn list_storage_destinations(&self) -> Result<Vec<StorageDestination>> {
        self.call_json(Method::GET, "storage/destinations", None)
    }

    /// Create a storage destination (`POST /storage/destinations`). Requires
    /// `storage:write` and owner/admin membership. Verify it with
    /// [`Client::verify_storage_destination`] before use.
    ///
    /// ```no_run
    /// use etchv::NewStorageDestination;
    /// # let client = etchv::Client::new("etchv_...")?;
    /// let dest = client.create_storage_destination(&NewStorageDestination::s3(
    ///     "Exports",
    ///     "my-bucket",
    ///     "us-east-1",
    ///     "arn:aws:iam::123456789012:role/etchv-storage-exports",
    /// ))?;
    /// let dest = client.verify_storage_destination(&dest.id)?;
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn create_storage_destination(
        &self,
        destination: &NewStorageDestination,
    ) -> Result<StorageDestination> {
        self.call_json(
            Method::POST,
            "storage/destinations",
            Some(destination.to_json()),
        )
    }

    /// Enable/disable a destination or replace its credentials
    /// (`PATCH /storage/destinations/{id}`).
    pub fn update_storage_destination(
        &self,
        id: &str,
        changes: &StorageDestinationUpdate,
    ) -> Result<StorageDestination> {
        let mut body = Map::new();
        if let Some(enabled) = changes.enabled {
            body.insert("enabled".into(), json!(enabled));
        }
        if let Some(ref credentials) = changes.credentials {
            body.insert("credentials".into(), json!(credentials));
        }
        self.call_json(Method::PATCH, &destination(id)?, Some(Value::Object(body)))
    }

    /// Disconnect a destination and discard its stored credentials
    /// (`DELETE /storage/destinations/{id}`).
    pub fn delete_storage_destination(&self, id: &str) -> Result<()> {
        self.call(Method::DELETE, &destination(id)?, None)?;
        Ok(())
    }

    /// Write and read a connection probe (`POST /storage/destinations/{id}/verify`).
    /// HTTP 422 means the connection check failed.
    pub fn verify_storage_destination(&self, id: &str) -> Result<StorageDestination> {
        self.call_json(Method::POST, &format!("{}/verify", destination(id)?), None)
    }

    /// List a destination's deliveries (`GET /storage/destinations/{id}/deliveries`),
    /// 50 per page. Pass the previous page's `next_cursor` as `after`.
    pub fn list_storage_deliveries(
        &self,
        destination_id: &str,
        after: Option<&str>,
    ) -> Result<StorageDeliveryPage> {
        let mut pairs = Vec::new();
        if let Some(after) = after {
            check_id(after, "std_", 64, "storage delivery")?;
            pairs.push(("after", after));
        }
        let path = format!("{}/deliveries", destination(destination_id)?);
        self.call_json(Method::GET, &with_query(&path, &pairs), None)
    }

    /// Move an Etchv-hosted watermarked asset to a verified destination
    /// (`POST /storage/destinations/{id}/deliveries`). `key` is an optional
    /// relative object key beneath the destination prefix.
    pub fn create_storage_delivery(
        &self,
        destination_id: &str,
        asset_id: &str,
        key: Option<&str>,
    ) -> Result<StorageDelivery> {
        check_id(asset_id, "ast_", 64, "asset")?;
        let mut body = json!({ "asset_id": asset_id });
        if let Some(key) = key {
            body["key"] = json!(key);
        }
        self.call_json(
            Method::POST,
            &format!("{}/deliveries", destination(destination_id)?),
            Some(body),
        )
    }

    /// Read a storage delivery (`GET /storage/deliveries/{id}`). Requires `storage:read`.
    ///
    /// Poll until `status` is `stored`, or handle `failed` / `cancelled`.
    pub fn get_storage_delivery(&self, id: &str) -> Result<StorageDelivery> {
        self.call_json(Method::GET, &delivery(id)?, None)
    }

    /// Retry a failed or cancelled upload without another charge
    /// (`POST /storage/deliveries/{id}/retry`).
    pub fn retry_storage_delivery(&self, id: &str) -> Result<StorageDelivery> {
        self.call_json(Method::POST, &format!("{}/retry", delivery(id)?), None)
    }

    /// Download a stored object through Etchv (`GET /storage/deliveries/{id}/content`).
    /// HTTP 409 means the delivery is not yet `stored`.
    pub fn download_storage_delivery(&self, id: &str) -> Result<Vec<u8>> {
        Ok(self
            .call(Method::GET, &format!("{}/content", delivery(id)?), None)?
            .1)
    }
}
