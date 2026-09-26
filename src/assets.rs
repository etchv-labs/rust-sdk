use crate::{Client, Error, Result, check_id, with_query};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};

/// An original upload or verified watermarked output in the asset library.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct Asset {
    /// Asset ID (`ast_…`).
    pub id: String,
    /// Display name.
    pub name: String,
    /// `source` or `watermarked`.
    pub kind: String,
    /// `image`, `document` or `video`.
    pub media_type: String,
    /// File format, for example `PNG`.
    pub format: String,
    /// MIME type.
    pub content_type: String,
    /// File size in bytes.
    pub size_bytes: u64,
    /// SHA-256 of the file (hex).
    pub sha256: String,
    /// Source asset of a watermarked output.
    pub parent_asset_id: Option<String>,
    /// Job that created the asset (`req_…`).
    pub request_id: String,
    /// Embedded watermark ID for watermarked outputs.
    pub watermark_id: Option<String>,
    /// Creation time (ISO 8601).
    pub created_at: String,
    /// Last update time (ISO 8601).
    pub updated_at: String,
    /// When the Etchv-hosted file expires; `None` for customer storage.
    pub file_expires_at: Option<String>,
    /// Whether the file can currently be downloaded.
    pub file_available: bool,
    /// Optimistic concurrency version; pass it to [`Client::update_asset`].
    pub version: u64,
    /// Custom JSON metadata.
    #[serde(default)]
    pub metadata: Option<Value>,
    /// Relative authenticated download path.
    pub download_url: Option<String>,
    /// `etchv`, `s3`, `gcs` or `azure`.
    #[serde(default)]
    pub storage_provider: Option<String>,
    /// Delivery state for the selected storage location.
    #[serde(default)]
    pub storage_status: Option<String>,
    /// Customer storage destination (`dst_…`).
    #[serde(default)]
    pub storage_destination_id: Option<String>,
    /// Customer storage delivery (`std_…`).
    #[serde(default)]
    pub storage_delivery_id: Option<String>,
    /// When the temporary Etchv copy expires during customer delivery.
    #[serde(default)]
    pub staging_expires_at: Option<String>,
    /// When the temporary Etchv copy was removed.
    #[serde(default)]
    pub staging_deleted_at: Option<String>,
}

/// One page of [`Client::list_assets`] results.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct AssetPage {
    /// Assets on this page, newest first.
    pub items: Vec<Asset>,
    /// Pass as [`AssetListOptions::cursor`] with the same filters to continue.
    pub next_cursor: Option<String>,
}

/// Filters and pagination for [`Client::list_assets`].
///
/// ```
/// let options = etchv::AssetListOptions::new().kind("watermarked").limit(50);
/// ```
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct AssetListOptions {
    /// Page size, 1–100 (server default 25).
    pub limit: Option<u32>,
    /// Cursor from a previous page.
    pub cursor: Option<String>,
    /// `source` or `watermarked`.
    pub kind: Option<String>,
    /// `image`, `document` or `video`.
    pub media_type: Option<String>,
    /// Only assets carrying this 64-character watermark ID.
    pub watermark_id: Option<String>,
}

impl AssetListOptions {
    /// No filters; equivalent to `AssetListOptions::default()`.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the page size.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    /// Continue from a previous page's `next_cursor`.
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }
    /// Filter by `source` or `watermarked`.
    pub fn kind(mut self, kind: impl Into<String>) -> Self {
        self.kind = Some(kind.into());
        self
    }
    /// Filter by `image`, `document` or `video`.
    pub fn media_type(mut self, media_type: impl Into<String>) -> Self {
        self.media_type = Some(media_type.into());
        self
    }
    /// Filter by watermark ID.
    pub fn watermark_id(mut self, id: impl Into<String>) -> Self {
        self.watermark_id = Some(id.into());
        self
    }
}

fn path(id: &str) -> Result<String> {
    check_id(id, "ast_", 64, "asset")?;
    Ok(format!("assets/{id}"))
}

impl Client {
    /// List assets (`GET /assets`), newest first. Requires `assets:read`.
    ///
    /// ```no_run
    /// # let client = etchv::Client::new("etchv_...")?;
    /// let mut options = etchv::AssetListOptions::new().kind("watermarked");
    /// loop {
    ///     let page = client.list_assets(options.clone())?;
    ///     for asset in &page.items {
    ///         println!("{} {}", asset.id, asset.name);
    ///     }
    ///     match page.next_cursor {
    ///         Some(cursor) => options = options.cursor(cursor),
    ///         None => break,
    ///     }
    /// }
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn list_assets(&self, options: AssetListOptions) -> Result<AssetPage> {
        let limit = options.limit.map(|l| l.to_string());
        let mut pairs: Vec<(&str, &str)> = Vec::new();
        if let Some(ref limit) = limit {
            pairs.push(("limit", limit));
        }
        for (name, value) in [
            ("cursor", &options.cursor),
            ("kind", &options.kind),
            ("media_type", &options.media_type),
            ("watermark_id", &options.watermark_id),
        ] {
            if let Some(value) = value {
                pairs.push((name, value));
            }
        }
        self.call_json(Method::GET, &with_query("assets", &pairs), None)
    }

    /// Read one asset record (`GET /assets/{id}`). Requires `assets:read`.
    pub fn get_asset(&self, id: &str) -> Result<Asset> {
        self.call_json(Method::GET, &path(id)?, None)
    }

    /// Rename an asset or replace its metadata (`PATCH /assets/{id}`).
    ///
    /// `changes` is a JSON object with `name` and/or `metadata`; `version` must
    /// be the asset's current version (HTTP 409 means reload and retry).
    /// Requires `assets:write`.
    pub fn update_asset(&self, id: &str, version: u64, changes: &Value) -> Result<Asset> {
        let mut body = changes
            .as_object()
            .ok_or_else(|| Error::input("Changes must be a JSON object"))?
            .clone();
        body.insert("version".into(), json!(version));
        self.call_json(Method::PATCH, &path(id)?, Some(Value::Object(body)))
    }

    /// Delete one asset (`DELETE /assets/{id}`). Requires `assets:delete`
    /// and owner/admin membership.
    pub fn delete_asset(&self, id: &str) -> Result<()> {
        self.call(Method::DELETE, &path(id)?, None)?;
        Ok(())
    }

    /// Atomically delete 1–50 assets (`POST /assets/bulk-delete`).
    pub fn delete_assets<S: AsRef<str>>(&self, ids: &[S]) -> Result<()> {
        if ids.is_empty() || ids.len() > 50 {
            return Err(Error::input("Provide 1–50 asset IDs"));
        }
        let ids: Vec<&str> = ids.iter().map(AsRef::as_ref).collect();
        for id in &ids {
            path(id)?;
        }
        self.call(
            Method::POST,
            "assets/bulk-delete",
            Some(json!({ "asset_ids": ids })),
        )?;
        Ok(())
    }

    /// Download an asset's file (`GET /assets/{id}/content`). HTTP 410 means
    /// the file expired while the record remains.
    pub fn download_asset(&self, id: &str) -> Result<Vec<u8>> {
        Ok(self
            .call(Method::GET, &format!("{}/content", path(id)?), None)?
            .1)
    }
}
