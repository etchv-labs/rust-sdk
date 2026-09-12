use crate::{Client, Error, MAX_FILE_SIZE, Result, error};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Read;

#[derive(Debug, Deserialize)]
pub struct Asset {
    pub storage_provider: Option<String>,
    pub storage_status: Option<String>,
    pub storage_destination_id: Option<String>,
    pub storage_delivery_id: Option<String>,
    pub staging_expires_at: Option<String>,
    pub staging_deleted_at: Option<String>,
    pub id: String,
    pub name: String,
    pub kind: String,
    pub media_type: String,
    pub format: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub sha256: String,
    pub parent_asset_id: Option<String>,
    pub request_id: String,
    pub watermark_id: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    pub file_expires_at: Option<String>,
    pub file_available: bool,
    pub version: u64,
    pub metadata: Option<Value>,
    pub download_url: Option<String>,
}
#[derive(Debug, Deserialize)]
pub struct AssetPage {
    pub items: Vec<Asset>,
    pub next_cursor: Option<String>,
}
#[derive(Default)]
pub struct AssetListOptions {
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub kind: Option<String>,
    pub media_type: Option<String>,
    pub watermark_id: Option<String>,
}
fn path(id: &str) -> Result<String> {
    if !id.strip_prefix("ast_").is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    }) {
        return Err(error(0, "Invalid asset ID"));
    }
    Ok(format!("assets/{id}"))
}
impl Client {
    pub fn get_storage_delivery(&self, id: &str) -> Result<Value> {
        if id.len() != 68
            || !id.starts_with("std_")
            || !id[4..]
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(error(0, "Invalid storage delivery ID"));
        }
        let bytes = self.asset_request(&format!("storage/deliveries/{id}"), Method::GET, None)?;
        serde_json::from_slice(&bytes).map_err(|e| error(0, e))
    }
    fn asset_request(&self, path: &str, method: Method, body: Option<Value>) -> Result<Vec<u8>> {
        let mut request = self
            .http
            .request(method, format!("{}/{path}", self.base))
            .header("X-API-Key", &self.key)
            .timeout(self.timeout);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().map_err(|e| error(0, e))?;
        let status = response.status().as_u16();
        let request_id = response
            .headers()
            .get("x-request-id")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        let mut bytes = Vec::new();
        response
            .take((MAX_FILE_SIZE + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| error(0, e))?;
        if bytes.len() > MAX_FILE_SIZE {
            return Err(error(status, "Response exceeds 20 MB"));
        }
        if status != 200 && status != 204 {
            return Err(Error {
                status_code: status,
                detail: String::from_utf8_lossy(&bytes[..bytes.len().min(10000)]).into_owned(),
                request_id,
                idempotency_key: None,
            });
        }
        Ok(bytes)
    }
    pub fn list_assets(&self, options: AssetListOptions) -> Result<AssetPage> {
        let mut url =
            reqwest::Url::parse(&format!("{}/assets", self.base)).map_err(|e| error(0, e))?;
        {
            let mut query = url.query_pairs_mut();
            if let Some(limit) = options.limit {
                query.append_pair("limit", &limit.to_string());
            }
            for (name, value) in [
                ("cursor", options.cursor),
                ("kind", options.kind),
                ("media_type", options.media_type),
                ("watermark_id", options.watermark_id),
            ] {
                if let Some(value) = value {
                    query.append_pair(name, &value);
                }
            }
        }
        let target = format!("assets?{}", url.query().unwrap_or(""));
        serde_json::from_slice(&self.asset_request(&target, Method::GET, None)?)
            .map_err(|e| error(200, e))
    }
    pub fn get_asset(&self, id: &str) -> Result<Asset> {
        serde_json::from_slice(&self.asset_request(&path(id)?, Method::GET, None)?)
            .map_err(|e| error(200, e))
    }
    pub fn update_asset(&self, id: &str, version: u64, changes: &Value) -> Result<Asset> {
        let mut body = changes
            .as_object()
            .ok_or_else(|| error(0, "Changes must be an object"))?
            .clone();
        body.insert("version".into(), json!(version));
        serde_json::from_slice(&self.asset_request(
            &path(id)?,
            Method::PATCH,
            Some(Value::Object(body)),
        )?)
        .map_err(|e| error(200, e))
    }
    pub fn delete_asset(&self, id: &str) -> Result<()> {
        self.asset_request(&path(id)?, Method::DELETE, None)?;
        Ok(())
    }
    pub fn delete_assets(&self, ids: &[String]) -> Result<()> {
        if ids.is_empty() || ids.len() > 50 {
            return Err(error(0, "Provide 1–50 asset IDs"));
        }
        for id in ids {
            path(id)?;
        }
        self.asset_request(
            "assets/bulk-delete",
            Method::POST,
            Some(json!({"asset_ids":ids})),
        )?;
        Ok(())
    }
    pub fn download_asset(&self, id: &str) -> Result<Vec<u8>> {
        self.asset_request(&format!("{}/content", path(id)?), Method::GET, None)
    }
}
