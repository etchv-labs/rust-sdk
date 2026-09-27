//! Official Rust client for the [Etchv](https://etchv.com) watermarking API.
//!
//! Embed and detect invisible forensic watermarks in images, PDFs and videos,
//! manage the asset library, webhooks and customer storage destinations.
//!
//! This is a **blocking**, server-side client. Call it from ordinary threads,
//! or from `tokio::task::spawn_blocking` inside an async runtime. Keep API keys
//! on your server.
//!
//! # Quickstart
//!
//! ```no_run
//! use etchv::{Client, Options};
//! use serde_json::json;
//!
//! fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::new(std::env::var("ETCHV_API_KEY")?)?;
//!
//!     // Embed forensic data; the result is the watermarked file in its original format.
//!     let embedded = client.embed_image(
//!         &std::fs::read("photo.jpg")?,
//!         &json!({"recipient": "customer-123"}),
//!         Options::new().filename("photo.jpg"),
//!     )?;
//!     std::fs::write(&embedded.filename, &embedded.bytes)?;
//!
//!     // Detect it again later.
//!     let detection = client.detect_image(&embedded.bytes, Options::new().filename("photo.jpg"))?;
//!     assert_eq!(detection.watermark_id.as_deref(), Some(embedded.watermark_id.as_str()));
//!     Ok(())
//! }
//! ```
//!
//! # Errors
//!
//! Every operation returns [`Result`]. [`Error`] carries the HTTP status (or `0`
//! for client-side failures), the Etchv request ID and the idempotency key used,
//! so interrupted work can be recovered without another charge.
#![warn(missing_docs)]
#![forbid(unsafe_code)]

use reqwest::{
    Method,
    blocking::{Client as Http, Response, multipart},
    header::{HeaderMap, HeaderValue},
};
use serde::{Deserialize, de::DeserializeOwned};
use serde_json::Value;
use std::{
    fmt,
    io::Read,
    thread::sleep,
    time::{Duration, Instant},
};

mod assets;
mod error;
mod storage;
mod webhooks;

pub use assets::{Asset, AssetListOptions, AssetPage};
pub use error::{Error, ErrorKind, Result};
pub use storage::{
    NewStorageDestination, StorageAttempt, StorageDelivery, StorageDeliveryPage,
    StorageDestination, StorageDestinationUpdate, StorageVisibility,
};
pub use webhooks::{
    WEBHOOK_TOLERANCE, WebhookAttempt, WebhookDelivery, WebhookDeliveryPage, WebhookEndpoint,
    WebhookEvent, verify_webhook_signature, verify_webhook_signature_at,
};

/// Version of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
/// `User-Agent` header sent with every request.
pub const USER_AGENT: &str = concat!("etchv-rust/", env!("CARGO_PKG_VERSION"));
/// Production API base URL used by [`Client::new`].
pub const DEFAULT_BASE_URL: &str = "https://api.etchv.com";
/// Default client deadline used by [`Client::new`].
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
/// Largest upload the API accepts (50 MB). The API rejects PDFs and videos
/// over 20 MB with status 413.
pub const MAX_FILE_SIZE: usize = 50 * 1024 * 1024;
/// Largest response or result file the SDK will buffer (256 MB).
pub const MAX_DOWNLOAD_SIZE: usize = 256 * 1024 * 1024;

/// Media family of an upload; selects the `/watermarks/{media}` endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Media {
    /// PNG, JPEG, WebP, GIF, TIFF, BMP, PPM, PSD or PSB (`/watermarks/images`).
    Image,
    /// PDF (`/watermarks/documents`).
    Document,
    /// MP4 or MOV with H.264 video (`/watermarks/videos`).
    Video,
}

impl Media {
    fn segment(self) -> &'static str {
        match self {
            Media::Image => "images",
            Media::Document => "documents",
            Media::Video => "videos",
        }
    }
    fn default_filename(self) -> &'static str {
        match self {
            Media::Image => "image.png",
            Media::Document => "document.pdf",
            Media::Video => "video.mp4",
        }
    }
}

/// Per-request options for watermarking and detection calls.
///
/// Build with [`Options::new`] and the chained setters:
///
/// ```
/// let options = etchv::Options::new()
///     .filename("report.pdf")
///     .idempotency_key("report-export-001");
/// ```
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct Options {
    /// Original filename sent with the upload. Defaults to a generic name for the media type.
    pub filename: Option<String>,
    /// `Idempotency-Key` header (8–128 letters, digits, `-` or `_`). Generated
    /// automatically for durable operations when absent. Persist your own key to
    /// recover a job across process restarts.
    pub idempotency_key: Option<String>,
    /// Verified customer storage destination (`dst_…`) for the watermarked result.
    /// Embedding only.
    pub storage_destination_id: Option<String>,
    /// Relative object key beneath the destination prefix. Requires
    /// `storage_destination_id`.
    pub storage_key: Option<String>,
}

impl Options {
    /// Empty options; equivalent to `Options::default()`.
    pub fn new() -> Self {
        Self::default()
    }
    /// Set the original filename.
    pub fn filename(mut self, filename: impl Into<String>) -> Self {
        self.filename = Some(filename.into());
        self
    }
    /// Set the idempotency key.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
    /// Deliver the watermarked result to a verified customer storage destination.
    pub fn storage_destination_id(mut self, id: impl Into<String>) -> Self {
        self.storage_destination_id = Some(id.into());
        self
    }
    /// Set the object key used in the storage destination.
    pub fn storage_key(mut self, key: impl Into<String>) -> Self {
        self.storage_key = Some(key.into());
        self
    }
}

/// A verified watermarked file returned by an embed call.
#[non_exhaustive]
pub struct EmbedResult {
    /// Watermarked file bytes in the original format.
    pub bytes: Vec<u8>,
    /// 64-character hexadecimal watermark ID. Store it with your record.
    pub watermark_id: String,
    /// Etchv request ID.
    pub request_id: Option<String>,
    /// MIME type of `bytes`.
    pub content_type: String,
    /// Safe filename suggested by the API.
    pub filename: String,
    /// Asset library ID of the watermarked output.
    pub asset_id: Option<String>,
    /// Asset library ID of the uploaded original.
    pub source_asset_id: Option<String>,
    /// Customer storage delivery ID when a destination was selected.
    pub storage_delivery_id: Option<String>,
}

impl fmt::Debug for EmbedResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EmbedResult")
            .field("bytes", &format_args!("<{} bytes>", self.bytes.len()))
            .field("watermark_id", &self.watermark_id)
            .field("request_id", &self.request_id)
            .field("content_type", &self.content_type)
            .field("filename", &self.filename)
            .field("asset_id", &self.asset_id)
            .field("source_asset_id", &self.source_asset_id)
            .field("storage_delivery_id", &self.storage_delivery_id)
            .finish()
    }
}

/// Detection result for one frame, page or composite.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct DetectionUnit {
    /// Zero-based frame or page index.
    pub index: usize,
    /// Whether a watermark was decoded in this unit.
    pub watermarked: bool,
    /// Decoding certainty from 0 to 1.
    pub confidence: f64,
    /// Decoded watermark ID, when found.
    pub watermark_id: Option<String>,
}

/// Result of a detection call.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct DetectionResult {
    /// `true` only when every unit decoded the same watermark.
    pub watermarked: bool,
    /// Lowest per-unit confidence (0–1).
    pub confidence: f64,
    /// Watermark ID shared by all units, when `watermarked`.
    pub watermark_id: Option<String>,
    /// Per-frame / per-page results.
    #[serde(default)]
    pub units: Vec<DetectionUnit>,
    /// Etchv request ID.
    #[serde(skip)]
    pub request_id: Option<String>,
}

/// Processing state of an asynchronous job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum JobStatus {
    /// Accepted and waiting for a worker.
    Queued,
    /// Being processed.
    Running,
    /// A processing attempt failed and will be retried.
    Retrying,
    /// Finished; fetch the result.
    Succeeded,
    /// Finished without a result; reserved credits were released.
    Failed,
    /// A state introduced after this SDK version.
    #[serde(other)]
    Unknown,
}

impl JobStatus {
    /// `true` for [`JobStatus::Succeeded`] and [`JobStatus::Failed`].
    pub fn is_terminal(self) -> bool {
        matches!(self, JobStatus::Succeeded | JobStatus::Failed)
    }
}

/// Job receipt returned by async submissions and job status endpoints.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct JobReceipt {
    /// Job ID (`req_…`).
    pub request_id: String,
    /// Current state.
    pub status: JobStatus,
    /// `embed` or `detect`.
    #[serde(default)]
    pub operation: String,
    /// Relative status path (requires the API key).
    #[serde(default)]
    pub status_url: String,
    /// Relative result path (requires the API key).
    #[serde(default)]
    pub result_url: String,
    /// Webhook endpoint selected at submission.
    #[serde(default)]
    pub webhook_id: Option<String>,
    /// Watermarked output asset ID, when available.
    #[serde(default)]
    pub asset_id: Option<String>,
    /// Original upload asset ID, when available.
    #[serde(default)]
    pub source_asset_id: Option<String>,
    /// Detected input format, for example `PNG` or `PDF`.
    #[serde(default)]
    pub format: String,
    /// Frames or pages in the upload.
    #[serde(default)]
    pub frame_count: u64,
    /// Credits reserved or charged.
    #[serde(default)]
    pub credits: u64,
    /// Processing attempts so far.
    #[serde(default)]
    pub attempts: u64,
    /// Failure reason when `status` is `failed`.
    #[serde(default)]
    pub error_code: Option<String>,
    /// When the saved result expires (ISO 8601).
    #[serde(default)]
    pub result_expires_at: Option<String>,
    /// `etchv` or the customer storage provider.
    #[serde(default)]
    pub storage_provider: Option<String>,
    /// Customer storage destination, if selected.
    #[serde(default)]
    pub storage_destination_id: Option<String>,
    /// Customer storage delivery, if selected.
    #[serde(default)]
    pub storage_delivery_id: Option<String>,
}

/// Identity of the API key, returned by [`Client::get_api_key_info`].
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct ApiKeyInfo {
    /// Organization that owns the key.
    pub organization_id: String,
    /// Key ID (not the secret).
    pub key_id: String,
    /// Granted scopes, for example `watermarks:embed`.
    pub scopes: Vec<String>,
}

/// Blocking Etchv API client.
///
/// Cheap to clone; clones share one connection pool.
#[derive(Clone)]
pub struct Client {
    key: HeaderValue,
    base: String,
    timeout: Duration,
    http: Http,
}

impl fmt::Debug for Client {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Client")
            .field("api_key", &"[redacted]")
            .field("base_url", &self.base)
            .field("timeout", &self.timeout)
            .finish()
    }
}

pub(crate) fn hex_id(id: &str, prefix: &str, len: usize) -> bool {
    id.strip_prefix(prefix).is_some_and(|s| {
        s.len() == len
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}
pub(crate) fn check_id(id: &str, prefix: &str, len: usize, what: &str) -> Result<()> {
    if hex_id(id, prefix, len) {
        Ok(())
    } else {
        Err(Error::input(format!("Invalid {what} ID")))
    }
}
fn valid_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
}
fn valid_job(s: &str) -> bool {
    hex_id(s, "req_", 64)
}
fn header(h: &HeaderMap, name: &str) -> Option<String> {
    h.get(name).and_then(|v| v.to_str().ok()).map(str::to_owned)
}
fn read_body(response: Response) -> std::io::Result<Vec<u8>> {
    // Read with a hard bound even when the server omits Content-Length.
    let mut bytes = Vec::new();
    response
        .take((MAX_DOWNLOAD_SIZE + 1) as u64)
        .read_to_end(&mut bytes)?;
    Ok(bytes)
}
fn retry_after(headers: &HeaderMap) -> Option<f64> {
    header(headers, "retry-after")
        .and_then(|s| s.trim().parse::<f64>().ok())
        .filter(|s| s.is_finite())
}
fn with_query(path: &str, pairs: &[(&str, &str)]) -> String {
    if pairs.is_empty() {
        return path.to_owned();
    }
    let query: String = reqwest::Url::parse("https://etchv.invalid/")
        .map(|mut u| {
            u.query_pairs_mut().extend_pairs(pairs);
            u.query().unwrap_or_default().to_owned()
        })
        .unwrap_or_default();
    format!("{path}?{query}")
}

struct Upload<'a> {
    file: &'a [u8],
    filename: String,
    data: Option<String>,
}

impl Client {
    /// Create a client for the production API with the default 120-second deadline.
    ///
    /// ```no_run
    /// let client = etchv::Client::new(std::env::var("ETCHV_API_KEY")?)?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// ```
    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        Self::with_options(api_key, DEFAULT_BASE_URL, DEFAULT_TIMEOUT)
    }

    /// Create a client with a custom base URL and overall deadline.
    ///
    /// The base URL must use HTTPS; plain HTTP is allowed only for `localhost`,
    /// `127.0.0.1` and `[::1]`. The deadline bounds each SDK call including
    /// retries and polling. Redirects are never followed.
    pub fn with_options(api_key: impl Into<String>, base: &str, timeout: Duration) -> Result<Self> {
        let key: String = api_key.into();
        let u = reqwest::Url::parse(base).map_err(|_| Error::input("Invalid base URL"))?;
        let local = matches!(u.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if key.is_empty()
            || !key.bytes().all(|b| b.is_ascii_graphic())
            || timeout.is_zero()
            || u.host_str().is_none()
            || !u.username().is_empty()
            || u.password().is_some()
            || u.query().is_some()
            || u.fragment().is_some()
            || !(u.scheme() == "https" || (u.scheme() == "http" && local))
        {
            return Err(Error::input(
                "API key (printable ASCII), positive timeout and HTTPS base URL required; HTTP allowed for localhost",
            ));
        }
        let mut key = HeaderValue::from_str(&key)
            .map_err(|_| Error::input("API key contains invalid characters"))?;
        key.set_sensitive(true);
        let http = Http::builder()
            .user_agent(USER_AGENT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(Error::transport)?;
        Ok(Self {
            key,
            base: base.trim_end_matches('/').into(),
            timeout,
            http,
        })
    }

    /// Check the API key without consuming credits (`GET /auth/api-key`).
    ///
    /// Returns the owning organization, key ID and granted scopes. Useful as a
    /// connection test during setup.
    ///
    /// ```no_run
    /// # let client = etchv::Client::new("etchv_...")?;
    /// let info = client.get_api_key_info()?;
    /// println!("{} has scopes {:?}", info.key_id, info.scopes);
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn get_api_key_info(&self) -> Result<ApiKeyInfo> {
        self.call_json(Method::GET, "auth/api-key", None)
    }

    /// Embed forensic `data` in an image and wait for the verified result.
    ///
    /// `data` must be a non-empty JSON object; its SHA-256 digest is embedded.
    /// Transient failures are retried with the same idempotency key and pending
    /// jobs are polled until the client deadline.
    pub fn embed_image(&self, file: &[u8], data: &Value, options: Options) -> Result<EmbedResult> {
        self.embed(Media::Image, file, data, options)
    }
    /// Embed forensic `data` in a PDF and wait for the verified result.
    pub fn embed_document(
        &self,
        file: &[u8],
        data: &Value,
        options: Options,
    ) -> Result<EmbedResult> {
        self.embed(Media::Document, file, data, options)
    }
    /// Embed forensic `data` in a video and wait for the verified result.
    pub fn embed_video(&self, file: &[u8], data: &Value, options: Options) -> Result<EmbedResult> {
        self.embed(Media::Video, file, data, options)
    }
    /// Detect a watermark in an image. Synchronous and not retried automatically.
    pub fn detect_image(&self, file: &[u8], options: Options) -> Result<DetectionResult> {
        self.detect(Media::Image, file, options)
    }
    /// Detect watermarks in each page of a PDF. Synchronous and not retried automatically.
    pub fn detect_document(&self, file: &[u8], options: Options) -> Result<DetectionResult> {
        self.detect(Media::Document, file, options)
    }
    /// Detect watermarks in each frame of a video, retrying and polling like embedding.
    pub fn detect_video(&self, file: &[u8], options: Options) -> Result<DetectionResult> {
        self.detect(Media::Video, file, options)
    }

    /// Submit a background embedding job (`POST /watermarks/{media}/async`)
    /// and return its receipt without polling.
    ///
    /// Pass a webhook endpoint ID (`wh_…`) to receive a signed terminal event.
    ///
    /// ```no_run
    /// use etchv::{Media, Options};
    /// # let client = etchv::Client::new("etchv_...")?;
    /// # let pdf = Vec::new();
    /// let job = client.submit_embed(
    ///     Media::Document,
    ///     &pdf,
    ///     &serde_json::json!({"delivery": "delivery_001"}),
    ///     Options::new().filename("document.pdf").idempotency_key("delivery_001"),
    ///     None,
    /// )?;
    /// let status = client.get_job(&job.request_id)?;
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn submit_embed(
        &self,
        media: Media,
        file: &[u8],
        data: &Value,
        options: Options,
        webhook_id: Option<&str>,
    ) -> Result<JobReceipt> {
        let data = embed_data(data)?;
        self.submit(media, file, Some(data), options, webhook_id)
    }

    /// Submit a background detection job (`POST /watermarks/{media}/detect/async`)
    /// and return its receipt without polling.
    pub fn submit_detection(
        &self,
        media: Media,
        file: &[u8],
        options: Options,
        webhook_id: Option<&str>,
    ) -> Result<JobReceipt> {
        self.submit(media, file, None, options, webhook_id)
    }

    /// Read an embedding job's status (`GET /watermarks/jobs/{id}`).
    pub fn get_job(&self, request_id: &str) -> Result<JobReceipt> {
        self.job_status(request_id, "jobs")
    }
    /// Read a detection job's status (`GET /watermarks/detection-jobs/{id}`).
    pub fn get_detection_job(&self, request_id: &str) -> Result<JobReceipt> {
        self.job_status(request_id, "detection-jobs")
    }

    /// Download an embedding job's result (`GET /watermarks/jobs/{id}/result`),
    /// polling while the job is still processing (HTTP 202) until the client
    /// deadline. A result that expired or whose asset was deleted fails with
    /// HTTP 410 (see [`Error::is_gone`]).
    pub fn get_embed_result(&self, request_id: &str) -> Result<EmbedResult> {
        check_id(request_id, "req_", 64, "request")?;
        let (b, h) = self.media_request(
            format!("watermarks/jobs/{request_id}/result"),
            None,
            None,
            true,
            false,
        )?;
        embedding(b, h)
    }
    /// Fetch a detection job's result (`GET /watermarks/detection-jobs/{id}/result`),
    /// polling while it is still processing. See [`Client::get_embed_result`].
    pub fn get_detection_result(&self, request_id: &str) -> Result<DetectionResult> {
        check_id(request_id, "req_", 64, "request")?;
        let (b, h) = self.media_request(
            format!("watermarks/detection-jobs/{request_id}/result"),
            None,
            None,
            true,
            true,
        )?;
        detection(&b, &h)
    }

    fn job_status(&self, request_id: &str, kind: &str) -> Result<JobReceipt> {
        check_id(request_id, "req_", 64, "request")?;
        self.call_json(
            Method::GET,
            &format!("watermarks/{kind}/{request_id}"),
            None,
        )
    }

    fn embed(
        &self,
        media: Media,
        file: &[u8],
        data: &Value,
        options: Options,
    ) -> Result<EmbedResult> {
        let data = embed_data(data)?;
        let (b, h) = self.post(media, file, Some(data), options)?;
        embedding(b, h)
    }
    fn detect(&self, media: Media, file: &[u8], options: Options) -> Result<DetectionResult> {
        let (b, h) = self.post(media, file, None, options)?;
        detection(&b, &h)
    }

    fn submit(
        &self,
        media: Media,
        file: &[u8],
        data: Option<String>,
        options: Options,
        webhook_id: Option<&str>,
    ) -> Result<JobReceipt> {
        if let Some(id) = webhook_id {
            check_id(id, "wh_", 32, "webhook")?;
        }
        let detect = data.is_none();
        let webhook: Vec<(&str, &str)> = webhook_id
            .map(|id| ("webhook_id", id))
            .into_iter()
            .collect();
        let (path, upload, key) =
            prepare(media, file, data, options, true, detect, Some(&webhook))?;
        let (bytes, _) = self.media_request(path, Some(upload), key, true, detect)?;
        serde_json::from_slice(&bytes).map_err(|e| Error::decode(202, e))
    }

    fn post(
        &self,
        media: Media,
        file: &[u8],
        data: Option<String>,
        options: Options,
    ) -> Result<(Vec<u8>, HeaderMap)> {
        let detect = data.is_none();
        // Embedding and video detection are durable jobs; image/PDF detection is synchronous.
        let durable = !detect || media == Media::Video;
        let (path, upload, key) = prepare(media, file, data, options, durable, detect, None)?;
        self.media_request(
            path,
            Some(upload),
            key,
            durable,
            detect && media == Media::Video,
        )
    }

    /// Upload/poll loop shared by media operations.
    fn media_request(
        &self,
        mut path: String,
        mut upload: Option<Upload<'_>>,
        idempotency_key: Option<String>,
        durable: bool,
        detection_job: bool,
    ) -> Result<(Vec<u8>, HeaderMap)> {
        let is_async = path.split('?').next().unwrap_or("").ends_with("/async");
        let started = Instant::now();
        let mut request_id = None;
        let fail = |mut e: Error, request_id: Option<String>| {
            e.request_id = e.request_id.take().or(request_id);
            e.idempotency_key = idempotency_key.clone();
            e
        };
        let pause = |seconds: f64| {
            sleep(
                Duration::from_secs_f64(seconds.clamp(0.01, 5.0))
                    .min(self.timeout.saturating_sub(started.elapsed())),
            )
        };
        while started.elapsed() < self.timeout {
            let url = format!("{}/{}", self.base, path);
            let mut req = if let Some(ref up) = upload {
                let part = multipart::Part::bytes(up.file.to_vec()).file_name(up.filename.clone());
                let mut form = multipart::Form::new().part("file", part);
                if let Some(ref value) = up.data {
                    form = form.text("data", value.clone())
                }
                self.http.post(url).multipart(form)
            } else {
                self.http.get(url)
            };
            req = req.header("X-API-Key", self.key.clone()).timeout(
                self.timeout
                    .saturating_sub(started.elapsed())
                    .max(Duration::from_millis(1)),
            );
            if let Some(ref key) = idempotency_key {
                req = req.header("Idempotency-Key", key)
            }
            let response = match req.send() {
                Ok(r) => r,
                Err(e) => {
                    if !durable || e.is_builder() {
                        return Err(fail(Error::transport(e), request_id));
                    }
                    pause(1.0);
                    continue;
                }
            };
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            request_id = header(&headers, "x-request-id").or(request_id);
            let bytes = match read_body(response) {
                Ok(b) => b,
                Err(e) => {
                    if !durable {
                        return Err(fail(Error::transport(e), request_id));
                    }
                    pause(1.0);
                    continue;
                }
            };
            if bytes.len() > MAX_DOWNLOAD_SIZE {
                return Err(fail(
                    Error::response(status, "Response exceeds the SDK size limit"),
                    request_id,
                ));
            }
            if status == 200 || (status == 202 && is_async) {
                return Ok((bytes, headers));
            }
            let detail: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            if durable && status == 202 {
                let Some(id) = detail["request_id"].as_str().filter(|id| valid_job(id)) else {
                    return Err(fail(
                        Error::response(202, "Invalid job response"),
                        request_id,
                    ));
                };
                request_id = Some(id.into());
                // Never follow server-provided URLs; build the path from a validated ID.
                path = format!(
                    "watermarks/{}/{id}/result",
                    if detection_job {
                        "detection-jobs"
                    } else {
                        "jobs"
                    }
                );
                upload = None;
                pause(retry_after(&headers).unwrap_or(1.0));
                continue;
            }
            if durable && [429, 502, 503, 504].contains(&status) && detail["status"] != "failed" {
                pause(retry_after(&headers).unwrap_or(1.0));
                continue;
            }
            let body = String::from_utf8_lossy(&bytes[..bytes.len().min(10000)]).into_owned();
            return Err(fail(Error::new(ErrorKind::Api, status, body), request_id));
        }
        Err(fail(
            Error::new(
                ErrorKind::Timeout,
                0,
                "Client deadline exceeded; job may still complete",
            ),
            request_id,
        ))
    }

    /// Single JSON/binary request used by management endpoints. Not retried.
    pub(crate) fn call(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<(u16, Vec<u8>)> {
        let mut request = self
            .http
            .request(method, format!("{}/{path}", self.base))
            .header("X-API-Key", self.key.clone())
            .timeout(self.timeout);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().map_err(|e| {
            if e.is_timeout() {
                Error::new(ErrorKind::Timeout, 0, "Client deadline exceeded")
            } else {
                Error::transport(e)
            }
        })?;
        let status = response.status().as_u16();
        let request_id = header(response.headers(), "x-request-id");
        let bytes = read_body(response).map_err(|e| {
            let mut err = Error::transport(e);
            err.request_id = request_id.clone();
            err
        })?;
        if bytes.len() > MAX_DOWNLOAD_SIZE {
            return Err(Error::response(
                status,
                "Response exceeds the SDK size limit",
            ));
        }
        if !(200..300).contains(&status) {
            let mut err = Error::new(
                ErrorKind::Api,
                status,
                String::from_utf8_lossy(&bytes[..bytes.len().min(10000)]).into_owned(),
            );
            err.request_id = request_id;
            return Err(err);
        }
        Ok((status, bytes))
    }

    pub(crate) fn call_json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<Value>,
    ) -> Result<T> {
        let (status, bytes) = self.call(method, path, body)?;
        serde_json::from_slice(&bytes).map_err(|e| Error::decode(status, e))
    }
}

fn embed_data(data: &Value) -> Result<String> {
    if !data.as_object().is_some_and(|v| !v.is_empty()) {
        return Err(Error::input("data must be a non-empty JSON object"));
    }
    Ok(data.to_string())
}

fn valid_idempotency_key(key: &str) -> bool {
    (8..=128).contains(&key.len())
        && key
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

/// Validate a media upload and build its path, upload body and idempotency key.
fn prepare<'a>(
    media: Media,
    file: &'a [u8],
    data: Option<String>,
    options: Options,
    durable: bool,
    detect: bool,
    // `Some` selects the `/async` endpoint with these extra query pairs.
    async_query: Option<&[(&str, &str)]>,
) -> Result<(String, Upload<'a>, Option<String>)> {
    if file.is_empty() || file.len() > MAX_FILE_SIZE {
        return Err(Error::input("file must contain 1 byte to 50 MB"));
    }
    let mut key = options.idempotency_key.filter(|k| !k.is_empty());
    if let Some(ref k) = key
        && !valid_idempotency_key(k)
    {
        return Err(Error::input(
            "Idempotency key must contain 8–128 letters, digits, hyphens or underscores",
        ));
    }
    if durable && key.is_none() {
        key = Some(uuid::Uuid::new_v4().to_string());
    }
    if options.storage_key.is_some() && options.storage_destination_id.is_none() {
        return Err(Error::input("Storage key requires a storage destination"));
    }
    let is_async = async_query.is_some();
    let mut pairs: Vec<(&str, &str)> = async_query.unwrap_or_default().to_vec();
    if let Some(ref id) = options.storage_destination_id {
        if detect {
            return Err(Error::input(
                "Storage destinations apply to embedding requests only",
            ));
        }
        check_id(id, "dst_", 32, "storage destination")?;
        pairs.push(("storage_destination_id", id));
        if let Some(ref k) = options.storage_key {
            pairs.push(("storage_key", k));
        }
    }
    let path = format!(
        "watermarks/{}{}{}",
        media.segment(),
        if detect { "/detect" } else { "" },
        if is_async { "/async" } else { "" }
    );
    let path = with_query(&path, &pairs);
    let upload = Upload {
        file,
        filename: options
            .filename
            .filter(|f| !f.is_empty())
            .unwrap_or_else(|| media.default_filename().into()),
        data,
    };
    Ok((path, upload, key))
}

fn embedding(bytes: Vec<u8>, headers: HeaderMap) -> Result<EmbedResult> {
    let mime = header(&headers, "content-type")
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_owned();
    let id = header(&headers, "x-watermark-id").unwrap_or_default();
    let request_id = header(&headers, "x-request-id");
    let Some(ext) = extension(&bytes, &mime).filter(|_| valid_id(&id)) else {
        let mut err = Error::response(200, "Invalid embedding response");
        err.request_id = request_id;
        return Err(err);
    };
    let disposition = header(&headers, "content-disposition").unwrap_or_default();
    let filename = disposition
        .split("filename=\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .filter(|s| {
            !s.is_empty()
                && !s.starts_with('.')
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        })
        .map(str::to_owned)
        .unwrap_or_else(|| format!("watermarked.{ext}"));
    Ok(EmbedResult {
        bytes,
        watermark_id: id,
        request_id,
        content_type: mime,
        filename,
        asset_id: header(&headers, "x-asset-id"),
        source_asset_id: header(&headers, "x-source-asset-id"),
        storage_delivery_id: header(&headers, "x-storage-delivery-id"),
    })
}

fn valid_detection(v: &Value) -> bool {
    let Some(w) = v["watermarked"].as_bool() else {
        return false;
    };
    let Some(c) = v["confidence"].as_f64() else {
        return false;
    };
    (0.0..=1.0).contains(&c)
        && v.get("watermark_id").is_some()
        && if w {
            v["watermark_id"].as_str().is_some_and(valid_id)
        } else {
            v["watermark_id"].is_null()
        }
}

fn detection(b: &[u8], h: &HeaderMap) -> Result<DetectionResult> {
    let request_id = header(h, "x-request-id");
    let invalid = |msg: &str| {
        let mut err = Error::response(200, msg);
        err.request_id = request_id.clone();
        err
    };
    let mut v: Value = serde_json::from_slice(b).map_err(|_| invalid("Invalid detection JSON"))?;
    if !valid_detection(&v) {
        return Err(invalid("Invalid detection response"));
    }
    if v.get("units").is_none() {
        let mut unit = v.clone();
        unit["index"] = 0.into();
        v["units"] = serde_json::json!([unit])
    }
    if !v["units"].as_array().is_some_and(|a| {
        !a.is_empty()
            && a.iter()
                .enumerate()
                .all(|(i, u)| valid_detection(u) && u["index"].as_u64() == Some(i as u64))
    }) {
        return Err(invalid("Invalid detection units"));
    }
    let mut d: DetectionResult =
        serde_json::from_value(v).map_err(|_| invalid("Invalid detection response"))?;
    d.request_id = request_id;
    Ok(d)
}

fn extension(b: &[u8], mime: &str) -> Option<&'static str> {
    match mime {
        "image/png" if b.starts_with(b"\x89PNG\r\n\x1a\n") => Some("png"),
        "image/jpeg" if b.starts_with(b"\xff\xd8\xff") => Some("jpg"),
        "image/gif" if b.starts_with(b"GIF87a") || b.starts_with(b"GIF89a") => Some("gif"),
        "image/tiff" if b.starts_with(b"II*\0") || b.starts_with(b"MM\0*") => Some("tiff"),
        "image/bmp" if b.starts_with(b"BM") => Some("bmp"),
        "image/x-portable-pixmap" if b.starts_with(b"P6") || b.starts_with(b"P3") => Some("ppm"),
        "image/webp" if b.starts_with(b"RIFF") && b.get(8..12) == Some(b"WEBP") => Some("webp"),
        "image/vnd.adobe.photoshop" if b.starts_with(b"8BPS\0\x01") => Some("psd"),
        "image/vnd.adobe.photoshop" if b.starts_with(b"8BPS\0\x02") => Some("psb"),
        "application/pdf" if b.starts_with(b"%PDF-") => Some("pdf"),
        "video/mp4" if b.get(4..8) == Some(b"ftyp") => Some("mp4"),
        "video/quicktime" if b.get(4..8) == Some(b"ftyp") => Some("mov"),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn user_agent_carries_version() {
        assert_eq!(VERSION, "1.0.0");
        assert_eq!(USER_AGENT, "etchv-rust/1.0.0");
    }

    #[test]
    fn debug_redacts_api_key() {
        let c = Client::new("etchv_secret_value").unwrap();
        let text = format!("{c:?}");
        assert!(!text.contains("etchv_secret_value"));
        assert!(text.contains("[redacted]"));
    }

    #[test]
    fn query_encoding() {
        assert_eq!(
            with_query("x", &[("storage_key", "a b/#c")]),
            "x?storage_key=a+b%2F%23c"
        );
    }
}
