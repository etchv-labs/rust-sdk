//! Batches: up to 100 files per call, each with its own forensic data.
use crate::{
    Accelerator, Client, EmbedResult, Error, ErrorKind, Result, check_id, embed_data, header,
    rate_limit_delay, read_body, retry_after, signed::UploadBody, signed_url_ok,
    valid_idempotency_key, with_query,
};
use reqwest::{Method, blocking::multipart, header::HeaderMap};
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::{
    fmt,
    io::{Read, Write},
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread::sleep,
    time::{Duration, Instant},
};

/// Most files in one batch (100).
pub const MAX_BATCH_ITEMS: usize = 100;
/// Largest zip [`Client::submit_batch_zip`] sends (55 MB).
pub const MAX_BATCH_ZIP_SIZE: usize = 55 * 1024 * 1024;
/// Largest batch archive [`Client::download_batch_archive`] reads: 1 GiB of
/// results plus 64 MiB for the zip structure and `manifest.json`.
pub const MAX_BATCH_ARCHIVE_SIZE: u64 = 1024 * 1024 * 1024 + 64 * 1024 * 1024;
/// Files uploaded at once by [`Client::submit_batch`] unless
/// [`BatchOptions::upload_concurrency`] says otherwise.
pub const DEFAULT_UPLOAD_CONCURRENCY: usize = 4;
/// How long [`Client::wait_for_batch`], [`Client::batch_results`] and
/// [`Client::download_batch_archive`] wait when given a zero timeout (1 hour).
pub const DEFAULT_BATCH_WAIT: Duration = Duration::from_secs(3600);
/// Poll delay when the API sends no `Retry-After`.
const DEFAULT_POLL_SECONDS: f64 = 2.0;
/// Shortest poll delay, whatever `Retry-After` says: one request per second at most.
const MIN_POLL_SECONDS: f64 = 1.0;

/// Where a [`BatchItem`]'s bytes come from.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BatchFile {
    /// The file's bytes, shared (not copied) by every upload attempt.
    Bytes(Arc<[u8]>),
    /// A file on disk, streamed when it is uploaded.
    Path(PathBuf),
}

/// One file of a batch, with its own forensic data.
///
/// ```
/// use serde_json::json;
/// let from_memory = etchv::BatchItem::new("logo.png", vec![0x89, b'P'], json!({"recipient": "acme"}));
/// let from_disk = etchv::BatchItem::from_path("in/contract-acme.pdf", json!({"recipient": "acme"}));
/// assert_eq!(from_disk.filename, "contract-acme.pdf");
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BatchItem {
    /// The file's name; its extension sets the media type (images, `pdf`, `mp4` or `mov`).
    pub filename: String,
    /// Non-empty JSON object embedded for this file (up to 8 KB as JSON).
    pub data: Value,
    /// The file itself.
    pub file: BatchFile,
}

impl BatchItem {
    /// A file held in memory (a `Vec<u8>`, `&[u8]` or `Arc<[u8]>`).
    pub fn new(filename: impl Into<String>, bytes: impl Into<Arc<[u8]>>, data: Value) -> Self {
        Self {
            filename: filename.into(),
            data,
            file: BatchFile::Bytes(bytes.into()),
        }
    }
    /// A file on disk, named after the path's last component.
    pub fn from_path(path: impl Into<PathBuf>, data: Value) -> Self {
        let path = path.into();
        let filename = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_default();
        Self {
            filename,
            data,
            file: BatchFile::Path(path),
        }
    }
    /// Override the filename sent to the API.
    pub fn filename(mut self, filename: impl Into<String>) -> Self {
        self.filename = filename.into();
        self
    }
    fn size(&self) -> Result<u64> {
        match &self.file {
            BatchFile::Bytes(bytes) => Ok(bytes.len() as u64),
            BatchFile::Path(path) => std::fs::metadata(path)
                .map(|m| m.len())
                .map_err(|e| Error::input(format!("{}: {e}", path.display()))),
        }
    }
    /// A fresh reader for one upload attempt. A file on disk whose length no
    /// longer matches the size sent at creation fails at once.
    fn body(&self, size: u64) -> Result<UploadBody> {
        match &self.file {
            BatchFile::Bytes(bytes) => {
                Ok((Box::new(std::io::Cursor::new(Arc::clone(bytes))), size))
            }
            BatchFile::Path(path) => {
                let failed = |e: std::io::Error| Error::input(format!("{}: {e}", path.display()));
                let file = std::fs::File::open(path).map_err(failed)?;
                let length = file.metadata().map_err(failed)?.len();
                if length != size {
                    return Err(Error::input(format!(
                        "{} changed size after the batch was created ({size} bytes, now {length})",
                        path.display()
                    )));
                }
                Ok((Box::new(file), size))
            }
        }
    }
}

/// One member of a zip sent to [`Client::submit_batch_zip`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct BatchZipItem {
    /// The member's path inside the zip, exactly as stored.
    pub filename: String,
    /// Non-empty JSON object embedded for this file.
    pub data: Value,
}

impl BatchZipItem {
    /// A zip member and its forensic data.
    pub fn new(filename: impl Into<String>, data: Value) -> Self {
        Self {
            filename: filename.into(),
            data,
        }
    }
}

/// Options for [`Client::submit_batch`] and [`Client::submit_batch_zip`].
///
/// ```
/// let options = etchv::BatchOptions::new()
///     .archive(true)
///     .idempotency_key("contracts-2026-10")
///     .upload_concurrency(8);
/// ```
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct BatchOptions {
    /// Also zip every result into one download ([`Client::download_batch_archive`]).
    pub archive: bool,
    /// Webhook endpoint (`wh_…`) that receives one `watermark.batch.*` event when the batch ends.
    pub webhook_id: Option<String>,
    /// Processing hardware for every file. See [`Accelerator`].
    pub accelerator: Option<Accelerator>,
    /// Verified customer storage destination (`dst_…`) for every result. Not with `archive`.
    pub storage_destination_id: Option<String>,
    /// `Idempotency-Key` (8–128 letters, digits, `-` or `_`); generated when absent.
    /// Submitting again with the same key returns the same batch and resumes its uploads.
    pub idempotency_key: Option<String>,
    /// Files uploaded at once (default [`DEFAULT_UPLOAD_CONCURRENCY`]).
    pub upload_concurrency: Option<usize>,
}

impl BatchOptions {
    /// Default options; equivalent to `BatchOptions::default()`.
    pub fn new() -> Self {
        Self::default()
    }
    /// Also zip every result into one download.
    pub fn archive(mut self, archive: bool) -> Self {
        self.archive = archive;
        self
    }
    /// Send a `watermark.batch.*` event to this webhook endpoint when the batch ends.
    pub fn webhook_id(mut self, id: impl Into<String>) -> Self {
        self.webhook_id = Some(id.into());
        self
    }
    /// Request CPU or GPU processing for every file.
    pub fn accelerator(mut self, accelerator: Accelerator) -> Self {
        self.accelerator = Some(accelerator);
        self
    }
    /// Deliver every result to a verified customer storage destination.
    pub fn storage_destination_id(mut self, id: impl Into<String>) -> Self {
        self.storage_destination_id = Some(id.into());
        self
    }
    /// Set the idempotency key.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }
    /// Upload this many files at once (at least 1).
    pub fn upload_concurrency(mut self, files: usize) -> Self {
        self.upload_concurrency = Some(files);
        self
    }

    /// Validate the options and return the request fields and idempotency key.
    fn prepare(&self) -> Result<(Map<String, Value>, String)> {
        let mut body = Map::new();
        body.insert("archive".into(), json!(self.archive));
        if let Some(ref id) = self.webhook_id {
            check_id(id, "wh_", 32, "webhook")?;
            body.insert("webhook_id".into(), json!(id));
        }
        if let Some(accelerator) = self.accelerator {
            body.insert("accelerator".into(), json!(accelerator.as_str()));
        }
        if let Some(ref id) = self.storage_destination_id {
            check_id(id, "dst_", 32, "storage destination")?;
            body.insert("storage_destination_id".into(), json!(id));
        }
        if self.upload_concurrency == Some(0) {
            return Err(Error::input("upload concurrency must be at least 1"));
        }
        let key = match self.idempotency_key.as_deref().filter(|k| !k.is_empty()) {
            Some(key) if !valid_idempotency_key(key) => {
                return Err(Error::input(
                    "Idempotency key must contain 8–128 letters, digits, hyphens or underscores",
                ));
            }
            Some(key) => key.to_owned(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        Ok((body, key))
    }
}

/// State of a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum BatchStatus {
    /// Created; waiting for uploads and the start call.
    Draft,
    /// Started; files are being checked and admitted.
    Starting,
    /// Files are being watermarked.
    Processing,
    /// The archive is being assembled.
    Assembling,
    /// Finished with at least one file watermarked.
    Completed,
    /// Finished without any file watermarked.
    Failed,
    /// Canceled; files not yet running were refunded.
    Cancelled,
    /// A draft that was not started within 24 hours.
    Expired,
    /// A state introduced after this SDK version.
    #[serde(other)]
    Unknown,
}

impl BatchStatus {
    /// `true` once the batch can no longer change: completed, failed, canceled or expired.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            BatchStatus::Completed
                | BatchStatus::Failed
                | BatchStatus::Cancelled
                | BatchStatus::Expired
        )
    }
    /// Wire value, for example `processing`.
    pub fn as_str(self) -> &'static str {
        match self {
            BatchStatus::Draft => "draft",
            BatchStatus::Starting => "starting",
            BatchStatus::Processing => "processing",
            BatchStatus::Assembling => "assembling",
            BatchStatus::Completed => "completed",
            BatchStatus::Failed => "failed",
            BatchStatus::Cancelled => "cancelled",
            BatchStatus::Expired => "expired",
            BatchStatus::Unknown => "unknown",
        }
    }
}

impl fmt::Display for BatchStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// State of one file in a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum BatchItemState {
    /// Waiting for its upload or for the batch to start.
    Pending,
    /// Not admitted (see `error_code`); never charged.
    Rejected,
    /// Admitted and waiting for a worker.
    Queued,
    /// Being watermarked.
    Running,
    /// A processing attempt failed and will be retried.
    Retrying,
    /// Watermarked; download it from `result_url`.
    Succeeded,
    /// Finished without a result; credits refunded.
    Failed,
    /// A state introduced after this SDK version.
    #[serde(other)]
    Unknown,
}

impl BatchItemState {
    /// Wire value, for example `rejected`.
    pub fn as_str(self) -> &'static str {
        match self {
            BatchItemState::Pending => "pending",
            BatchItemState::Rejected => "rejected",
            BatchItemState::Queued => "queued",
            BatchItemState::Running => "running",
            BatchItemState::Retrying => "retrying",
            BatchItemState::Succeeded => "succeeded",
            BatchItemState::Failed => "failed",
            BatchItemState::Unknown => "unknown",
        }
    }
}

/// File counts of a batch.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct BatchCounts {
    /// Not yet admitted.
    pub pending: u64,
    /// Admitted as jobs.
    pub accepted: u64,
    /// Rejected at admission.
    pub rejected: u64,
    /// Watermarked.
    pub succeeded: u64,
    /// Failed after admission.
    pub failed: u64,
    /// Admitted and not yet finished.
    pub in_progress: u64,
}

/// Credits of a batch.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default)]
#[non_exhaustive]
pub struct BatchCredits {
    /// Reserved for files still running.
    pub reserved: u64,
    /// Charged for watermarked files.
    pub charged: u64,
    /// Refunded for failed or canceled files.
    pub refunded: u64,
}

/// Signed upload target of a pending file.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct BatchUpload {
    /// `PUT`.
    pub method: String,
    /// Signed URL; it carries its own authorization (never send the API key to it).
    pub url: String,
    /// When the URL stops working (ISO 8601).
    #[serde(default)]
    pub expires_at: Option<String>,
}

/// One file of a [`Batch`].
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct BatchItemStatus {
    /// Position in the submitted list.
    pub index: usize,
    /// Filename as submitted.
    pub filename: String,
    /// Size in bytes, when known.
    #[serde(default)]
    pub size: Option<u64>,
    /// Upload session (`upl_…`).
    #[serde(default)]
    pub upload_id: Option<String>,
    /// Job ID (`req_…`) once admitted.
    #[serde(default)]
    pub request_id: Option<String>,
    /// Current state.
    pub status: BatchItemState,
    /// Why the file was rejected or failed, for example `upload_size_mismatch`.
    #[serde(default)]
    pub error_code: Option<String>,
    /// Human-readable detail for `error_code`, when available.
    #[serde(default)]
    pub error_detail: Option<String>,
    /// Credits reserved or charged; `0` for a failed (refunded) file.
    #[serde(default)]
    pub credits: Option<u64>,
    /// Relative job status path.
    #[serde(default)]
    pub status_url: Option<String>,
    /// Relative result path.
    #[serde(default)]
    pub result_url: Option<String>,
    /// When the result expires (ISO 8601).
    #[serde(default)]
    pub result_expires_at: Option<String>,
    /// Signed upload target, while the file is pending in a draft.
    #[serde(default)]
    pub upload: Option<BatchUpload>,
    /// `Some(true)` when the file already arrived (on a resubmitted draft);
    /// it is not uploaded again.
    #[serde(default)]
    pub upload_received: Option<bool>,
}

/// A batch of up to 100 files.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct Batch {
    /// Batch ID (`bat_…`).
    pub batch_id: String,
    /// Current state.
    pub status: BatchStatus,
    /// Number of files.
    #[serde(default)]
    pub item_count: u64,
    /// Whether every result is also zipped into one archive.
    #[serde(default)]
    pub archive: bool,
    /// Processing hardware requested for every file.
    #[serde(default, deserialize_with = "crate::accelerator_field")]
    pub accelerator: Option<Accelerator>,
    /// Webhook endpoint notified when the batch ends.
    #[serde(default)]
    pub webhook_id: Option<String>,
    /// Customer storage destination for every result.
    #[serde(default)]
    pub storage_destination_id: Option<String>,
    /// File counts.
    #[serde(default)]
    pub counts: BatchCounts,
    /// Credits reserved, charged and refunded.
    #[serde(default)]
    pub credits: BatchCredits,
    /// Whether cancellation was requested.
    #[serde(default)]
    pub cancel_requested: bool,
    /// Creation time (ISO 8601).
    #[serde(default)]
    pub created_at: String,
    /// Start time (ISO 8601).
    #[serde(default)]
    pub started_at: Option<String>,
    /// End time (ISO 8601).
    #[serde(default)]
    pub completed_at: Option<String>,
    /// Deadline for uploads and the start call (ISO 8601).
    #[serde(default)]
    pub upload_expires_at: Option<String>,
    /// Relative status path.
    #[serde(default)]
    pub status_url: String,
    /// Archive state (`queued`, `assembling`, `ready`, `too_large`, `empty`, `failed`), with `archive`.
    #[serde(default)]
    pub archive_status: Option<String>,
    /// Relative archive path, with `archive`.
    #[serde(default)]
    pub archive_url: Option<String>,
    /// When the archive expires (ISO 8601).
    #[serde(default)]
    pub archive_expires_at: Option<String>,
    /// Every file, in submitted order (empty in [`Client::list_batches`]).
    #[serde(default)]
    pub items: Vec<BatchItemStatus>,
}

impl Batch {
    /// `true` once the batch can no longer change. See [`BatchStatus::is_terminal`].
    pub fn is_done(&self) -> bool {
        self.status.is_terminal()
    }
}

/// One page of [`Client::list_batches`], newest first.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct BatchPage {
    /// Batches on this page, without their items.
    pub data: Vec<Batch>,
    /// Pass as `before` to continue; `None` on the last page.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// The outcome of one file, from [`Client::batch_results`].
#[derive(Debug)]
#[non_exhaustive]
pub struct BatchItemResult {
    /// Position in the submitted list.
    pub index: usize,
    /// Filename as submitted.
    pub filename: String,
    /// Final (or current) state.
    pub status: BatchItemState,
    /// Job ID (`req_…`) once admitted.
    pub request_id: Option<String>,
    /// Why the file did not succeed; its credits were refunded. The item's own
    /// code, else `cancelled` or `expired` for a batch that ended that way,
    /// else the item's state. `None` for a succeeded file.
    pub error_code: Option<String>,
    /// Human-readable detail for `error_code`, when available.
    pub error_detail: Option<String>,
    /// Credits charged; `0` for a failed file.
    pub credits: Option<u64>,
    /// The watermarked file, for a succeeded item.
    pub result: Option<EmbedResult>,
}

impl BatchItemResult {
    /// `true` when the file was watermarked and `result` holds it.
    pub fn is_ok(&self) -> bool {
        self.result.is_some()
    }
}

/// Iterator over a batch's files returned by [`Client::batch_results`].
///
/// Each succeeded file's result is downloaded as the iterator reaches it. A
/// failed download yields `Err` for that file and iteration continues.
#[derive(Debug)]
pub struct BatchResults<'a> {
    client: &'a Client,
    status: BatchStatus,
    items: std::vec::IntoIter<BatchItemStatus>,
}

impl Iterator for BatchResults<'_> {
    type Item = Result<BatchItemResult>;

    fn next(&mut self) -> Option<Self::Item> {
        let item = self.items.next()?;
        let error_code = if item.status == BatchItemState::Succeeded {
            item.error_code
        } else {
            item.error_code.or_else(|| {
                Some(match self.status {
                    BatchStatus::Cancelled | BatchStatus::Expired => {
                        self.status.as_str().to_owned()
                    }
                    _ => item.status.as_str().to_owned(),
                })
            })
        };
        let mut result = BatchItemResult {
            index: item.index,
            filename: item.filename,
            status: item.status,
            request_id: item.request_id,
            error_code,
            error_detail: item.error_detail,
            credits: item.credits,
            result: None,
        };
        if result.status == BatchItemState::Succeeded
            && let Some(ref id) = result.request_id
        {
            match self.client.get_embed_result(id) {
                Ok(embedded) => result.result = Some(embedded),
                Err(e) => {
                    let detail = format!(
                        "Result of item {} ({}) failed to download: {}",
                        result.index,
                        result.filename,
                        describe(&e)
                    );
                    let mut err = Error::context(e, detail);
                    err.request_id = err.request_id.or(Some(id.clone()));
                    return Some(Err(err));
                }
            }
        }
        Some(Ok(result))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.items.size_hint()
    }
}

/// Request body of a batch call.
enum Payload<'a> {
    None,
    Json(&'a Value),
    Zip { zip: &'a [u8], manifest: &'a str },
}

/// How a batch call treats statuses other than success.
#[derive(Clone, Copy)]
struct Policy {
    /// Retry HTTP 503 (not for creation, where it means uploads are unavailable).
    retry_503: bool,
    /// Largest response body read.
    limit: usize,
}

const READ: Policy = Policy {
    retry_503: true,
    limit: crate::MAX_DOWNLOAD_SIZE,
};
const CREATE: Policy = Policy {
    retry_503: false,
    ..READ
};

fn batch_path(id: &str) -> Result<String> {
    check_id(id, "bat_", 32, "batch")?;
    Ok(format!("watermarks/batches/{id}"))
}

fn check_count(count: usize) -> Result<()> {
    if (1..=MAX_BATCH_ITEMS).contains(&count) {
        Ok(())
    } else {
        Err(Error::input(format!(
            "A batch takes 1 to {MAX_BATCH_ITEMS} files (got {count}); split larger sets into several batches"
        )))
    }
}

fn check_item(index: usize, filename: &str, data: &Value) -> Result<()> {
    if filename.is_empty() || filename.chars().count() > 255 {
        return Err(Error::input(format!(
            "Item {index}: filename must be 1–255 characters"
        )));
    }
    embed_data(data).map_err(|_| {
        Error::input(format!(
            "Item {index} ({filename}): data must be a non-empty JSON object"
        ))
    })?;
    Ok(())
}

impl Client {
    /// Submit up to 100 files as one batch and start it.
    ///
    /// Creates the batch (`POST /watermarks/batches`), uploads every file to
    /// its signed URL (up to [`BatchOptions::upload_concurrency`] at a time,
    /// never sending the API key there) and starts it. Creation retries
    /// transient failures with the same `Idempotency-Key`. If an upload fails,
    /// the error names the file and batch; submitting again with the same
    /// idempotency key returns the same batch and uploads only the files it
    /// has not received. Resubmitting a batch that already started returns it
    /// as it is; one that expired unstarted (24 hours) fails with
    /// HTTP 410 and [`Error::code`] `batch_expired`.
    ///
    /// Each upload fails only when no bytes move for the client timeout, so
    /// slow links are fine. A file on disk that changed size since its size
    /// was read fails at once.
    ///
    /// ```no_run
    /// use etchv::{BatchItem, BatchOptions};
    /// use serde_json::json;
    /// # let client = etchv::Client::new("etchv_...")?;
    /// let items = vec![
    ///     BatchItem::from_path("in/contract-acme.pdf", json!({"recipient": "acme"})),
    ///     BatchItem::from_path("in/contract-bolt.pdf", json!({"recipient": "bolt"})),
    /// ];
    /// let batch = client.submit_batch(&items, BatchOptions::new().archive(true))?;
    /// println!("{} is {}", batch.batch_id, batch.status);
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn submit_batch(&self, items: &[BatchItem], options: BatchOptions) -> Result<Batch> {
        check_count(items.len())?;
        let mut entries = Vec::with_capacity(items.len());
        let mut sizes = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            check_item(index, &item.filename, &item.data)?;
            let size = item.size()?;
            if size == 0 {
                return Err(Error::input(format!(
                    "Item {index} ({}): file must contain at least 1 byte",
                    item.filename
                )));
            }
            sizes.push(size);
            entries.push(json!({"filename": item.filename, "size": size, "data": item.data}));
        }
        let (mut body, key) = options.prepare()?;
        body.insert("items".into(), Value::Array(entries));
        let with_key = |mut e: Error| {
            e.idempotency_key = Some(key.clone());
            e
        };
        let created: Batch = self
            .batch_json(
                Method::POST,
                "watermarks/batches".into(),
                Payload::Json(&Value::Object(body)),
                Some(&key),
                CREATE,
            )
            .map_err(with_key)?;
        match created.status {
            BatchStatus::Draft => {}
            BatchStatus::Expired => {
                let message = format!(
                    "Batch {} expired before it was started (a batch must start within 24 hours); submit again with a new idempotency key",
                    created.batch_id
                );
                return Err(with_key(Error::new(
                    ErrorKind::Api,
                    410,
                    json!({"detail": {"code": "batch_expired", "message": message}}).to_string(),
                )));
            }
            // A resubmission of a batch that already started (or ended): nothing to upload.
            _ => return Ok(created),
        }
        let mut uploads = Vec::new();
        for status in &created.items {
            let Some(ref upload) = status.upload else {
                if status.upload_received == Some(true) {
                    continue;
                }
                // Neither a link nor the file: the link expired or the response is broken.
                return Err(with_key(Error::response(
                    201,
                    format!(
                        "Batch {} has no upload URL for item {} ({}) and has not received it; submit again with the same idempotency key",
                        created.batch_id, status.index, status.filename
                    ),
                )));
            };
            if status.index >= items.len() || upload.method != "PUT" || !signed_url_ok(&upload.url)
            {
                return Err(with_key(Error::response(201, "Invalid batch upload URL")));
            }
            uploads.push((status.index, upload.url.as_str()));
        }
        let workers = options
            .upload_concurrency
            .unwrap_or(DEFAULT_UPLOAD_CONCURRENCY)
            .min(uploads.len());
        let next = AtomicUsize::new(0);
        let stop = AtomicBool::new(false);
        let failure: Mutex<Option<Error>> = Mutex::new(None);
        std::thread::scope(|scope| {
            for _ in 0..workers {
                scope.spawn(|| {
                    while !stop.load(Ordering::Relaxed) {
                        let Some(&(index, url)) = uploads.get(next.fetch_add(1, Ordering::Relaxed))
                        else {
                            break;
                        };
                        let item = &items[index];
                        let size = sizes[index];
                        if let Err(e) = self.put_signed(url, &|| item.body(size)) {
                            stop.store(true, Ordering::Relaxed);
                            let detail = format!(
                                "Upload of item {index} ({}) to batch {} failed: {}; submit again with the same idempotency key to resume",
                                item.filename,
                                created.batch_id,
                                describe(&e)
                            );
                            let mut slot = failure.lock().unwrap_or_else(|p| p.into_inner());
                            if slot.is_none() {
                                *slot = Some(Error::context(e, detail));
                            }
                        }
                    }
                });
            }
        });
        if let Some(e) = failure.into_inner().unwrap_or_else(|p| p.into_inner()) {
            return Err(with_key(e));
        }
        self.batch_json(
            Method::POST,
            format!("{}/start", batch_path(&created.batch_id)?),
            Payload::None,
            None,
            READ,
        )
        .map_err(with_key)
    }

    /// Create and start a batch from one zip of up to 55 MB
    /// (`POST /watermarks/batches/zip`), for files that are already together.
    ///
    /// `items` names every member of the zip, exactly as stored, with its
    /// forensic data. The returned batch is already starting.
    /// [`BatchOptions::upload_concurrency`] does not apply.
    pub fn submit_batch_zip(
        &self,
        zip: &[u8],
        items: &[BatchZipItem],
        options: BatchOptions,
    ) -> Result<Batch> {
        check_count(items.len())?;
        if zip.is_empty() || zip.len() > MAX_BATCH_ZIP_SIZE {
            return Err(Error::input("The zip must contain 1 byte to 55 MB"));
        }
        let mut entries = Vec::with_capacity(items.len());
        for (index, item) in items.iter().enumerate() {
            check_item(index, &item.filename, &item.data)?;
            entries.push(json!({"filename": item.filename, "data": item.data}));
        }
        let (mut manifest, key) = options.prepare()?;
        manifest.insert("items".into(), Value::Array(entries));
        let manifest = Value::Object(manifest).to_string();
        self.batch_json(
            Method::POST,
            "watermarks/batches/zip".into(),
            Payload::Zip {
                zip,
                manifest: &manifest,
            },
            Some(&key),
            CREATE,
        )
        .map_err(|mut e| {
            e.idempotency_key = Some(key.clone());
            e
        })
    }

    /// Read a batch and every file's status (`GET /watermarks/batches/{id}`).
    pub fn get_batch(&self, batch_id: &str) -> Result<Batch> {
        self.batch_json(
            Method::GET,
            batch_path(batch_id)?,
            Payload::None,
            None,
            READ,
        )
    }

    /// Poll a batch until it is completed, failed, canceled or expired, one
    /// request per poll, waiting as long as the API's `Retry-After` asks.
    ///
    /// A zero `timeout` waits up to [`DEFAULT_BATCH_WAIT`] (1 hour). When the
    /// timeout passes first, the error is [`ErrorKind::Timeout`]; the batch
    /// keeps running, so call again to keep waiting.
    pub fn wait_for_batch(&self, batch_id: &str, timeout: Duration) -> Result<Batch> {
        let path = batch_path(batch_id)?;
        let timeout = if timeout.is_zero() {
            DEFAULT_BATCH_WAIT
        } else {
            timeout
        };
        let started = Instant::now();
        loop {
            let (status, headers, bytes) =
                self.batch_call(Method::GET, &path, &Payload::None, None, READ)?;
            let batch: Batch =
                serde_json::from_slice(&bytes).map_err(|e| Error::decode(status, e))?;
            if batch.is_done() {
                return Ok(batch);
            }
            let remaining = timeout.saturating_sub(started.elapsed());
            if remaining.is_zero() {
                return Err(Error::new(
                    ErrorKind::Timeout,
                    0,
                    format!(
                        "Batch {batch_id} is still {} after {:?}; call wait_for_batch again to keep waiting",
                        batch.status, timeout
                    ),
                ));
            }
            sleep(poll_delay(&headers).min(remaining));
        }
    }

    /// Wait for a batch to end ([`Client::wait_for_batch`] with `timeout`; zero
    /// means 1 hour), then iterate over its files in submitted order,
    /// downloading each watermarked result as the iterator reaches it. Files
    /// that did not succeed carry an `error_code` (see
    /// [`BatchItemResult::error_code`]) and no result.
    ///
    /// ```no_run
    /// # let client = etchv::Client::new("etchv_...")?;
    /// # let batch_id = "bat_00000000000000000000000000000000";
    /// for item in client.batch_results(batch_id, std::time::Duration::from_secs(1800))? {
    ///     let item = item?;
    ///     match item.result {
    ///         Some(result) => std::fs::write(format!("out/{}", item.filename), &result.bytes).unwrap(),
    ///         None => eprintln!("{}: {:?} (refunded)", item.filename, item.error_code),
    ///     }
    /// }
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn batch_results(&self, batch_id: &str, timeout: Duration) -> Result<BatchResults<'_>> {
        let batch = self.wait_for_batch(batch_id, timeout)?;
        let mut items = batch.items;
        items.sort_by_key(|item| item.index);
        Ok(BatchResults {
            client: self,
            status: batch.status,
            items: items.into_iter(),
        })
    }

    /// Download the zip of every result of a batch created with
    /// [`BatchOptions::archive`] into memory. See
    /// [`Client::download_batch_archive_to`], which streams it to a file instead.
    pub fn download_batch_archive(&self, batch_id: &str, timeout: Duration) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        self.download_batch_archive_to(batch_id, &mut bytes, timeout)?;
        Ok(bytes)
    }

    /// Download the zip of every result of a batch created with
    /// [`BatchOptions::archive`] (`GET /watermarks/batches/{id}/archive`)
    /// into `sink`, returning the number of bytes written.
    ///
    /// Waits while the batch runs or the archive is assembled (HTTP 202),
    /// honoring `Retry-After`, for up to `timeout` (zero means 1 hour); then
    /// the error is [`ErrorKind::Timeout`] ("the archive is not ready yet").
    /// Each request must answer within the client timeout, and the download
    /// fails only when no data arrives for the client timeout ("the archive
    /// download stalled"), however long the whole download takes. Time spent
    /// writing to `sink` does not count: the timer runs only while reading.
    /// The zip holds up to 1 GB of results plus
    /// `manifest.json` ([`MAX_BATCH_ARCHIVE_SIZE`]); a larger one fails at once
    /// with [`Error::code`] `archive_too_large`. HTTP 409 means no
    /// archive (see [`Error::code`]: `batch_not_started`,
    /// `archive_not_requested`, `archive_too_large` or `archive_unavailable`);
    /// HTTP 410 means it expired.
    ///
    /// ```no_run
    /// # let client = etchv::Client::new("etchv_...")?;
    /// # let batch_id = "bat_00000000000000000000000000000000";
    /// let mut file = std::fs::File::create("results.zip").unwrap();
    /// let bytes = client.download_batch_archive_to(batch_id, &mut file, std::time::Duration::ZERO)?;
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn download_batch_archive_to<W: Write>(
        &self,
        batch_id: &str,
        sink: &mut W,
        timeout: Duration,
    ) -> Result<u64> {
        self.archive_to(batch_id, sink, timeout, MAX_BATCH_ARCHIVE_SIZE)
    }

    fn archive_to<W: Write>(
        &self,
        batch_id: &str,
        sink: &mut W,
        timeout: Duration,
        limit: u64,
    ) -> Result<u64> {
        let url = format!("{}/{}/archive", self.base, batch_path(batch_id)?);
        let wait = if timeout.is_zero() {
            DEFAULT_BATCH_WAIT
        } else {
            timeout
        };
        let started = Instant::now();
        let remaining = || wait.saturating_sub(started.elapsed());
        let mut request_id = None;
        loop {
            // No per-request timeout: in reqwest that is a total deadline including
            // the body. The streaming client's own timeout bounds the wait for
            // headers and then each read of the body separately (an idle timeout).
            let sent = self
                .streaming
                .get(&url)
                .header("X-API-Key", self.key.clone())
                .send();
            let response = match sent {
                Ok(response) => response,
                Err(e) if e.is_builder() || remaining().is_zero() => {
                    let mut err = Error::transport(e);
                    err.request_id = request_id;
                    return Err(err);
                }
                Err(_) => {
                    sleep(Duration::from_secs(1).min(remaining()));
                    continue;
                }
            };
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            request_id = header(&headers, "x-request-id").or(request_id);
            if status == 200 {
                return stream_archive(response, sink, limit, self.timeout).map_err(|mut e| {
                    e.request_id = e.request_id.take().or(request_id);
                    e
                });
            }
            let bytes = read_body(response).unwrap_or_default();
            let failed =
                serde_json::from_slice::<Value>(&bytes).is_ok_and(|v| v["status"] == "failed");
            let delay = match status {
                202 => Some(poll_delay(&headers)),
                429 | 502 | 503 | 504 if !failed => Some(Duration::from_secs_f64(
                    retry_after(&headers).unwrap_or(1.0).clamp(0.01, 86_400.0),
                )),
                _ => None,
            };
            let left = remaining();
            match delay {
                Some(delay) if !left.is_zero() => sleep(delay.min(left)),
                _ if status == 202 => {
                    let mut err = Error::new(
                        ErrorKind::Timeout,
                        0,
                        format!(
                            "Client deadline exceeded; the archive is not ready yet (batch {batch_id}, waited {wait:?})"
                        ),
                    );
                    err.request_id = request_id;
                    return Err(err);
                }
                _ => {
                    let mut err = Error::new(
                        ErrorKind::Api,
                        status,
                        String::from_utf8_lossy(&bytes[..bytes.len().min(10000)]).into_owned(),
                    );
                    err.request_id = request_id;
                    err.retry_after = rate_limit_delay(status, &headers);
                    return Err(err);
                }
            }
        }
    }

    /// Cancel a batch (`POST /watermarks/batches/{id}/cancel`). Files not yet
    /// running are refunded and fail with `cancelled`; running files finish.
    pub fn cancel_batch(&self, batch_id: &str) -> Result<Batch> {
        self.batch_json(
            Method::POST,
            format!("{}/cancel", batch_path(batch_id)?),
            Payload::None,
            None,
            READ,
        )
    }

    /// List batches newest first, without their items (`GET /watermarks/batches`).
    /// `limit` is 1–50 (default 20); pass a page's `next_cursor` as `before`.
    pub fn list_batches(&self, limit: Option<u32>, before: Option<&str>) -> Result<BatchPage> {
        let limit = limit
            .map(|l| {
                if (1..=50).contains(&l) {
                    Ok(l.to_string())
                } else {
                    Err(Error::input("limit must be 1–50"))
                }
            })
            .transpose()?;
        let mut pairs = Vec::new();
        if let Some(ref limit) = limit {
            pairs.push(("limit", limit.as_str()));
        }
        if let Some(before) = before {
            check_id(before, "bat_", 32, "batch")?;
            pairs.push(("before", before));
        }
        self.batch_json(
            Method::GET,
            with_query("watermarks/batches", &pairs),
            Payload::None,
            None,
            READ,
        )
    }

    fn batch_json<T: serde::de::DeserializeOwned>(
        &self,
        method: Method,
        path: String,
        payload: Payload<'_>,
        key: Option<&str>,
        policy: Policy,
    ) -> Result<T> {
        let (status, _, bytes) = self.batch_call(method, &path, &payload, key, policy)?;
        serde_json::from_slice(&bytes).map_err(|e| Error::decode(status, e))
    }

    /// One batch request, retrying transport failures and HTTP 429, 502, 504
    /// (and 503 when allowed) with the same idempotency key until the client
    /// deadline. Every batch call is safe to repeat.
    fn batch_call(
        &self,
        method: Method,
        path: &str,
        payload: &Payload<'_>,
        key: Option<&str>,
        policy: Policy,
    ) -> Result<(u16, HeaderMap, Vec<u8>)> {
        let started = Instant::now();
        let mut request_id = None;
        let remaining = || self.timeout.saturating_sub(started.elapsed());
        while started.elapsed() < self.timeout {
            let mut request = self
                .http
                .request(method.clone(), format!("{}/{path}", self.base))
                .header("X-API-Key", self.key.clone())
                .timeout(remaining().max(Duration::from_millis(1)));
            if let Some(key) = key {
                request = request.header("Idempotency-Key", key);
            }
            request = match *payload {
                Payload::None => request,
                Payload::Json(body) => request.json(body),
                Payload::Zip { zip, manifest } => {
                    let part = multipart::Part::bytes(zip.to_vec())
                        .file_name("batch.zip")
                        .mime_str("application/zip")
                        .map_err(Error::transport)?;
                    request.multipart(
                        multipart::Form::new()
                            .part("archive", part)
                            .text("manifest", manifest.to_owned()),
                    )
                }
            };
            let response = match request.send() {
                Ok(r) => r,
                Err(e) if e.is_builder() => return Err(Error::transport(e)),
                Err(_) => {
                    sleep(Duration::from_secs(1).min(remaining()));
                    continue;
                }
            };
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            request_id = header(&headers, "x-request-id").or(request_id);
            let mut bytes = Vec::new();
            if response
                .take(policy.limit as u64 + 1)
                .read_to_end(&mut bytes)
                .is_err()
            {
                sleep(Duration::from_secs(1).min(remaining()));
                continue;
            }
            if bytes.len() > policy.limit {
                let mut err = Error::response(status, "Response exceeds the SDK size limit");
                err.request_id = request_id;
                return Err(err);
            }
            if (200..300).contains(&status) {
                return Ok((status, headers, bytes));
            }
            let failed = serde_json::from_slice::<Value>(&bytes)
                .is_ok_and(|detail| detail["status"] == "failed");
            if (matches!(status, 429 | 502 | 504) || (status == 503 && policy.retry_503)) && !failed
            {
                let seconds = retry_after(&headers).unwrap_or(1.0).clamp(0.01, 5.0);
                sleep(Duration::from_secs_f64(seconds).min(remaining()));
                continue;
            }
            let mut err = Error::new(
                ErrorKind::Api,
                status,
                String::from_utf8_lossy(&bytes[..bytes.len().min(10000)]).into_owned(),
            );
            err.request_id = request_id;
            err.retry_after = rate_limit_delay(status, &headers);
            return Err(err);
        }
        let mut err = Error::new(ErrorKind::Timeout, 0, "Client deadline exceeded");
        err.request_id = request_id;
        Err(err)
    }
}

/// Copy a 200 archive response into `sink`: it must be a zip within `limit`,
/// and each read may wait up to `idle` for data.
fn stream_archive<W: Write>(
    mut response: reqwest::blocking::Response,
    sink: &mut W,
    limit: u64,
    idle: Duration,
) -> Result<u64> {
    // Reported like the API's own refusal, so `code()` is `archive_too_large`.
    let too_large = || {
        let message = format!(
            "The archive is larger than the SDK downloads ({limit} bytes); download each item's result_url instead"
        );
        Error::new(
            ErrorKind::Api,
            200,
            json!({"detail": {"code": "archive_too_large", "message": message}}).to_string(),
        )
    };
    let not_zip = || Error::response(200, "Invalid archive response (not a zip)");
    if response
        .content_length()
        .is_some_and(|length| length > limit)
    {
        return Err(too_large());
    }
    let mut buffer = vec![0; 64 * 1024];
    let mut head = Vec::with_capacity(2);
    let mut written = 0u64;
    loop {
        let n = match response.read(&mut buffer) {
            Ok(n) => n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => {
                let timed_out = e.kind() == std::io::ErrorKind::TimedOut
                    || e.get_ref()
                        .and_then(|inner| inner.downcast_ref::<reqwest::Error>())
                        .is_some_and(reqwest::Error::is_timeout);
                return Err(if timed_out {
                    Error::new(
                        ErrorKind::Timeout,
                        0,
                        format!(
                            "Client deadline exceeded; the archive download stalled (no data for {idle:?})"
                        ),
                    )
                } else {
                    Error::transport(e)
                });
            }
        };
        if n == 0 {
            break;
        }
        if written + head.len() as u64 + n as u64 > limit {
            return Err(too_large());
        }
        let mut chunk = &buffer[..n];
        if head.len() < 2 {
            // Check the zip signature before writing anything.
            let take = (2 - head.len()).min(chunk.len());
            head.extend_from_slice(&chunk[..take]);
            chunk = &chunk[take..];
            if head.len() < 2 {
                continue;
            }
            if head != b"PK" {
                return Err(not_zip());
            }
            sink.write_all(&head).map_err(Error::transport)?;
            written += 2;
        }
        sink.write_all(chunk).map_err(Error::transport)?;
        written += chunk.len() as u64;
    }
    if written < 2 {
        return Err(not_zip());
    }
    sink.flush().map_err(Error::transport)?;
    Ok(written)
}

/// The API's message, or the start of a raw (non-JSON) error body.
fn describe(e: &Error) -> String {
    e.message()
        .unwrap_or_else(|| e.detail.chars().take(500).collect())
}

/// The API's `Retry-After` for a running batch (at least 1 second), or 2 seconds.
fn poll_delay(headers: &HeaderMap) -> Duration {
    let seconds = retry_after(headers).unwrap_or(DEFAULT_POLL_SECONDS);
    Duration::from_secs_f64(seconds.clamp(MIN_POLL_SECONDS, 86_400.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serve one 200 archive of `len` bytes, with or without Content-Length.
    fn serve_archive(len: usize, sized: bool) -> (String, std::thread::JoinHandle<()>) {
        let server = tiny_http::Server::http("127.0.0.1:0").unwrap();
        let base = format!("http://{}", server.server_addr());
        let worker = std::thread::spawn(move || {
            let request = server.recv().unwrap();
            let mut body = vec![0u8; len];
            body[..2].copy_from_slice(b"PK");
            let response = tiny_http::Response::new(
                200.into(),
                vec![],
                std::io::Cursor::new(body),
                sized.then_some(len),
                None,
            );
            let _ = request.respond(response);
        });
        (base, worker)
    }

    #[test]
    fn archives_over_the_cap_are_refused_without_writing() {
        for sized in [true, false] {
            let (base, worker) = serve_archive(200_000, sized);
            let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
            let mut sink = Vec::new();
            let error = client
                .archive_to(
                    "bat_0123456789abcdef0123456789abcdef",
                    &mut sink,
                    Duration::from_secs(5),
                    100_000,
                )
                .unwrap_err();
            drop(client);
            worker.join().unwrap();
            assert_eq!(error.kind, ErrorKind::Api);
            assert_eq!(error.code().as_deref(), Some("archive_too_large"));
            assert!(error.to_string().contains("larger than"), "{error}");
            assert!(sink.len() <= 100_000);
        }
    }
}
