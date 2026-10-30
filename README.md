# Etchv Rust SDK

Server-side Rust client for [Etchv](https://etchv.com): embed and detect invisible forensic watermarks in images, PDFs and videos.

## Install

```sh
cargo add etchv serde_json
```

Requires Rust 1.88+. This is a blocking client; from async code, call it inside `tokio::task::spawn_blocking`.

## Quickstart

```rust
use etchv::{Client, Options};
use serde_json::json;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(std::env::var("ETCHV_API_KEY")?)?;

    let result = client.embed_image(
        &std::fs::read("photo.jpg")?,
        &json!({"recipient": "customer-123"}),
        Options::new().filename("photo.jpg"),
    )?;
    std::fs::write(&result.filename, &result.bytes)?;

    let detection = client.detect_image(&result.bytes, Options::new().filename(&result.filename))?;
    println!("watermarked: {} ({:.2})", detection.watermarked, detection.confidence);
    Ok(())
}
```

Image and PDF uploads are limited to 50 MB; DOCX, PPTX and video uploads to 20 MB. A PDF has 1 to 2,000
pages, and its watermarked copy must fit in 95 MB with 48 KB per page, so a 2,000-page PDF can be up to
about 1 MB. Detection takes the files Etchv delivered: up to 192 MB for images, 95 MB for PDFs and 100 MB
for DOCX, PPTX and video.

## Large files

Files over 40 MB are uploaded once to a signed URL and then referenced by ID, so the request never carries
the file. This is automatic in every embed, detect and submit method; retries reuse the same upload. Image
and document detection above 95 MB runs as a background job and the call waits for it. Change the threshold
with `with_large_file_threshold`, or upload explicitly:

```rust
use etchv::UploadKind;

let client = etchv::Client::new(api_key)?.with_large_file_threshold(20 * 1024 * 1024)?;
let upload = client.upload_file(UploadKind::Detect, &delivered, "delivered.tiff")?;
upload.upload_id; // send as the upload_id form field instead of file
```

## Many files at once

Submit up to 100 files in one batch, each with its own forensic data:

```rust
use etchv::{BatchItem, BatchOptions};
use serde_json::json;
use std::time::Duration;

let items: Vec<BatchItem> = std::fs::read_dir("in")?
    .map(|entry| {
        let path = entry?.path();
        let stem = path.file_stem().unwrap().to_string_lossy().into_owned();
        Ok(BatchItem::from_path(path, json!({"recipient": stem})))
    })
    .collect::<std::io::Result<_>>()?;
let batch = client.submit_batch(&items, BatchOptions::new().archive(true))?;

// Waits up to 30 minutes for the batch to finish, then downloads each result in turn.
for item in client.batch_results(&batch.batch_id, Duration::from_secs(30 * 60))? {
    let item = item?;
    match item.result {
        Some(result) => std::fs::write(format!("out/{}", item.filename), &result.bytes)?,
        None => eprintln!("{}: {:?}", item.filename, item.error_code), // credits refunded
    }
}
let mut zip = std::fs::File::create("out.zip")?;
client.download_batch_archive_to(&batch.batch_id, &mut zip, Duration::from_secs(10 * 60))?;
```

`submit_batch` creates the batch, uploads each file straight to its own signed upload URL (the API key
is never sent there), up to 4 at a time (`BatchOptions::upload_concurrency`), and starts it. Files can
be bytes (`BatchItem::new`) or paths (`BatchItem::from_path`, streamed from disk). The batch's
idempotency key (generated, or `BatchOptions::idempotency_key`) makes resubmitting safe: the same key
returns the same batch and uploads only the files it has not received. Files that are rejected or fail
are never charged. Upload URLs last 6 hours and a batch must start within 24 hours. Results, and the
archive of every result (`archive(true)`, up to 1 GB), stay available for 24 hours. `BatchOptions` also
takes `webhook_id`, `accelerator`, `storage_destination_id` and `active_content`, as for single files.

`wait_for_batch`, `batch_results` and the archive downloads wait as long as their timeout (zero means
1 hour), polling as often as the API's `Retry-After` allows and at most once a second. File uploads
and archive downloads fail only when no data moves for the client timeout, however long they take in
all, so large files on slow links are fine; time your own `Write` spends saving the archive does not
count. `download_batch_archive` returns the zip in memory; `download_batch_archive_to` streams it into
any `Write`. If the connection drops mid-download, `download_batch_archive` starts again from the
beginning; `download_batch_archive_to` returns the error, since what it already wrote to a `Write`
cannot be taken back. Archive errors carry `code()`: `batch_not_started`, `archive_not_requested`,
`archive_too_large` or `archive_unavailable` (HTTP 409); HTTP 410 means it expired. Resubmitting a
batch that expired before it started fails with HTTP 410 and `batch_expired`; use a new idempotency key.
`cancel_batch` refunds files that have not started; they end with `error_code` `cancelled`, and files
already running finish. Canceled batches report `BatchStatus::Cancelled`.

Files already together in a zip of up to 55 MB can go in one request:

```rust
use etchv::{BatchOptions, BatchZipItem};

let batch = client.submit_batch_zip(
    &std::fs::read("contracts.zip")?,
    &[BatchZipItem::new("contracts/acme.pdf", serde_json::json!({"recipient": "acme"}))],
    BatchOptions::new(),
)?;
```

Also: `get_batch`, `cancel_batch` (files not yet running are refunded) and `list_batches`.

## PDF scripts and actions

A watermarked PDF keeps its scripts, actions, form fields and attachments exactly as uploaded. Pass
`Options::new().active_content(ActiveContent::RemoveAutoRun)` to `embed_document` or
`submit_embed(Media::Document, ...)`, or `BatchOptions::new().active_content(...)`, to also remove what a
PDF viewer runs by itself: the open action, document and page actions, document-level JavaScript and XFA
forms. Links, form fields and attachments stay. The SDK refuses the option for other media and detection.
`RemoveAutoRun` is for PDFs only: the API refuses it for a DOCX or PPTX, and for a batch unless every
file is a PDF. `Keep` is accepted for any document and any batch.
[Scripts and actions](https://etchv.com/docs/api/documents#scripts-and-actions).

```rust
use etchv::{ActiveContent, Options};

let result = client.embed_document(
    &std::fs::read("contract.pdf")?,
    &serde_json::json!({"recipient": "counsel-7"}),
    Options::new().filename("contract.pdf").active_content(ActiveContent::RemoveAutoRun),
)?;
```

## GPU processing

Business and Enterprise plans can request GPU processing for any embed, detect or async submission
(other plans receive HTTP 403). GPU operations cost 3× credits; when no GPU is ready the job runs on CPU
at normal credits. `accelerator` on the result (and on job receipts) reports the hardware that actually ran.

```rust
use etchv::{Accelerator, Options};

let result = client.embed_image(&image, &data, Options::new().accelerator(Accelerator::Gpu))?;
println!("processed on {:?}", result.accelerator); // Some(Accelerator::Gpu) or Some(Accelerator::Cpu)
```

## Async jobs

```rust
use etchv::{JobStatus, Media, Options};

let job = client.submit_embed(
    Media::Document,
    &pdf,
    &serde_json::json!({"delivery": "delivery_001"}),
    Options::new().filename("report.pdf").idempotency_key("delivery_001"),
    None, // or Some(webhook_id)
)?;
if client.get_job(&job.request_id)?.status == JobStatus::Succeeded {
    let result = client.get_embed_result(&job.request_id)?;
}
```

Resending the same request with the same idempotency key returns the existing job without another charge.
Detection uses `submit_detection`, `get_detection_job` and `get_detection_result`.

## Traceable links

Share one file so each recipient downloads their own watermarked copy. Use the
source asset of a file you've watermarked; send the returned `url` yourself.

```rust
use etchv::NewLink;

let source = result.source_asset_id.as_deref().expect("the original of a watermarked file");
let link = client.create_link(
    &NewLink::new(source, "Board pack, October")
        .recipients(["alex@example.com", "sam@example.com"])
        .download_limit(3),
)?;
println!("{}", link.url);
```

Give `.allowed_domains(["example.com"])` instead of `.recipients(...)` to let
anyone at those domains open it. `create_link` retries with one idempotency key,
so a retry returns the same link; without the credits for the copies it fails
with HTTP 402 and `code()` `insufficient_credits`. `estimate_link` takes the same
`NewLink` and reports the credits the copies would take without creating
anything. Also `get_link` (each recipient, their opens and their copy),
`list_links`, `list_link_events` and `revoke_link`.

`delete_link(id)` deletes a link with every recipient's opens, downloads and
copy; `remove_link_recipient(id, recipient_id)` does the same for one recipient
(their `id` in `get_link`) and returns the link without them. Neither can be
undone, and detection no longer names a deleted recipient. Both need an owner or
admin and, for an API key, `assets:delete` as well as `links:write`.

When a detected file is one of your links' copies, `DetectionResult::copy` names
the link, the recipient and when they first opened it (needs `links:read`);
`DetectionResult::asset` names any other watermarked asset of yours (needs
`assets:read`). See https://etchv.com/docs/api/links.

## Also included

- API key check: `get_api_key_info`
- Batches: `submit_batch`, `submit_batch_zip`, `get_batch`, `wait_for_batch`, `batch_results`, `download_batch_archive`, `download_batch_archive_to`, `cancel_batch`, `list_batches`
- Assets: `list_assets`, `get_asset`, `update_asset`, `delete_asset`, `delete_assets`, `download_asset`
- Webhooks: `list_webhooks`, `create_webhook`, `update_webhook`, `delete_webhook`, `list_webhook_deliveries`, `redeliver_webhook`, and `etchv::verify_webhook_signature`
- Customer storage: `list_storage_destinations`, `create_storage_destination`, `update_storage_destination`, `delete_storage_destination`, `verify_storage_destination`, `list_storage_deliveries`, `create_storage_delivery`, `get_storage_delivery`, `retry_storage_delivery`, `download_storage_delivery`

`verify_webhook_signature` checks only the signature and timestamp; your handler must also compare the body `id` with `X-Etchv-Event-ID`.

## Errors

Every method returns `Result<T, etchv::Error>`. `Error` has `kind` (`ErrorKind::Api`, `Timeout`, `Transport`, ...),
`status_code` (`0` when no HTTP response), `request_id`, `idempotency_key` and `message()`.
Include the request ID when contacting support.
Embedding and video detection retry HTTP 429, 502, 503 and 504 (honoring `Retry-After`) until the client deadline.

These methods also retry a dropped connection until the client deadline: `embed_image`,
`embed_document`, `embed_video`, `detect_video`, `submit_embed`, `submit_detection`,
`get_embed_result`, `get_detection_result`, `submit_batch`, `submit_batch_zip`, `get_batch`,
`wait_for_batch`, `batch_results`, `cancel_batch`, `list_batches`, `create_link`, `detect_image`,
`detect_document`, `download_asset`, `download_storage_delivery` and `download_batch_archive`
(`download_batch_archive_to` only until the first bytes arrive). Other methods return the
`Transport` error.

`detect_image` and `detect_document` resend with the same idempotency key (generated when absent), and
the API returns the first result without charging again. A retry the API reports as still running
(HTTP 409, `code()` `request_in_progress`, or `upload_in_use` for a file sent through an upload)
waits and is sent again; `replay_expired` and `request_failed` are returned as errors. The API keeps
the result 24 hours for this; pass `Options::new().keep_result(false)` to keep nothing, and a retry
then runs detection again at no charge, after the SDK uploads the file again if it went through an
upload. The detection requests, resends included, stay within one client timeout counted from the
first send; uploading the file again is bounded like any upload. Errors from a keyed call carry its
`idempotency_key`, so you can resend it yourself.
Other HTTP 429 errors carry `retry_after` (a `Duration`, `None` without the header), `code()` (`rate_limited` or `concurrency_limited`) and `limit()`.

```rust
if let Err(e) = client.get_embed_result(&request_id) {
    eprintln!("HTTP {} (request {:?}): {e}", e.status_code, e.request_id);
}
```

## Links

- Full guide: https://etchv.com/docs/sdks/rust
- API reference: https://etchv.com/docs
- Support: hello@etchv.com

License: MIT.

Questions or bug reports: open an issue here or email hello@etchv.com.
