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

## Also included

- API key check: `get_api_key_info`
- Assets: `list_assets`, `get_asset`, `update_asset`, `delete_asset`, `delete_assets`, `download_asset`
- Webhooks: `list_webhooks`, `create_webhook`, `update_webhook`, `delete_webhook`, `list_webhook_deliveries`, `redeliver_webhook`, and `etchv::verify_webhook_signature`
- Customer storage: `list_storage_destinations`, `create_storage_destination`, `update_storage_destination`, `delete_storage_destination`, `verify_storage_destination`, `list_storage_deliveries`, `create_storage_delivery`, `get_storage_delivery`, `retry_storage_delivery`, `download_storage_delivery`

`verify_webhook_signature` checks only the signature and timestamp; your handler must also compare the body `id` with `X-Etchv-Event-ID`.

## Errors

Every method returns `Result<T, etchv::Error>`. `Error` has `kind` (`ErrorKind::Api`, `Timeout`, `Transport`, ...),
`status_code` (`0` when no HTTP response), `request_id`, `idempotency_key` and `message()`.
Include the request ID when contacting support.
Embedding and video detection retry HTTP 429, 502, 503 and 504 (honoring `Retry-After`) until the client deadline.
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
