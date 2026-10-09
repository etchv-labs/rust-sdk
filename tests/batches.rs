use etchv::{
    BatchItem, BatchItemState, BatchOptions, BatchStatus, BatchZipItem, Client, ErrorKind,
    UploadKind,
};
use serde_json::{Value, json};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tiny_http::{Header, Response, Server};

const BATCH: &str = "bat_0123456789abcdef0123456789abcdef";

fn job(c: char) -> String {
    format!("req_{}", c.to_string().repeat(64))
}

fn png(len: usize) -> Vec<u8> {
    let mut bytes = vec![b'x'; len];
    bytes[..8].copy_from_slice(b"\x89PNG\r\n\x1a\n");
    bytes
}

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    url: String,
    api_key: Option<String>,
    authorization: Option<String>,
    idempotency_key: Option<String>,
    content_type: Option<String>,
    body: Vec<u8>,
    at: Instant,
}

type Reply = (u16, Vec<u8>, Vec<(&'static str, String)>);

/// A local API answering until finished; `respond` gets the request, the server origin and
/// the request's position.
struct Fake {
    base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Fake {
    fn new(respond: impl Fn(&Seen, &str, usize) -> Reply + Send + 'static) -> Self {
        let server = Server::http("127.0.0.1:0").unwrap();
        let base = format!("http://{}", server.server_addr());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (log, done, origin) = (seen.clone(), stop.clone(), base.clone());
        let worker = thread::spawn(move || {
            let mut index = 0;
            while !done.load(Ordering::Relaxed) {
                let Some(mut req) = server.recv_timeout(Duration::from_millis(20)).unwrap() else {
                    continue;
                };
                let header = |name: &str| {
                    req.headers()
                        .iter()
                        .find(|h| h.field.to_string().eq_ignore_ascii_case(name))
                        .map(|h| h.value.to_string())
                };
                let mut seen = Seen {
                    method: req.method().as_str().to_owned(),
                    url: req.url().to_owned(),
                    api_key: header("X-API-Key"),
                    authorization: header("Authorization"),
                    idempotency_key: header("Idempotency-Key"),
                    content_type: header("Content-Type"),
                    body: Vec::new(),
                    at: Instant::now(),
                };
                req.as_reader().read_to_end(&mut seen.body).unwrap();
                let (status, body, headers) = respond(&seen, &origin, index);
                index += 1;
                log.lock().unwrap().push(seen);
                let mut response = Response::from_data(body).with_status_code(status);
                for (name, value) in headers {
                    response.add_header(Header::from_bytes(name, value).unwrap());
                }
                req.respond(response).unwrap();
            }
        });
        Self {
            base,
            seen,
            stop,
            worker: Some(worker),
        }
    }

    fn client(&self) -> Client {
        self.client_with(Duration::from_secs(10))
    }

    fn client_with(&self, timeout: Duration) -> Client {
        Client::with_options("test-key", &self.base, timeout).unwrap()
    }

    fn finish(mut self) -> Vec<Seen> {
        self.stop.store(true, Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
        self.seen.lock().unwrap().clone()
    }
}

fn path(seen: &Seen) -> &str {
    seen.url.split('?').next().unwrap()
}

fn batch(status: &str, items: Value) -> Value {
    json!({"batch_id": BATCH, "status": status, "item_count": items.as_array().unwrap().len(),
           "archive": false, "accelerator": "cpu", "webhook_id": null, "storage_destination_id": null,
           "counts": {"pending": 0, "accepted": 0, "rejected": 0, "succeeded": 0, "failed": 0, "in_progress": 0},
           "credits": {"reserved": 0, "charged": 0, "refunded": 0}, "cancel_requested": false,
           "created_at": "2026-10-09T00:00:00Z", "started_at": null, "completed_at": null,
           "upload_expires_at": "2026-10-10T00:00:00Z", "status_url": format!("/watermarks/batches/{BATCH}"),
           "items": items})
}

fn pending(origin: &str, index: usize, filename: &str, size: usize) -> Value {
    json!({"index": index, "filename": filename, "size": size, "upload_id": format!("upl_{index:032}"),
           "request_id": null, "status": "pending", "error_code": null, "error_detail": null, "credits": null,
           "upload": {"method": "PUT", "url": format!("{origin}/signed/{index}?Signature=s"), "expires_at": "x"}})
}

fn ok_json(value: Value) -> Reply {
    (
        200,
        value.to_string().into_bytes(),
        vec![("Content-Type", "application/json".into())],
    )
}

/// Answers create, signed PUTs and start for a two-file batch.
fn flow(seen: &Seen, origin: &str) -> Reply {
    match path(seen) {
        "/watermarks/batches" => {
            let mut reply = ok_json(batch(
                "draft",
                json!([
                    pending(origin, 0, "logo.png", 300),
                    pending(origin, 1, "contract.pdf", 9)
                ]),
            ));
            reply.0 = 201;
            reply
        }
        p if p.starts_with("/signed/") => (200, Vec::new(), vec![]),
        p if p == format!("/watermarks/batches/{BATCH}/start") => {
            let mut reply = ok_json(batch("starting", json!([])));
            reply.0 = 202;
            reply
        }
        other => panic!("unexpected request {other}"),
    }
}

#[test]
fn submit_batch_creates_uploads_without_the_api_key_and_starts() {
    let fake = Fake::new(|seen, origin, _| flow(seen, origin));
    let dir = std::env::temp_dir().join(format!("etchv-batch-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let pdf_path = dir.join("contract.pdf");
    std::fs::write(&pdf_path, b"%PDF-1.7\n").unwrap();
    let items = vec![
        BatchItem::new("logo.png", png(300), json!({"recipient": "acme"})),
        BatchItem::from_path(&pdf_path, json!({"recipient": "bolt"})),
    ];
    let started = fake
        .client()
        .submit_batch(&items, BatchOptions::new().upload_concurrency(2))
        .unwrap();
    let seen = fake.finish();
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(started.status, BatchStatus::Starting);
    assert_eq!(started.batch_id, BATCH);

    assert_eq!(seen.len(), 4);
    let create = &seen[0];
    assert_eq!(
        (create.method.as_str(), path(create)),
        ("POST", "/watermarks/batches")
    );
    assert_eq!(create.api_key.as_deref(), Some("test-key"));
    assert!(
        create
            .idempotency_key
            .as_deref()
            .is_some_and(|k| k.len() >= 8)
    );
    let body: Value = serde_json::from_slice(&create.body).unwrap();
    assert_eq!(
        body,
        json!({"archive": false, "items": [
            {"filename": "logo.png", "size": 300, "data": {"recipient": "acme"}},
            {"filename": "contract.pdf", "size": 9, "data": {"recipient": "bolt"}}]})
    );

    let mut puts: Vec<&Seen> = seen.iter().filter(|s| s.method == "PUT").collect();
    puts.sort_by_key(|s| s.url.clone());
    assert_eq!(puts.len(), 2);
    for put in &puts {
        assert_eq!(put.api_key, None, "API key sent to a signed upload URL");
        assert_eq!(put.authorization, None);
        assert_eq!(put.idempotency_key, None);
        assert_eq!(
            put.content_type.as_deref(),
            Some("application/octet-stream")
        );
    }
    assert_eq!(path(puts[0]), "/signed/0");
    assert_eq!(puts[0].body, png(300));
    assert_eq!(path(puts[1]), "/signed/1");
    assert_eq!(puts[1].body, b"%PDF-1.7\n");

    let start = &seen[3];
    assert_eq!(
        (start.method.as_str(), path(start).to_owned()),
        ("POST", format!("/watermarks/batches/{BATCH}/start"))
    );
    assert_eq!(start.api_key.as_deref(), Some("test-key"));
}

#[test]
fn creation_retries_reuse_the_idempotency_key() {
    let fake = Fake::new(|seen, origin, index| {
        if index == 0 {
            return (
                502,
                b"{\"detail\":\"bad gateway\"}".to_vec(),
                vec![("Retry-After", "0.01".into())],
            );
        }
        flow(seen, origin)
    });
    let items = vec![
        BatchItem::new("logo.png", png(300), json!({"recipient": "acme"})),
        BatchItem::new(
            "contract.pdf",
            b"%PDF-1.7\n".to_vec(),
            json!({"recipient": "bolt"}),
        ),
    ];
    fake.client()
        .submit_batch(&items, BatchOptions::new().idempotency_key("batch_key_001"))
        .unwrap();
    let seen = fake.finish();
    let creates: Vec<_> = seen
        .iter()
        .filter(|s| path(s) == "/watermarks/batches")
        .collect();
    assert_eq!(creates.len(), 2);
    assert!(
        creates
            .iter()
            .all(|s| s.idempotency_key.as_deref() == Some("batch_key_001"))
    );
}

#[test]
fn generated_keys_are_reused_and_503_is_not_retried() {
    let fake = Fake::new(|seen, origin, index| {
        if index == 0 {
            return (
                429,
                b"{\"detail\":\"slow down\"}".to_vec(),
                vec![("Retry-After", "0.01".into())],
            );
        }
        flow(seen, origin)
    });
    let items = vec![
        BatchItem::new("logo.png", png(300), json!({"a": 1})),
        BatchItem::new("contract.pdf", b"%PDF-1.7\n".to_vec(), json!({"b": 2})),
    ];
    fake.client()
        .submit_batch(&items, BatchOptions::new())
        .unwrap();
    let seen = fake.finish();
    let keys: Vec<_> = seen
        .iter()
        .filter(|s| path(s) == "/watermarks/batches")
        .map(|s| s.idempotency_key.clone().unwrap())
        .collect();
    assert_eq!(keys.len(), 2);
    assert_eq!(keys[0], keys[1]);

    let fake = Fake::new(|_, _, _| {
        (
            503,
            json!({"detail": "Batch uploads are unavailable; send one zip to /watermarks/batches/zip instead"})
                .to_string()
                .into_bytes(),
            vec![],
        )
    });
    let error = fake
        .client()
        .submit_batch(&items, BatchOptions::new().idempotency_key("batch_key_002"))
        .unwrap_err();
    let seen = fake.finish();
    assert_eq!(seen.len(), 1);
    assert_eq!(error.status_code, 503);
    assert!(error.to_string().contains("zip"));
    assert_eq!(error.idempotency_key.as_deref(), Some("batch_key_002"));
}

#[test]
fn a_failed_upload_names_the_file_and_does_not_start() {
    let fake = Fake::new(|seen, origin, _| match path(seen) {
        p if p.starts_with("/signed/") => (403, b"Forbidden".to_vec(), vec![]),
        _ => flow(seen, origin),
    });
    let items = vec![
        BatchItem::new("logo.png", png(300), json!({"recipient": "acme"})),
        BatchItem::new(
            "contract.pdf",
            b"%PDF-1.7\n".to_vec(),
            json!({"recipient": "bolt"}),
        ),
    ];
    let error = fake
        .client()
        .submit_batch(
            &items,
            BatchOptions::new()
                .idempotency_key("batch_key_003")
                .upload_concurrency(1),
        )
        .unwrap_err();
    let seen = fake.finish();
    assert_eq!(error.status_code, 403);
    let text = error.to_string();
    assert!(
        text.contains("logo.png") && text.contains(BATCH) && text.contains("Forbidden"),
        "{text}"
    );
    assert_eq!(error.idempotency_key.as_deref(), Some("batch_key_003"));
    assert!(!seen.iter().any(|s| path(s).ends_with("/start")));
    assert_eq!(seen.iter().filter(|s| s.method == "PUT").count(), 1);
}

#[test]
fn wait_for_batch_honors_retry_after() {
    let fake = Fake::new(|_, _, index| {
        let mut reply = ok_json(batch(
            if index == 0 {
                "processing"
            } else {
                "completed"
            },
            json!([]),
        ));
        if index == 0 {
            reply.2.push(("Retry-After", "0.3".into()));
        }
        reply
    });
    let done = fake
        .client()
        .wait_for_batch(BATCH, Duration::from_secs(10))
        .unwrap();
    let seen = fake.finish();
    assert_eq!(done.status, BatchStatus::Completed);
    assert!(done.is_done());
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter()
            .all(|s| s.method == "GET" && path(s) == format!("/watermarks/batches/{BATCH}"))
    );
    // A sub-second Retry-After is floored at 1 second between polls.
    assert!(seen[1].at - seen[0].at >= Duration::from_secs(1));
}

#[test]
fn wait_for_batch_times_out_with_the_last_status() {
    let fake = Fake::new(|_, _, _| {
        let mut reply = ok_json(batch("processing", json!([])));
        reply.2.push(("Retry-After", "0.05".into()));
        reply
    });
    let error = fake
        .client()
        .wait_for_batch(BATCH, Duration::from_millis(120))
        .unwrap_err();
    fake.finish();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(error.to_string().contains("processing"));
}

#[test]
fn batch_results_surface_each_file() {
    let succeeded = job('c');
    let result_path = format!("/watermarks/jobs/{succeeded}/result");
    let view = batch(
        "completed",
        json!([
            {"index": 1, "filename": "b.png", "size": 9, "upload_id": "upl_1", "request_id": null,
             "status": "rejected", "error_code": "upload_size_mismatch", "error_detail": null, "credits": null},
            {"index": 0, "filename": "a.png", "size": 300, "upload_id": "upl_0", "request_id": succeeded,
             "status": "succeeded", "error_code": null, "error_detail": null, "credits": 1,
             "status_url": format!("/watermarks/jobs/{succeeded}"), "result_url": result_path,
             "result_expires_at": "2026-10-10T00:00:00Z"},
        ]),
    );
    let fake = Fake::new(move |seen, _, _| match path(seen) {
        p if p == result_path => (
            200,
            png(300),
            vec![
                ("Content-Type", "image/png".into()),
                ("X-Watermark-ID", "ab".repeat(32)),
            ],
        ),
        _ => ok_json(view.clone()),
    });
    let client = fake.client();
    let results: Vec<_> = client
        .batch_results(BATCH, Duration::from_secs(5))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let seen = fake.finish();
    assert_eq!(results.len(), 2);
    let (ok, rejected) = (&results[0], &results[1]);
    assert!(ok.is_ok());
    assert_eq!(ok.filename, "a.png");
    assert_eq!(ok.status, BatchItemState::Succeeded);
    assert_eq!(ok.result.as_ref().unwrap().watermark_id, "ab".repeat(32));
    assert_eq!(ok.credits, Some(1));
    assert!(!rejected.is_ok());
    assert_eq!(rejected.status, BatchItemState::Rejected);
    assert_eq!(rejected.error_code.as_deref(), Some("upload_size_mismatch"));
    assert_eq!(seen.len(), 2);
}

#[test]
fn a_failed_result_download_is_reported_for_that_file() {
    let view = batch(
        "completed",
        json!([{"index": 0, "filename": "a.png", "request_id": job('d'), "status": "succeeded", "credits": 1}]),
    );
    let fake = Fake::new(move |seen, _, _| match path(seen) {
        p if p.ends_with("/result") => (410, b"{\"detail\":\"Result expired\"}".to_vec(), vec![]),
        _ => ok_json(view.clone()),
    });
    let client = fake.client();
    let mut results = client.batch_results(BATCH, Duration::from_secs(5)).unwrap();
    let error = results.next().unwrap().unwrap_err();
    assert!(results.next().is_none());
    fake.finish();
    assert!(error.is_gone());
    assert!(error.to_string().contains("a.png"));
    assert_eq!(error.request_id, Some(job('d')));
}

#[test]
fn download_batch_archive_waits_while_the_archive_is_assembled() {
    let zip = b"PK\x03\x04archive".to_vec();
    let bytes = zip.clone();
    let fake = Fake::new(move |_, _, index| {
        if index == 0 {
            let mut view = batch("assembling", json!([]));
            view["archive"] = json!(true);
            view["archive_status"] = json!("assembling");
            return (
                202,
                view.to_string().into_bytes(),
                vec![("Retry-After", "0.05".into())],
            );
        }
        (
            200,
            bytes.clone(),
            vec![("Content-Type", "application/zip".into())],
        )
    });
    let archive = fake
        .client()
        .download_batch_archive(BATCH, Duration::from_secs(5))
        .unwrap();
    let seen = fake.finish();
    assert_eq!(archive, zip);
    assert_eq!(seen.len(), 2);
    assert!(
        seen.iter()
            .all(|s| path(s) == format!("/watermarks/batches/{BATCH}/archive"))
    );
    assert!(seen[1].at - seen[0].at >= Duration::from_millis(50));
}

#[test]
fn archive_conflicts_surface_their_code() {
    let fake = Fake::new(|_, _, _| {
        (
            409,
            json!({"detail": {"code": "archive_not_requested", "message": "No archive"}})
                .to_string()
                .into_bytes(),
            vec![],
        )
    });
    let error = fake
        .client()
        .download_batch_archive(BATCH, Duration::from_secs(5))
        .unwrap_err();
    fake.finish();
    assert_eq!(error.status_code, 409);
    assert_eq!(error.code().as_deref(), Some("archive_not_requested"));
}

#[test]
fn more_than_100_files_fail_before_any_request() {
    let fake = Fake::new(|_, _, _| panic!("no request expected"));
    let items: Vec<_> = (0..101)
        .map(|i| BatchItem::new(format!("{i}.png"), png(10), json!({"i": i})))
        .collect();
    let error = fake
        .client()
        .submit_batch(&items, BatchOptions::new())
        .unwrap_err();
    let zip_error = fake
        .client()
        .submit_batch_zip(b"PK", &[], BatchOptions::new())
        .unwrap_err();
    let empty_data = fake
        .client()
        .submit_batch(
            &[BatchItem::new("a.png", png(10), json!({}))],
            BatchOptions::new(),
        )
        .unwrap_err();
    let bad_concurrency = fake
        .client()
        .submit_batch(&items[..1], BatchOptions::new().upload_concurrency(0))
        .unwrap_err();
    let bad_id = fake.client().get_batch("bat_nope").unwrap_err();
    assert!(fake.finish().is_empty());
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    assert!(error.to_string().contains("1 to 100"));
    assert_eq!(zip_error.kind, ErrorKind::InvalidInput);
    assert_eq!(empty_data.kind, ErrorKind::InvalidInput);
    assert_eq!(bad_concurrency.kind, ErrorKind::InvalidInput);
    assert_eq!(bad_id.kind, ErrorKind::InvalidInput);
}

#[test]
fn submit_batch_zip_sends_the_zip_and_manifest() {
    let fake = Fake::new(|_, _, _| {
        let mut reply = ok_json(batch("starting", json!([])));
        reply.0 = 202;
        reply
    });
    let started = fake
        .client()
        .submit_batch_zip(
            b"PK\x03\x04zip",
            &[BatchZipItem::new("in/a.png", json!({"recipient": "acme"}))],
            BatchOptions::new().archive(true),
        )
        .unwrap();
    let seen = fake.finish();
    assert_eq!(started.status, BatchStatus::Starting);
    assert_eq!(seen.len(), 1);
    let request = &seen[0];
    assert_eq!(
        (request.method.as_str(), path(request)),
        ("POST", "/watermarks/batches/zip")
    );
    assert!(request.idempotency_key.is_some());
    assert!(
        request
            .content_type
            .as_deref()
            .unwrap()
            .starts_with("multipart/form-data")
    );
    let form = String::from_utf8_lossy(&request.body);
    assert!(form.contains("name=\"archive\"; filename=\"batch.zip\""));
    assert!(form.contains("Content-Type: application/zip"));
    assert!(form.contains("PK\u{3}\u{4}zip"));
    assert!(form.contains("name=\"manifest\""));
    assert!(form.contains(
        &json!({"archive": true, "items": [{"filename": "in/a.png", "data": {"recipient": "acme"}}]}).to_string()
    ));
}

#[test]
fn cancel_and_list() {
    let fake = Fake::new(|seen, _, _| match (seen.method.as_str(), path(seen)) {
        ("POST", p) if p.ends_with("/cancel") => ok_json(batch("cancelled", json!([]))),
        ("GET", "/watermarks/batches") => {
            let mut row = batch("completed", json!([]));
            row.as_object_mut().unwrap().remove("items");
            ok_json(json!({"data": [row], "next_cursor": BATCH}))
        }
        other => panic!("unexpected {other:?}"),
    });
    let client = fake.client();
    let cancelled = client.cancel_batch(BATCH).unwrap();
    let page = client.list_batches(Some(5), Some(BATCH)).unwrap();
    assert!(client.list_batches(Some(51), None).is_err());
    let seen = fake.finish();
    assert_eq!(cancelled.status, BatchStatus::Cancelled);
    assert_eq!(page.data.len(), 1);
    assert!(page.data[0].items.is_empty());
    assert_eq!(page.next_cursor.as_deref(), Some(BATCH));
    assert_eq!(seen[0].url, format!("/watermarks/batches/{BATCH}/cancel"));
    assert_eq!(
        seen[1].url,
        format!("/watermarks/batches?limit=5&before={BATCH}")
    );
}

fn archive_pending(retry_after: &str) -> Reply {
    let mut view = batch("assembling", json!([]));
    view["archive"] = json!(true);
    view["archive_status"] = json!("assembling");
    (
        202,
        view.to_string().into_bytes(),
        vec![("Retry-After", retry_after.into())],
    )
}

#[test]
fn archive_polling_is_bounded_by_the_wait_timeout_not_the_client_timeout() {
    let fake = Fake::new(|_, _, index| {
        if index == 0 {
            return archive_pending("0.2");
        }
        (200, b"PK\x03\x04zip".to_vec(), vec![])
    });
    let started = Instant::now();
    let archive = fake
        .client_with(Duration::from_millis(300))
        .download_batch_archive(BATCH, Duration::from_secs(5))
        .unwrap();
    let elapsed = started.elapsed();
    assert_eq!(fake.finish().len(), 2);
    assert_eq!(archive, b"PK\x03\x04zip");
    assert!(elapsed >= Duration::from_secs(1), "{elapsed:?}");
}

#[test]
fn archive_not_ready_within_the_wait_timeout() {
    let fake = Fake::new(|_, _, _| archive_pending("0.05"));
    let error = fake
        .client()
        .download_batch_archive(BATCH, Duration::from_millis(200))
        .unwrap_err();
    assert!(fake.finish().len() >= 2);
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(error.to_string().contains("not ready"), "{error}");
}

#[test]
fn archive_streams_to_a_sink_and_checks_the_cap() {
    let zip: Vec<u8> = [b"PK\x03\x04".as_slice(), &[7u8; 100_000]].concat();
    let body = zip.clone();
    let fake = Fake::new(move |_, _, _| (200, body.clone(), vec![]));
    let mut sink = Vec::new();
    let written = fake
        .client()
        .download_batch_archive_to(BATCH, &mut sink, Duration::ZERO)
        .unwrap();
    fake.finish();
    assert_eq!(written, zip.len() as u64);
    assert_eq!(sink, zip);
    assert_eq!(
        etchv::MAX_BATCH_ARCHIVE_SIZE,
        1024 * 1024 * 1024 + 64 * 1024 * 1024
    );

    let fake = Fake::new(|_, _, _| (200, b"<html>".to_vec(), vec![]));
    let error = fake
        .client()
        .download_batch_archive(BATCH, Duration::ZERO)
        .unwrap_err();
    fake.finish();
    assert_eq!(error.kind, ErrorKind::InvalidResponse);
}

#[test]
fn batch_results_wait_and_fill_in_cancelled_codes() {
    let fake = Fake::new(|_, _, index| {
        if index == 0 {
            let mut reply = ok_json(batch("processing", json!([])));
            reply.2.push(("Retry-After", "0.05".into()));
            return reply;
        }
        ok_json(batch(
            "cancelled",
            json!([
                {"index": 0, "filename": "a.png", "status": "pending", "error_code": null},
                {"index": 1, "filename": "b.png", "status": "rejected", "error_code": "invalid_input",
                 "error_detail": "not an image"},
            ]),
        ))
    });
    let results: Vec<_> = fake
        .client()
        .batch_results(BATCH, Duration::from_secs(5))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(fake.finish().len(), 2);
    assert_eq!(results[0].error_code.as_deref(), Some("cancelled"));
    assert_eq!(results[0].status, BatchItemState::Pending);
    assert_eq!(results[1].error_code.as_deref(), Some("invalid_input"));
    assert_eq!(results[1].error_detail.as_deref(), Some("not an image"));
}

#[test]
fn an_expired_replay_fails_and_a_started_replay_is_returned_as_is() {
    let fake = Fake::new(|_, origin, _| {
        ok_json(batch(
            "expired",
            json!([pending(origin, 0, "logo.png", 300)]),
        ))
    });
    let items = vec![BatchItem::new("logo.png", png(300), json!({"a": 1}))];
    let error = fake
        .client()
        .submit_batch(&items, BatchOptions::new().idempotency_key("batch_key_004"))
        .unwrap_err();
    assert_eq!(fake.finish().len(), 1);
    assert_eq!(error.status_code, 410);
    assert_eq!(error.code().as_deref(), Some("batch_expired"));
    assert!(error.to_string().contains("expired before it was started"));
    assert_eq!(error.idempotency_key.as_deref(), Some("batch_key_004"));

    let fake = Fake::new(|_, _, _| ok_json(batch("processing", json!([]))));
    let batch = fake
        .client()
        .submit_batch(&items, BatchOptions::new())
        .unwrap();
    assert_eq!(fake.finish().len(), 1);
    assert_eq!(batch.status, BatchStatus::Processing);
}

#[test]
fn a_replay_uploads_only_the_files_not_yet_received() {
    let fake = Fake::new(|seen, origin, _| match path(seen) {
        "/watermarks/batches" => ok_json(batch(
            "draft",
            json!([
                {"index": 0, "filename": "logo.png", "size": 300, "upload_id": "upl_0",
                 "status": "pending", "upload_received": true},
                pending(origin, 1, "contract.pdf", 9),
            ]),
        )),
        _ => flow(seen, origin),
    });
    let items = vec![
        BatchItem::new("logo.png", png(300), json!({"a": 1})),
        BatchItem::new("contract.pdf", b"%PDF-1.7\n".to_vec(), json!({"b": 2})),
    ];
    fake.client()
        .submit_batch(&items, BatchOptions::new())
        .unwrap();
    let seen = fake.finish();
    let puts: Vec<_> = seen
        .iter()
        .filter(|s| s.method == "PUT")
        .map(path)
        .collect();
    assert_eq!(puts, ["/signed/1"]);
    assert!(seen.iter().any(|s| path(s).ends_with("/start")));
}

#[test]
fn a_file_that_changed_size_fails_at_once() {
    let dir = std::env::temp_dir().join(format!("etchv-batch-size-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("contract.pdf");
    std::fs::write(&file, b"%PDF-1.7\n").unwrap();
    let grown = file.clone();
    let fake = Fake::new(move |seen, origin, _| {
        if path(seen) == "/watermarks/batches" {
            std::fs::write(&grown, b"%PDF-1.7\n% grown").unwrap();
        }
        flow(seen, origin)
    });
    let items = vec![
        BatchItem::new("logo.png", png(300), json!({"a": 1})),
        BatchItem::from_path(&file, json!({"b": 2})),
    ];
    let error = fake
        .client()
        .submit_batch(&items, BatchOptions::new().upload_concurrency(1))
        .unwrap_err();
    let seen = fake.finish();
    std::fs::remove_dir_all(&dir).unwrap();
    assert_eq!(error.kind, ErrorKind::InvalidInput);
    let text = error.to_string();
    assert!(
        text.contains("changed size") && text.contains(BATCH),
        "{text}"
    );
    assert!(!seen.iter().any(|s| path(s) == "/signed/1"));
    assert!(!seen.iter().any(|s| path(s).ends_with("/start")));
}

/// A server for `upload_file` whose signed URL is answered by `put`.
fn upload_server(
    put: impl Fn(tiny_http::Request) + Send + 'static,
) -> (String, Arc<AtomicBool>, thread::JoinHandle<()>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let stop = Arc::new(AtomicBool::new(false));
    let (done, origin) = (stop.clone(), base.clone());
    let worker = thread::spawn(move || {
        while !done.load(Ordering::Relaxed) {
            let Some(req) = server.recv_timeout(Duration::from_millis(20)).unwrap() else {
                continue;
            };
            if req.url() == "/uploads" {
                let session = json!({"upload_id": "upl_1", "kind": "image", "filename": "a.png", "size": 1,
                    "status": "pending", "expires_at": "x",
                    "upload": {"method": "PUT", "url": format!("{origin}/signed/a.png"), "expires_at": "x"}});
                req.respond(
                    Response::from_data(session.to_string().into_bytes()).with_status_code(201),
                )
                .unwrap();
            } else {
                put(req);
            }
        }
    });
    (base, stop, worker)
}

#[test]
fn a_slow_upload_is_not_cut_off_while_bytes_keep_flowing() {
    let received = Arc::new(Mutex::new(0usize));
    let total = received.clone();
    let (base, stop, worker) = upload_server(move |mut req| {
        let mut chunk = vec![0; 512 * 1024];
        let reader = req.as_reader();
        loop {
            let n = reader.read(&mut chunk).unwrap();
            if n == 0 {
                break;
            }
            *total.lock().unwrap() += n;
            thread::sleep(Duration::from_millis(30));
        }
        req.respond(Response::empty(200)).unwrap();
    });
    let file = png(24 * 1024 * 1024);
    let client = Client::with_options("test-key", &base, Duration::from_secs(1)).unwrap();
    let started = Instant::now();
    let session = client
        .upload_file(UploadKind::Image, &file, "a.png")
        .unwrap();
    let elapsed = started.elapsed();
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    assert_eq!(session.status, "received");
    assert_eq!(*received.lock().unwrap(), file.len());
    assert!(elapsed > Duration::from_secs(1), "{elapsed:?}");
}

#[test]
fn a_stalled_upload_fails_after_the_idle_timeout() {
    let held = Arc::new(Mutex::new(Vec::new()));
    let hold = held.clone();
    let (base, stop, worker) = upload_server(move |req| hold.lock().unwrap().push(req));
    let client = Client::with_options("test-key", &base, Duration::from_millis(300)).unwrap();
    let started = Instant::now();
    let error = client
        .upload_file(UploadKind::Image, &png(1000), "a.png")
        .unwrap_err();
    let elapsed = started.elapsed();
    let attempts = held.lock().unwrap().len();
    stop.store(true, Ordering::Relaxed);
    worker.join().unwrap();
    held.lock().unwrap().clear();
    assert_eq!(error.kind, ErrorKind::Timeout);
    assert!(error.to_string().contains("stalled"), "{error}");
    assert!(attempts >= 2, "{attempts}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
}

#[test]
fn an_item_without_a_link_or_its_file_fails_before_any_upload() {
    let fake = Fake::new(|seen, origin, _| match path(seen) {
        "/watermarks/batches" => ok_json(batch(
            "draft",
            json!([
                {"index": 0, "filename": "logo.png", "size": 300, "upload_id": "upl_0", "status": "pending"},
                pending(origin, 1, "contract.pdf", 9),
            ]),
        )),
        _ => flow(seen, origin),
    });
    let items = vec![
        BatchItem::new("logo.png", png(300), json!({"a": 1})),
        BatchItem::new("contract.pdf", b"%PDF-1.7\n".to_vec(), json!({"b": 2})),
    ];
    let error = fake
        .client()
        .submit_batch(&items, BatchOptions::new().idempotency_key("batch_key_005"))
        .unwrap_err();
    let seen = fake.finish();
    assert_eq!(seen.len(), 1);
    assert_eq!(error.kind, ErrorKind::InvalidResponse);
    assert!(error.to_string().contains("logo.png"), "{error}");
    assert_eq!(error.idempotency_key.as_deref(), Some("batch_key_005"));
}

/// Yields `chunks` pieces of 64 KiB, the first starting with "PK", `pause` apart.
struct Trickle {
    chunks: usize,
    sent: usize,
    pause: Duration,
}

impl std::io::Read for Trickle {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        if self.sent == self.chunks {
            return Ok(0);
        }
        if self.sent > 0 {
            thread::sleep(self.pause);
        }
        let n = buf.len().min(64 * 1024);
        buf[..n].fill(7);
        if self.sent == 0 {
            buf[..2].copy_from_slice(b"PK");
        }
        self.sent += 1;
        Ok(n)
    }
}

/// Serve one archive whose body trickles out over `chunks` × `pause`.
fn trickle_server(chunks: usize, pause: Duration) -> (String, thread::JoinHandle<()>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let worker = thread::spawn(move || {
        let request = server.recv().unwrap();
        let body = Trickle {
            chunks,
            sent: 0,
            pause,
        };
        let response = Response::new(200.into(), vec![], body, None, None);
        let _ = request.respond(response);
    });
    (base, worker)
}

#[test]
fn a_slow_but_steady_archive_outlasts_the_client_timeout() {
    // 12 pieces 300 ms apart: about 3.3 s in all, never 1 s without data.
    let (base, worker) = trickle_server(12, Duration::from_millis(300));
    let client = Client::with_options("test-key", &base, Duration::from_secs(1)).unwrap();
    let started = Instant::now();
    let mut sink = Vec::new();
    let written = client
        .download_batch_archive_to(BATCH, &mut sink, Duration::ZERO)
        .unwrap();
    let elapsed = started.elapsed();
    worker.join().unwrap();
    assert!(elapsed > Duration::from_secs(3), "{elapsed:?}");
    assert!(sink.starts_with(b"PK"));
    assert_eq!(written, sink.len() as u64);
    assert!(written >= 12);
}

/// A sink that takes longer than the client timeout to accept each write.
struct SlowSink(Vec<u8>);

impl std::io::Write for SlowSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        thread::sleep(Duration::from_millis(400));
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
fn time_spent_writing_to_the_sink_is_not_idle_time() {
    let (base, worker) = trickle_server(3, Duration::from_millis(10));
    let client = Client::with_options("test-key", &base, Duration::from_millis(300)).unwrap();
    let mut sink = SlowSink(Vec::new());
    let written = client
        .download_batch_archive_to(BATCH, &mut sink, Duration::ZERO)
        .unwrap();
    worker.join().unwrap();
    assert_eq!(written, sink.0.len() as u64);
    assert!(sink.0.starts_with(b"PK"));
}
