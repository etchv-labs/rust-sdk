use etchv::{Client, Options, UploadKind};
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use tiny_http::{Header, Response, Server};

const UPLOAD: &str = "upl_11111111111111111111111111111111";

fn id() -> String {
    "ab".repeat(32)
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
    idempotency_key: Option<String>,
    body: Vec<u8>,
}

/// A local server answering the session flow; `respond` picks the status per request.
fn serve(
    count: usize,
    respond: impl Fn(&Seen, &str, usize) -> (u16, Vec<u8>, Vec<(&'static str, String)>) + Send + 'static,
) -> (String, Arc<Mutex<Vec<Seen>>>, thread::JoinHandle<()>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let seen = Arc::new(Mutex::new(Vec::new()));
    let log = seen.clone();
    let origin = base.clone();
    let worker = thread::spawn(move || {
        for index in 0..count {
            let mut req = server
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
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
                idempotency_key: header("Idempotency-Key"),
                body: Vec::new(),
            };
            req.as_reader().read_to_end(&mut seen.body).unwrap();
            let (status, body, headers) = respond(&seen, &origin, index);
            log.lock().unwrap().push(seen);
            let mut response = Response::from_data(body).with_status_code(status);
            for (name, value) in headers {
                response.add_header(Header::from_bytes(name, value).unwrap());
            }
            req.respond(response).unwrap();
        }
    });
    (base, seen, worker)
}

fn session(origin: &str, kind: &str) -> String {
    json!({"upload_id": UPLOAD, "kind": kind, "filename": "photo.png", "size": 300, "status": "pending",
           "expires_at": "2026-10-10T00:00:00Z",
           "upload": {"method": "PUT", "url": format!("{origin}/signed/{kind}/{UPLOAD}.png?Signature=s"), "expires_at": "x"}})
    .to_string()
}

fn embedded() -> (u16, Vec<u8>, Vec<(&'static str, String)>) {
    (
        200,
        png(300),
        vec![
            ("Content-Type", "image/png".into()),
            ("X-Watermark-ID", id()),
        ],
    )
}

#[test]
fn large_embeds_upload_once_without_the_api_key_then_send_the_upload_id() {
    let file = png(300);
    let (base, seen, worker) = serve(3, |req, origin, _| match req.url.as_str() {
        "/uploads" => (201, session(origin, "image").into_bytes(), vec![]),
        u if u.starts_with("/signed/") => (200, Vec::new(), vec![]),
        _ => embedded(),
    });
    let client = Client::with_options("test-key", &base, Duration::from_secs(5))
        .unwrap()
        .with_large_file_threshold(100)
        .unwrap();
    let result = client
        .embed_image(
            &file,
            &json!({"recipient": "test"}),
            Options::new()
                .filename("photo.png")
                .idempotency_key("key_00001"),
        )
        .unwrap();
    worker.join().unwrap();
    assert_eq!(result.watermark_id, id());
    let seen = seen.lock().unwrap();
    let calls: Vec<_> = seen
        .iter()
        .map(|s| format!("{} {}", s.method, s.url.split('?').next().unwrap()))
        .collect();
    assert_eq!(
        calls,
        [
            "POST /uploads".to_owned(),
            format!("PUT /signed/image/{UPLOAD}.png"),
            "POST /watermarks/images".to_owned(),
        ]
    );
    let created: serde_json::Value = serde_json::from_slice(&seen[0].body).unwrap();
    assert_eq!(
        created,
        json!({"kind": "image", "filename": "photo.png", "size": 300})
    );
    assert_eq!(seen[1].api_key, None);
    assert_eq!(seen[1].body, file);
    let form = String::from_utf8_lossy(&seen[2].body);
    assert!(form.contains("name=\"upload_id\"") && form.contains(UPLOAD));
    assert!(!form.contains("name=\"file\""));
    assert!(form.contains("{\"recipient\":\"test\"}"));
    assert_eq!(seen[2].idempotency_key.as_deref(), Some("key_00001"));
}

#[test]
fn retries_resend_the_same_upload_without_uploading_again() {
    let (base, seen, worker) = serve(4, |req, origin, index| match req.url.as_str() {
        "/uploads" => (201, session(origin, "image").into_bytes(), vec![]),
        u if u.starts_with("/signed/") => (200, Vec::new(), vec![]),
        _ if index == 2 => (
            503,
            json!({"detail": "busy"}).to_string().into_bytes(),
            vec![("Retry-After", "0.01".into())],
        ),
        _ => embedded(),
    });
    let client = Client::with_options("test-key", &base, Duration::from_secs(5))
        .unwrap()
        .with_large_file_threshold(100)
        .unwrap();
    client
        .embed_image(&png(300), &json!({"recipient": "test"}), Options::new())
        .unwrap();
    worker.join().unwrap();
    let seen = seen.lock().unwrap();
    assert_eq!(seen.iter().filter(|s| s.method == "PUT").count(), 1);
    let posts: Vec<_> = seen
        .iter()
        .filter(|s| s.url == "/watermarks/images")
        .collect();
    assert_eq!(posts.len(), 2);
    assert!(
        posts
            .iter()
            .all(|s| String::from_utf8_lossy(&s.body).contains(UPLOAD))
    );
    assert_eq!(posts[0].idempotency_key, posts[1].idempotency_key);
}

#[test]
fn large_sync_image_detection_runs_as_a_job() {
    let job = format!("req_{}", "c".repeat(64));
    let result_path = format!("/watermarks/detection-jobs/{job}/result");
    let receipt = json!({"request_id": job, "status": "queued"}).to_string();
    let (base, seen, worker) = serve(4, move |req, origin, _| match req.url.as_str() {
        "/uploads" => {
            let created: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            assert_eq!(created["kind"], "detect");
            (201, session(origin, "detect").into_bytes(), vec![])
        }
        u if u.starts_with("/signed/") => (200, Vec::new(), vec![]),
        "/watermarks/images/detect/async" => (202, receipt.clone().into_bytes(), vec![]),
        _ => (
            200,
            json!({"watermarked": true, "confidence": 0.99, "watermark_id": id()})
                .to_string()
                .into_bytes(),
            vec![],
        ),
    });
    let client = Client::with_options("test-key", &base, Duration::from_secs(30)).unwrap();
    let result = client
        .detect_image(&png(95 * 1024 * 1024 + 1), Options::new())
        .unwrap();
    worker.join().unwrap();
    assert_eq!(result.watermark_id, Some(id()));
    let paths: Vec<_> = seen
        .lock()
        .unwrap()
        .iter()
        .map(|s| s.url.split('?').next().unwrap().to_owned())
        .collect();
    assert_eq!(
        paths,
        [
            "/uploads".to_owned(),
            format!("/signed/detect/{UPLOAD}.png"),
            "/watermarks/images/detect/async".to_owned(),
            result_path,
        ]
    );
}

#[test]
fn small_files_stay_in_the_request_body_and_limits_follow_the_operation() {
    let (base, seen, worker) = serve(1, |_, _, _| embedded());
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    client
        .embed_image(&png(300), &json!({"a": 1}), Options::new())
        .unwrap();
    worker.join().unwrap();
    assert!(String::from_utf8_lossy(&seen.lock().unwrap()[0].body).contains("name=\"file\""));

    let offline = Client::new("test-key").unwrap();
    let embed = offline.embed_image(
        &vec![0; etchv::MAX_FILE_SIZE + 1],
        &json!({"a": 1}),
        Options::new(),
    );
    assert!(embed.unwrap_err().to_string().contains("50 MB"));
    let detect = offline.detect_image(&vec![0; etchv::MAX_DETECTION_FILE_SIZE + 1], Options::new());
    assert!(detect.unwrap_err().to_string().contains("192 MB"));
    assert!(
        offline
            .upload_file(UploadKind::Image, &[], "a.png")
            .is_err()
    );
    assert!(
        Client::new("test-key")
            .unwrap()
            .with_large_file_threshold(0)
            .is_err()
    );
}

#[test]
fn a_refused_upload_raises_with_its_status() {
    let (base, _, worker) = serve(2, |req, origin, _| match req.url.as_str() {
        "/uploads" => (201, session(origin, "image").into_bytes(), vec![]),
        _ => (403, b"Forbidden".to_vec(), vec![]),
    });
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let error = client
        .upload_file(UploadKind::Image, &png(300), "photo.png")
        .unwrap_err();
    worker.join().unwrap();
    assert_eq!(error.status_code, 403);
}

#[test]
fn upload_file_returns_the_received_session() {
    let (base, _, worker) = serve(2, |req, origin, _| match req.url.as_str() {
        "/uploads" => (201, session(origin, "document").into_bytes(), vec![]),
        _ => (200, Vec::new(), vec![]),
    });
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let session = client
        .upload_file(UploadKind::Document, &png(300), "photo.png")
        .unwrap();
    worker.join().unwrap();
    assert_eq!(
        (
            session.upload_id.as_str(),
            session.kind.as_str(),
            session.status.as_str()
        ),
        (UPLOAD, "document", "received")
    );
}
