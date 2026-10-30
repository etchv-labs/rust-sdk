//! Dropped connections: instant detection, downloads and batch archives.

use etchv::{Client, ErrorKind, Options};
use serde_json::json;
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

const ID: &str = "abababababababababababababababababababababababababababababababab";
const BATCH: &str = "bat_0123456789abcdef0123456789abcdef";

#[derive(Debug, Clone)]
struct Seen {
    method: String,
    url: String,
    idempotency_key: Option<String>,
    body: Vec<u8>,
    at: Instant,
}

/// How the fake API answers one request.
enum Answer {
    /// Close the connection without a response.
    Reset,
    Reply(u16, Vec<(&'static str, String)>, Vec<u8>),
    /// Promise `declared` bytes, send `head`, then close the connection.
    Cut(Vec<u8>, usize),
}

fn json_reply(status: u16, value: serde_json::Value) -> Answer {
    Answer::Reply(
        status,
        vec![("Content-Type", "application/json".into())],
        value.to_string().into_bytes(),
    )
}

fn detected() -> Answer {
    json_reply(
        200,
        json!({"watermarked": true, "confidence": 0.99, "watermark_id": ID}),
    )
}

fn conflict(code: &str) -> Answer {
    json_reply(409, json!({"detail": {"code": code, "message": code}}))
}

/// A local API on raw sockets, so a test can drop a connection anywhere. Every
/// response closes its connection.
struct Raw {
    base: String,
    seen: Arc<Mutex<Vec<Seen>>>,
    stop: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}

impl Raw {
    fn new(answer: impl Fn(&Seen, &str, usize) -> Answer + Send + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (log, done, origin) = (seen.clone(), stop.clone(), base.clone());
        let worker = thread::spawn(move || {
            let mut index = 0;
            while !done.load(Ordering::Relaxed) {
                let Ok((stream, _)) = listener.accept() else {
                    thread::sleep(Duration::from_millis(5));
                    continue;
                };
                stream.set_nonblocking(false).unwrap();
                let request = read_request(&stream);
                let reply = answer(&request, &origin, index);
                index += 1;
                log.lock().unwrap().push(request);
                respond(stream, reply);
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
        Client::with_options("test-key", &self.base, Duration::from_secs(10)).unwrap()
    }

    fn finish(mut self) -> Vec<Seen> {
        self.stop.store(true, Ordering::Relaxed);
        self.worker.take().unwrap().join().unwrap();
        self.seen.lock().unwrap().clone()
    }
}

fn read_request(stream: &TcpStream) -> Seen {
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let mut parts = line.split_whitespace();
    let (method, url) = (
        parts.next().unwrap().to_owned(),
        parts.next().unwrap().to_owned(),
    );
    let (mut length, mut chunked, mut idempotency_key) = (0, false, None);
    loop {
        let mut header = String::new();
        reader.read_line(&mut header).unwrap();
        let header = header.trim_end();
        if header.is_empty() {
            break;
        }
        let (name, value) = header.split_once(':').unwrap();
        let value = value.trim();
        match name.to_ascii_lowercase().as_str() {
            "content-length" => length = value.parse().unwrap(),
            "transfer-encoding" => chunked = value.eq_ignore_ascii_case("chunked"),
            "idempotency-key" => idempotency_key = Some(value.to_owned()),
            _ => {}
        }
    }
    let mut body = vec![0; length];
    reader.read_exact(&mut body).unwrap();
    while chunked {
        let mut size = String::new();
        reader.read_line(&mut size).unwrap();
        let size = usize::from_str_radix(size.trim(), 16).unwrap();
        let mut chunk = vec![0; size + 2];
        reader.read_exact(&mut chunk).unwrap();
        body.extend_from_slice(&chunk[..size]);
        chunked = size > 0;
    }
    Seen {
        method,
        url,
        idempotency_key,
        body,
        at: Instant::now(),
    }
}

fn respond(mut stream: TcpStream, answer: Answer) {
    let (status, headers, body, declared) = match answer {
        Answer::Reset => return,
        Answer::Reply(status, headers, body) => {
            let length = body.len();
            (status, headers, body, length)
        }
        Answer::Cut(head, declared) => (200, vec![], head, declared),
    };
    let mut head =
        format!("HTTP/1.1 {status} X\r\nConnection: close\r\nContent-Length: {declared}\r\n");
    for (name, value) in headers {
        head.push_str(&format!("{name}: {value}\r\n"));
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(&body);
    let _ = stream.flush();
}

fn path(seen: &Seen) -> &str {
    seen.url.split('?').next().unwrap()
}

fn png() -> Vec<u8> {
    let mut bytes = vec![b'x'; 64];
    bytes[..8].copy_from_slice(b"\x89PNG\r\n\x1a\n");
    bytes
}

#[test]
fn instant_detection_resends_a_dropped_connection_with_the_same_generated_key() {
    for document in [false, true] {
        let fake = Raw::new(|_, _, index| {
            if index == 0 {
                Answer::Reset
            } else {
                detected()
            }
        });
        let client = fake.client();
        let result = if document {
            client.detect_document(&png(), Options::new())
        } else {
            client.detect_image(&png(), Options::new())
        };
        assert_eq!(result.unwrap().watermark_id.as_deref(), Some(ID));
        let seen = fake.finish();
        assert_eq!(seen.len(), 2);
        let key = seen[0].idempotency_key.clone().expect("a generated key");
        assert_eq!(key.len(), 36);
        assert_eq!(seen[1].idempotency_key.as_deref(), Some(key.as_str()));
    }
}

#[test]
fn a_response_cut_off_is_resent_with_the_given_key() {
    let fake = Raw::new(|_, _, index| {
        if index == 0 {
            Answer::Cut(b"{\"watermarked\"".to_vec(), 500)
        } else {
            detected()
        }
    });
    fake.client()
        .detect_image(&png(), Options::new().idempotency_key("scan-0001"))
        .unwrap();
    let keys: Vec<_> = fake
        .finish()
        .into_iter()
        .map(|s| s.idempotency_key)
        .collect();
    assert_eq!(
        keys,
        vec![Some("scan-0001".to_owned()), Some("scan-0001".to_owned())]
    );
}

#[test]
fn request_in_progress_waits_for_retry_after_and_sends_again() {
    let fake = Raw::new(|_, _, index| match index {
        0 => Answer::Reply(
            409,
            vec![("Retry-After", "0.3".into())],
            json!({"detail": {"code": "request_in_progress", "message": "Still running"}})
                .to_string()
                .into_bytes(),
        ),
        _ => detected(),
    });
    assert!(
        fake.client()
            .detect_image(&png(), Options::new())
            .unwrap()
            .watermarked
    );
    let seen = fake.finish();
    assert_eq!(seen.len(), 2);
    assert!(seen[1].at - seen[0].at >= Duration::from_millis(300));
    assert_eq!(seen[0].idempotency_key, seen[1].idempotency_key);
}

#[test]
fn replay_expired_and_request_failed_are_returned_without_retrying() {
    for code in ["replay_expired", "request_failed"] {
        let fake = Raw::new(move |_, _, _| conflict(code));
        let error = fake
            .client()
            .detect_document(&png(), Options::new().idempotency_key("scan-0002"))
            .unwrap_err();
        assert_eq!(fake.finish().len(), 1);
        assert_eq!((error.kind, error.status_code), (ErrorKind::Api, 409));
        assert_eq!(error.code().as_deref(), Some(code));
        assert_eq!(error.idempotency_key.as_deref(), Some("scan-0002"));
    }
}

#[test]
fn other_detection_errors_are_still_not_retried() {
    let fake = Raw::new(|_, _, _| json_reply(503, json!({"detail": "busy"})));
    let error = fake
        .client()
        .detect_image(&png(), Options::new())
        .unwrap_err();
    assert_eq!((error.status_code, fake.finish().len()), (503, 1));
}

#[test]
fn keep_result_is_a_query_parameter_for_instant_detection_only() {
    let fake = Raw::new(|_, _, _| detected());
    let client = fake.client();
    client.detect_image(&png(), Options::new()).unwrap();
    client
        .detect_image(&png(), Options::new().keep_result(false))
        .unwrap();
    client
        .detect_document(&png(), Options::new().keep_result(true))
        .unwrap();
    let refused = client
        .embed_image(&png(), &json!({"a": 1}), Options::new().keep_result(false))
        .unwrap_err();
    assert_eq!(refused.kind, ErrorKind::InvalidInput);
    let urls: Vec<_> = fake.finish().into_iter().map(|s| s.url).collect();
    assert_eq!(
        urls,
        vec![
            "/watermarks/images/detect",
            "/watermarks/images/detect?keep_result=false",
            "/watermarks/documents/detect?keep_result=true",
        ]
    );
}

#[test]
fn a_consumed_upload_is_replaced_by_a_new_upload_with_the_same_key() {
    let (first, second) = (
        format!("upl_{}", "1".repeat(32)),
        format!("upl_{}", "2".repeat(32)),
    );
    let (uploads, detects) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let fake = Raw::new(
        move |seen, origin, _| match (seen.method.as_str(), path(seen)) {
            ("POST", "/uploads") => {
                let id = if uploads.fetch_add(1, Ordering::SeqCst) == 0 {
                    &first
                } else {
                    &second
                };
                json_reply(
                    201,
                    json!({"upload_id": id, "kind": "detect", "filename": "image.png", "size": 64,
                       "status": "pending", "expires_at": "2026-10-31T00:00:00+00:00",
                       "upload": {"method": "PUT", "url": format!("{origin}/signed/{id}")}}),
                )
            }
            ("PUT", _) => Answer::Reply(200, vec![], vec![]),
            _ => {
                let form = String::from_utf8_lossy(&seen.body);
                match (
                    form.contains(&first),
                    detects.fetch_add(1, Ordering::SeqCst),
                ) {
                    (true, 0) => Answer::Reset,
                    (true, _) => conflict("upload_consumed"),
                    _ => detected(),
                }
            }
        },
    );
    let client = fake.client().with_large_file_threshold(10).unwrap();
    client
        .detect_image(&png(), Options::new().keep_result(false))
        .unwrap();
    let seen = fake.finish();
    let detects: Vec<_> = seen
        .iter()
        .filter(|s| path(s) == "/watermarks/images/detect")
        .collect();
    assert_eq!(detects.len(), 3);
    let uploads: Vec<_> = detects
        .iter()
        .map(|s| String::from_utf8_lossy(&s.body).contains(&"1".repeat(32)))
        .collect();
    assert_eq!(uploads, vec![true, true, false]);
    assert!(
        detects
            .iter()
            .all(|s| s.idempotency_key == detects[0].idempotency_key)
    );
    assert_eq!(seen.iter().filter(|s| s.method == "PUT").count(), 2);
}

/// The answers to an upload session and its signed PUT, or `None` for any other request.
fn upload_answer(seen: &Seen, origin: &str, id: &str) -> Option<Answer> {
    match (seen.method.as_str(), path(seen)) {
        ("POST", "/uploads") => Some(json_reply(
            201,
            json!({"upload_id": id, "kind": "detect", "filename": "image.png", "size": 64,
                   "status": "pending", "expires_at": "2026-10-31T00:00:00+00:00",
                   "upload": {"method": "PUT", "url": format!("{origin}/signed/{id}")}}),
        )),
        ("PUT", _) => Some(Answer::Reply(200, vec![], vec![])),
        _ => None,
    }
}

#[test]
fn an_upload_still_held_by_the_first_copy_is_waited_for() {
    let id = format!("upl_{}", "1".repeat(32));
    let upload = id.clone();
    let detects = AtomicUsize::new(0);
    let fake = Raw::new(move |seen, origin, _| {
        upload_answer(seen, origin, &upload).unwrap_or_else(|| {
            match detects.fetch_add(1, Ordering::SeqCst) {
                0 => Answer::Reset,
                1 => conflict("upload_in_use"),
                _ => detected(),
            }
        })
    });
    let client = fake.client().with_large_file_threshold(10).unwrap();
    assert!(
        client
            .detect_image(&png(), Options::new())
            .unwrap()
            .watermarked
    );
    let seen = fake.finish();
    let detects: Vec<_> = seen
        .iter()
        .filter(|s| path(s) == "/watermarks/images/detect")
        .collect();
    assert_eq!(detects.len(), 3);
    assert!(detects.iter().all(|s| {
        s.idempotency_key == detects[0].idempotency_key
            && String::from_utf8_lossy(&s.body).contains(&id)
    }));
    assert!(detects[2].at - detects[1].at >= Duration::from_millis(900));
    assert_eq!(seen.iter().filter(|s| path(s) == "/uploads").count(), 1);
}

#[test]
fn upload_in_use_is_not_waited_for_on_a_file_in_the_request_body() {
    let fake = Raw::new(|_, _, _| conflict("upload_in_use"));
    let error = fake
        .client()
        .detect_image(&png(), Options::new())
        .unwrap_err();
    let seen = fake.finish();
    assert_eq!((error.status_code, seen.len()), (409, 1));
    assert_eq!(error.code().as_deref(), Some("upload_in_use"));
    // The generated key comes back with the error, so the caller can resend.
    assert_eq!(error.idempotency_key, seen[0].idempotency_key);
    assert_eq!(error.idempotency_key.map(|k| k.len()), Some(36));
}

#[test]
fn the_resend_after_a_new_upload_keeps_the_first_deadline() {
    let (first, second) = (
        format!("upl_{}", "1".repeat(32)),
        format!("upl_{}", "2".repeat(32)),
    );
    let (sessions, puts) = (AtomicUsize::new(0), AtomicUsize::new(0));
    let fake = Raw::new(move |seen, origin, _| {
        // The re-upload is slow: its session and PUT together outlast the 2 s timeout, each
        // well within it. A slow runner only adds time, so it can only reach the deadline sooner.
        let counter = match (seen.method.as_str(), path(seen)) {
            ("POST", "/uploads") => Some(&sessions),
            ("PUT", _) => Some(&puts),
            _ => None,
        };
        if let Some(counter) = counter {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            if n == 1 {
                thread::sleep(Duration::from_millis(1200));
            }
            let id = if sessions.load(Ordering::SeqCst) < 2 {
                &first
            } else {
                &second
            };
            return upload_answer(seen, origin, id).unwrap();
        }
        if String::from_utf8_lossy(&seen.body).contains(&"1".repeat(32)) {
            conflict("upload_consumed")
        } else {
            // Reached only with a fresh timeout window per send, the regression this catches.
            detected()
        }
    });
    let client = Client::with_options("test-key", &fake.base, Duration::from_secs(2))
        .unwrap()
        .with_large_file_threshold(10)
        .unwrap();
    let error = client
        .detect_image(&png(), Options::new().idempotency_key("scan-0005"))
        .unwrap_err();
    assert_eq!((error.kind, error.status_code), (ErrorKind::Timeout, 0));
    assert_eq!(error.idempotency_key.as_deref(), Some("scan-0005"));
    let seen = fake.finish();
    let count = |method: &str, route: &str| {
        seen.iter()
            .filter(|s| s.method == method && (route.is_empty() || path(s) == route))
            .count()
    };
    assert_eq!(count("POST", "/watermarks/images/detect"), 1);
    assert_eq!((count("POST", "/uploads"), count("PUT", "")), (2, 2));
}

#[test]
fn a_download_out_of_time_returns_the_dropped_connection() {
    let fake = Raw::new(|_, _, _| Answer::Reset);
    let client = Client::with_options("test-key", &fake.base, Duration::from_millis(1500)).unwrap();
    let error = client
        .download_asset(&format!("ast_{}", "a".repeat(64)))
        .unwrap_err();
    assert_eq!((error.kind, error.status_code), (ErrorKind::Transport, 0));
    assert!(error.idempotency_key.is_none());
    // Sent again after the first drop, then out of time for another try.
    assert!((1..=2).contains(&fake.finish().len()));
}

#[test]
fn downloads_retry_a_dropped_connection() {
    let asset = format!("ast_{}", "a".repeat(64));
    let delivery = format!("std_{}", "d".repeat(64));
    for (download, expected) in [
        (0, format!("/assets/{asset}/content")),
        (1, format!("/storage/deliveries/{delivery}/content")),
    ] {
        let fake = Raw::new(|_, _, index| match index {
            0 => Answer::Reset,
            1 => Answer::Cut(b"fi".to_vec(), 4),
            _ => Answer::Reply(200, vec![], b"file".to_vec()),
        });
        let client = fake.client();
        let bytes = if download == 0 {
            client.download_asset(&asset)
        } else {
            client.download_storage_delivery(&delivery)
        };
        assert_eq!(bytes.unwrap(), b"file");
        let paths: Vec<_> = fake.finish().iter().map(|s| path(s).to_owned()).collect();
        assert_eq!(paths, vec![expected.clone(), expected.clone(), expected]);
    }
}

#[test]
fn download_errors_are_not_retried() {
    let fake = Raw::new(|_, _, _| json_reply(503, json!({"detail": "busy"})));
    let error = fake
        .client()
        .download_asset(&format!("ast_{}", "a".repeat(64)))
        .unwrap_err();
    assert_eq!((error.status_code, fake.finish().len()), (503, 1));
}

#[test]
fn a_dropped_archive_starts_again_in_memory() {
    let mut zip = b"PK\x03\x04".to_vec();
    zip.extend(vec![b'z'; 200_000]);
    let full = zip.clone();
    let fake = Raw::new(move |seen, _, index| {
        assert!(seen.url.ends_with("/archive"));
        if index == 0 {
            Answer::Cut(full[..100_000].to_vec(), full.len())
        } else {
            Answer::Reply(200, vec![], full.clone())
        }
    });
    let archive = fake
        .client()
        .download_batch_archive(BATCH, Duration::from_secs(5))
        .unwrap();
    assert_eq!(archive, zip);
    assert_eq!(fake.finish().len(), 2);
}

#[test]
fn a_dropped_archive_into_a_writer_returns_the_error() {
    let fake = Raw::new(|_, _, _| Answer::Cut(b"PK\x03\x04partial".to_vec(), 1000));
    let mut sink = Vec::new();
    let error = fake
        .client()
        .download_batch_archive_to(BATCH, &mut sink, Duration::from_secs(5))
        .unwrap_err();
    assert_eq!(error.kind, ErrorKind::Transport);
    assert_eq!(fake.finish().len(), 1);
    assert_eq!(sink, b"PK\x03\x04partial");
}
