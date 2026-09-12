use etchv::{Client, Options};
use serde_json::json;
use std::{thread, time::Duration};
use tiny_http::{Response, Server};

#[test]
fn async_receipts_never_poll() {
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let worker = thread::spawn(move || {
        for _ in 0..6 {
            let req = server
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .unwrap();
            if !req.url().contains("/detect/") {
                let u = reqwest::Url::parse(&format!("http://localhost{}", req.url())).unwrap();
                let q: std::collections::HashMap<_, _> = u.query_pairs().collect();
                assert_eq!(
                    q.get("storage_destination_id").unwrap(),
                    &format!("dst_{}", "c".repeat(32))
                );
                assert_eq!(q.get("storage_key").unwrap(), "a b/#file.pdf");
            }
            assert_eq!(req.method().as_str(), "POST");
            assert!(req.url().contains("/async?webhook_id=wh_"));
            assert!(
                req.headers()
                    .iter()
                    .any(|h| h.field.equiv("Idempotency-Key")
                        && h.value.as_str() == "stable_test_key")
            );
            req.respond(
                Response::from_string(
                    json!({"status":"queued", "request_id":format!("req_{}", "b".repeat(64))})
                        .to_string(),
                )
                .with_status_code(202),
            )
            .unwrap();
        }
    });
    let client = Client::with_options("test-key", &base, Duration::from_secs(2)).unwrap();
    let webhook = format!("wh_{}", "a".repeat(32));
    for media in ["images", "documents", "videos"] {
        let options = || Options {
            filename: None,
            idempotency_key: Some("stable_test_key".into()),
            ..Default::default()
        };
        assert_eq!(
            client
                .submit_embed(
                    media,
                    b"file",
                    &json!({"asset":"test"}),
                    Options {
                        storage_destination_id: Some(format!("dst_{}", "c".repeat(32))),
                        storage_key: Some("a b/#file.pdf".into()),
                        ..options()
                    },
                    Some(&webhook)
                )
                .unwrap()["status"],
            "queued"
        );
        assert_eq!(
            client
                .submit_detection(media, b"file", options(), Some(&webhook))
                .unwrap()["status"],
            "queued"
        );
    }
    worker.join().unwrap();
}
