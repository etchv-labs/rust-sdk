use etchv::{
    Client, ErrorKind, JobStatus, Media, NewStorageDestination, Options, StorageDestinationUpdate,
    StorageVisibility,
};
use serde_json::{Value, json};
use std::{thread, time::Duration};
use tiny_http::{Header, Response, Server};

type Handler = fn(&str, &str, Option<Value>) -> (u16, Vec<u8>);

/// Serve exactly `count` requests, checking common headers on each.
fn serve(count: usize, handler: Handler) -> (String, thread::JoinHandle<()>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let worker = thread::spawn(move || {
        for _ in 0..count {
            let mut req = server
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .expect("request missing");
            let header = |name: &'static str| {
                req.headers()
                    .iter()
                    .find(|h| h.field.equiv(name))
                    .map(|h| h.value.to_string())
            };
            assert_eq!(header("X-API-Key").as_deref(), Some("test-key"));
            assert_eq!(header("User-Agent").as_deref(), Some("etchv-rust/1.2.0"));
            let method = req.method().as_str().to_owned();
            let url = req.url().to_owned();
            let mut body = String::new();
            req.as_reader().read_to_string(&mut body).unwrap();
            let body = (!body.is_empty()).then(|| serde_json::from_str(&body).unwrap());
            let (status, payload) = handler(&method, &url, body);
            let response = Response::from_data(payload)
                .with_status_code(status)
                .with_header(Header::from_bytes("X-Request-ID", "req_trace").unwrap())
                .with_header(Header::from_bytes("Content-Type", "application/json").unwrap());
            req.respond(response).unwrap();
        }
    });
    (base, worker)
}

fn client(base: &str) -> Client {
    Client::with_options("test-key", base, Duration::from_secs(5)).unwrap()
}

fn j(v: Value) -> Vec<u8> {
    v.to_string().into_bytes()
}

const WH: &str = "wh_0123456789abcdef0123456789abcdef";
fn evt() -> String {
    format!("evt_{}", "e".repeat(64))
}
const DST: &str = "dst_0123456789abcdef0123456789abcdef";
fn std_id() -> String {
    format!("std_{}", "d".repeat(64))
}
fn ast() -> String {
    format!("ast_{}", "a".repeat(64))
}
fn req() -> String {
    format!("req_{}", "b".repeat(64))
}

fn endpoint(extra: Value) -> Value {
    let mut v = json!({"id": WH, "url": "https://example.com/hook", "enabled": true,
        "created_at": "2026-09-12T00:00:00Z"});
    v.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    v
}

#[test]
fn api_key_info() {
    let (base, worker) = serve(1, |method, url, _| {
        assert_eq!((method, url), ("GET", "/auth/api-key"));
        (
            200,
            j(json!({"organization_id": "org_1", "key_id": "key_1",
                "scopes": ["watermarks:embed", "assets:read"]})),
        )
    });
    let info = client(&base).get_api_key_info().unwrap();
    assert_eq!(info.organization_id, "org_1");
    assert_eq!(info.key_id, "key_1");
    assert_eq!(info.scopes, ["watermarks:embed", "assets:read"]);
    worker.join().unwrap();
}

#[test]
fn webhooks() {
    let (base, worker) = serve(6, |method, url, body| {
        let hook = format!("/webhooks/{WH}");
        match (method, url) {
            ("GET", "/webhooks") => (200, j(json!([endpoint(json!({}))]))),
            ("POST", "/webhooks") => {
                assert_eq!(body.unwrap(), json!({"url": "https://example.com/hook"}));
                (
                    201,
                    j(endpoint(json!({"signing_secret": "whsec_secretvalue"}))),
                )
            }
            ("PATCH", u) if u == hook => {
                assert_eq!(body.unwrap(), json!({"enabled": false}));
                (200, j(endpoint(json!({"enabled": false}))))
            }
            ("DELETE", u) if u == hook => (204, Vec::new()),
            ("GET", u) if u == format!("{hook}/deliveries?after={}", evt()) => (
                200,
                j(
                    json!({"data": [{"id": evt(), "request_id": req(), "status": "delivered",
                    "attempts": 1, "created_at": "t", "next_attempt_at": "t",
                    "history": [{"at": "t", "status_code": 200, "error": null}],
                    "payload": {"id": evt(), "type": "watermark.embed.succeeded"}}],
                    "next_cursor": null}),
                ),
            ),
            ("POST", u) if u == format!("{hook}/deliveries/{}/redeliver", evt()) => {
                (202, j(json!({"id": evt(), "status": "queued"})))
            }
            other => panic!("unexpected {other:?}"),
        }
    });
    let c = client(&base);
    assert_eq!(c.list_webhooks().unwrap()[0].id, WH);
    let created = c.create_webhook("https://example.com/hook").unwrap();
    assert_eq!(created.signing_secret.as_deref(), Some("whsec_secretvalue"));
    assert!(!format!("{created:?}").contains("secretvalue"));
    assert!(!c.update_webhook(WH, false).unwrap().enabled);
    c.delete_webhook(WH).unwrap();
    let page = c.list_webhook_deliveries(WH, Some(&evt())).unwrap();
    assert_eq!(page.data[0].history[0].status_code, Some(200));
    assert!(page.next_cursor.is_none());
    c.redeliver_webhook(WH, &evt()).unwrap();
    worker.join().unwrap();

    assert!(c.update_webhook("wh_../../x", true).is_err());
    assert!(c.redeliver_webhook(WH, "evt_bad").is_err());
}

fn destination(extra: Value) -> Value {
    let mut v = json!({"id": DST, "name": "Exports", "provider": "azure", "bucket": "exports",
        "prefix": "etchv", "visibility": "private", "region": null, "role_arn": null,
        "account": "acct", "external_id": "etchv-x", "enabled": true, "verified_at": null,
        "created_at": "t", "last_error": null, "credential_expires_at": "t", "gcs_auth": null,
        "gcs_workload_identity_provider": null, "gcs_service_account": null,
        "aws_principal_arn": null, "gcs_subject": null});
    v.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    v
}

fn delivery(status: &str) -> Value {
    json!({"id": std_id(), "asset_id": ast(), "request_id": req(), "destination_id": DST,
        "provider": "azure", "key": "etchv/out.png", "uri": "https://acct.blob/x", "public_url": null,
        "status": status, "attempts": 0, "created_at": "t", "next_attempt_at": "t",
        "completed_at": null, "error_code": null,
        "history": [{"at": "t", "status": "failed", "error_code": "provider_unavailable"}],
        "expires_at": "t"})
}

#[test]
fn storage() {
    let (base, worker) = serve(10, |method, url, body| {
        let d = format!("/storage/destinations/{DST}");
        let s = format!("/storage/deliveries/{}", std_id());
        match (method, url) {
            ("GET", "/storage/destinations") => (200, j(json!([destination(json!({}))]))),
            ("POST", "/storage/destinations") => {
                assert_eq!(
                    body.unwrap(),
                    json!({"name": "Exports", "provider": "azure", "bucket": "exports",
                        "account": "acct", "credentials": "sv=1&sig=x", "prefix": "out",
                        "visibility": "public"})
                );
                (201, j(destination(json!({"visibility": "public"}))))
            }
            ("PATCH", u) if u == d => {
                assert_eq!(body.unwrap(), json!({"enabled": false}));
                (200, j(destination(json!({"enabled": false}))))
            }
            ("DELETE", u) if u == d => (204, Vec::new()),
            ("POST", u) if u == format!("{d}/verify") => {
                (200, j(destination(json!({"verified_at": "now"}))))
            }
            ("GET", u) if u == format!("{d}/deliveries?after={}", std_id()) => (
                200,
                j(json!({"items": [delivery("stored")], "next_cursor": std_id()})),
            ),
            ("POST", u) if u == format!("{d}/deliveries") => {
                assert_eq!(
                    body.unwrap(),
                    json!({"asset_id": ast(), "key": "reports/out.png"})
                );
                (202, j(delivery("queued")))
            }
            ("GET", u) if u == s => (200, j(delivery("stored"))),
            ("POST", u) if u == format!("{s}/retry") => (202, j(delivery("queued"))),
            ("GET", u) if u == format!("{s}/content") => (200, b"object".to_vec()),
            other => panic!("unexpected {other:?}"),
        }
    });
    let c = client(&base);
    assert_eq!(c.list_storage_destinations().unwrap()[0].id, DST);
    let new = NewStorageDestination::azure("Exports", "acct", "exports", "sv=1&sig=x")
        .prefix("out")
        .visibility(StorageVisibility::Public);
    assert!(!format!("{new:?}").contains("sig=x"));
    assert_eq!(
        c.create_storage_destination(&new)
            .unwrap()
            .visibility
            .as_deref(),
        Some("public")
    );
    assert!(
        !c.update_storage_destination(DST, &StorageDestinationUpdate::new().enabled(false))
            .unwrap()
            .enabled
    );
    c.delete_storage_destination(DST).unwrap();
    assert!(
        c.verify_storage_destination(DST)
            .unwrap()
            .verified_at
            .is_some()
    );
    let page = c.list_storage_deliveries(DST, Some(&std_id())).unwrap();
    assert_eq!(page.items[0].status, "stored");
    assert_eq!(
        page.items[0].history[0].error_code.as_deref(),
        Some("provider_unavailable")
    );
    assert_eq!(
        c.create_storage_delivery(DST, &ast(), Some("reports/out.png"))
            .unwrap()
            .status,
        "queued"
    );
    assert_eq!(c.get_storage_delivery(&std_id()).unwrap().status, "stored");
    assert_eq!(
        c.retry_storage_delivery(&std_id()).unwrap().status,
        "queued"
    );
    assert_eq!(c.download_storage_delivery(&std_id()).unwrap(), b"object");
    worker.join().unwrap();

    assert!(c.get_storage_delivery("std_short").is_err());
    assert!(c.verify_storage_destination("dst_../x").is_err());
}

#[test]
fn jobs_and_gone_results() {
    let (base, worker) = serve(4, |method, url, _| {
        assert_eq!(method, "GET");
        let receipt = |op: &str, prefix: &str| {
            json!({"request_id": req(), "status": "succeeded", "operation": op,
                "status_url": format!("/watermarks/{prefix}/{}", req()),
                "result_url": format!("/watermarks/{prefix}/{}/result", req()),
                "webhook_id": null, "asset_id": null, "source_asset_id": null, "format": "PNG",
                "frame_count": 1, "credits": 1, "attempts": 1, "error_code": null,
                "result_expires_at": "2026-09-13T00:00:00+00:00", "storage_provider": "etchv",
                "storage_delivery_id": null, "storage_destination_id": null, "future_field": 1})
        };
        if url == format!("/watermarks/jobs/{}", req()) {
            (200, j(receipt("embed", "jobs")))
        } else if url == format!("/watermarks/detection-jobs/{}", req()) {
            (200, j(receipt("detect", "detection-jobs")))
        } else if url.ends_with("/result") {
            (
                410,
                j(json!({"status": "expired", "request_id": req(),
                    "detail": "Saved result has expired; this key will not be charged again"})),
            )
        } else {
            panic!("unexpected {url}")
        }
    });
    let c = client(&base);
    let job = c.get_job(&req()).unwrap();
    assert_eq!(job.status, JobStatus::Succeeded);
    assert!(job.status.is_terminal());
    assert_eq!(job.operation, "embed");
    assert_eq!(c.get_detection_job(&req()).unwrap().operation, "detect");
    let e = c.get_embed_result(&req()).unwrap_err();
    assert!(e.is_gone());
    assert_eq!(e.kind, ErrorKind::Api);
    assert_eq!(e.request_id.as_deref(), Some("req_trace"));
    assert_eq!(
        e.message().as_deref(),
        Some("Saved result has expired; this key will not be charged again")
    );
    assert!(
        e.to_string()
            .contains("(HTTP 410): Saved result has expired")
    );
    assert!(c.get_detection_result(&req()).unwrap_err().is_gone());
    worker.join().unwrap();
}

#[test]
fn input_errors_are_local() {
    let c = Client::new("test-key").unwrap();
    let data = json!({"a": 1});
    let check = |e: etchv::Error| {
        assert_eq!(e.kind, ErrorKind::InvalidInput);
        assert_eq!(e.status_code, 0);
    };
    check(
        c.embed_image(b"x", &data, Options::new().idempotency_key("short"))
            .unwrap_err(),
    );
    check(
        c.embed_image(b"x", &data, Options::new().storage_key("k"))
            .unwrap_err(),
    );
    check(
        c.submit_detection(
            Media::Image,
            b"x",
            Options::new().storage_destination_id(DST),
            None,
        )
        .unwrap_err(),
    );
    check(
        c.submit_embed(Media::Video, b"x", &json!({}), Options::new(), None)
            .unwrap_err(),
    );
    check(
        c.submit_embed(Media::Video, b"x", &data, Options::new(), Some("wh_bad"))
            .unwrap_err(),
    );
    check(c.get_job("req_../../x").unwrap_err());
    check(c.delete_assets::<&str>(&[]).unwrap_err());
    assert!(Client::new("key\u{e9}").is_err());
    assert!(Client::new("key\r\nX-Injected: 1").is_err());
}
