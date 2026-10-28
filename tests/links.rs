use etchv::{Client, ErrorKind, LinkPrepare, NewLink, Options};
use serde_json::{Value, json};
use std::{thread, time::Duration};
use tiny_http::{Header, Response, Server};

const LNK: &str = "lnk_0123456789abcdef0123456789abcdef";
const LEV: &str = "lev_0123456789abcdef0123456789abcdef";
const WH: &str = "wh_0123456789abcdef0123456789abcdef";

fn ast(c: char) -> String {
    format!("ast_{}", c.to_string().repeat(64))
}

struct Seen {
    method: String,
    url: String,
    key: Option<String>,
    body: String,
}

impl Seen {
    fn json(&self) -> Value {
        serde_json::from_str(&self.body).unwrap()
    }
}

/// Answer one request per reply, in order, and return what was sent.
fn serve(replies: Vec<(u16, Value)>) -> (Client, thread::JoinHandle<Vec<Seen>>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let worker = thread::spawn(move || {
        let mut seen = Vec::new();
        for (status, reply) in replies {
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
            let (method, url, key) = (
                req.method().as_str().to_owned(),
                req.url().to_owned(),
                header("Idempotency-Key"),
            );
            let mut bytes = Vec::new();
            req.as_reader().read_to_end(&mut bytes).unwrap();
            let body = String::from_utf8_lossy(&bytes).into_owned();
            seen.push(Seen {
                method,
                url,
                key,
                body,
            });
            let response = Response::from_data(reply.to_string().into_bytes())
                .with_status_code(status)
                .with_header(Header::from_bytes("Content-Type", "application/json").unwrap())
                .with_header(Header::from_bytes("Retry-After", "0.01").unwrap());
            req.respond(response).unwrap();
        }
        seen
    });
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    (client, worker)
}

fn link(recipients: Value) -> Value {
    json!({"id": LNK, "url": "https://etchv.com/l/abc", "name": "Board pack", "asset_id": ast('a'),
        "media_type": "document", "status": "active", "prepare": "on_create", "allowed_domains": [],
        "expires_at": "2026-12-31T23:59:59Z", "download_limit": 3, "recipient_count": 1,
        "credits_per_copy": 4, "estimated_credits": 4, "batch_id": null, "webhook_id": WH,
        "created_at": "2026-10-28T00:00:00Z", "revoked_at": null, "recipients": recipients})
}

fn recipient() -> Value {
    json!({"email": "alex@example.com", "status": "opened", "opened_at": "2026-10-28T01:00:00Z",
        "last_opened_at": "2026-10-28T02:00:00Z", "open_count": 2, "download_count": 1,
        "copy": {"status": "ready", "asset_id": ast('c'), "error_code": null}})
}

#[test]
fn create_link_sends_the_body_and_reuses_one_key_across_a_503() {
    let (client, worker) = serve(vec![
        (503, json!({"detail": "temporary"})),
        (201, link(json!([recipient()]))),
    ]);
    let created = client
        .create_link(
            &NewLink::new(ast('a'), "Board pack")
                .recipients(["alex@example.com"])
                .expires_at("2026-12-31T23:59:59Z")
                .prepare(LinkPrepare::OnCreate)
                .download_limit(3)
                .webhook_id(WH),
        )
        .unwrap();
    assert_eq!(created.id, LNK);
    assert_eq!(created.download_limit, Some(3));
    assert_eq!(created.recipients[0].email, "alex@example.com");
    assert_eq!(created.recipients[0].open_count, 2);
    assert_eq!(created.recipients[0].copy.status, "ready");
    assert_eq!(
        created.recipients[0].copy.asset_id.as_deref(),
        Some(ast('c').as_str())
    );
    let seen = worker.join().unwrap();
    assert_eq!(seen.len(), 2);
    let expected = json!({"asset_id": ast('a'), "name": "Board pack",
        "recipients": ["alex@example.com"], "expires_at": "2026-12-31T23:59:59Z",
        "prepare": "on_create", "download_limit": 3, "webhook_id": WH});
    for s in &seen {
        assert_eq!((s.method.as_str(), s.url.as_str()), ("POST", "/links"));
        assert_eq!(s.json(), expected);
    }
    let key = seen[0].key.clone().expect("generated key");
    assert!(key.len() >= 8);
    assert_eq!(seen[1].key.as_deref(), Some(key.as_str()));
}

#[test]
fn create_link_for_domains_with_a_given_key() {
    let (client, worker) = serve(vec![(
        200,
        json!({"id": LNK, "url": "https://etchv.com/l/abc", "name": "Price list",
            "asset_id": ast('a'), "media_type": "image", "status": "active", "prepare": "on_open",
            "allowed_domains": ["example.com"], "expires_at": "2026-12-31T23:59:59Z",
            "download_limit": null, "recipient_count": 0, "credits_per_copy": 1,
            "estimated_credits": null, "batch_id": null, "webhook_id": null,
            "created_at": "2026-10-28T00:00:00Z", "revoked_at": null, "recipients": []}),
    )]);
    let created = client
        .create_link(
            &NewLink::new(ast('a'), "Price list")
                .allowed_domains(["example.com"])
                .idempotency_key("price-list_2026"),
        )
        .unwrap();
    assert_eq!(created.allowed_domains, ["example.com"]);
    assert_eq!(created.estimated_credits, None);
    assert!(created.recipients.is_empty());
    let seen = worker.join().unwrap();
    assert_eq!(seen[0].key.as_deref(), Some("price-list_2026"));
    assert_eq!(
        seen[0].json(),
        json!({"asset_id": ast('a'), "name": "Price list", "allowed_domains": ["example.com"]})
    );
}

#[test]
fn too_few_credits_is_a_402_with_its_code() {
    let (client, worker) = serve(vec![(
        402,
        json!({"detail": {"code": "insufficient_credits",
            "message": "This link needs 8 credits for its copies; 2 are available",
            "estimated_credits": 8, "available_credits": 2}}),
    )]);
    let err = client
        .create_link(
            &NewLink::new(ast('a'), "Board pack")
                .recipients(["a@b.co"])
                .idempotency_key("board-pack-1"),
        )
        .unwrap_err();
    assert_eq!((err.kind, err.status_code), (ErrorKind::Api, 402));
    assert_eq!(err.code().as_deref(), Some("insufficient_credits"));
    assert_eq!(err.idempotency_key.as_deref(), Some("board-pack-1"));
    assert_eq!(worker.join().unwrap().len(), 1);
}

#[test]
fn invalid_link_input_is_refused_before_any_request() {
    // Nothing listens here: a request would fail as a transport error, not invalid input.
    let client =
        Client::with_options("test-key", "http://127.0.0.1:9", Duration::from_secs(1)).unwrap();
    let base = || NewLink::new(ast('a'), "Board pack").recipients(["a@b.co"]);
    let bad = [
        NewLink::new("ast_short", "Board pack").recipients(["a@b.co"]),
        NewLink::new(ast('A'), "Board pack").recipients(["a@b.co"]),
        NewLink::new(ast('a'), "  ").recipients(["a@b.co"]),
        NewLink::new(ast('a'), "😀".repeat(201)).recipients(["a@b.co"]),
        NewLink::new(ast('a'), "Board pack"),
        base().allowed_domains(["example.com"]),
        base().download_limit(0),
        base().download_limit(101),
        base().webhook_id("wh_nope"),
        base().expires_at("  "),
        base().idempotency_key("short"),
        base().idempotency_key("has spaces in it"),
    ];
    for link in &bad {
        let err = client.create_link(link).unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidInput, "{link:?}: {err}");
        if link.idempotency_key.is_none() {
            let err = client.estimate_link(link).unwrap_err();
            assert_eq!(err.kind, ErrorKind::InvalidInput, "{link:?}: {err}");
        }
    }
    let refused = |e: etchv::Error| assert_eq!(e.kind, ErrorKind::InvalidInput, "{e}");
    refused(client.get_link("lnk_nope").unwrap_err());
    refused(client.get_link(&LNK.to_uppercase()).unwrap_err());
    refused(client.revoke_link("../links").unwrap_err());
    refused(client.list_links(Some(0), None).unwrap_err());
    refused(client.list_links(Some(51), None).unwrap_err());
    refused(client.list_links(None, Some(LEV)).unwrap_err());
    refused(client.list_link_events("lnk_nope", None, None).unwrap_err());
    refused(client.list_link_events(LNK, Some(101), None).unwrap_err());
    refused(client.list_link_events(LNK, None, Some(LNK)).unwrap_err());
}

#[test]
fn a_200_character_name_is_accepted() {
    let estimate = json!({"asset_id": ast('a'), "media_type": "document", "prepare": "on_open",
        "credits_per_copy": 4, "estimated_credits": 4, "available_credits": 10,
        "enough_credits": true, "latest_expires_at": "2027-01-31T00:00:00Z"});
    let (client, worker) = serve(vec![(200, estimate)]);
    let name = "😀".repeat(200);
    let got = client
        .estimate_link(&NewLink::new(ast('a'), &name).recipients(["a@b.co"]))
        .unwrap();
    assert!(got.enough_credits);
    let seen = worker.join().unwrap();
    assert_eq!(seen[0].json()["name"], json!(name));
}

#[test]
fn every_link_route() {
    let event = json!({"id": LEV, "type": "downloaded", "recipient": "alex@example.com",
        "created_at": "2026-10-28T03:00:00Z", "verified_by": null, "asset_id": ast('c')});
    let summary = {
        let mut l = link(json!([]));
        l.as_object_mut().unwrap().remove("recipients");
        l
    };
    let (client, worker) = serve(vec![
        (
            200,
            json!({"asset_id": ast('a'), "media_type": "document", "prepare": "on_open",
                "credits_per_copy": 4, "estimated_credits": null, "available_credits": 2,
                "enough_credits": true, "latest_expires_at": "2027-01-31T00:00:00Z"}),
        ),
        (200, json!({"data": [summary], "next_cursor": LNK})),
        (200, json!({"data": [], "next_cursor": null})),
        (200, link(json!([recipient()]))),
        (200, json!({"data": [event], "next_cursor": LEV})),
        (200, json!({"data": [], "next_cursor": null})),
        (200, {
            let mut l = link(json!([recipient()]));
            l["status"] = json!("revoked");
            l["revoked_at"] = json!("2026-10-28T04:00:00Z");
            l
        }),
    ]);
    let estimate = client
        .estimate_link(
            &NewLink::new(ast('a'), "Price list")
                .allowed_domains(["example.com"])
                .idempotency_key("ignored-for-estimates"),
        )
        .unwrap();
    assert_eq!(estimate.estimated_credits, None);
    assert_eq!(estimate.available_credits, 2);

    let page = client.list_links(Some(10), Some(LNK)).unwrap();
    assert_eq!(page.data[0].id, LNK);
    assert!(page.data[0].recipients.is_empty());
    assert_eq!(page.next_cursor.as_deref(), Some(LNK));
    assert!(client.list_links(None, None).unwrap().next_cursor.is_none());

    let got = client.get_link(LNK).unwrap();
    assert_eq!(
        got.recipients[0].opened_at.as_deref(),
        Some("2026-10-28T01:00:00Z")
    );

    let events = client.list_link_events(LNK, Some(100), Some(LEV)).unwrap();
    assert_eq!(events.data[0].kind, "downloaded");
    assert_eq!(events.data[0].asset_id.as_deref(), Some(ast('c').as_str()));
    assert!(
        client
            .list_link_events(LNK, None, None)
            .unwrap()
            .data
            .is_empty()
    );

    let revoked = client.revoke_link(LNK).unwrap();
    assert_eq!(revoked.status, "revoked");

    let seen = worker.join().unwrap();
    let routes: Vec<(&str, &str)> = seen
        .iter()
        .map(|s| (s.method.as_str(), s.url.as_str()))
        .collect();
    let (before_link, events_page) = (
        format!("/links?limit=10&before={LNK}"),
        format!("/links/{LNK}/events?limit=100&before={LEV}"),
    );
    let (one, events, revoke) = (
        format!("/links/{LNK}"),
        format!("/links/{LNK}/events"),
        format!("/links/{LNK}/revoke"),
    );
    assert_eq!(
        routes,
        [
            ("POST", "/links/estimate"),
            ("GET", before_link.as_str()),
            ("GET", "/links"),
            ("GET", one.as_str()),
            ("GET", events_page.as_str()),
            ("GET", events.as_str()),
            ("POST", revoke.as_str()),
        ]
    );
    assert_eq!(
        seen[0].json(),
        json!({"asset_id": ast('a'), "name": "Price list", "allowed_domains": ["example.com"]})
    );
    assert!(seen[0].key.is_none(), "estimates take no idempotency key");
}

#[test]
fn detection_names_the_copy_or_asset_when_present() {
    let id = "f".repeat(64);
    let detection = |extra: Value| {
        let mut v = json!({"watermarked": true, "confidence": 0.98, "watermark_id": id});
        v.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        v
    };
    let copy = json!({"copy": {"asset_id": ast('c'), "link": {"id": LNK, "name": "Board pack"},
        "recipient": "alex@example.com", "opened_at": "2026-10-28T01:00:00Z"}});
    let asset = json!({"asset": {"asset_id": ast('d'), "name": "Price list.pdf",
        "metadata": {"customer": "acme"}, "created_at": "2026-10-01T00:00:00Z"}});
    let (client, worker) = serve(vec![
        (200, detection(copy.clone())),
        (200, detection(asset)),
        (200, detection(json!({}))),
        (200, detection(copy)),
        (200, detection(json!({}))),
    ]);
    let image = || client.detect_image(b"\x89PNG\r\n\x1a\nimage", Options::new());

    let with_copy = image().unwrap();
    let c = with_copy.copy.expect("copy");
    assert_eq!(c.asset_id, Some(ast('c')));
    assert_eq!(
        (c.link.id.as_str(), c.link.name.as_str()),
        (LNK, "Board pack")
    );
    assert_eq!(c.recipient, "alex@example.com");
    assert_eq!(c.opened_at.as_deref(), Some("2026-10-28T01:00:00Z"));
    assert!(with_copy.asset.is_none());

    let with_asset = image().unwrap();
    let a = with_asset.asset.expect("asset");
    assert_eq!(a.asset_id, ast('d'));
    assert_eq!(a.name, "Price list.pdf");
    assert_eq!(a.metadata, json!({"customer": "acme"}));
    assert!(with_asset.copy.is_none());

    let neither = image().unwrap();
    assert!(neither.copy.is_none() && neither.asset.is_none());

    // Async detection results carry them too.
    let job = format!("req_{}", "b".repeat(64));
    let async_copy = client.get_detection_result(&job).unwrap();
    assert_eq!(async_copy.copy.expect("copy").recipient, "alex@example.com");
    let async_neither = client.get_detection_result(&job).unwrap();
    assert!(async_neither.copy.is_none() && async_neither.asset.is_none());

    let seen = worker.join().unwrap();
    assert_eq!(seen[0].url, "/watermarks/images/detect");
    assert_eq!(
        seen[3].url,
        format!("/watermarks/detection-jobs/{job}/result")
    );
}
