use etchv::{AssetListOptions, Client};
use serde_json::{Value, json};
use std::{thread, time::Duration};
use tiny_http::{Response, Server};
#[test]
fn asset_protocol() {
    let record: Value = serde_json::from_str(include_str!("assets.json")).unwrap();
    let id = record["id"].as_str().unwrap().to_owned();
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let worker = thread::spawn(move || {
        for _ in 0..7 {
            let mut req = server
                .recv_timeout(Duration::from_secs(5))
                .unwrap()
                .expect("request missing");
            assert!(
                req.headers()
                    .iter()
                    .any(|h| h.field.equiv("X-API-Key") && h.value.as_str() == "test-key")
            );
            let url = req.url().to_owned();
            let method = req.method().as_str().to_owned();
            let mut body = String::new();
            req.as_reader().read_to_string(&mut body).unwrap();
            let (status, payload) = if url.contains("cursor=") {
                (409, json!({"detail":"changed"}).to_string())
            } else if method == "PATCH" {
                let b: Value = serde_json::from_str(&body).unwrap();
                assert_eq!(b["version"], 1);
                assert_eq!(b["name"], "renamed");
                let mut a = record.clone();
                a["version"] = json!(2);
                (200, a.to_string())
            } else if method == "DELETE" {
                (204, String::new())
            } else if method == "POST" {
                let b: Value = serde_json::from_str(&body).unwrap();
                assert_eq!(b["asset_ids"][0], record["id"]);
                (204, String::new())
            } else if url.ends_with("/content") {
                (200, "file".into())
            } else if url.starts_with("/assets?") {
                assert!(url.contains("kind=watermarked"));
                (
                    200,
                    json!({"items":[record],"next_cursor":"next-page"}).to_string(),
                )
            } else {
                (200, record.to_string())
            };
            req.respond(Response::from_string(payload).with_status_code(status))
                .unwrap();
        }
    });
    let c = Client::with_options("test-key", &base, Duration::from_secs(2)).unwrap();
    assert_eq!(
        c.list_assets(AssetListOptions {
            kind: Some("watermarked".into()),
            ..Default::default()
        })
        .unwrap()
        .next_cursor
        .as_deref(),
        Some("next-page")
    );
    let asset = c.get_asset(&id).unwrap();
    assert_eq!(asset.metadata.unwrap()["campaign"], "launch");
    assert!(asset.file_expires_at.is_none());
    assert_eq!(asset.storage_provider.as_deref(), Some("s3"));
    assert_eq!(
        c.update_asset(&id, 1, &json!({"name":"renamed"}))
            .unwrap()
            .version,
        2
    );
    assert_eq!(c.download_asset(&id).unwrap(), b"file");
    c.delete_asset(&id).unwrap();
    c.delete_assets(&[id]).unwrap();
    assert_eq!(
        c.list_assets(AssetListOptions {
            cursor: Some("next-page".into()),
            ..Default::default()
        })
        .unwrap_err()
        .status_code,
        409
    );
    assert!(c.get_asset("../other").is_err());
    worker.join().unwrap();
}
