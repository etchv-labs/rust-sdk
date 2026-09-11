use etchv::{Client, Options};
use serde_json::{Value, json};
use std::time::Duration;
mod support;
fn decode(s: &str) -> Vec<u8> {
    // Fixture-only base64 decoder; SDK takes bytes directly.
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::new();
    let (mut bits, mut n) = (0u32, 0);
    for c in s.bytes().filter(|c| *c != b'=') {
        bits = (bits << 6) | alphabet.iter().position(|v| *v == c).unwrap() as u32;
        n += 6;
        if n >= 8 {
            n -= 8;
            out.push((bits >> n) as u8)
        }
    }
    out
}
#[test]
fn contract() {
    let formats: Vec<Value> = serde_json::from_str(include_str!("formats.json")).unwrap();
    let server = support::TestServer::new(formats.clone());
    let base = &server.base;
    let data = json!({"asset":"example"});
    for f in &formats {
        let ext = f["extension"].as_str().unwrap();
        let b = decode(f["base64"].as_str().unwrap());
        let opts = Options {
            filename: Some(format!("input.{ext}")),
            ..Default::default()
        };
        let c = Client::with_options(
            "test-key",
            &format!("{base}/formats/{ext}"),
            Duration::from_secs(10),
        )
        .unwrap();
        let result = match f["media"].as_str().unwrap() {
            "documents" => c.embed_document(&b, &data, opts.clone()),
            "videos" => c.embed_video(&b, &data, opts.clone()),
            _ => c.embed_image(&b, &data, opts.clone()),
        }
        .unwrap();
        assert_eq!(result.bytes, b);
        assert_eq!(result.content_type, f["mime"].as_str().unwrap());
        assert_eq!(result.filename, format!("protected.{ext}"));
        let d = match f["media"].as_str().unwrap() {
            "documents" => c.detect_document(&b, opts),
            "videos" => c.detect_video(&b, opts),
            _ => c.detect_image(&b, opts),
        }
        .unwrap();
        assert!(d.watermarked);
        assert_eq!(d.units.len(), 2);
    }
    for scenario in [
        "retry",
        "embed-job",
        "detect-job",
        "failed",
        "redirect",
        "invalid",
        "bad-job",
        "bad-id",
        "bad-detection",
        "bad-units",
        "deadline",
    ] {
        let ext = match scenario {
            "embed-job" => "pdf",
            "detect-job" => "mp4",
            _ => "png",
        };
        let f = formats.iter().find(|f| f["extension"] == ext).unwrap();
        let b = decode(f["base64"].as_str().unwrap());
        let c = Client::with_options(
            "test-key",
            &format!("{base}/{scenario}/{ext}"),
            Duration::from_millis(if scenario == "deadline" { 80 } else { 10000 }),
        )
        .unwrap();
        let opts = Options {
            filename: Some(format!("input.{ext}")),
            idempotency_key: Some("stable-key".into()),
        };
        let r = match scenario {
            "embed-job" => c.embed_document(&b, &data, opts).map(|_| ()),
            "detect-job" => c.detect_video(&b, opts).map(|_| ()),
            "bad-detection" | "bad-units" => c.detect_image(&b, opts).map(|_| ()),
            _ => c.embed_image(&b, &data, opts).map(|_| ()),
        };
        if ["retry", "embed-job", "detect-job"].contains(&scenario) {
            r.unwrap()
        } else {
            let e = r.unwrap_err();
            if scenario == "deadline" {
                assert_eq!(e.status_code, 0);
                assert_eq!(e.idempotency_key.as_deref(), Some("stable-key"));
                assert!(e.request_id.is_some())
            }
        }
    }
}
#[test]
fn validates_input() {
    for base in [
        "http://example.com",
        "https://user:pass@example.com",
        "https://example.com?x=1",
    ] {
        assert!(Client::with_options("key", base, Duration::from_secs(1)).is_err())
    }
    let c = Client::new("key").unwrap();
    assert!(c.get_embed_result("../../steal").is_err());
    assert!(
        c.embed_image(&[], &json!({"a":1}), Options::default())
            .is_err()
    );
}
