use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};
use tiny_http::{Header, Response, Server};

pub struct TestServer {
    pub base: String,
    server: Arc<Server>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}
impl TestServer {
    pub fn new(formats: Vec<Value>) -> Self {
        let server = Arc::new(Server::http("127.0.0.1:0").unwrap());
        let base = format!("http://{}", server.server_addr());
        let stop = Arc::new(AtomicBool::new(false));
        let worker = server.clone();
        let done = stop.clone();
        let thread = thread::spawn(move || {
            let mut counts: HashMap<String, usize> = HashMap::new();
            let mut keys = HashMap::new();
            let job = format!("req_{}", "b".repeat(64));
            let id = "a".repeat(64);
            while !done.load(Ordering::Relaxed) {
                let Some(mut req) = worker
                    .recv_timeout(std::time::Duration::from_millis(50))
                    .unwrap()
                else {
                    continue;
                };
                let headers: HashMap<String, String> = req
                    .headers()
                    .iter()
                    .map(|h| (h.field.to_string().to_lowercase(), h.value.to_string()))
                    .collect();
                assert_eq!(
                    headers.get("x-api-key").map(String::as_str),
                    Some("test-key")
                );
                let url = req.url().to_owned();
                let parts: Vec<_> = url.trim_matches('/').split('/').collect();
                assert!(parts.len() >= 3);
                let (scenario, ext) = (parts[0], parts[1]);
                let route = parts[2..].join("/");
                let method = req.method().as_str().to_owned();
                let f = formats.iter().find(|f| f["extension"] == ext).unwrap();
                let expected = super::decode(f["base64"].as_str().unwrap());
                let detection = route.ends_with("/detect") || route.contains("detection-jobs/");
                let count_key = format!("{scenario}/{ext}/{method}");
                *counts.entry(count_key.clone()).or_default() += 1;
                if method == "POST" {
                    let mut body = Vec::new();
                    req.as_reader().read_to_end(&mut body).unwrap();
                    assert!(body.windows(expected.len()).any(|w| w == expected));
                    let text = String::from_utf8_lossy(&body);
                    assert!(text.contains(&format!("filename=\"input.{ext}\"")));
                    assert_eq!(
                        route,
                        format!(
                            "watermarks/{}{}",
                            f["media"].as_str().unwrap(),
                            if detection { "/detect" } else { "" }
                        )
                    );
                    if !detection {
                        assert!(text.contains("{\"asset\":\"example\"}"))
                    }
                    if !detection || f["media"] == "videos" {
                        let key = headers.get("idempotency-key").unwrap();
                        assert!(!key.is_empty());
                        let previous = keys
                            .entry(format!("{scenario}/{ext}/{route}"))
                            .or_insert_with(|| key.clone());
                        assert_eq!(previous, key)
                    }
                } else {
                    assert_eq!(
                        route,
                        format!(
                            "watermarks/{}/{job}/result",
                            if detection { "detection-jobs" } else { "jobs" }
                        )
                    )
                }
                let (mut status, mut payload, mut mime) =
                    (200, expected, f["mime"].as_str().unwrap().to_owned());
                let mut extra = vec![("X-Request-ID", job.clone())];
                let pending = ["embed-job", "detect-job", "deadline"].contains(&scenario)
                    && (method == "POST" || scenario == "deadline");
                let json = match scenario {
                    "retry" if counts[&count_key] == 1 => {
                        status = 503;
                        Some(json!({"detail":"temporary"}))
                    }
                    "failed" => {
                        status = 503;
                        Some(json!({"status":"failed"}))
                    }
                    "redirect" => {
                        status = 307;
                        extra.push(("Location", "/forbidden".into()));
                        Some(json!({}))
                    }
                    "invalid" => {
                        status = 422;
                        Some(json!({"detail":"unsupported profile"}))
                    }
                    "bad-job" => {
                        status = 202;
                        Some(json!({"request_id":"../../forbidden"}))
                    }
                    _ if pending => {
                        status = 202;
                        extra.push(("Retry-After", "0.01".into()));
                        extra.push(("Location", "https://untrusted.example/steal".into()));
                        Some(
                            json!({"request_id":job,"result_url":"https://untrusted.example/steal"}),
                        )
                    }
                    _ if detection => {
                        let mut v = json!({"watermarked":true,"confidence":0.99,"watermark_id":id,"units":[{"index":0,"watermarked":true,"confidence":0.99,"watermark_id":id},{"index":1,"watermarked":true,"confidence":0.99,"watermark_id":id}]});
                        if scenario == "bad-detection" {
                            v["confidence"] = json!(1.5)
                        }
                        if scenario == "bad-units" {
                            v["units"][1]["index"] = json!(4)
                        }
                        Some(v)
                    }
                    _ => None,
                };
                if let Some(value) = json {
                    payload = value.to_string().into_bytes();
                    mime = "application/json".into()
                } else {
                    extra.push((
                        "X-Watermark-ID",
                        if scenario == "bad-id" {
                            "invalid".into()
                        } else {
                            id.clone()
                        },
                    ));
                    extra.push((
                        "Content-Disposition",
                        format!("attachment; filename=\"protected.{ext}\""),
                    ))
                }
                let mut response = Response::from_data(payload)
                    .with_status_code(status)
                    .with_header(Header::from_bytes("Content-Type", mime).unwrap());
                for (k, v) in extra {
                    response = response.with_header(Header::from_bytes(k, v).unwrap())
                }
                let _ = req.respond(response);
            }
            for f in &formats {
                assert_eq!(
                    counts.get(&format!(
                        "formats/{}/POST",
                        f["extension"].as_str().unwrap()
                    )),
                    Some(&2)
                )
            }
            assert_eq!(counts.get("retry/png/POST"), Some(&2));
            assert_eq!(counts.get("failed/png/POST"), Some(&1));
            assert!(counts.get("embed-job/pdf/GET").is_some_and(|n| *n >= 1));
            assert!(counts.get("detect-job/mp4/GET").is_some_and(|n| *n >= 1));
        });
        Self {
            base,
            server,
            stop,
            thread: Some(thread),
        }
    }
}
impl Drop for TestServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.server.unblock();
        if let Some(t) = self.thread.take() {
            let result = t.join();
            if !thread::panicking() {
                result.unwrap()
            }
        }
    }
}
