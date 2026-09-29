use etchv::{Accelerator, Client, Media, Options};
use serde_json::json;
use std::{collections::HashMap, thread, time::Duration};
use tiny_http::{Header, Request, Response, Server};

const PNG: &[u8] = b"\x89PNG\r\n\x1a\nrest";

fn query(req: &Request) -> HashMap<String, String> {
    reqwest::Url::parse(&format!("http://localhost{}", req.url()))
        .unwrap()
        .query_pairs()
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect()
}

fn route(req: &Request) -> String {
    req.url().split('?').next().unwrap().to_owned()
}

fn header(name: &str, value: &str) -> Header {
    Header::from_bytes(name.as_bytes(), value.as_bytes()).unwrap()
}

fn respond(req: Request, status: u16, body: Vec<u8>, headers: &[(&str, &str)]) {
    let mut response = Response::from_data(body).with_status_code(status);
    for (name, value) in headers {
        response.add_header(header(name, value));
    }
    req.respond(response).unwrap();
}

fn png(req: Request, accelerator: Option<&str>) {
    let id = "a".repeat(64);
    let mut headers = vec![
        ("Content-Type", "image/png"),
        ("X-Watermark-ID", id.as_str()),
    ];
    if let Some(value) = accelerator {
        headers.push(("X-Etchv-Accelerator", value));
    }
    respond(req, 200, PNG.to_vec(), &headers);
}

fn detection(req: Request, body: serde_json::Value, accelerator: Option<&str>) {
    let mut headers = vec![("Content-Type", "application/json")];
    if let Some(value) = accelerator {
        headers.push(("X-Etchv-Accelerator", value));
    }
    respond(req, 200, body.to_string().into_bytes(), &headers);
}

fn serve(
    handler: impl FnMut(Request) + Send + 'static,
    requests: usize,
) -> (String, thread::JoinHandle<()>) {
    let server = Server::http("127.0.0.1:0").unwrap();
    let base = format!("http://{}", server.server_addr());
    let mut handler = handler;
    let worker = thread::spawn(move || {
        for _ in 0..requests {
            handler(
                server
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap()
                    .unwrap(),
            );
        }
    });
    (base, worker)
}

#[test]
fn accelerator_query_is_sent_only_when_set() {
    let (base, worker) = serve(
        |req| {
            let q = query(&req);
            match route(&req).as_str() {
                "/watermarks/images" if q.get("accelerator").map(String::as_str) == Some("gpu") => {
                    png(req, Some("gpu"))
                }
                "/watermarks/images" => {
                    assert!(q.is_empty());
                    png(req, None)
                }
                "/watermarks/documents/detect" => {
                    assert_eq!(q.get("accelerator").unwrap(), "gpu");
                    // Automatic CPU fallback is reported by the header.
                    detection(
                        req,
                        json!({"watermarked":false,"confidence":0.1,"watermark_id":null}),
                        Some("cpu"),
                    )
                }
                other => panic!("unexpected route {other}"),
            }
        },
        3,
    );
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let data = json!({"asset":"test"});
    let gpu = client
        .embed_image(PNG, &data, Options::new().accelerator(Accelerator::Gpu))
        .unwrap();
    assert_eq!(gpu.accelerator, Some(Accelerator::Gpu));
    let default = client.embed_image(PNG, &data, Options::new()).unwrap();
    assert_eq!(default.accelerator, None);
    let detected = client
        .detect_document(b"%PDF-1.7", Options::new().accelerator(Accelerator::Gpu))
        .unwrap();
    assert_eq!(detected.accelerator, Some(Accelerator::Cpu));
    worker.join().unwrap();
}

#[test]
fn durable_jobs_keep_accelerator_and_honor_retry_after_on_429() {
    let receipt = format!("req_{}", "b".repeat(64));
    let mut posts = 0;
    let (base, worker) = serve(
        move |req| {
            let q = query(&req);
            match (req.method().as_str(), route(&req).as_str()) {
                ("POST", "/watermarks/videos") => {
                    posts += 1;
                    assert_eq!(q.get("accelerator").unwrap(), "gpu");
                    if posts == 1 {
                        respond(
                            req,
                            429,
                            json!({"detail":{"code":"rate_limited"}})
                                .to_string()
                                .into_bytes(),
                            &[
                                ("Retry-After", "0.05"),
                                ("Content-Type", "application/json"),
                            ],
                        )
                    } else {
                        respond(
                            req,
                            202,
                            json!({"request_id": receipt, "status":"queued"})
                                .to_string()
                                .into_bytes(),
                            &[("Retry-After", "0.01")],
                        )
                    }
                }
                ("GET", path) if path == format!("/watermarks/jobs/{receipt}/result") => {
                    png(req, Some("gpu"))
                }
                ("POST", "/watermarks/videos/detect") => {
                    assert_eq!(q.get("accelerator").unwrap(), "gpu");
                    respond(
                        req,
                        202,
                        json!({"request_id": receipt, "status":"queued"})
                            .to_string()
                            .into_bytes(),
                        &[("Retry-After", "0.01")],
                    )
                }
                ("GET", path) if path == format!("/watermarks/detection-jobs/{receipt}/result") => {
                    // Job result JSON without the header.
                    detection(
                        req,
                        json!({"watermarked":false,"confidence":0.2,"watermark_id":null,"accelerator":"gpu","accelerator_requested":"gpu"}),
                        None,
                    )
                }
                other => panic!("unexpected request {other:?}"),
            }
        },
        5,
    );
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let started = std::time::Instant::now();
    let embedded = client
        .embed_video(
            b"video",
            &json!({"asset":"test"}),
            Options::new().accelerator(Accelerator::Gpu),
        )
        .unwrap();
    assert!(started.elapsed() >= Duration::from_millis(50));
    assert_eq!(embedded.accelerator, Some(Accelerator::Gpu));
    let detected = client
        .detect_video(b"video", Options::new().accelerator(Accelerator::Gpu))
        .unwrap();
    assert_eq!(detected.accelerator, Some(Accelerator::Gpu));
    worker.join().unwrap();
}

#[test]
fn async_submissions_combine_accelerator_with_other_parameters() {
    let (base, worker) = serve(
        |req| {
            let q = query(&req);
            assert!(route(&req).ends_with("/async"));
            assert_eq!(q.get("accelerator").unwrap(), "gpu");
            assert_eq!(
                q.get("webhook_id").unwrap(),
                &format!("wh_{}", "a".repeat(32))
            );
            if !route(&req).contains("/detect/") {
                assert_eq!(
                    q.get("storage_destination_id").unwrap(),
                    &format!("dst_{}", "c".repeat(32))
                );
                assert_eq!(q.get("storage_key").unwrap(), "out/file.png");
            }
            respond(
                req,
                202,
                json!({"request_id":format!("req_{}", "b".repeat(64)),"status":"queued","accelerator_requested":"gpu","accelerator":"future"})
                    .to_string()
                    .into_bytes(),
                &[],
            )
        },
        2,
    );
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let webhook = format!("wh_{}", "a".repeat(32));
    let job = client
        .submit_embed(
            Media::Image,
            PNG,
            &json!({"asset":"test"}),
            Options::new()
                .accelerator(Accelerator::Gpu)
                .storage_destination_id(format!("dst_{}", "c".repeat(32)))
                .storage_key("out/file.png"),
            Some(&webhook),
        )
        .unwrap();
    assert_eq!(job.accelerator_requested, Some(Accelerator::Gpu));
    // Unknown values never fail the response.
    assert_eq!(job.accelerator, None);
    let job = client
        .submit_detection(
            Media::Image,
            PNG,
            Options::new().accelerator(Accelerator::Gpu),
            Some(&webhook),
        )
        .unwrap();
    assert_eq!(job.accelerator_requested, Some(Accelerator::Gpu));
    worker.join().unwrap();
}

#[test]
fn rate_limit_errors_expose_retry_after_and_code() {
    let (base, worker) = serve(
        |req| {
            let body = if route(&req) == "/auth/api-key" {
                json!({"detail": {"message": "Too many concurrent jobs", "code": "concurrency_limited", "limit": 2}})
            } else {
                json!({"detail": {"message": "Rate limit exceeded", "code": "rate_limited", "limit": 60}})
            };
            respond(
                req,
                429,
                body.to_string().into_bytes(),
                &[("Content-Type", "application/json"), ("Retry-After", "7")],
            )
        },
        2,
    );
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let err = client.detect_image(PNG, Options::new()).unwrap_err();
    assert_eq!(err.status_code, 429);
    assert_eq!(err.retry_after, Some(Duration::from_secs(7)));
    assert_eq!(err.code().as_deref(), Some("rate_limited"));
    assert_eq!(err.limit(), Some(60));
    assert_eq!(err.message().as_deref(), Some("Rate limit exceeded"));
    assert!(err.to_string().contains("Rate limit exceeded"));
    let err = client.get_api_key_info().unwrap_err();
    assert_eq!(err.retry_after, Some(Duration::from_secs(7)));
    assert_eq!(err.code().as_deref(), Some("concurrency_limited"));
    assert_eq!(err.limit(), Some(2));
    worker.join().unwrap();

    let (base, worker) = serve(
        |req| {
            let body = json!({"detail": {"message": "Slow down", "code": "rate_limited"}});
            respond(
                req,
                429,
                body.to_string().into_bytes(),
                &[("Content-Type", "application/json")],
            )
        },
        1,
    );
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let err = client.detect_image(PNG, Options::new()).unwrap_err();
    assert_eq!(err.status_code, 429);
    assert_eq!(err.retry_after, None);
    assert_eq!(err.limit(), None);
    assert_eq!(err.message().as_deref(), Some("Slow down"));
    worker.join().unwrap();

    let (base, worker) = serve(
        |req| {
            let body = json!({"detail": "Forbidden"}).to_string().into_bytes();
            respond(req, 403, body, &[("Retry-After", "7")])
        },
        1,
    );
    let client = Client::with_options("test-key", &base, Duration::from_secs(5)).unwrap();
    let err = client.detect_image(PNG, Options::new()).unwrap_err();
    assert_eq!(err.retry_after, None);
    assert_eq!(err.code(), None);
    assert_eq!(err.limit(), None);
    assert_eq!(err.message().as_deref(), Some("Forbidden"));
    worker.join().unwrap();
}
