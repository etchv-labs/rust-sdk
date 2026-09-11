//! Blocking, server-side Etchv client. Run outside an async runtime (or in spawn_blocking).
use reqwest::{
    blocking::{Client as Http, multipart},
    header::HeaderMap,
};
use serde::Deserialize;
use serde_json::Value;
use std::{
    fmt,
    thread::sleep,
    time::{Duration, Instant},
};

pub const MAX_FILE_SIZE: usize = 20 * 1024 * 1024;
#[derive(Debug)]
pub struct Error {
    pub status_code: u16,
    pub detail: String,
    pub request_id: Option<String>,
    pub idempotency_key: Option<String>,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Etchv request failed (HTTP {})", self.status_code)
    }
}
impl std::error::Error for Error {}
fn error(status: u16, detail: impl ToString) -> Error {
    Error {
        status_code: status,
        detail: detail.to_string(),
        request_id: None,
        idempotency_key: None,
    }
}
type Result<T> = std::result::Result<T, Error>;
#[derive(Default, Clone)]
pub struct Options {
    pub filename: Option<String>,
    pub idempotency_key: Option<String>,
}
#[derive(Debug)]
pub struct EmbedResult {
    pub bytes: Vec<u8>,
    pub watermark_id: String,
    pub request_id: Option<String>,
    pub content_type: String,
    pub filename: String,
}
#[derive(Debug, Deserialize)]
pub struct DetectionUnit {
    pub index: usize,
    pub watermarked: bool,
    pub confidence: f64,
    pub watermark_id: Option<String>,
}
#[derive(Debug, Deserialize)]
pub struct DetectionResult {
    pub watermarked: bool,
    pub confidence: f64,
    pub watermark_id: Option<String>,
    #[serde(default)]
    pub units: Vec<DetectionUnit>,
    #[serde(skip)]
    pub request_id: Option<String>,
}
pub struct Client {
    key: String,
    base: String,
    timeout: Duration,
    http: Http,
}
fn valid_id(s: &str) -> bool {
    s.len() == 64 && s.bytes().all(|c| c.is_ascii_hexdigit())
}
fn valid_job(s: &str) -> bool {
    s.strip_prefix("req_")
        .is_some_and(|id| valid_id(id) && !id.bytes().any(|c| c.is_ascii_uppercase()))
}
fn header(h: &HeaderMap, name: &str) -> Option<String> {
    h.get(name).and_then(|v| v.to_str().ok()).map(str::to_owned)
}
impl Client {
    pub fn new(api_key: impl Into<String>) -> Result<Self> {
        Self::with_options(api_key, "https://api.etchv.com", Duration::from_secs(120))
    }
    pub fn with_options(api_key: impl Into<String>, base: &str, timeout: Duration) -> Result<Self> {
        let key = api_key.into();
        let u = reqwest::Url::parse(base).map_err(|_| error(0, "Invalid base URL"))?;
        let local = matches!(u.host_str(), Some("localhost" | "127.0.0.1" | "[::1]"));
        if key.trim().is_empty()
            || key.contains(['\r', '\n'])
            || timeout.is_zero()
            || u.host_str().is_none()
            || !u.username().is_empty()
            || u.password().is_some()
            || u.query().is_some()
            || u.fragment().is_some()
            || !(u.scheme() == "https" || (u.scheme() == "http" && local))
        {
            return Err(error(
                0,
                "API key, positive timeout and HTTPS base URL required; HTTP allowed for localhost",
            ));
        }
        let http = Http::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| error(0, e))?;
        Ok(Self {
            key,
            base: base.trim_end_matches('/').into(),
            timeout,
            http,
        })
    }
    pub fn embed_image(&self, file: &[u8], data: &Value, options: Options) -> Result<EmbedResult> {
        self.embed("images", file, data, options)
    }
    pub fn embed_document(
        &self,
        file: &[u8],
        data: &Value,
        options: Options,
    ) -> Result<EmbedResult> {
        self.embed("documents", file, data, options)
    }
    pub fn embed_video(&self, file: &[u8], data: &Value, options: Options) -> Result<EmbedResult> {
        self.embed("videos", file, data, options)
    }
    pub fn detect_image(&self, file: &[u8], options: Options) -> Result<DetectionResult> {
        self.detect("images", file, options)
    }
    pub fn detect_document(&self, file: &[u8], options: Options) -> Result<DetectionResult> {
        self.detect("documents", file, options)
    }
    pub fn detect_video(&self, file: &[u8], options: Options) -> Result<DetectionResult> {
        self.detect("videos", file, options)
    }
    pub fn get_embed_result(&self, id: &str) -> Result<EmbedResult> {
        if !valid_job(id) {
            return Err(error(0, "Invalid request ID"));
        }
        let (b, h) = self.request(
            format!("watermarks/jobs/{id}/result"),
            None,
            None,
            Options::default(),
            true,
            false,
        )?;
        embedding(b, h)
    }
    pub fn get_detection_result(&self, id: &str) -> Result<DetectionResult> {
        if !valid_job(id) {
            return Err(error(0, "Invalid request ID"));
        }
        let (b, h) = self.request(
            format!("watermarks/detection-jobs/{id}/result"),
            None,
            None,
            Options::default(),
            true,
            true,
        )?;
        detection(&b, &h)
    }
    fn embed(
        &self,
        media: &str,
        file: &[u8],
        data: &Value,
        options: Options,
    ) -> Result<EmbedResult> {
        if !data.as_object().is_some_and(|v| !v.is_empty()) {
            return Err(error(0, "data must be a non-empty JSON object"));
        }
        let (b, h) = self.post(media, file, Some(data.to_string()), options)?;
        embedding(b, h)
    }
    fn detect(&self, media: &str, file: &[u8], options: Options) -> Result<DetectionResult> {
        let (b, h) = self.post(media, file, None, options)?;
        detection(&b, &h)
    }
    fn post(
        &self,
        media: &str,
        file: &[u8],
        data: Option<String>,
        mut options: Options,
    ) -> Result<(Vec<u8>, HeaderMap)> {
        if file.is_empty() || file.len() > MAX_FILE_SIZE {
            return Err(error(0, "file must contain 1 byte to 20 MB"));
        }
        if options.filename.is_none() {
            options.filename = Some(
                match media {
                    "documents" => "document.pdf",
                    "videos" => "video.mp4",
                    _ => "image.png",
                }
                .into(),
            )
        }
        let detect = data.is_none();
        let durable = !detect || media == "videos";
        if durable && options.idempotency_key.as_deref().is_none_or(str::is_empty) {
            options.idempotency_key = Some(uuid::Uuid::new_v4().to_string())
        }
        self.request(
            format!("watermarks/{media}{}", if detect { "/detect" } else { "" }),
            Some(file),
            data,
            options,
            durable,
            detect && media == "videos",
        )
    }
    fn request(
        &self,
        mut path: String,
        mut file: Option<&[u8]>,
        data: Option<String>,
        options: Options,
        durable: bool,
        detection_job: bool,
    ) -> Result<(Vec<u8>, HeaderMap)> {
        let started = Instant::now();
        let mut request_id = None;
        let pause = |seconds: f64| {
            sleep(
                Duration::from_secs_f64(seconds.clamp(0.01, 5.0))
                    .min(self.timeout.saturating_sub(started.elapsed())),
            )
        };
        while started.elapsed() < self.timeout {
            let url = format!("{}/{}", self.base, path);
            let mut req = if let Some(bytes) = file {
                let part = multipart::Part::bytes(bytes.to_vec())
                    .file_name(options.filename.clone().unwrap_or_else(|| "file".into()));
                let mut form = multipart::Form::new().part("file", part);
                if let Some(ref value) = data {
                    form = form.text("data", value.clone())
                }
                self.http.post(url).multipart(form)
            } else {
                self.http.get(url)
            };
            req = req.header("X-API-Key", &self.key).timeout(
                self.timeout
                    .saturating_sub(started.elapsed())
                    .max(Duration::from_millis(1)),
            );
            if let Some(ref key) = options.idempotency_key {
                req = req.header("Idempotency-Key", key)
            }
            let response = match req.send() {
                Ok(r) => r,
                Err(e) => {
                    if !durable {
                        return Err(error(0, e));
                    }
                    pause(1.0);
                    continue;
                }
            };
            let status = response.status().as_u16();
            let headers = response.headers().clone();
            request_id = header(&headers, "x-request-id").or(request_id);
            // Read with a hard bound even when the server omits Content-Length.
            use std::io::Read;
            let mut bytes = Vec::new();
            let read = response
                .take((MAX_FILE_SIZE + 1) as u64)
                .read_to_end(&mut bytes);
            if let Err(e) = read {
                if !durable {
                    return Err(error(0, e));
                }
                pause(1.0);
                continue;
            }
            if bytes.len() > MAX_FILE_SIZE {
                return Err(error(status, "Response exceeds 20 MB"));
            }
            if status == 200 {
                return Ok((bytes, headers));
            }
            let detail: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            if durable && status == 202 {
                let id = detail["request_id"]
                    .as_str()
                    .filter(|id| valid_job(id))
                    .ok_or_else(|| error(202, "Invalid job response"))?;
                request_id = Some(id.into());
                path = format!(
                    "watermarks/{}/{id}/result",
                    if detection_job {
                        "detection-jobs"
                    } else {
                        "jobs"
                    }
                );
                file = None;
                let seconds = header(&headers, "retry-after")
                    .and_then(|s| s.parse::<f64>().ok())
                    .filter(|s| s.is_finite())
                    .unwrap_or(1.0);
                pause(seconds);
                continue;
            }
            if durable && [429, 502, 503, 504].contains(&status) && detail["status"] != "failed" {
                pause(1.0);
                continue;
            }
            return Err(Error {
                status_code: status,
                detail: String::from_utf8_lossy(&bytes[..bytes.len().min(10000)]).into(),
                request_id,
                idempotency_key: options.idempotency_key,
            });
        }
        Err(Error {
            status_code: 0,
            detail: "Client deadline exceeded; job may still complete".into(),
            request_id,
            idempotency_key: options.idempotency_key,
        })
    }
}
fn embedding(bytes: Vec<u8>, headers: HeaderMap) -> Result<EmbedResult> {
    let mime = header(&headers, "content-type")
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or("")
        .to_owned();
    let id = header(&headers, "x-watermark-id").unwrap_or_default();
    let ext = extension(&bytes, &mime)
        .filter(|_| valid_id(&id))
        .ok_or_else(|| error(200, "Invalid embedding response"))?;
    let disposition = header(&headers, "content-disposition").unwrap_or_default();
    let filename = disposition
        .split("filename=\"")
        .nth(1)
        .and_then(|s| s.split('"').next())
        .filter(|s| {
            !s.is_empty()
                && s.bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"._-".contains(&c))
        })
        .map(str::to_owned)
        .unwrap_or_else(|| format!("watermarked.{ext}"));
    Ok(EmbedResult {
        bytes,
        watermark_id: id,
        request_id: header(&headers, "x-request-id"),
        content_type: mime,
        filename,
    })
}
fn valid_detection(v: &Value) -> bool {
    let Some(w) = v["watermarked"].as_bool() else {
        return false;
    };
    let Some(c) = v["confidence"].as_f64() else {
        return false;
    };
    (0.0..=1.0).contains(&c)
        && v.get("watermark_id").is_some()
        && if w {
            v["watermark_id"].as_str().is_some_and(valid_id)
        } else {
            v["watermark_id"].is_null()
        }
}
fn detection(b: &[u8], h: &HeaderMap) -> Result<DetectionResult> {
    let mut v: Value =
        serde_json::from_slice(b).map_err(|_| error(200, "Invalid detection JSON"))?;
    if !valid_detection(&v) {
        return Err(error(200, "Invalid detection response"));
    }
    if v.get("units").is_none() {
        let mut unit = v.clone();
        unit["index"] = 0.into();
        v["units"] = serde_json::json!([unit])
    }
    if !v["units"].as_array().is_some_and(|a| {
        !a.is_empty()
            && a.iter()
                .enumerate()
                .all(|(i, u)| valid_detection(u) && u["index"].as_u64() == Some(i as u64))
    }) {
        return Err(error(200, "Invalid detection units"));
    }
    let mut d: DetectionResult =
        serde_json::from_value(v).map_err(|_| error(200, "Invalid detection response"))?;
    d.request_id = header(h, "x-request-id");
    Ok(d)
}
fn extension(b: &[u8], mime: &str) -> Option<&'static str> {
    match mime {
        "image/png" if b.starts_with(b"\x89PNG\r\n\x1a\n") => Some("png"),
        "image/jpeg" if b.starts_with(b"\xff\xd8\xff") => Some("jpg"),
        "image/gif" if b.starts_with(b"GIF87a") || b.starts_with(b"GIF89a") => Some("gif"),
        "image/tiff" if b.starts_with(b"II*\0") || b.starts_with(b"MM\0*") => Some("tiff"),
        "image/bmp" if b.starts_with(b"BM") => Some("bmp"),
        "image/x-portable-pixmap" if b.starts_with(b"P6") || b.starts_with(b"P3") => Some("ppm"),
        "image/webp" if b.starts_with(b"RIFF") && b.get(8..12) == Some(b"WEBP") => Some("webp"),
        "image/vnd.adobe.photoshop" if b.starts_with(b"8BPS\0\x01") => Some("psd"),
        "image/vnd.adobe.photoshop" if b.starts_with(b"8BPS\0\x02") => Some("psb"),
        "application/pdf" if b.starts_with(b"%PDF-") => Some("pdf"),
        "video/mp4" if b.get(4..8) == Some(b"ftyp") => Some("mp4"),
        "video/quicktime" if b.get(4..8) == Some(b"ftyp") => Some("mov"),
        _ => None,
    }
}
