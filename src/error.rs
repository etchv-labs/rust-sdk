use serde_json::Value;
use std::fmt;
use std::time::Duration;

/// Broad category of an [`Error`].
///
/// New categories may be added in minor releases, so match with a wildcard arm.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The SDK rejected an argument before sending a request
    /// (for example an invalid ID, empty file or malformed option).
    InvalidInput,
    /// The request could not be sent or the response could not be read.
    Transport,
    /// The client deadline elapsed. Server-side work may still complete; use
    /// [`Error::request_id`] or [`Error::idempotency_key`] to recover it.
    Timeout,
    /// The API returned a non-success HTTP status. Inspect [`Error::status_code`].
    Api,
    /// The API returned a success status with a body the SDK could not validate.
    InvalidResponse,
}

/// Error returned by every fallible SDK operation.
///
/// `status_code` is the HTTP status, or `0` when no HTTP response was
/// involved (invalid input, transport failure or client deadline).
/// Neither `Debug` nor `Display` ever contain the API key.
#[derive(Debug)]
#[non_exhaustive]
pub struct Error {
    /// Broad category of the failure.
    pub kind: ErrorKind,
    /// HTTP status code, or `0` for client-side failures.
    pub status_code: u16,
    /// Raw response body (truncated to 10 000 bytes) for API errors, or a
    /// description of the client-side failure.
    pub detail: String,
    /// Etchv request ID (`X-Request-ID` or job ID) when known.
    pub request_id: Option<String>,
    /// Idempotency key used for the request, if any. Retry with the same key
    /// to recover the same job without another charge.
    pub idempotency_key: Option<String>,
    /// Delay requested by the `Retry-After` header of an HTTP 429 response.
    pub retry_after: Option<Duration>,
    source: Option<Box<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Error {
    pub(crate) fn new(kind: ErrorKind, status_code: u16, detail: impl Into<String>) -> Self {
        Self {
            kind,
            status_code,
            detail: detail.into(),
            request_id: None,
            idempotency_key: None,
            retry_after: None,
            source: None,
        }
    }
    pub(crate) fn input(detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidInput, 0, detail)
    }
    pub(crate) fn response(status: u16, detail: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidResponse, status, detail)
    }
    pub(crate) fn transport(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        let mut err = Self::new(ErrorKind::Transport, 0, e.to_string());
        err.source = Some(Box::new(e));
        err
    }
    pub(crate) fn decode(status: u16, e: serde_json::Error) -> Self {
        let mut err = Self::response(status, format!("Invalid response JSON: {e}"));
        err.source = Some(Box::new(e));
        err
    }

    /// The human-readable `detail` message from an API error body, when present.
    ///
    /// Falls back to the raw [`Error::detail`] text for client-side errors.
    pub fn message(&self) -> Option<String> {
        if self.kind != ErrorKind::Api {
            return Some(self.detail.clone()).filter(|s| !s.is_empty());
        }
        match serde_json::from_str::<Value>(&self.detail) {
            Ok(Value::Object(body)) => match body.get("detail") {
                Some(Value::String(s)) => Some(s.clone()),
                Some(Value::Object(detail)) => detail
                    .get("message")
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                Some(Value::Array(items)) => {
                    let parts: Vec<&str> = items
                        .iter()
                        .filter_map(|i| i.get("msg").and_then(Value::as_str))
                        .collect();
                    (!parts.is_empty()).then(|| parts.join("; "))
                }
                _ => None,
            },
            _ => None,
        }
    }

    /// Machine-readable `detail.code` from an API error body, when present
    /// (for example `rate_limited` or `concurrency_limited` on HTTP 429).
    pub fn code(&self) -> Option<String> {
        if self.kind != ErrorKind::Api {
            return None;
        }
        serde_json::from_str::<Value>(&self.detail)
            .ok()?
            .get("detail")?
            .get("code")?
            .as_str()
            .map(str::to_owned)
    }

    /// The limit that was exceeded (`detail.limit`, for example requests per
    /// window or concurrent jobs), when the API reports one.
    pub fn limit(&self) -> Option<u64> {
        if self.kind != ErrorKind::Api {
            return None;
        }
        serde_json::from_str::<Value>(&self.detail)
            .ok()?
            .get("detail")?
            .get("limit")?
            .as_u64()
    }

    /// `true` for HTTP 410: a saved result expired or its asset was deleted.
    /// Retrying the same idempotency key will not charge again.
    pub fn is_gone(&self) -> bool {
        self.kind == ErrorKind::Api && self.status_code == 410
    }

    /// `true` when the client deadline elapsed before the operation finished.
    pub fn is_timeout(&self) -> bool {
        self.kind == ErrorKind::Timeout
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            ErrorKind::Api => write!(f, "Etchv request failed (HTTP {})", self.status_code)?,
            ErrorKind::InvalidResponse => write!(
                f,
                "Etchv returned an invalid response (HTTP {})",
                self.status_code
            )?,
            ErrorKind::InvalidInput => f.write_str("Invalid Etchv request")?,
            ErrorKind::Transport => f.write_str("Etchv transport error")?,
            ErrorKind::Timeout => f.write_str("Etchv client deadline exceeded")?,
        }
        if let Some(message) = self.message() {
            let message: String = message.chars().take(500).collect();
            write!(f, ": {message}")?;
        }
        if let Some(ref id) = self.request_id {
            write!(f, " (request {id})")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_deref()
            .map(|e| e as &(dyn std::error::Error + 'static))
    }
}

/// Result alias used throughout the SDK.
pub type Result<T, E = Error> = std::result::Result<T, E>;
