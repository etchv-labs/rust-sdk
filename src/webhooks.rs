use crate::{Client, Error, Result, check_id, hex_id, with_query};
use aws_lc_rs::hmac;
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fmt,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

/// Maximum clock difference accepted by [`verify_webhook_signature`] (five minutes).
pub const WEBHOOK_TOLERANCE: Duration = Duration::from_secs(300);

/// A webhook endpoint configured for the organization.
#[derive(Clone, Deserialize)]
#[non_exhaustive]
pub struct WebhookEndpoint {
    /// Endpoint ID (`wh_…`); pass it when submitting async jobs.
    pub id: String,
    /// Public HTTPS URL receiving events.
    pub url: String,
    /// Whether deliveries are sent.
    pub enabled: bool,
    /// Creation time (ISO 8601).
    pub created_at: String,
    /// One-time signing secret (`whsec_…`), present only in the
    /// [`Client::create_webhook`] response. Store it securely.
    #[serde(default)]
    pub signing_secret: Option<String>,
}

impl fmt::Debug for WebhookEndpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WebhookEndpoint")
            .field("id", &self.id)
            .field("url", &self.url)
            .field("enabled", &self.enabled)
            .field("created_at", &self.created_at)
            .field(
                "signing_secret",
                &self.signing_secret.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}

/// One delivery attempt of a webhook event.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct WebhookAttempt {
    /// Attempt time (ISO 8601).
    pub at: String,
    /// HTTP status returned by your endpoint, if any.
    #[serde(default)]
    pub status_code: Option<u16>,
    /// Transport error code, if any.
    #[serde(default)]
    pub error: Option<String>,
}

/// A webhook event delivery record.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct WebhookDelivery {
    /// Event ID (`evt_…`).
    pub id: String,
    /// Job that produced the event.
    #[serde(default)]
    pub request_id: Option<String>,
    /// `queued`, `delivering`, `retrying`, `delivered`, `exhausted` or `cancelled`.
    pub status: String,
    /// Attempts made for the current delivery.
    #[serde(default)]
    pub attempts: u32,
    /// Creation time (ISO 8601).
    pub created_at: String,
    /// Next scheduled attempt (ISO 8601).
    #[serde(default)]
    pub next_attempt_at: Option<String>,
    /// Up to the last 30 attempts.
    #[serde(default)]
    pub history: Vec<WebhookAttempt>,
    /// The signed event body.
    pub payload: Value,
}

/// One page of [`Client::list_webhook_deliveries`] results.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct WebhookDeliveryPage {
    /// Deliveries on this page, newest first (at most 50).
    pub data: Vec<WebhookDelivery>,
    /// Pass as `after` to fetch the next page.
    pub next_cursor: Option<String>,
}

/// A webhook event body. Parse it only after [`verify_webhook_signature`] succeeds.
///
/// ```
/// let body = br#"{"id":"evt_1","type":"watermark.embed.succeeded","api_version":"2026-09-12","created_at":"2026-09-12T14:30:00+00:00","data":{"status":"succeeded"}}"#;
/// let event: etchv::WebhookEvent = serde_json::from_slice(body)?;
/// assert_eq!(event.event_type, "watermark.embed.succeeded");
/// # Ok::<(), serde_json::Error>(())
/// ```
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct WebhookEvent {
    /// Event ID (`evt_…`); matches the `X-Etchv-Event-ID` header. Deduplicate on it.
    pub id: String,
    /// For example `watermark.embed.succeeded` or `storage.delivery.failed`.
    #[serde(rename = "type")]
    pub event_type: String,
    /// Payload schema version.
    #[serde(default)]
    pub api_version: String,
    /// Event time (ISO 8601).
    #[serde(default)]
    pub created_at: String,
    /// Job receipt or storage delivery snapshot.
    pub data: Value,
}

/// Verify an Etchv webhook delivery using the current system time.
///
/// Pass the endpoint's full signing secret (including `whsec_`), the
/// `X-Etchv-Timestamp` and `X-Etchv-Signature` header values, and the **raw**
/// request body bytes. Returns `true` only when the HMAC-SHA256 signature
/// matches (compared in constant time) and the timestamp is within
/// [`WEBHOOK_TOLERANCE`].
///
/// ```no_run
/// # fn handle(headers: &std::collections::HashMap<String, String>, raw_body: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
/// let secret = std::env::var("ETCHV_WEBHOOK_SECRET")?;
/// let ok = etchv::verify_webhook_signature(
///     &secret,
///     &headers["x-etchv-timestamp"],
///     &headers["x-etchv-signature"],
///     raw_body,
/// );
/// if !ok {
///     return Err("invalid signature".into());
/// }
/// let event: etchv::WebhookEvent = serde_json::from_slice(raw_body)?;
/// if event.id != headers["x-etchv-event-id"] {
///     return Err("event ID mismatch".into());
/// }
/// # Ok(()) }
/// ```
pub fn verify_webhook_signature(
    signing_secret: &str,
    timestamp: &str,
    signature: &str,
    body: &[u8],
) -> bool {
    verify_webhook_signature_at(
        signing_secret,
        timestamp,
        signature,
        body,
        SystemTime::now(),
    )
}

/// Like [`verify_webhook_signature`], checking the timestamp against `now`.
pub fn verify_webhook_signature_at(
    signing_secret: &str,
    timestamp: &str,
    signature: &str,
    body: &[u8],
    now: SystemTime,
) -> bool {
    let Ok(sent) = timestamp.trim().parse::<u64>() else {
        return false;
    };
    let Ok(now) = now.duration_since(UNIX_EPOCH) else {
        return false;
    };
    if now.as_secs().abs_diff(sent) > WEBHOOK_TOLERANCE.as_secs() {
        return false;
    }
    let Some(tag) = signature
        .trim()
        .strip_prefix("v1=")
        .and_then(decode_hex)
        .filter(|t| t.len() == 32)
    else {
        return false;
    };
    if signing_secret.is_empty() {
        return false;
    }
    let key = hmac::Key::new(hmac::HMAC_SHA256, signing_secret.as_bytes());
    let mut signed = Vec::with_capacity(body.len() + 21);
    signed.extend_from_slice(timestamp.trim().as_bytes());
    signed.push(b'.');
    signed.extend_from_slice(body);
    hmac::verify(&key, &signed, &tag).is_ok()
}

fn decode_hex(s: &str) -> Option<Vec<u8>> {
    if !s.len().is_multiple_of(2) {
        return None;
    }
    let digit = |c: u8| (c as char).to_digit(16).map(|d| d as u8);
    s.as_bytes()
        .chunks(2)
        .map(|p| Some(digit(p[0])? << 4 | digit(p[1])?))
        .collect()
}

fn endpoint(id: &str) -> Result<String> {
    check_id(id, "wh_", 32, "webhook")?;
    Ok(format!("webhooks/{id}"))
}

impl Client {
    /// List webhook endpoints (`GET /webhooks`). Requires `webhooks:read`.
    pub fn list_webhooks(&self) -> Result<Vec<WebhookEndpoint>> {
        self.call_json(Method::GET, "webhooks", None)
    }

    /// Create a webhook endpoint (`POST /webhooks`). Requires `webhooks:write`
    /// and owner/admin membership.
    ///
    /// The returned [`WebhookEndpoint::signing_secret`] is shown only once.
    ///
    /// ```no_run
    /// # let client = etchv::Client::new("etchv_...")?;
    /// let hook = client.create_webhook("https://example.com/webhooks/etchv")?;
    /// let secret = hook.signing_secret.expect("returned on creation");
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn create_webhook(&self, url: &str) -> Result<WebhookEndpoint> {
        self.call_json(Method::POST, "webhooks", Some(json!({ "url": url })))
    }

    /// Enable or disable a webhook endpoint (`PATCH /webhooks/{id}`).
    pub fn update_webhook(&self, id: &str, enabled: bool) -> Result<WebhookEndpoint> {
        self.call_json(
            Method::PATCH,
            &endpoint(id)?,
            Some(json!({ "enabled": enabled })),
        )
    }

    /// Permanently delete a webhook endpoint (`DELETE /webhooks/{id}`).
    pub fn delete_webhook(&self, id: &str) -> Result<()> {
        self.call(Method::DELETE, &endpoint(id)?, None)?;
        Ok(())
    }

    /// List an endpoint's deliveries (`GET /webhooks/{id}/deliveries`), 50 per
    /// page, newest first. Pass the previous page's `next_cursor` as `after`.
    pub fn list_webhook_deliveries(
        &self,
        id: &str,
        after: Option<&str>,
    ) -> Result<WebhookDeliveryPage> {
        let mut pairs = Vec::new();
        if let Some(after) = after {
            check_id(after, "evt_", 64, "event")?;
            pairs.push(("after", after));
        }
        let path = format!("{}/deliveries", endpoint(id)?);
        self.call_json(Method::GET, &with_query(&path, &pairs), None)
    }

    /// Queue a delivered, exhausted or cancelled event for redelivery
    /// (`POST /webhooks/{id}/deliveries/{event_id}/redeliver`).
    pub fn redeliver_webhook(&self, id: &str, event_id: &str) -> Result<()> {
        if !hex_id(event_id, "evt_", 64) {
            return Err(Error::input("Invalid event ID"));
        }
        let path = format!("{}/deliveries/{event_id}/redeliver", endpoint(id)?);
        self.call(Method::POST, &path, None)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sign(secret: &str, ts: &str, body: &[u8]) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes());
        let mut msg = ts.as_bytes().to_vec();
        msg.push(b'.');
        msg.extend_from_slice(body);
        let tag = hmac::sign(&key, &msg);
        let hex: String = tag.as_ref().iter().map(|b| format!("{b:02x}")).collect();
        format!("v1={hex}")
    }

    #[test]
    fn signature_round_trip() {
        let now = UNIX_EPOCH + Duration::from_secs(1_800_000_000);
        let body = br#"{"id":"evt_1"}"#;
        let sig = sign("whsec_test", "1800000000", body);
        assert!(verify_webhook_signature_at(
            "whsec_test",
            "1800000000",
            &sig,
            body,
            now
        ));
        assert!(!verify_webhook_signature_at(
            "whsec_other",
            "1800000000",
            &sig,
            body,
            now
        ));
        assert!(!verify_webhook_signature_at(
            "whsec_test",
            "1800000000",
            &sig,
            b"{}",
            now
        ));
        assert!(!verify_webhook_signature_at(
            "whsec_test",
            "1800000000",
            &sig,
            body,
            now + Duration::from_secs(301)
        ));
        assert!(!verify_webhook_signature_at(
            "whsec_test",
            "1800000000",
            "v1=zz",
            body,
            now
        ));
        assert!(!verify_webhook_signature_at(
            "whsec_test",
            "soon",
            &sig,
            body,
            now
        ));
    }

    #[test]
    fn debug_redacts_signing_secret() {
        let hook: WebhookEndpoint = serde_json::from_value(json!({
            "id": "wh_x", "url": "https://x", "enabled": true,
            "created_at": "t", "signing_secret": "whsec_supersecret"
        }))
        .unwrap();
        assert!(!format!("{hook:?}").contains("supersecret"));
    }
}
