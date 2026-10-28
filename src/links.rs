//! Traceable links: share a source asset so each recipient opens their own
//! watermarked copy. See <https://etchv.com/docs/api/links>.
use crate::{
    Client, Error, Result,
    batches::{Payload, READ},
    check_id, valid_idempotency_key, with_query,
};
use reqwest::Method;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use std::fmt;

/// When a link's copies are made.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum LinkPrepare {
    /// When each recipient first opens the link.
    OnOpen,
    /// At once, for every listed recipient.
    OnCreate,
}

impl LinkPrepare {
    /// Wire value: `on_open` or `on_create`.
    pub fn as_str(self) -> &'static str {
        match self {
            LinkPrepare::OnOpen => "on_open",
            LinkPrepare::OnCreate => "on_create",
        }
    }
}

impl fmt::Display for LinkPrepare {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A link for [`Client::create_link`] and [`Client::estimate_link`].
///
/// Give exactly one of [`NewLink::recipients`] and [`NewLink::allowed_domains`].
///
/// ```
/// use etchv::{LinkPrepare, NewLink};
/// let asset = format!("ast_{}", "a".repeat(64));
/// let link = NewLink::new(&asset, "Board pack, October")
///     .recipients(["alex@example.com", "sam@example.com"])
///     .download_limit(3)
///     .prepare(LinkPrepare::OnCreate);
/// let anyone_at = NewLink::new(&asset, "Price list").allowed_domains(["example.com"]);
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NewLink {
    /// A source asset (`ast_…`, kind `source`); each copy is watermarked from it.
    pub asset_id: String,
    /// Shown to recipients, 1–200 characters.
    pub name: String,
    /// Recipients' emails, up to 100.
    pub recipients: Vec<String>,
    /// Email domains whose addresses may open the link (exact match), up to 10.
    pub allowed_domains: Vec<String>,
    /// ISO 8601 time with a time zone, for example `2026-12-31T23:59:59Z`. Defaults to 30
    /// days from now, or the source file's expiry if sooner; it must be at
    /// least five minutes away and at most a year.
    pub expires_at: Option<String>,
    /// When copies are made; the API chooses when absent.
    pub prepare: Option<LinkPrepare>,
    /// Downloads allowed per recipient, 1–100; unlimited when absent.
    pub download_limit: Option<u32>,
    /// Webhook endpoint (`wh_…`) that receives `link.opened` and `link.locked` events.
    pub webhook_id: Option<String>,
    /// `Idempotency-Key` for [`Client::create_link`] (8–128 letters, digits,
    /// `-` or `_`); generated when absent. Ignored by [`Client::estimate_link`].
    pub idempotency_key: Option<String>,
}

impl NewLink {
    /// A link to `asset_id` named `name`; add recipients or allowed domains.
    pub fn new(asset_id: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            asset_id: asset_id.into(),
            name: name.into(),
            recipients: Vec::new(),
            allowed_domains: Vec::new(),
            expires_at: None,
            prepare: None,
            download_limit: None,
            webhook_id: None,
            idempotency_key: None,
        }
    }
    /// Recipients' emails, up to 100.
    pub fn recipients<I: IntoIterator<Item = S>, S: Into<String>>(mut self, emails: I) -> Self {
        self.recipients = emails.into_iter().map(Into::into).collect();
        self
    }
    /// Let anyone with an email at these domains open the link, up to 10.
    pub fn allowed_domains<I: IntoIterator<Item = S>, S: Into<String>>(
        mut self,
        domains: I,
    ) -> Self {
        self.allowed_domains = domains.into_iter().map(Into::into).collect();
        self
    }
    /// Expiry as an ISO 8601 time with a time zone, for example `2026-12-31T23:59:59Z`. The API
    /// checks it: at least five minutes away, at most a year, and no later than the source file's expiry.
    pub fn expires_at(mut self, timestamp: impl Into<String>) -> Self {
        self.expires_at = Some(timestamp.into());
        self
    }
    /// Make copies when each recipient opens the link, or at once.
    pub fn prepare(mut self, prepare: LinkPrepare) -> Self {
        self.prepare = Some(prepare);
        self
    }
    /// Downloads allowed per recipient, 1–100.
    pub fn download_limit(mut self, downloads: u32) -> Self {
        self.download_limit = Some(downloads);
        self
    }
    /// Send `link.opened` and `link.locked` events to this webhook endpoint.
    pub fn webhook_id(mut self, id: impl Into<String>) -> Self {
        self.webhook_id = Some(id.into());
        self
    }
    /// Make [`Client::create_link`] safely repeatable with your own key.
    pub fn idempotency_key(mut self, key: impl Into<String>) -> Self {
        self.idempotency_key = Some(key.into());
        self
    }

    /// Validate the link and return its request body.
    fn body(&self) -> Result<Value> {
        check_id(&self.asset_id, "ast_", 64, "asset")?;
        if self.name.trim().is_empty() || self.name.chars().count() > 200 {
            return Err(Error::input("name must be 1–200 characters"));
        }
        let mut body = Map::new();
        body.insert("asset_id".into(), json!(self.asset_id));
        body.insert("name".into(), json!(self.name));
        match (self.recipients.is_empty(), self.allowed_domains.is_empty()) {
            (false, true) => body.insert("recipients".into(), json!(self.recipients)),
            (true, false) => body.insert("allowed_domains".into(), json!(self.allowed_domains)),
            _ => return Err(Error::input("Give either recipients or allowed domains")),
        };
        if let Some(ref at) = self.expires_at {
            if at.trim().is_empty() {
                return Err(Error::input(
                    "expires_at must be a time with a time zone, for example 2026-12-31T23:59:59Z",
                ));
            }
            body.insert("expires_at".into(), json!(at));
        }
        if let Some(prepare) = self.prepare {
            body.insert("prepare".into(), json!(prepare.as_str()));
        }
        if let Some(limit) = self.download_limit {
            if !(1..=100).contains(&limit) {
                return Err(Error::input("download limit must be 1–100"));
            }
            body.insert("download_limit".into(), json!(limit));
        }
        if let Some(ref id) = self.webhook_id {
            check_id(id, "wh_", 32, "webhook")?;
            body.insert("webhook_id".into(), json!(id));
        }
        Ok(Value::Object(body))
    }
}

/// A recipient's watermarked copy.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LinkCopy {
    /// `not_started`, `preparing`, `ready` or `failed`.
    pub status: String,
    /// The copy in the asset library, once ready.
    #[serde(default)]
    pub asset_id: Option<String>,
    /// Why the copy failed, when it did.
    #[serde(default)]
    pub error_code: Option<String>,
}

/// One recipient of a link, with their opens, downloads and copy.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LinkRecipient {
    /// The recipient's email.
    pub email: String,
    /// `not_opened` or `opened`.
    pub status: String,
    /// The first open, after the recipient confirmed their email (RFC 3339).
    #[serde(default)]
    pub opened_at: Option<String>,
    /// The latest open (RFC 3339).
    #[serde(default)]
    pub last_opened_at: Option<String>,
    /// Opens so far.
    pub open_count: u64,
    /// Downloads so far.
    pub download_count: u64,
    /// The recipient's watermarked copy.
    pub copy: LinkCopy,
}

/// A traceable link. Timestamps are RFC 3339 strings.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct Link {
    /// Link ID (`lnk_…`).
    pub id: String,
    /// What you send to your recipients.
    pub url: String,
    /// Shown to recipients.
    pub name: String,
    /// The source asset (`ast_…`).
    pub asset_id: String,
    /// `image`, `document` or `video`.
    pub media_type: String,
    /// `active`, `revoked`, `expired` or `unavailable` (the source file was
    /// deleted or expired, so no new copies are made).
    pub status: String,
    /// `on_open` or `on_create`.
    pub prepare: String,
    /// Email domains whose addresses may open the link; empty for a recipient list.
    #[serde(default)]
    pub allowed_domains: Vec<String>,
    /// When the link expires.
    pub expires_at: String,
    /// Downloads allowed per recipient; `None` is unlimited.
    #[serde(default)]
    pub download_limit: Option<u32>,
    /// Listed recipients, or those who have opened a link for allowed domains.
    pub recipient_count: u64,
    /// Credits each copy takes (one embed of the source).
    pub credits_per_copy: u64,
    /// Every listed recipient's copies; `None` for allowed domains.
    #[serde(default)]
    pub estimated_credits: Option<u64>,
    /// With `on_create`, the batch making the copies (`bat_…`).
    #[serde(default)]
    pub batch_id: Option<String>,
    /// Webhook endpoint receiving the link's events.
    #[serde(default)]
    pub webhook_id: Option<String>,
    /// Creation time.
    pub created_at: String,
    /// When the link was revoked.
    #[serde(default)]
    pub revoked_at: Option<String>,
    /// Each recipient with their copy; empty in [`Client::list_links`].
    #[serde(default)]
    pub recipients: Vec<LinkRecipient>,
}

/// One page of [`Client::list_links`], newest first.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LinkPage {
    /// Links on this page, without recipients.
    pub data: Vec<Link>,
    /// Pass as `before` for the next page; `None` on the last page.
    pub next_cursor: Option<String>,
}

/// What a link's copies would cost, from [`Client::estimate_link`].
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LinkEstimate {
    /// The source asset.
    pub asset_id: String,
    /// `image`, `document` or `video`.
    pub media_type: String,
    /// When copies would be made: `on_open` or `on_create`.
    pub prepare: String,
    /// Credits each copy takes.
    pub credits_per_copy: u64,
    /// Every listed recipient's copies; `None` for allowed domains.
    #[serde(default)]
    pub estimated_credits: Option<u64>,
    /// Credits the workspace has now.
    pub available_credits: u64,
    /// Whether creating the link now would pass the credit check.
    pub enough_credits: bool,
    /// The latest `expires_at` this link could have.
    pub latest_expires_at: String,
}

/// An open, download or lock of a link.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LinkEvent {
    /// Event ID (`lev_…`).
    pub id: String,
    /// `opened`, `downloaded` or `locked` (too many wrong codes).
    #[serde(rename = "type")]
    pub kind: String,
    /// The recipient's email; `None` for `locked`.
    #[serde(default)]
    pub recipient: Option<String>,
    /// When it happened (RFC 3339).
    pub created_at: String,
    /// For an open: how the recipient proved their email (`email_code`).
    #[serde(default)]
    pub verified_by: Option<String>,
    /// For a download: the copy downloaded.
    #[serde(default)]
    pub asset_id: Option<String>,
}

/// One page of [`Client::list_link_events`], newest first.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct LinkEventPage {
    /// Events on this page.
    pub data: Vec<LinkEvent>,
    /// Pass as `before` for the next page; `None` on the last page.
    pub next_cursor: Option<String>,
}

/// The link a detected copy was made for.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct DetectedLink {
    /// Link ID (`lnk_…`).
    pub id: String,
    /// The link's name.
    pub name: String,
}

/// The traceable link copy a detected watermark is on.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct DetectedCopy {
    /// The recipient's copy in the asset library.
    pub asset_id: Option<String>,
    /// The link it was made for.
    pub link: DetectedLink,
    /// The email the copy was made for.
    pub recipient: String,
    /// When the recipient first opened the link (RFC 3339).
    #[serde(default)]
    pub opened_at: Option<String>,
}

/// Another of your watermarked assets a detected watermark is on.
#[derive(Debug, Clone, Deserialize)]
#[non_exhaustive]
pub struct DetectedAsset {
    /// Asset ID (`ast_…`).
    pub asset_id: String,
    /// The asset's name.
    pub name: String,
    /// The asset's own metadata, as set in the library.
    #[serde(default)]
    pub metadata: Value,
    /// Creation time (RFC 3339).
    pub created_at: String,
}

fn link_path(id: &str) -> Result<String> {
    check_id(id, "lnk_", 32, "link")?;
    Ok(format!("links/{id}"))
}

/// `limit` (1 to `most`) and an optional `before` cursor as query pairs.
fn page_query(
    path: &str,
    limit: Option<u32>,
    most: u32,
    before: Option<&str>,
    prefix: &str,
    what: &str,
) -> Result<String> {
    let limit = match limit {
        Some(l) if !(1..=most).contains(&l) => {
            return Err(Error::input(format!("limit must be 1–{most}")));
        }
        l => l.map(|l| l.to_string()),
    };
    let mut pairs = Vec::new();
    if let Some(ref limit) = limit {
        pairs.push(("limit", limit.as_str()));
    }
    if let Some(before) = before {
        check_id(before, prefix, 32, what)?;
        pairs.push(("before", before));
    }
    Ok(with_query(path, &pairs))
}

impl Client {
    /// Share a source asset so each recipient downloads their own watermarked
    /// copy (`POST /links`, `links:write`). Send the returned [`Link::url`]
    /// yourself.
    ///
    /// Transient failures (transport errors, HTTP 429, 502, 503, 504) are
    /// retried until the client deadline with one `Idempotency-Key`, so a retry
    /// returns the same link. A workspace without the credits for the copies
    /// gets HTTP 402 with [`Error::code`] `insufficient_credits`.
    ///
    /// ```no_run
    /// use etchv::NewLink;
    /// # let client = etchv::Client::new("etchv_...")?;
    /// # let source_asset_id = "ast_...";
    /// let link = client.create_link(
    ///     &NewLink::new(source_asset_id, "Board pack, October")
    ///         .recipients(["alex@example.com", "sam@example.com"])
    ///         .download_limit(3),
    /// )?;
    /// println!("{}", link.url);
    /// # Ok::<(), etchv::Error>(())
    /// ```
    pub fn create_link(&self, link: &NewLink) -> Result<Link> {
        let body = link.body()?;
        let key = match link.idempotency_key.as_deref().filter(|k| !k.is_empty()) {
            Some(key) if !valid_idempotency_key(key) => {
                return Err(Error::input(
                    "Idempotency key must contain 8–128 letters, digits, hyphens or underscores",
                ));
            }
            Some(key) => key.to_owned(),
            None => uuid::Uuid::new_v4().to_string(),
        };
        self.batch_json(
            Method::POST,
            "links".into(),
            Payload::Json(&body),
            Some(&key),
            READ,
        )
        .map_err(|mut e| {
            e.idempotency_key = Some(key);
            e
        })
    }

    /// Run every check of [`Client::create_link`] without creating anything,
    /// and report the credits its copies would take (`POST /links/estimate`,
    /// `links:write`). Not retried.
    pub fn estimate_link(&self, link: &NewLink) -> Result<LinkEstimate> {
        self.call_json(Method::POST, "links/estimate", Some(link.body()?))
    }

    /// List links newest first, without recipients (`GET /links`, `links:read`).
    /// `limit` is 1–50 (API default 20); pass a page's `next_cursor` as `before`.
    pub fn list_links(&self, limit: Option<u32>, before: Option<&str>) -> Result<LinkPage> {
        let path = page_query("links", limit, 50, before, "lnk_", "link cursor")?;
        self.call_json(Method::GET, &path, None)
    }

    /// Read a link with each recipient, their opens and downloads, and their
    /// copy (`GET /links/{id}`, `links:read`).
    pub fn get_link(&self, link_id: &str) -> Result<Link> {
        self.call_json(Method::GET, &link_path(link_id)?, None)
    }

    /// List a link's opens, downloads and locks newest first
    /// (`GET /links/{id}/events`, `links:read`). `limit` is 1–100 (API
    /// default 50); pass a page's `next_cursor` as `before`.
    pub fn list_link_events(
        &self,
        link_id: &str,
        limit: Option<u32>,
        before: Option<&str>,
    ) -> Result<LinkEventPage> {
        let path = format!("{}/events", link_path(link_id)?);
        let path = page_query(&path, limit, 100, before, "lev_", "link event cursor")?;
        self.call_json(Method::GET, &path, None)
    }

    /// Stop new opens and downloads at once (`POST /links/{id}/revoke`,
    /// `links:write`). Copies not yet queued are canceled; downloaded copies
    /// keep their watermark. Revoking again returns the link.
    pub fn revoke_link(&self, link_id: &str) -> Result<Link> {
        self.call_json(
            Method::POST,
            &format!("{}/revoke", link_path(link_id)?),
            None,
        )
    }
}
