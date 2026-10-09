//! `PUT` to signed upload URLs, bounded by an idle timeout rather than a total deadline.
use crate::{Client, Error, ErrorKind, Result};
use std::{
    io::{self, Read},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, RecvTimeoutError},
    },
    thread::sleep,
    time::{Duration, Instant},
};

/// A file body for [`Client::put_signed`]: a fresh reader and its exact length.
pub(crate) type UploadBody = (Box<dyn Read + Send>, u64);

/// Backstop for one `PUT` attempt: upload URLs stop working after 6 hours,
/// so no upload that is still moving can need longer. Stalls are caught by
/// the idle timeout, and abandoned attempts by their `abandoned` flag.
const MAX_PUT_DURATION: Duration = Duration::from_secs(6 * 3600);

/// How one attempt ended: success, or an error and whether to try again.
type Attempt = std::result::Result<(), (Error, bool)>;

/// Reads the upload body, recording when bytes last moved and refusing a
/// file whose length no longer matches.
struct Watched {
    inner: Box<dyn Read + Send>,
    remaining: u64,
    started: Instant,
    progress: Arc<AtomicU64>,
    size_changed: Arc<AtomicBool>,
    /// Set when the caller gave up on this attempt; the next read fails.
    abandoned: Arc<AtomicBool>,
}

impl Read for Watched {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.abandoned.load(Ordering::Relaxed) {
            return Err(io::Error::other("the upload attempt was abandoned"));
        }
        // Never read past the declared length; reqwest stops asking once it is sent.
        let want = buf
            .len()
            .min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let n = self.inner.read(&mut buf[..want])?;
        self.progress
            .store(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
        if n == 0 && want > 0 {
            self.size_changed.store(true, Ordering::Relaxed);
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the file got shorter while uploading",
            ));
        }
        self.remaining -= n as u64;
        Ok(n)
    }
}

impl Client {
    /// `PUT` a file to a signed upload URL. The URL carries its own
    /// authorization, so the API key is never sent there.
    ///
    /// Each attempt runs while bytes keep moving: it fails when no body bytes
    /// are taken and no response arrives for the client timeout (an idle
    /// timeout), never because the whole upload takes longer. Transport
    /// errors, stalls and HTTP 500/502/503/504 are retried, 1 second apart,
    /// for up to the client timeout after the first failure. `body` is called
    /// once per attempt; its errors are returned at once.
    ///
    /// Blocking requests cannot be canceled from outside, so a stalled
    /// attempt is abandoned on its background thread while this call moves
    /// on: it fails as soon as it reads the body again, and at the latest
    /// after 6 hours, when the URL expires.
    pub(crate) fn put_signed(
        &self,
        url: &str,
        body: &dyn Fn() -> Result<UploadBody>,
    ) -> Result<()> {
        let mut first_failure: Option<Instant> = None;
        loop {
            let (reader, length) = body()?;
            let error = match self.put_once(url, reader, length) {
                Ok(()) => return Ok(()),
                Err((error, false)) => return Err(error),
                Err((error, true)) => error,
            };
            let since = *first_failure.get_or_insert_with(Instant::now);
            let left = self.timeout.saturating_sub(since.elapsed());
            if left.is_zero() {
                return Err(error);
            }
            sleep(Duration::from_secs(1).min(left));
        }
    }

    fn put_once(&self, url: &str, reader: Box<dyn Read + Send>, length: u64) -> Attempt {
        let started = Instant::now();
        let progress = Arc::new(AtomicU64::new(0));
        let size_changed = Arc::new(AtomicBool::new(false));
        let abandoned = Arc::new(AtomicBool::new(false));
        let body = Watched {
            inner: reader,
            remaining: length,
            started,
            progress: progress.clone(),
            size_changed: size_changed.clone(),
            abandoned: abandoned.clone(),
        };
        let (tx, rx) = mpsc::channel();
        let (http, url) = (self.http.clone(), url.to_owned());
        std::thread::spawn(move || {
            let outcome = http
                .put(&url)
                .header("Content-Type", "application/octet-stream")
                .body(reqwest::blocking::Body::sized(body, length))
                .timeout(MAX_PUT_DURATION)
                .send()
                .map(|response| {
                    let status = response.status().as_u16();
                    let mut text = Vec::new();
                    if status != 200 {
                        let _ = response.take(1000).read_to_end(&mut text);
                    }
                    (status, text)
                });
            let _ = tx.send(outcome);
        });
        let tick = (self.timeout / 10).clamp(Duration::from_millis(5), Duration::from_secs(1));
        let outcome = self.watch_attempt(&rx, tick, started, &progress, &size_changed);
        // Whatever happened, a still-running attempt must stop at its next read.
        abandoned.store(true, Ordering::Relaxed);
        outcome
    }

    fn watch_attempt(
        &self,
        rx: &mpsc::Receiver<reqwest::Result<(u16, Vec<u8>)>>,
        tick: Duration,
        started: Instant,
        progress: &AtomicU64,
        size_changed: &AtomicBool,
    ) -> Attempt {
        loop {
            match rx.recv_timeout(tick) {
                Ok(Ok((200, _))) => return Ok(()),
                Ok(Ok((status, text))) => {
                    let text = String::from_utf8_lossy(&text).into_owned();
                    let error = Error::new(
                        ErrorKind::Api,
                        status,
                        if text.is_empty() {
                            "Upload refused".into()
                        } else {
                            text
                        },
                    );
                    return Err((error, matches!(status, 500 | 502 | 503 | 504)));
                }
                Ok(Err(_)) if size_changed.load(Ordering::Relaxed) => {
                    return Err((
                        Error::input("The file changed size after its upload was created"),
                        false,
                    ));
                }
                Ok(Err(e)) => {
                    let retry = !e.is_builder();
                    return Err((Error::transport(e), retry));
                }
                Err(RecvTimeoutError::Timeout) => {
                    let moved = Duration::from_millis(progress.load(Ordering::Relaxed));
                    if started.elapsed().saturating_sub(moved) >= self.timeout {
                        let error = Error::new(
                            ErrorKind::Timeout,
                            0,
                            format!(
                                "Client deadline exceeded; the upload stalled (no progress for {:?})",
                                self.timeout
                            ),
                        );
                        return Err((error, true));
                    }
                }
                Err(RecvTimeoutError::Disconnected) => {
                    let error =
                        Error::new(ErrorKind::Transport, 0, "The upload ended unexpectedly");
                    return Err((error, true));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Flags = (Arc<AtomicBool>, Arc<AtomicBool>);

    fn watched(bytes: &'static [u8], length: u64) -> (Watched, Flags) {
        let changed = Arc::new(AtomicBool::new(false));
        let abandoned = Arc::new(AtomicBool::new(false));
        let body = Watched {
            inner: Box::new(bytes),
            remaining: length,
            started: Instant::now(),
            progress: Arc::new(AtomicU64::new(0)),
            size_changed: changed.clone(),
            abandoned: abandoned.clone(),
        };
        (body, (changed, abandoned))
    }

    #[test]
    fn an_abandoned_attempt_fails_at_its_next_read() {
        let (mut body, (_, abandoned)) = watched(b"abcdef", 6);
        let mut buf = [0; 3];
        assert_eq!(body.read(&mut buf).unwrap(), 3);
        abandoned.store(true, Ordering::Relaxed);
        assert!(body.read(&mut buf).is_err());
    }

    #[test]
    fn a_shorter_file_is_reported_as_changed() {
        let (mut body, (changed, _)) = watched(b"abc", 6);
        let mut buf = [0; 8];
        assert_eq!(body.read(&mut buf).unwrap(), 3);
        assert!(body.read(&mut buf).is_err());
        assert!(changed.load(Ordering::Relaxed));
    }

    #[test]
    fn the_attempt_backstop_is_the_upload_url_lifetime() {
        // No total deadline shorter than the URL's 6 hours, whatever the file size.
        assert_eq!(MAX_PUT_DURATION, Duration::from_secs(6 * 3600));
    }
}
