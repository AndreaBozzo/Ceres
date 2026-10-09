//! Guards for sending credentials to portal APIs.

use ceres_core::error::AppError;
use reqwest::redirect::Policy;
use url::{Host, Url};

/// Redirect hops followed before giving up, matching reqwest's default policy.
const MAX_REDIRECTS: usize = 10;

/// Refuses to send a credential to `url` unless the request is encrypted.
///
/// Credentials go only to `https` URLs. Plain `http` is accepted for loopback
/// hosts, where the request never leaves the machine (local proxies and mock
/// servers in tests).
pub(crate) fn require_secure_key_transport(url: &Url, key_name: &str) -> Result<(), AppError> {
    if url.scheme() == "https" || is_loopback(url) {
        return Ok(());
    }
    Err(AppError::ConfigError(format!(
        "{key_name} is set but {url} is not https; refusing to send the key in cleartext"
    )))
}

/// Redirect policy for a client that carries a credential.
///
/// Follows redirects like reqwest's default policy, but stops at any hop that
/// would leave https. reqwest drops `Authorization` only when the host or port
/// changes, and a key in the query string travels wherever `Location` repeats
/// it, so the initial https check alone does not cover redirects. The error
/// names only the host, because the target URL may carry the key.
pub(crate) fn credential_redirect_policy() -> Policy {
    Policy::custom(|attempt| {
        if attempt.previous().len() >= MAX_REDIRECTS {
            return attempt.error("too many redirects");
        }
        if require_secure_key_transport(attempt.url(), "API key").is_err() {
            let host = attempt
                .url()
                .host_str()
                .unwrap_or("unknown host")
                .to_string();
            return attempt.error(format!(
                "refusing a redirect to non-https {host} while sending an API key"
            ));
        }
        attempt.follow()
    })
}

fn is_loopback(url: &Url) -> bool {
    match url.host() {
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => ip.is_loopback(),
        Some(Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(url: &str) -> Result<(), AppError> {
        require_secure_key_transport(&Url::parse(url).unwrap(), "TEST_API_KEY")
    }

    #[test]
    fn accepts_https_and_loopback() {
        for url in [
            "https://data.example.org/api",
            "http://127.0.0.1:8080/api",
            "http://[::1]:8080/api",
            "http://localhost:8080/api",
        ] {
            assert!(check(url).is_ok(), "{url}");
        }
    }

    #[test]
    fn rejects_cleartext_remote_hosts() {
        for url in [
            "http://data.example.org/api",
            "http://10.0.0.5/api",
            "http://localhost.example.org/api",
        ] {
            let err = check(url).unwrap_err().to_string();
            assert!(err.contains("TEST_API_KEY"), "{url}: {err}");
            assert!(err.contains("not https"), "{url}: {err}");
        }
    }
}
