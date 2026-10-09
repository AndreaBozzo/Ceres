//! Guards for sending credentials to portal APIs.

use ceres_core::error::AppError;
use url::{Host, Url};

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
