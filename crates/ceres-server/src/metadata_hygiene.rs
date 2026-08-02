//! Sensitive-key filtering for source-specific metadata served by the public API.

use serde_json::Value;

use crate::config::DEFAULT_METADATA_REDACT_KEYS;

/// Recursively strips configured keys from JSON metadata.
///
/// Rules are ASCII case-insensitive. A rule ending in `*` matches a key prefix;
/// all other rules match a complete key. Empty comma-separated entries are
/// ignored.
#[derive(Clone, Debug)]
pub struct MetadataRedactor {
    rules: Vec<KeyRule>,
}

impl Default for MetadataRedactor {
    fn default() -> Self {
        Self::from_csv(DEFAULT_METADATA_REDACT_KEYS)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum KeyRule {
    Exact(String),
    Prefix(String),
}

impl MetadataRedactor {
    /// Builds a redactor from comma-separated exact names and prefix patterns.
    pub fn from_csv(patterns: &str) -> Self {
        let mut rules = Vec::new();

        for pattern in patterns.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let normalized = pattern.to_ascii_lowercase();
            let rule = normalized.strip_suffix('*').map_or_else(
                || KeyRule::Exact(normalized.clone()),
                |prefix| KeyRule::Prefix(prefix.to_string()),
            );

            if !rules.contains(&rule) {
                rules.push(rule);
            }
        }

        Self { rules }
    }

    /// Removes denied keys in place, including keys nested in objects or arrays.
    pub fn redact(&self, metadata: &mut Value) {
        match metadata {
            Value::Object(object) => {
                object.retain(|key, value| {
                    if self.denies(key) {
                        false
                    } else {
                        self.redact(value);
                        true
                    }
                });
            }
            Value::Array(values) => {
                for value in values {
                    self.redact(value);
                }
            }
            _ => {}
        }
    }

    fn denies(&self, key: &str) -> bool {
        let key = key.to_ascii_lowercase();
        self.rules.iter().any(|rule| match rule {
            KeyRule::Exact(expected) => key == *expected,
            KeyRule::Prefix(prefix) => key.starts_with(prefix),
        })
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::MetadataRedactor;

    #[test]
    fn strips_exact_and_prefix_matches_recursively() {
        let redactor = MetadataRedactor::from_csv(
            "maintainer_email, author_email, contact_*, maintainer_email",
        );
        let mut metadata = json!({
            "maintainer_email": "maintainer@example.org",
            "Author_Email": "author@example.org",
            "title": "Public title",
            "nested": {
                "contact_name": "A person",
                "notes": "Public notes",
                "items": [{
                    "contact_endpoint": "https://internal.example.org",
                    "public_url": "https://data.example.org"
                }]
            }
        });

        redactor.redact(&mut metadata);

        assert!(metadata.get("maintainer_email").is_none());
        assert!(metadata.get("Author_Email").is_none());
        assert_eq!(metadata["title"], "Public title");
        assert!(metadata["nested"].get("contact_name").is_none());
        assert_eq!(metadata["nested"]["notes"], "Public notes");
        assert!(
            metadata["nested"]["items"][0]
                .get("contact_endpoint")
                .is_none()
        );
        assert_eq!(
            metadata["nested"]["items"][0]["public_url"],
            "https://data.example.org"
        );
    }

    #[test]
    fn custom_rules_replace_the_default_policy() {
        let redactor = MetadataRedactor::from_csv("secret,private_*");
        let mut metadata = json!({
            "secret": "remove",
            "private_token": "remove",
            "contact_email": "preserve because the default was replaced"
        });

        redactor.redact(&mut metadata);

        assert!(metadata.get("secret").is_none());
        assert!(metadata.get("private_token").is_none());
        assert!(metadata.get("contact_email").is_some());
    }

    #[test]
    fn default_uses_the_public_api_policy() {
        let redactor = MetadataRedactor::default();
        let mut metadata = json!({
            "maintainer_email": "remove",
            "contact_phone": "remove",
            "title": "preserve"
        });

        redactor.redact(&mut metadata);

        assert!(metadata.get("maintainer_email").is_none());
        assert!(metadata.get("contact_phone").is_none());
        assert!(metadata.get("title").is_some());
    }
}
