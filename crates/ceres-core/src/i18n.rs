//! Multilingual field support for open data portals.
//!
//! Some portals (e.g., `ckan.opendata.swiss`) return metadata fields as
//! language-keyed JSON objects instead of plain strings:
//!
//! ```json
//! { "en": "English title", "de": "German title", "fr": "French title" }
//! ```
//!
//! The [`LocalizedField`] type transparently handles both formats via custom
//! serde deserialization, allowing a single struct to work with both
//! monolingual and multilingual portals.

use serde::Deserialize;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use std::collections::HashMap;
use std::fmt;

/// A field that may be either a plain string or a multilingual map.
///
/// # Examples
///
/// ```
/// use ceres_core::LocalizedField;
///
/// // Plain string (most portals)
/// let plain: LocalizedField = serde_json::from_str(r#""My Dataset""#).unwrap();
/// assert_eq!(plain.resolve("en"), "My Dataset");
/// assert_eq!(plain.resolve("de"), "My Dataset"); // language ignored for plain
///
/// // Multilingual object (e.g., Swiss portals)
/// let multi: LocalizedField = serde_json::from_str(
///     r#"{"en": "English", "de": "Deutsch", "fr": "Francais"}"#
/// ).unwrap();
/// assert_eq!(multi.resolve("de"), "Deutsch");
/// assert_eq!(multi.resolve("en"), "English");
/// assert_eq!(multi.resolve("it"), "English"); // falls back to "en"
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalizedField {
    /// A simple string value (most portals).
    Plain(String),
    /// A map of language code to localized text (multilingual portals).
    Multilingual(HashMap<String, String>),
}

impl LocalizedField {
    /// Resolves the field to a single string using the preferred language.
    ///
    /// Resolution strategy:
    /// 1. If plain string, return it directly (language is ignored).
    /// 2. If multilingual, try the preferred language.
    /// 3. Fall back to `"en"` if the preferred language is unavailable.
    /// 4. Fall back to the first available translation.
    /// 5. Return an empty string if no translations exist.
    pub fn resolve(&self, preferred_language: &str) -> String {
        match self {
            LocalizedField::Plain(s) => s.clone(),
            LocalizedField::Multilingual(map) => map
                .get(preferred_language)
                .or_else(|| {
                    if preferred_language != "en" {
                        map.get("en")
                    } else {
                        None
                    }
                })
                .or_else(|| map.values().next())
                .cloned()
                .unwrap_or_default(),
        }
    }
}

impl<'de> Deserialize<'de> for LocalizedField {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct LocalizedFieldVisitor;

        impl<'de> Visitor<'de> for LocalizedFieldVisitor {
            type Value = LocalizedField;

            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string or a map of language codes to strings")
            }

            fn visit_str<E: de::Error>(self, value: &str) -> Result<LocalizedField, E> {
                Ok(LocalizedField::Plain(value.to_string()))
            }

            fn visit_string<E: de::Error>(self, value: String) -> Result<LocalizedField, E> {
                Ok(LocalizedField::Plain(value))
            }

            fn visit_map<M>(self, map: M) -> Result<LocalizedField, M::Error>
            where
                M: MapAccess<'de>,
            {
                let translations: HashMap<String, String> =
                    Deserialize::deserialize(de::value::MapAccessDeserializer::new(map))?;
                Ok(LocalizedField::Multilingual(translations))
            }
        }

        deserializer.deserialize_any(LocalizedFieldVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_plain_ignores_language() {
        let field = LocalizedField::Plain("Hello".to_string());
        assert_eq!(field.resolve("en"), "Hello");
        assert_eq!(field.resolve("de"), "Hello");
        assert_eq!(field.resolve("fr"), "Hello");
    }

    #[test]
    fn test_resolve_multilingual_preferred_language() {
        let mut map = HashMap::new();
        map.insert("en".to_string(), "English".to_string());
        map.insert("de".to_string(), "Deutsch".to_string());
        map.insert("fr".to_string(), "Francais".to_string());
        let field = LocalizedField::Multilingual(map);

        assert_eq!(field.resolve("de"), "Deutsch");
        assert_eq!(field.resolve("fr"), "Francais");
        assert_eq!(field.resolve("en"), "English");
    }

    #[test]
    fn test_resolve_multilingual_falls_back_to_en() {
        let mut map = HashMap::new();
        map.insert("en".to_string(), "English".to_string());
        map.insert("de".to_string(), "Deutsch".to_string());
        let field = LocalizedField::Multilingual(map);

        // Italian not available, falls back to English
        assert_eq!(field.resolve("it"), "English");
    }

    #[test]
    fn test_resolve_multilingual_falls_back_to_first_available() {
        let mut map = HashMap::new();
        map.insert("de".to_string(), "Deutsch".to_string());
        let field = LocalizedField::Multilingual(map);

        // Neither "fr" nor "en" available, falls back to first available
        assert_eq!(field.resolve("fr"), "Deutsch");
    }

    #[test]
    fn test_resolve_multilingual_empty_map() {
        let field = LocalizedField::Multilingual(HashMap::new());
        assert_eq!(field.resolve("en"), "");
    }

    #[test]
    fn test_deserialize_plain_string() {
        let field: LocalizedField = serde_json::from_str(r#""My Dataset""#).unwrap();
        assert_eq!(field, LocalizedField::Plain("My Dataset".to_string()));
    }

    #[test]
    fn test_deserialize_multilingual_object() {
        let field: LocalizedField =
            serde_json::from_str(r#"{"en": "English Title", "de": "Deutscher Titel"}"#).unwrap();
        match &field {
            LocalizedField::Multilingual(map) => {
                assert_eq!(map.get("en").unwrap(), "English Title");
                assert_eq!(map.get("de").unwrap(), "Deutscher Titel");
            }
            _ => panic!("Expected Multilingual variant"),
        }
    }

    #[test]
    fn test_deserialize_option_null() {
        let field: Option<LocalizedField> = serde_json::from_str("null").unwrap();
        assert!(field.is_none());
    }

    #[test]
    fn test_deserialize_option_plain() {
        let field: Option<LocalizedField> = serde_json::from_str(r#""description""#).unwrap();
        assert_eq!(
            field,
            Some(LocalizedField::Plain("description".to_string()))
        );
    }

    #[test]
    fn test_deserialize_option_multilingual() {
        let field: Option<LocalizedField> =
            serde_json::from_str(r#"{"en": "English", "fr": "Francais"}"#).unwrap();
        assert!(matches!(field, Some(LocalizedField::Multilingual(_))));
    }
}
