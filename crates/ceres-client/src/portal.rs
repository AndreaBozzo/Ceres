//! Portal client factory and enum dispatch.
//!
//! This module provides a unified interface for working with different
//! portal clients through the [`PortalClientEnum`] enum.
//!
//! # Why an Enum Instead of `dyn Trait`?
//!
//! The [`PortalClient`] trait uses `impl Future` return types (RPITIT)
//! and an associated type `PortalData`, making it not object-safe.
//! We use an enum for static dispatch, following the same pattern as
//! [`EmbeddingProviderEnum`](crate::provider::EmbeddingProviderEnum).

use ceres_core::config::PortalType;
use ceres_core::error::AppError;
use ceres_core::models::NewDataset;
use ceres_core::traits::{PortalClient, PortalClientFactory};
use chrono::{DateTime, Utc};

use crate::ckan::{CkanClient, CkanDataset};

/// Portal-specific dataset data, wrapping concrete types from each portal client.
#[derive(Debug, Clone)]
pub enum PortalDataEnum {
    /// Data from a CKAN portal.
    Ckan(CkanDataset),
}

/// Unified portal client that wraps concrete portal implementations.
///
/// This enum allows runtime selection of portal clients while
/// implementing the `PortalClient` trait.
#[derive(Clone)]
pub enum PortalClientEnum {
    /// CKAN portal client.
    Ckan(CkanClient),
}

impl PortalClient for PortalClientEnum {
    type PortalData = PortalDataEnum;

    fn portal_type(&self) -> &'static str {
        match self {
            Self::Ckan(c) => c.portal_type(),
        }
    }

    fn base_url(&self) -> &str {
        match self {
            Self::Ckan(c) => c.base_url(),
        }
    }

    async fn list_dataset_ids(&self) -> Result<Vec<String>, AppError> {
        match self {
            Self::Ckan(c) => c.list_dataset_ids().await,
        }
    }

    async fn get_dataset(&self, id: &str) -> Result<Self::PortalData, AppError> {
        match self {
            Self::Ckan(c) => c.get_dataset(id).await.map(PortalDataEnum::Ckan),
        }
    }

    fn into_new_dataset(
        data: Self::PortalData,
        portal_url: &str,
        url_template: Option<&str>,
        language: &str,
    ) -> NewDataset {
        match data {
            PortalDataEnum::Ckan(ckan_data) => {
                CkanClient::into_new_dataset(ckan_data, portal_url, url_template, language)
            }
        }
    }

    async fn search_modified_since(
        &self,
        since: DateTime<Utc>,
    ) -> Result<Vec<Self::PortalData>, AppError> {
        match self {
            Self::Ckan(c) => c
                .search_modified_since(since)
                .await
                .map(|datasets| datasets.into_iter().map(PortalDataEnum::Ckan).collect()),
        }
    }

    async fn search_all_datasets(&self) -> Result<Vec<Self::PortalData>, AppError> {
        match self {
            Self::Ckan(c) => c
                .search_all_datasets()
                .await
                .map(|datasets| datasets.into_iter().map(PortalDataEnum::Ckan).collect()),
        }
    }
}

/// Factory that creates the appropriate portal client based on portal type.
///
/// This factory dispatches to the correct concrete client implementation
/// based on the [`PortalType`] parameter.
#[derive(Debug, Clone, Default)]
pub struct PortalClientFactoryEnum;

impl PortalClientFactoryEnum {
    /// Creates a new portal client factory.
    pub fn new() -> Self {
        Self
    }
}

impl PortalClientFactory for PortalClientFactoryEnum {
    type Client = PortalClientEnum;

    fn create(&self, portal_url: &str, portal_type: PortalType) -> Result<Self::Client, AppError> {
        match portal_type {
            PortalType::Ckan => Ok(PortalClientEnum::Ckan(CkanClient::new(portal_url)?)),
            other => Err(AppError::ConfigError(format!(
                "Portal type '{}' is not yet supported. Currently only 'ckan' is implemented.",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factory_creates_ckan_client() {
        let factory = PortalClientFactoryEnum::new();
        let client = factory.create("https://dati.comune.milano.it", PortalType::Ckan);
        assert!(client.is_ok());
        let client = client.unwrap();
        assert_eq!(client.portal_type(), "ckan");
        assert_eq!(client.base_url(), "https://dati.comune.milano.it/");
    }

    #[test]
    fn test_factory_rejects_unsupported_type() {
        let factory = PortalClientFactoryEnum::new();
        let result = factory.create("https://data.cityofnewyork.us", PortalType::Socrata);
        assert!(result.is_err());
    }
}
