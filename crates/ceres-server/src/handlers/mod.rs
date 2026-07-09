//! HTTP request handlers for API endpoints.

use ceres_core::{CreateJobRequest, PortalEntry};

pub mod datasets;
pub mod export;
pub mod harvest;
pub mod health;
pub mod portals;
pub mod search;
pub mod stats;

pub(crate) fn build_harvest_job_request(
    portal: &PortalEntry,
    force_full_sync: bool,
) -> CreateJobRequest {
    let mut request = CreateJobRequest::new(portal.url.as_str())
        .with_name(portal.name.as_str())
        .with_portal_type(portal.portal_type);

    if let Some(ref template) = portal.url_template {
        request = request.with_url_template(template.as_str());
    }

    if let Some(ref language) = portal.language {
        request = request.with_language(language.as_str());
    }

    if let Some(profile) = portal.profile {
        request = request.with_profile(profile);
    }

    if let Some(ref sparql_endpoint) = portal.sparql_endpoint {
        request = request.with_sparql_endpoint(sparql_endpoint.as_str());
    }

    if force_full_sync {
        request = request.with_full_sync();
    }

    request
}
