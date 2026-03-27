//! Amp Admin API

use std::sync::Arc;

use axum::{
    Router,
    routing::{get, post, put},
};

pub mod build_info;
pub mod ctx;
pub mod handlers;
pub mod scheduler;

use ctx::Ctx;
use handlers::{datasets, jobs, manifests, providers, schema};

use crate::ctx::{RevisionGuardImpl, WorkerServiceImpl};

/// Create the admin API router with all routes registered
///
/// Returns a router configured with all admin API endpoints.
pub fn router(ctx: Ctx) -> Router<()> {
    let tables_ctx = amp_controller_admin_tables::ctx::Ctx {
        metadata_db: ctx.metadata_db.clone(),
        datasets_registry: ctx.datasets_registry.clone(),
        datasets_cache: ctx.datasets_cache.clone(),
        revision_guard: Arc::new(RevisionGuardImpl(ctx.scheduler.clone())),
        data_store: ctx.data_store.clone(),
    };
    let system_ctx = amp_controller_admin_system::ctx::Ctx {
        worker_service: Arc::new(WorkerServiceImpl(ctx.scheduler.clone())),
    };

    Router::new()
        .route(
            "/datasets",
            get(datasets::list_all::handler).post(datasets::register::handler),
        )
        .route(
            "/datasets/{namespace}/{name}",
            get(datasets::get::handler).delete(datasets::delete::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions",
            get(datasets::list_versions::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{version}",
            get(datasets::get::handler).delete(datasets::delete_version::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/manifest",
            get(datasets::get_manifest::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/deploy",
            post(datasets::deploy::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/restore",
            post(datasets::restore::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/tables/{table_name}/restore",
            post(datasets::restore_table::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/jobs",
            get(datasets::list_jobs::handler),
        )
        .route(
            "/jobs",
            get(jobs::get_all::handler)
                .post(jobs::create::handler)
                .delete(jobs::delete::handler),
        )
        .route(
            "/jobs/{id}",
            get(jobs::get_by_id::handler).delete(jobs::delete_by_id::handler),
        )
        .route("/jobs/{id}/stop", put(jobs::stop::handler))
        .route("/jobs/{id}/progress", get(jobs::progress::handler))
        .route("/jobs/{id}/events", get(jobs::events::handler))
        .route(
            "/jobs/{id}/events/{event_id}",
            get(jobs::event_by_id::handler),
        )
        .route(
            "/manifests",
            get(manifests::list_all::handler)
                .post(manifests::register::handler)
                .delete(manifests::prune::handler),
        )
        .route(
            "/manifests/{hash}",
            get(manifests::get_by_id::handler).delete(manifests::delete_by_id::handler),
        )
        .route(
            "/manifests/{hash}/datasets",
            get(manifests::list_datasets::handler),
        )
        .route(
            "/providers",
            get(providers::get_all::handler).post(providers::create::handler),
        )
        .route(
            "/providers/{name}",
            get(providers::get_by_id::handler).delete(providers::delete_by_id::handler),
        )
        .route("/schema", post(schema::handler))
        .with_state(ctx)
        .merge(amp_controller_admin_system::router().with_state(system_ctx))
        .merge(amp_controller_admin_tables::router().with_state(tables_ctx))
}

#[cfg(feature = "utoipa")]
#[derive(utoipa::OpenApi)]
#[openapi(
    info(
        title = "Amp Admin API",
        version = "1.0.0",
        description = include_str!("../SPEC_DESCRIPTION.md")
    ),
    paths(
        // Dataset endpoints
        handlers::datasets::list_all::handler,
        handlers::datasets::list_versions::handler,
        handlers::datasets::list_jobs::handler,
        handlers::datasets::get::handler,
        handlers::datasets::get_manifest::handler,
        handlers::datasets::register::handler,
        handlers::datasets::deploy::handler,
        handlers::datasets::restore::handler,
        handlers::datasets::restore_table::handler,
        handlers::datasets::delete::handler,
        handlers::datasets::delete_version::handler,
        // Manifest endpoints
        handlers::manifests::list_all::handler,
        handlers::manifests::register::handler,
        handlers::manifests::get_by_id::handler,
        handlers::manifests::delete_by_id::handler,
        handlers::manifests::list_datasets::handler,
        handlers::manifests::prune::handler,
        // Job endpoints
        handlers::jobs::get_all::handler,
        handlers::jobs::get_by_id::handler,
        handlers::jobs::stop::handler,
        handlers::jobs::progress::handler,
        handlers::jobs::events::handler,
        handlers::jobs::event_by_id::handler,
        handlers::jobs::delete::handler,
        handlers::jobs::delete_by_id::handler,
        // Provider endpoints
        handlers::providers::get_all::handler,
        handlers::providers::get_by_id::handler,
        handlers::providers::create::handler,
        handlers::providers::delete_by_id::handler,
        // Files endpoints
        amp_controller_admin_tables::files::handlers::get_by_id::handler,
        // Schema endpoints
        handlers::schema::handler,
        // Revision endpoints
        amp_controller_admin_tables::revisions::handlers::list::handler,
        amp_controller_admin_tables::revisions::handlers::restore::handler,
        amp_controller_admin_tables::revisions::handlers::activate::handler,
        amp_controller_admin_tables::revisions::handlers::deactivate::handler,
        amp_controller_admin_tables::revisions::handlers::get_by_id::handler,
        amp_controller_admin_tables::revisions::handlers::create::handler,
        amp_controller_admin_tables::revisions::handlers::delete::handler,
        amp_controller_admin_tables::revisions::handlers::truncate::handler,
        amp_controller_admin_tables::revisions::handlers::prune::handler,
        // Worker endpoints
        amp_controller_admin_system::workers::handlers::get_all::handler,
        amp_controller_admin_system::workers::handlers::get_by_id::handler,
    ),
    components(schemas(
        // Common schemas
        handlers::error::ErrorResponse,
        // Manifest schemas
        handlers::manifests::list_all::ManifestsResponse,
        handlers::manifests::list_all::ManifestInfo,
        handlers::manifests::register::RegisterManifestResponse,
        handlers::manifests::list_datasets::ManifestDatasetsResponse,
        handlers::manifests::list_datasets::Dataset,
        handlers::manifests::prune::PruneResponse,
        // Dataset schemas
        handlers::datasets::get::DatasetInfo,
        handlers::datasets::list_all::DatasetsResponse,
        handlers::datasets::list_all::DatasetSummary,
        handlers::datasets::list_versions::VersionsResponse,
        handlers::datasets::list_versions::VersionInfo,
        handlers::datasets::register::RegisterRequest,
        handlers::datasets::register::RegisterResponse,
        handlers::datasets::deploy::DeployRequest,
        handlers::datasets::deploy::DeployResponse,
        handlers::datasets::restore::RestoreResponse,
        handlers::datasets::restore::RestoredTableInfo,
        handlers::datasets::restore_table::RestoreTablePayload,
        // Job schemas
        handlers::jobs::progress::JobProgressResponse,
        handlers::jobs::progress::TableProgress,
        handlers::jobs::events::JobEventsResponse,
        handlers::jobs::events::JobEventInfo,
        handlers::jobs::event_by_id::JobEventDetailResponse,
        handlers::jobs::job_info::JobInfo,
        handlers::jobs::get_all::JobsResponse,
        handlers::jobs::delete::JobStatusFilter,
        // Provider schemas
        handlers::providers::provider_info::ProviderInfo,
        handlers::providers::get_all::ProvidersResponse,
        // File schemas
        amp_controller_admin_tables::files::handlers::get_by_id::FileInfo,
        // Schema schemas
        handlers::schema::SchemaRequest,
        handlers::schema::SchemaResponse,
        // Revision schemas
        amp_controller_admin_tables::revisions::handlers::restore::RestoreResponse,
        amp_controller_admin_tables::revisions::handlers::activate::ActivationPayload,
        amp_controller_admin_tables::revisions::handlers::deactivate::DeactivationPayload,
        amp_controller_admin_tables::revisions::handlers::get_by_id::RevisionInfo,
        amp_controller_admin_tables::revisions::handlers::get_by_id::RevisionMetadataInfo,
        amp_controller_admin_tables::revisions::handlers::create::CreatePayload,
        amp_controller_admin_tables::revisions::handlers::create::CreateRevisionResponse,
        amp_controller_admin_tables::revisions::handlers::truncate::TruncateResponse,
        amp_controller_admin_tables::revisions::handlers::prune::PruneResponse,
        // Worker schemas
        amp_controller_admin_system::workers::handlers::get_all::WorkerInfo,
        amp_controller_admin_system::workers::handlers::get_all::WorkersResponse,
        amp_controller_admin_system::workers::handlers::get_by_id::WorkerDetailResponse,
        amp_controller_admin_system::workers::handlers::get_by_id::WorkerMetadata,
    )),
    tags(
        (name = "datasets", description = "Dataset management endpoints"),
        (name = "jobs", description = "Job management endpoints"),
        (name = "manifests", description = "Manifest management endpoints"),
        (name = "providers", description = "Provider management endpoints"),
        (name = "files", description = "File access endpoints"),
        (name = "schema", description = "Schema generation endpoints"),
        (name = "revisions", description = "Revision management endpoints"),
        (name = "workers", description = "Worker management endpoints"),
    )
)]
struct ApiDoc;

#[cfg(feature = "utoipa")]
pub fn generate_openapi_spec() -> utoipa::openapi::OpenApi {
    <ApiDoc as utoipa::OpenApi>::openapi()
}
