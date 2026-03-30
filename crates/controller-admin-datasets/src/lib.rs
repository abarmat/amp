use axum::{
    Router,
    routing::{get, post},
};

pub mod common;
pub mod ctx;
pub mod datasets;
pub mod error;
pub mod manifests;
pub mod schema;

use self::{
    ctx::Ctx, datasets::handlers as dataset_handlers, manifests::handlers as manifest_handlers,
};

pub fn router() -> Router<Ctx> {
    Router::new()
        .route(
            "/datasets",
            get(dataset_handlers::list_all::handler).post(dataset_handlers::register::handler),
        )
        .route(
            "/datasets/{namespace}/{name}",
            get(dataset_handlers::get::handler).delete(dataset_handlers::delete::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions",
            get(dataset_handlers::list_versions::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{version}",
            get(dataset_handlers::get::handler).delete(dataset_handlers::delete_version::handler),
        )
        .route(
            "/datasets/{namespace}/{name}/versions/{revision}/manifest",
            get(dataset_handlers::get_manifest::handler),
        )
        .route(
            "/manifests",
            get(manifest_handlers::list_all::handler)
                .post(manifest_handlers::register::handler)
                .delete(manifest_handlers::prune::handler),
        )
        .route(
            "/manifests/{hash}",
            get(manifest_handlers::get_by_id::handler)
                .delete(manifest_handlers::delete_by_id::handler),
        )
        .route(
            "/manifests/{hash}/datasets",
            get(manifest_handlers::list_datasets::handler),
        )
        .route("/schema", post(schema::handler))
}
