pub mod ctx;
pub mod error;
pub mod files;
pub mod revisions;

use axum::{
    Router,
    routing::{delete, get, post},
};
use ctx::Ctx;
use files::handlers as file_handlers;
use revisions::handlers as revision_handlers;

pub fn router() -> Router<Ctx> {
    Router::new()
        .route(
            "/revisions",
            get(revision_handlers::list::handler).post(revision_handlers::create::handler),
        )
        .route(
            "/revisions/{id}",
            get(revision_handlers::get_by_id::handler).delete(revision_handlers::delete::handler),
        )
        .route(
            "/revisions/{id}/restore",
            post(revision_handlers::restore::handler),
        )
        .route(
            "/revisions/{id}/truncate",
            delete(revision_handlers::truncate::handler),
        )
        .route(
            "/revisions/{id}/prune",
            delete(revision_handlers::prune::handler),
        )
        .route(
            "/revisions/{id}/activate",
            post(revision_handlers::activate::handler),
        )
        .route(
            "/revisions/deactivate",
            post(revision_handlers::deactivate::handler),
        )
        .route("/files/{file_id}", get(file_handlers::get_by_id::handler))
}
