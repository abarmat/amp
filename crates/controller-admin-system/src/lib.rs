pub mod ctx;
pub mod error;
pub mod workers;

use axum::{Router, routing::get};
use ctx::Ctx;
use workers::handlers as worker_handlers;

pub fn router() -> Router<Ctx> {
    Router::new()
        .route("/workers", get(worker_handlers::get_all::handler))
        .route("/workers/{id}", get(worker_handlers::get_by_id::handler))
}
