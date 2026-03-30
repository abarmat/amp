use axum::{Router, routing::get};

pub mod ctx;
pub mod error;
pub mod providers;

use self::{ctx::Ctx, providers::handlers as providers_handlers};

pub fn router() -> Router<Ctx> {
    Router::new()
        .route(
            "/providers",
            get(providers_handlers::get_all::handler).post(providers_handlers::create::handler),
        )
        .route(
            "/providers/{name}",
            get(providers_handlers::get_by_id::handler)
                .delete(providers_handlers::delete_by_id::handler),
        )
}
