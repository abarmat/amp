//! Service context

use amp_providers_registry::ProvidersRegistry;

/// The controller-admin-providers context
#[derive(Clone)]
pub struct Ctx {
    /// Providers registry for provider configuration operations.
    pub providers_registry: ProvidersRegistry,
}
