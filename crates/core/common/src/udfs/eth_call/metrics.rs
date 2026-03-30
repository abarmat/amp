//! Metrics for eth_call UDF observability.

use monitoring::telemetry::metrics::{Counter, Histogram, KeyValue, Meter};

use super::{cache::EthCallForNetworkError, udf::EthCallRetryError};

/// Metrics registry for eth_call UDF observability.
#[derive(Debug, Clone)]
pub struct EthCallMetrics {
    /// Total eth_call UDF invocations.
    requests_total: Counter,
    /// Wall-clock latency including retries (milliseconds).
    latency_ms: Histogram<f64>,
    /// Failed invocations by error class.
    errors_total: Counter,
    /// Individual retry attempts.
    retries_total: Counter,
}

impl EthCallMetrics {
    /// Creates a new metrics registry from the given meter.
    pub fn new(meter: &Meter) -> Self {
        Self {
            requests_total: Counter::new(
                meter,
                "eth_call_requests_total",
                "Total number of eth_call UDF invocations",
            ),
            latency_ms: Histogram::new_f64(
                meter,
                "eth_call_latency_ms",
                "Wall-clock latency of eth_call including retries",
                "milliseconds",
            ),
            errors_total: Counter::new(
                meter,
                "eth_call_errors_total",
                "Total number of failed eth_call invocations",
            ),
            retries_total: Counter::new(
                meter,
                "eth_call_retries_total",
                "Total number of eth_call retry attempts",
            ),
        }
    }

    /// Record a single eth_call request.
    pub(crate) fn record_request(&self, network: &str, provider: &str) {
        self.requests_total
            .inc_with_kvs(&self.base_kvs(network, provider));
    }

    /// Record wall-clock latency for a completed invocation.
    pub(crate) fn record_latency(&self, latency_ms: f64, network: &str, provider: &str) {
        self.latency_ms
            .record_with_kvs(latency_ms, &self.base_kvs(network, provider));
    }

    /// Record a failed invocation from a retry error.
    pub(crate) fn record_retry_error(
        &self,
        network: &str,
        provider: &str,
        err: &EthCallRetryError,
    ) {
        self.record_error(network, provider, err.error_class());
    }

    /// Record a failed invocation from a network configuration error.
    pub(crate) fn record_network_error(&self, network: &str, err: &EthCallForNetworkError) {
        let class = match err {
            EthCallForNetworkError::ProviderNotFound { .. } => "network_not_configured",
            EthCallForNetworkError::ProviderCreation(_) => "provider_creation_failed",
        };
        self.record_error(network, "unknown", class);
    }

    fn record_error(&self, network: &str, provider: &str, class: &str) {
        let [nw, prov] = self.base_kvs(network, provider);
        let kvs = [nw, prov, KeyValue::new("class", class.to_string())];
        self.errors_total.inc_with_kvs(&kvs);
    }

    /// Record a single retry attempt.
    pub(crate) fn record_retry(&self, network: &str, provider: &str) {
        self.retries_total
            .inc_with_kvs(&self.base_kvs(network, provider));
    }

    fn base_kvs(&self, network: &str, provider: &str) -> [KeyValue; 2] {
        [
            KeyValue::new("network", network.to_string()),
            KeyValue::new("provider", provider.to_string()),
        ]
    }
}
