//! Optional dual-write sink (Basin). When configured, every ingested signal
//! is also published as flat JSON records — the same shape as the D1 tables
//! and the Basin Iceberg schemas in `basin/`. Sink errors are reported to the
//! caller as a plain `Err(String)`; ingest decides policy (P1: log + continue,
//! D1 is the committed source of truth).

use async_trait::async_trait;
use std::sync::Arc;

pub type BoxedSink = Arc<dyn SignalSink>;

#[async_trait]
pub trait SignalSink: Send + Sync {
    /// signal is one of: "spans" | "logs" | "metric_samples"
    async fn publish(&self, signal: &str, records: Vec<serde_json::Value>) -> Result<(), String>;
}

/// Sink used when dual-write is disabled.
pub struct NoopSink;

#[async_trait]
impl SignalSink for NoopSink {
    async fn publish(&self, _signal: &str, _records: Vec<serde_json::Value>) -> Result<(), String> {
        Ok(())
    }
}

pub fn span_record(span: &crate::api::models::Span, tenant_id: &str) -> serde_json::Value {
    let service_name = span
        .resource_attributes
        .as_ref()
        .map(|attrs| crate::data::models::service_name_from_resource(attrs))
        .unwrap_or_else(|| "unknown".to_string());
    serde_json::json!({
        "tenant_id": tenant_id,
        "trace_id": span.trace_id.as_inner(),
        "span_id": span.span_id.as_inner(),
        "parent_span_id": span.parent_span_id.as_ref().map(|id| id.as_inner()),
        "service_name": service_name,
        "name": span.name,
        "kind": span.kind.as_ref().map(|k| format!("{k:?}")),
        "start_ts": span.start_time.unix_timestamp_nanos() as f64 / 1e9,
        "end_ts": span.end_time.unix_timestamp_nanos() as f64 / 1e9,
        "attributes": crate::data::models::attributes_json(&span.attributes),
    })
}

pub fn log_record(log: &crate::data::models::LogRecord) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": log.tenant_id,
        "service_name": log.service_name,
        "trace_id": log.trace_id,
        "span_id": log.span_id,
        "severity_number": log.severity_number,
        "severity_text": log.severity_text,
        "body": log.body,
        "timestamp": log.timestamp.fractional(),
        "attributes": log.attributes,
        "resource_attributes": log.resource_attributes,
    })
}

pub fn metric_record(sample: &crate::data::models::MetricSample) -> serde_json::Value {
    serde_json::json!({
        "tenant_id": sample.tenant_id,
        "service_name": sample.service_name,
        "metric_name": sample.metric_name,
        "kind": sample.kind,
        "timestamp": sample.timestamp.fractional(),
        "value": sample.value,
        "attributes": sample.attributes,
        "resource_attributes": sample.resource_attributes,
    })
}
