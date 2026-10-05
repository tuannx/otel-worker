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

/// Flat Basin record for the P4 GenAI projection (see basin/genai_spans.sql).
/// Content attributes are never part of this shape — only the normalized,
/// already-sanitized projection fields.
pub fn genai_span_record(record: &crate::data::models::GenAiSpanRecord) -> serde_json::Value {
    let price_version = match (
        &record.price_provider,
        &record.price_model,
        &record.price_effective_from,
    ) {
        (Some(provider), Some(model), Some(effective_from)) => Some(format!(
            "{}:{}@{}",
            provider,
            model,
            effective_from.fractional()
        )),
        _ => None,
    };
    serde_json::json!({
        "tenant_id": record.tenant_id,
        "trace_id": record.trace_id.as_inner(),
        "span_id": record.span_id.as_inner(),
        "parent_span_id": record.parent_span_id.as_ref().map(|id| id.as_inner()),
        "service_name": record.service_name,
        "span_name": record.span_name,
        "operation": record.operation,
        "provider": record.provider,
        "request_model": record.request_model,
        "response_model": record.response_model,
        "agent_name": record.agent_name,
        "tool_name": record.tool_name,
        "conversation_id": record.conversation_id,
        "input_tokens": record.input_tokens,
        "output_tokens": record.output_tokens,
        "cache_read_tokens": record.cache_read_tokens,
        "cache_creation_tokens": record.cache_creation_tokens,
        "ttft_ms": record.ttft_ms,
        "duration_ms": record.duration_ms,
        "finish_reasons": record.finish_reasons,
        "cost_usd": record.cost_usd,
        "price_version": price_version,
        "is_error": record.is_error,
        "start_ts": record.start_time.fractional(),
        "end_ts": record.end_time.fractional(),
    })
}
