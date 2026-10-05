use crate::api::models::{Span, SpanAdded};
use crate::data::models::{LogRecord, MetricSample};
use crate::data::{BoxedEvents, BoxedStore, DbError};
use crate::sink::{self, BoxedSink};
use anyhow::Result;
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use opentelemetry_proto::tonic::collector::trace::v1::{
    ExportTraceServiceRequest, ExportTraceServiceResponse,
};
use std::sync::Arc;
use thiserror::Error;
use tracing::warn;

/// Service implements shared logic for both the gRPC and HTTP API, and possibly
/// any future API interactions.
///
/// An example of its functionality is the ingestion of traces, this is both
/// used by the gRPC and HTTP API. Luckily the models used by both APIs are the
/// same, so we don't have to define an extra model for the Service API.
#[derive(Clone)]
pub struct Service {
    store: BoxedStore,
    events: BoxedEvents,
    sink: BoxedSink,
}

impl Service {
    pub fn new(store: BoxedStore, events: BoxedEvents) -> Self {
        Self {
            store,
            events,
            sink: Arc::new(sink::NoopSink),
        }
    }

    /// Enable Basin dual-write. Publishing failures are logged, never fatal:
    /// D1 remains the committed source of truth (P1 policy).
    pub fn with_sink(mut self, sink: BoxedSink) -> Self {
        self.sink = sink;
        self
    }

    async fn publish(&self, signal: &str, records: Vec<serde_json::Value>) {
        if records.is_empty() {
            return;
        }
        if let Err(err) = self.sink.publish(signal, records).await {
            warn!(%signal, %err, "Basin dual-write failed (D1 write already committed)");
        }
    }

    /// Ingest the given export message and store it in the [`Store`]. On success
    /// this also broadcast the new trace/spans combinations to
    /// [`ServerEvents`].
    ///
    /// Note that we currently do not support partial success, so it will either
    /// succeed or fail.
    pub async fn ingest_export(
        &self,
        request: ExportTraceServiceRequest,
    ) -> Result<ExportTraceServiceResponse, IngestExportError> {
        self.ingest_traces(request, "default").await
    }

    pub async fn ingest_traces(
        &self,
        request: ExportTraceServiceRequest,
        tenant_id: &str,
    ) -> Result<ExportTraceServiceResponse, IngestExportError> {
        let trace_ids = Self::extract_trace_ids(&request);

        let tx = self.store.start_readwrite_transaction().await?;

        // P4: apply the tenant privacy policy and build the GenAI projection
        // before anything is persisted. Both the D1 write below and the Basin
        // dual-write consume the sanitized spans, so dropped prompt content
        // never reaches any store. Settings/prices reads fail safe: capture
        // off, no prices -> no invented cost.
        let capture_content = self
            .store
            .tenant_ai_settings_get(&tx, tenant_id)
            .await
            .map(|settings| settings.capture_content)
            .unwrap_or(false);
        let prices = self.store.model_prices_list(&tx).await.unwrap_or_default();

        let mut spans = Span::from_collector_request(request);
        for span in &mut spans {
            crate::genai::sanitize_span(span, capture_content);
        }
        let genai_records: Vec<crate::data::models::GenAiSpanRecord> = spans
            .iter()
            .filter_map(|span| crate::genai::build_genai_record(span, tenant_id, &prices))
            .collect();
        let genai_basin_records: Vec<_> =
            genai_records.iter().map(sink::genai_span_record).collect();

        let basin_records: Vec<_> = spans
            .iter()
            .map(|span| sink::span_record(span, tenant_id))
            .collect();
        for span in spans {
            let mut db_span: crate::data::models::Span = span.into();
            db_span.tenant_id = tenant_id.to_string();
            self.store.span_create(&tx, db_span).await?;
        }
        for record in genai_records {
            self.store.genai_span_create(&tx, record).await?;
        }

        self.store.commit_transaction(tx).await?;

        self.publish("spans", basin_records).await;
        self.publish("genai_spans", genai_basin_records).await;

        self.events
            .broadcast(SpanAdded::new(trace_ids).into())
            .await;

        Ok(ExportTraceServiceResponse {
            partial_success: None,
        })
    }

    pub async fn ingest_logs(
        &self,
        request: ExportLogsServiceRequest,
        tenant_id: &str,
    ) -> Result<ExportLogsServiceResponse, IngestExportError> {
        let logs = LogRecord::from_collector_request(request, tenant_id);
        let basin_records: Vec<_> = logs.iter().map(sink::log_record).collect();

        let tx = self.store.start_readwrite_transaction().await?;
        for log in logs {
            self.store.log_create(&tx, log).await?;
        }
        self.store.commit_transaction(tx).await?;

        self.publish("logs", basin_records).await;

        Ok(ExportLogsServiceResponse {
            partial_success: None,
        })
    }

    pub async fn ingest_metrics(
        &self,
        request: ExportMetricsServiceRequest,
        tenant_id: &str,
    ) -> Result<ExportMetricsServiceResponse, IngestExportError> {
        let samples = MetricSample::from_collector_request(request, tenant_id);
        let basin_records: Vec<_> = samples.iter().map(sink::metric_record).collect();

        let tx = self.store.start_readwrite_transaction().await?;
        for sample in samples {
            self.store.metric_create(&tx, sample).await?;
        }
        self.store.commit_transaction(tx).await?;

        self.publish("metric_samples", basin_records).await;

        Ok(ExportMetricsServiceResponse {
            partial_success: None,
        })
    }

    /// Go through the message and extract all trace and span IDs and return
    /// this as a vec of tuples.
    ///
    /// Both the trace and span IDs will be hex encoded.
    fn extract_trace_ids(message: &ExportTraceServiceRequest) -> Vec<(String, String)> {
        message
            .resource_spans
            .iter()
            .flat_map(|span| {
                span.scope_spans.iter().flat_map(|scope_span| {
                    scope_span.spans.iter().map(|inner| {
                        let trace_id = hex::encode(&inner.trace_id);
                        let span_id = hex::encode(&inner.span_id);
                        (trace_id, span_id)
                    })
                })
            })
            .collect()
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum IngestExportError {
    #[error("Database error: {0}")]
    DbError(#[from] DbError),
}
