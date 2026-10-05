use crate::data::BoxedStore;
use crate::query::{summarize_operations, summarize_services, OperationSummary, ServiceSummary};
use axum::extract::{Path, State};
use axum::Json;
use http::StatusCode;
use serde::Serialize;

#[tracing::instrument(skip_all)]
pub async fn services_list_handler(
    State(store): State<BoxedStore>,
) -> Result<Json<Vec<ServiceSummary>>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = store
        .spans_service_rows(&tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(summarize_services(&rows)))
}

#[tracing::instrument(skip_all)]
pub async fn service_operations_handler(
    State(store): State<BoxedStore>,
    Path(service_name): Path<String>,
) -> Result<Json<Vec<OperationSummary>>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = store
        .spans_service_rows(&tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let ops: Vec<_> = summarize_operations(&rows)
        .into_iter()
        .filter(|op| op.service_name == service_name)
        .collect();
    if ops.is_empty() {
        return Err(StatusCode::NOT_FOUND);
    }
    Ok(Json(ops))
}

#[derive(Clone, Debug, Serialize)]
pub struct MetricSummary {
    pub service_name: String,
    pub metric_name: String,
    pub kind: String,
    pub sample_count: u64,
    pub avg: f64,
    pub min: f64,
    pub max: f64,
    pub last_value: f64,
    pub last_seen: f64,
}

#[tracing::instrument(skip_all)]
pub async fn metrics_summary_handler(
    State(store): State<BoxedStore>,
) -> Result<Json<Vec<MetricSummary>>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    // Hot store is bounded for MVP; aggregate the most recent 1000 samples in Rust.
    let samples = store
        .metrics_list(&tx, Some(1000))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut by_key: std::collections::BTreeMap<(String, String, String), Vec<_>> =
        std::collections::BTreeMap::new();
    for sample in &samples {
        by_key
            .entry((
                sample.service_name.clone(),
                sample.metric_name.clone(),
                sample.kind.clone(),
            ))
            .or_default()
            .push(sample);
    }

    let summaries = by_key
        .into_iter()
        .map(|((service_name, metric_name, kind), samples)| {
            let count = samples.len() as u64;
            let sum: f64 = samples.iter().map(|s| s.value).sum();
            let last = samples
                .iter()
                .max_by(|a, b| {
                    a.timestamp
                        .fractional()
                        .partial_cmp(&b.timestamp.fractional())
                        .unwrap()
                })
                .unwrap();
            MetricSummary {
                service_name,
                metric_name,
                kind,
                sample_count: count,
                avg: if count == 0 { 0.0 } else { sum / count as f64 },
                min: samples
                    .iter()
                    .map(|s| s.value)
                    .fold(f64::INFINITY, f64::min),
                max: samples
                    .iter()
                    .map(|s| s.value)
                    .fold(f64::NEG_INFINITY, f64::max),
                last_value: last.value,
                last_seen: last.timestamp.fractional(),
            }
        })
        .collect();

    Ok(Json(summaries))
}
