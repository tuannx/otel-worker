use super::otel::JsonOrProtobuf;
use crate::data::models::{HexEncodedId, LogRecord, MetricSample};
use crate::data::BoxedStore;
use crate::service::Service;
use axum::extract::{Path, Query, State};
use axum::response::{IntoResponse, Response};
use axum::Json;
use http::header::CONTENT_TYPE;
use http::{HeaderMap, HeaderValue, StatusCode};
use opentelemetry_proto::tonic::collector::logs::v1::{
    ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    ExportMetricsServiceRequest, ExportMetricsServiceResponse,
};
use prost::Message;
use serde::Deserialize;
use tracing::error;

/// Tenant header set by the auth layer (worker). Defaults to "default".
pub const TENANT_HEADER: &str = "x-tenant-id";

pub fn tenant_from_headers(headers: &HeaderMap) -> String {
    headers
        .get(TENANT_HEADER)
        .and_then(|v| v.to_str().ok())
        .filter(|v| !v.is_empty())
        .unwrap_or("default")
        .to_string()
}

fn respond<T: Message + serde::Serialize>(
    headers: &HeaderMap,
    response: T,
) -> Result<Response, Response> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    match content_type {
        "application/json" => Ok(Json(response).into_response()),
        "application/x-protobuf" => {
            let mut buf = bytes::BytesMut::new();
            response.encode(&mut buf).map_err(|err| {
                error!(?err, "unable to encode protobuf message");
                StatusCode::INTERNAL_SERVER_ERROR.into_response()
            })?;
            Ok((
                [(
                    CONTENT_TYPE,
                    HeaderValue::from_static("application/x-protobuf"),
                )],
                buf,
            )
                .into_response())
        }
        _ => Err(StatusCode::UNSUPPORTED_MEDIA_TYPE.into_response()),
    }
}

#[tracing::instrument(skip_all)]
pub async fn log_collector_handler(
    State(service): State<Service>,
    headers: HeaderMap,
    JsonOrProtobuf(payload): JsonOrProtobuf<ExportLogsServiceRequest>,
) -> Result<Response, Response> {
    let tenant = tenant_from_headers(&headers);
    let response: ExportLogsServiceResponse =
        service.ingest_logs(payload, &tenant).await.map_err(|err| {
            error!(?err, "failed to ingest logs");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;
    respond(&headers, response)
}

#[tracing::instrument(skip_all)]
pub async fn metric_collector_handler(
    State(service): State<Service>,
    headers: HeaderMap,
    JsonOrProtobuf(payload): JsonOrProtobuf<ExportMetricsServiceRequest>,
) -> Result<Response, Response> {
    let tenant = tenant_from_headers(&headers);
    let response: ExportMetricsServiceResponse = service
        .ingest_metrics(payload, &tenant)
        .await
        .map_err(|err| {
            error!(?err, "failed to ingest metrics");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        })?;
    respond(&headers, response)
}

#[derive(Debug, Deserialize)]
pub struct ListQuery {
    pub limit: Option<u32>,
}

#[tracing::instrument(skip_all)]
pub async fn logs_list_handler(
    State(store): State<BoxedStore>,
    Query(params): Query<ListQuery>,
) -> Result<Json<Vec<LogRecord>>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let logs = store
        .logs_list(&tx, params.limit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(logs))
}

#[tracing::instrument(skip_all)]
pub async fn logs_by_trace_handler(
    State(store): State<BoxedStore>,
    Path(trace_id): Path<HexEncodedId>,
) -> Result<Json<Vec<LogRecord>>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let logs = store
        .logs_list_by_trace(&tx, &trace_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(logs))
}

#[tracing::instrument(skip_all)]
pub async fn metrics_list_handler(
    State(store): State<BoxedStore>,
    Query(params): Query<ListQuery>,
) -> Result<Json<Vec<MetricSample>>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let samples = store
        .metrics_list(&tx, params.limit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(samples))
}
