//! P4 AI trace API: LLM cost overview, agent-run replay, tool health,
//! span search, model prices, and the tenant content-capture setting.
//!
//! Handlers stay thin: all math lives in `crate::genai` so the HTTP API,
//! the UI, and the MCP tools (via ApiClient) return identical numbers.

use crate::data::models::{HexEncodedId, ModelPrice, TenantAiSettings};
use crate::data::BoxedStore;
use crate::genai::{
    search_spans, summarize_overview, summarize_runs, summarize_tools, AgentRun, AiOverview,
    AiSearchFilter, GenAiSpanView, ToolHealth,
};
use axum::extract::{Path, Query, State};
use axum::Json;
use http::{HeaderMap, StatusCode};
use serde::Deserialize;

use super::signals::tenant_from_headers;

const DEFAULT_LIST_LIMIT: u32 = 5000;
const MAX_LIST_LIMIT: u32 = 50_000;

#[derive(Debug, Deserialize)]
pub struct AiListQuery {
    pub limit: Option<u32>,
}

fn clamp_limit(limit: Option<u32>) -> u32 {
    limit.unwrap_or(DEFAULT_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT)
}

async fn load_records(
    store: &BoxedStore,
    tenant_id: &str,
    limit: Option<u32>,
) -> Result<Vec<crate::data::models::GenAiSpanRecord>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .genai_spans_list(&tx, tenant_id, Some(clamp_limit(limit)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[tracing::instrument(skip_all)]
pub async fn ai_overview_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Query(params): Query<AiListQuery>,
) -> Result<Json<AiOverview>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let records = load_records(&store, &tenant_id, params.limit).await?;
    Ok(Json(summarize_overview(&records)))
}

#[tracing::instrument(skip_all)]
pub async fn ai_runs_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Query(params): Query<AiListQuery>,
) -> Result<Json<Vec<crate::genai::AgentRunSummary>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let records = load_records(&store, &tenant_id, params.limit).await?;
    Ok(Json(summarize_runs(&records)))
}

#[tracing::instrument(skip_all)]
pub async fn ai_run_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Path(trace_id): Path<HexEncodedId>,
) -> Result<Json<AgentRun>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let records = store
        .genai_spans_list_by_trace(&tx, &tenant_id, &trace_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let run = crate::genai::build_agent_run(&tenant_id, trace_id.as_inner(), &records)
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(run))
}

#[tracing::instrument(skip_all)]
pub async fn ai_tools_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Query(params): Query<AiListQuery>,
) -> Result<Json<Vec<ToolHealth>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let records = load_records(&store, &tenant_id, params.limit).await?;
    Ok(Json(summarize_tools(&records)))
}

#[derive(Debug, Deserialize)]
pub struct AiSearchQuery {
    pub q: Option<String>,
    pub operation: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub conversation_id: Option<String>,
    pub has_error: Option<bool>,
    pub limit: Option<u32>,
}

#[tracing::instrument(skip_all)]
pub async fn ai_search_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Query(params): Query<AiSearchQuery>,
) -> Result<Json<Vec<GenAiSpanView>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let records = load_records(&store, &tenant_id, params.limit).await?;
    let filter = AiSearchFilter {
        query: params.q.filter(|v| !v.trim().is_empty()),
        operation: params.operation.filter(|v| !v.trim().is_empty()),
        provider: params.provider.filter(|v| !v.trim().is_empty()),
        model: params.model.filter(|v| !v.trim().is_empty()),
        conversation_id: params.conversation_id.filter(|v| !v.trim().is_empty()),
        has_error: params.has_error,
        limit: clamp_limit(params.limit) as usize,
    };
    Ok(Json(search_spans(&records, &filter)))
}

#[tracing::instrument(skip_all)]
pub async fn ai_prices_list_handler(
    State(store): State<BoxedStore>,
    _headers: HeaderMap,
) -> Result<Json<Vec<ModelPrice>>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let prices = store
        .model_prices_list(&tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(prices))
}

#[tracing::instrument(skip_all)]
pub async fn ai_price_upsert_handler(
    State(store): State<BoxedStore>,
    _headers: HeaderMap,
    Json(price): Json<ModelPrice>,
) -> Result<Json<ModelPrice>, StatusCode> {
    if price.provider.trim().is_empty()
        || price.model.trim().is_empty()
        || !price.input_per_mtok.is_finite()
        || !price.output_per_mtok.is_finite()
        || price.input_per_mtok < 0.0
        || price.output_per_mtok < 0.0
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let tx = store
        .start_readwrite_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let saved = store
        .model_price_upsert(&tx, price)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .commit_transaction(tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(saved))
}

#[tracing::instrument(skip_all)]
pub async fn ai_settings_get_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
) -> Result<Json<TenantAiSettings>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let settings = store
        .tenant_ai_settings_get(&tx, &tenant_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(settings))
}

#[derive(Debug, Deserialize)]
pub struct TenantAiSettingsInput {
    pub capture_content: bool,
}

#[tracing::instrument(skip_all)]
pub async fn ai_settings_update_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Json(input): Json<TenantAiSettingsInput>,
) -> Result<Json<TenantAiSettings>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readwrite_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let saved = store
        .tenant_ai_settings_upsert(
            &tx,
            TenantAiSettings {
                tenant_id,
                capture_content: input.capture_content,
            },
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .commit_transaction(tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(saved))
}
