use crate::alerting::{dashboard_from_record, evaluate_alerts};
use crate::control::{
    build_service_map, canonical_json, dashboard_config_hash, slug_id, AlertEvent, AlertRule,
    AlertRuleInput, Dashboard, DashboardImport, DashboardInput, ServiceMap,
};
use crate::data::models::{AlertRuleRecord, DashboardRecord};
use crate::data::util::Timestamp;
use crate::data::BoxedStore;
use axum::extract::{Path, Query, State};
use axum::Json;
use http::{HeaderMap, StatusCode};
use serde::Deserialize;

use super::signals::tenant_from_headers;

fn now() -> Timestamp {
    Timestamp::now()
}

fn dashboard_record(
    tenant_id: &str,
    input: DashboardInput,
    existing: Option<&Dashboard>,
) -> DashboardRecord {
    let timestamp = now();
    let id = input
        .id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| slug_id("dashboard", tenant_id, &input.name));
    DashboardRecord {
        id,
        tenant_id: tenant_id.to_string(),
        name: input.name,
        config: canonical_json(&input.config),
        config_hash: dashboard_config_hash(&input.config),
        created_at: existing.map(|d| d.created_at).unwrap_or(timestamp),
        updated_at: timestamp,
    }
}

async fn upsert_dashboard(
    store: &BoxedStore,
    tenant_id: &str,
    input: DashboardInput,
) -> Result<Dashboard, StatusCode> {
    if input.name.trim().is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let id = input
        .id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| slug_id("dashboard", tenant_id, &input.name));

    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let existing = store
        .dashboard_get(&tx, tenant_id, &id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|record| dashboard_from_record(record).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR))
        .transpose()?;

    let record = dashboard_record(tenant_id, input, existing.as_ref());
    let tx = store
        .start_readwrite_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let saved = store
        .dashboard_upsert(&tx, record)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .commit_transaction(tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    dashboard_from_record(saved).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[tracing::instrument(skip_all)]
pub async fn dashboards_list_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
) -> Result<Json<Vec<Dashboard>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let records = store
        .dashboards_list(&tx, &tenant_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let dashboards = records
        .into_iter()
        .map(dashboard_from_record)
        .collect::<crate::data::Result<Vec<_>>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(dashboards))
}

#[tracing::instrument(skip_all)]
pub async fn dashboard_upsert_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Json(input): Json<DashboardInput>,
) -> Result<Json<Dashboard>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    Ok(Json(upsert_dashboard(&store, &tenant_id, input).await?))
}

#[tracing::instrument(skip_all)]
pub async fn dashboards_import_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Json(input): Json<DashboardImport>,
) -> Result<Json<Vec<Dashboard>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let mut dashboards = Vec::with_capacity(input.dashboards.len());
    for dashboard in input.dashboards {
        dashboards.push(upsert_dashboard(&store, &tenant_id, dashboard).await?);
    }
    Ok(Json(dashboards))
}

#[tracing::instrument(skip_all)]
pub async fn dashboard_get_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<Json<Dashboard>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let record = store
        .dashboard_get(&tx, &tenant_id, &id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    Ok(Json(
        dashboard_from_record(record).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

#[tracing::instrument(skip_all)]
pub async fn dashboard_delete_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readwrite_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let deleted = store
        .dashboard_delete(&tx, &tenant_id, &id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .commit_transaction(tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if deleted.unwrap_or(0) == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

fn alert_rule_from_input(
    tenant_id: &str,
    input: AlertRuleInput,
    existing: Option<&AlertRule>,
) -> Result<AlertRule, StatusCode> {
    if input.name.trim().is_empty()
        || !input.threshold.is_finite()
        || input.window_seconds == 0
        || input.window_seconds > 86_400
        || input.cooldown_seconds > 604_800
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    if let Some(url) = &input.webhook_url {
        if !(url.starts_with("https://") || url.starts_with("http://")) {
            return Err(StatusCode::BAD_REQUEST);
        }
    }
    let timestamp = now();
    let id = input
        .id
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| slug_id("alert", tenant_id, &input.name));
    Ok(AlertRule {
        id,
        tenant_id: tenant_id.to_string(),
        name: input.name,
        service_name: input.service_name.filter(|value| !value.trim().is_empty()),
        metric: input.metric,
        operator: input.operator,
        threshold: input.threshold,
        window_seconds: input.window_seconds,
        cooldown_seconds: input.cooldown_seconds,
        webhook_url: input.webhook_url.filter(|value| !value.trim().is_empty()),
        enabled: input.enabled,
        created_at: existing.map(|rule| rule.created_at).unwrap_or(timestamp),
        updated_at: timestamp,
        last_fired_at: existing.and_then(|rule| rule.last_fired_at),
    })
}

async fn load_rule(
    store: &BoxedStore,
    tenant_id: &str,
    id: &str,
) -> Result<Option<AlertRule>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let record = store
        .alert_rule_get(&tx, tenant_id, id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    record
        .map(AlertRule::try_from)
        .transpose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

#[tracing::instrument(skip_all)]
pub async fn alert_rules_list_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
) -> Result<Json<Vec<AlertRule>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let records = store
        .alert_rules_list(&tx, &tenant_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rules = records
        .into_iter()
        .map(AlertRule::try_from)
        .collect::<crate::data::Result<Vec<_>>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(rules))
}

#[tracing::instrument(skip_all)]
pub async fn alert_rule_upsert_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Json(input): Json<AlertRuleInput>,
) -> Result<Json<AlertRule>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let requested_id = input
        .id
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| slug_id("alert", &tenant_id, &input.name));
    let existing = load_rule(&store, &tenant_id, &requested_id).await?;
    let rule = alert_rule_from_input(&tenant_id, input, existing.as_ref())?;

    let tx = store
        .start_readwrite_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let saved = store
        .alert_rule_upsert(&tx, AlertRuleRecord::from(&rule))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .commit_transaction(tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        AlertRule::try_from(saved).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
    ))
}

#[tracing::instrument(skip_all)]
pub async fn alert_rule_delete_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Result<StatusCode, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readwrite_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let deleted = store
        .alert_rule_delete(&tx, &tenant_id, &id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    store
        .commit_transaction(tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if deleted.unwrap_or(0) == 0 {
        Err(StatusCode::NOT_FOUND)
    } else {
        Ok(StatusCode::NO_CONTENT)
    }
}

#[derive(Debug, Deserialize)]
pub struct AlertEventsQuery {
    pub limit: Option<u32>,
}

#[tracing::instrument(skip_all)]
pub async fn alert_events_list_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
    Query(params): Query<AlertEventsQuery>,
) -> Result<Json<Vec<AlertEvent>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let records = store
        .alert_events_list(&tx, &tenant_id, params.limit)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let events = records
        .into_iter()
        .map(AlertEvent::try_from)
        .collect::<crate::data::Result<Vec<_>>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(events))
}

/// Evaluate now. The endpoint records events but does not itself perform an
/// outbound webhook request; Worker Cron calls the same core evaluator and
/// then delivers `pending` events through the Workers Fetch adapter.
#[tracing::instrument(skip_all)]
pub async fn alerts_evaluate_handler(
    State(store): State<BoxedStore>,
    headers: HeaderMap,
) -> Result<Json<Vec<crate::control::AlertEvaluationResult>>, StatusCode> {
    let tenant_id = tenant_from_headers(&headers);
    let results = evaluate_alerts(&store, &tenant_id, now())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(results))
}

#[tracing::instrument(skip_all)]
pub async fn service_map_handler(
    State(store): State<BoxedStore>,
) -> Result<Json<ServiceMap>, StatusCode> {
    let tx = store
        .start_readonly_transaction()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let rows = store
        .spans_service_rows(&tx)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(build_service_map(&rows)))
}
