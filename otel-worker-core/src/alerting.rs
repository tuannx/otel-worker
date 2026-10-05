//! Deterministic alert evaluation. This module owns the decisions; network
//! delivery is an adapter concern in the Worker (or a caller of the HTTP
//! evaluate endpoint) and consumes the persisted events returned here.

use crate::control::{
    AlertEvaluationResult, AlertEvent, AlertMetric, AlertRule, ComparisonOperator,
};
use crate::data::models::{AlertEventRecord, AlertRuleRecord, DashboardRecord};
use crate::data::{BoxedStore, DbError, Result};
use crate::query::{summarize_services, ServiceSpanRow, ServiceSummary};

impl TryFrom<AlertRuleRecord> for AlertRule {
    type Error = DbError;

    fn try_from(record: AlertRuleRecord) -> Result<Self, Self::Error> {
        let metric = AlertMetric::parse(&record.metric).ok_or_else(|| {
            DbError::InternalError(format!(
                "invalid alert metric in database: {}",
                record.metric
            ))
        })?;
        let operator = ComparisonOperator::parse(&record.operator).ok_or_else(|| {
            DbError::InternalError(format!(
                "invalid alert operator in database: {}",
                record.operator
            ))
        })?;
        if record.window_seconds < 0 || record.cooldown_seconds < 0 {
            return Err(DbError::InternalError(
                "alert rule has a negative window or cooldown".into(),
            ));
        }
        Ok(Self {
            id: record.id,
            tenant_id: record.tenant_id,
            name: record.name,
            service_name: record.service_name,
            metric,
            operator,
            threshold: record.threshold,
            window_seconds: record.window_seconds as u64,
            cooldown_seconds: record.cooldown_seconds as u64,
            webhook_url: record.webhook_url,
            enabled: record.enabled != 0,
            created_at: record.created_at,
            updated_at: record.updated_at,
            last_fired_at: record.last_fired_at,
        })
    }
}

impl From<&AlertRule> for AlertRuleRecord {
    fn from(rule: &AlertRule) -> Self {
        Self {
            id: rule.id.clone(),
            tenant_id: rule.tenant_id.clone(),
            name: rule.name.clone(),
            service_name: rule.service_name.clone(),
            metric: rule.metric.as_str().to_string(),
            operator: rule.operator.as_str().to_string(),
            threshold: rule.threshold,
            window_seconds: rule.window_seconds as i64,
            cooldown_seconds: rule.cooldown_seconds as i64,
            webhook_url: rule.webhook_url.clone(),
            enabled: if rule.enabled { 1 } else { 0 },
            created_at: rule.created_at,
            updated_at: rule.updated_at,
            last_fired_at: rule.last_fired_at,
        }
    }
}

impl TryFrom<AlertEventRecord> for AlertEvent {
    type Error = DbError;

    fn try_from(record: AlertEventRecord) -> Result<Self, Self::Error> {
        let metric = AlertMetric::parse(&record.metric).ok_or_else(|| {
            DbError::InternalError(format!(
                "invalid alert metric in database: {}",
                record.metric
            ))
        })?;
        let operator = ComparisonOperator::parse(&record.operator).ok_or_else(|| {
            DbError::InternalError(format!(
                "invalid alert operator in database: {}",
                record.operator
            ))
        })?;
        if record.window_seconds < 0 {
            return Err(DbError::InternalError(
                "alert event has a negative window".into(),
            ));
        }
        Ok(Self {
            id: record.id,
            rule_id: record.rule_id,
            tenant_id: record.tenant_id,
            service_name: record.service_name,
            metric,
            operator,
            threshold: record.threshold,
            observed_value: record.observed_value,
            window_seconds: record.window_seconds as u64,
            fired_at: record.fired_at,
            status: record.status,
            webhook_url: record.webhook_url,
            delivery_status: record.delivery_status,
            delivery_error: record.delivery_error,
        })
    }
}

impl From<&AlertEvent> for AlertEventRecord {
    fn from(event: &AlertEvent) -> Self {
        Self {
            id: event.id.clone(),
            rule_id: event.rule_id.clone(),
            tenant_id: event.tenant_id.clone(),
            service_name: event.service_name.clone(),
            metric: event.metric.as_str().to_string(),
            operator: event.operator.as_str().to_string(),
            threshold: event.threshold,
            observed_value: event.observed_value,
            window_seconds: event.window_seconds as i64,
            fired_at: event.fired_at,
            status: event.status.clone(),
            webhook_url: event.webhook_url.clone(),
            delivery_status: event.delivery_status.clone(),
            delivery_error: event.delivery_error.clone(),
        }
    }
}

pub fn dashboard_from_record(record: DashboardRecord) -> Result<crate::control::Dashboard> {
    let config: serde_json::Value = serde_json::from_str(&record.config)
        .map_err(|err| DbError::InternalError(format!("invalid dashboard JSON: {err}")))?;
    let computed_hash = crate::control::dashboard_config_hash(&config);
    if computed_hash != record.config_hash {
        return Err(DbError::InternalError(format!(
            "dashboard {} config_hash mismatch (stored {}, computed {})",
            record.id, record.config_hash, computed_hash
        )));
    }
    Ok(crate::control::Dashboard {
        id: record.id,
        tenant_id: record.tenant_id,
        name: record.name,
        config,
        config_hash: record.config_hash,
        created_at: record.created_at,
        updated_at: record.updated_at,
    })
}

fn metric_value(summary: &ServiceSummary, metric: AlertMetric) -> f64 {
    match metric {
        AlertMetric::ErrorRate => summary.error_rate,
        AlertMetric::P95Ms => summary.p95_ms,
        AlertMetric::SpanCount => summary.span_count as f64,
    }
}

/// Evaluate one rule against already-summarized window data. This is pure:
/// callers choose the clock and the rows, so tests can pin both.
pub fn evaluate_rule(
    rule: &AlertRule,
    summaries: &[ServiceSummary],
    now: crate::data::util::Timestamp,
) -> Vec<AlertEvaluationResult> {
    let selected: Vec<&ServiceSummary> = summaries
        .iter()
        .filter(|summary| {
            rule.service_name
                .as_deref()
                .map(|name| name == summary.service_name)
                .unwrap_or(true)
        })
        .collect();

    if selected.is_empty() {
        return vec![AlertEvaluationResult {
            rule_id: rule.id.clone(),
            service_name: rule.service_name.clone().unwrap_or_default(),
            metric: rule.metric,
            observed_value: None,
            matched: false,
            fired: false,
            suppressed_reason: Some("no_data".to_string()),
            event: None,
        }];
    }

    selected
        .into_iter()
        .map(|summary| {
            let observed = metric_value(summary, rule.metric);
            let matched = rule.operator.matches(observed, rule.threshold);
            let cooldown_active = rule
                .last_fired_at
                .map(|last| now.fractional() - last.fractional() < rule.cooldown_seconds as f64);
            let (fired, suppressed_reason) = if !rule.enabled {
                (false, Some("disabled".to_string()))
            } else if !matched {
                (false, None)
            } else if cooldown_active == Some(true) {
                (false, Some("cooldown".to_string()))
            } else {
                (true, None)
            };
            AlertEvaluationResult {
                rule_id: rule.id.clone(),
                service_name: summary.service_name.clone(),
                metric: rule.metric,
                observed_value: Some(observed),
                matched,
                fired,
                suppressed_reason,
                event: None,
            }
        })
        .collect()
}

/// Evaluate all enabled and disabled rules for a tenant, persist one event for
/// each firing service, and advance each fired rule's cooldown marker.
pub async fn evaluate_alerts(
    store: &BoxedStore,
    tenant_id: &str,
    now: crate::data::util::Timestamp,
) -> Result<Vec<AlertEvaluationResult>> {
    let tx = store.start_readonly_transaction().await?;
    let records = store.alert_rules_list(&tx, tenant_id).await?;
    let rows = store.spans_service_rows(&tx).await?;
    drop(tx);

    let mut results = Vec::new();
    for record in records {
        let rule = AlertRule::try_from(record)?;
        // Window filtering is per-rule. Rows are the hot-store projection;
        // Basin will replace this scan with a windowed aggregate later.
        let window_start = now.fractional() - rule.window_seconds as f64;
        let window_rows: Vec<ServiceSpanRow> = rows
            .iter()
            .filter(|row| {
                row.end_time.fractional() >= window_start
                    && row.end_time.fractional() <= now.fractional()
            })
            .cloned()
            .collect();
        let summaries = summarize_services(&window_rows);
        let mut rule_results = evaluate_rule(&rule, &summaries, now);
        let mut rule_fired = false;

        for result in &mut rule_results {
            if !result.fired {
                continue;
            }
            let event_id = crate::control::stable_id(
                "alert",
                &[
                    &rule.id,
                    &result.service_name,
                    &format!("{:.6}", now.fractional()),
                    &format!("{:?}", rule.metric),
                ],
            );
            let event = AlertEvent {
                id: event_id,
                rule_id: rule.id.clone(),
                tenant_id: tenant_id.to_string(),
                service_name: result.service_name.clone(),
                metric: rule.metric,
                operator: rule.operator,
                threshold: rule.threshold,
                observed_value: result.observed_value.unwrap_or_default(),
                window_seconds: rule.window_seconds,
                fired_at: now,
                status: "firing".to_string(),
                webhook_url: rule.webhook_url.clone(),
                delivery_status: if rule.webhook_url.is_some() {
                    "pending".to_string()
                } else {
                    "not_configured".to_string()
                },
                delivery_error: None,
            };
            let tx = store.start_readwrite_transaction().await?;
            let stored = store
                .alert_event_create(&tx, AlertEventRecord::from(&event))
                .await?;
            store.commit_transaction(tx).await?;
            result.event = Some(AlertEvent::try_from(stored)?);
            rule_fired = true;
        }

        if rule_fired {
            let tx = store.start_readwrite_transaction().await?;
            store
                .alert_rule_mark_fired(&tx, tenant_id, &rule.id, now)
                .await?;
            store.commit_transaction(tx).await?;
        }

        results.extend(rule_results);
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::util::Timestamp;

    fn rule(last_fired_at: Option<f64>) -> AlertRule {
        AlertRule {
            id: "rule-error-rate".to_string(),
            tenant_id: "default".to_string(),
            name: "checkout errors".to_string(),
            service_name: Some("checkout".to_string()),
            metric: AlertMetric::ErrorRate,
            operator: ComparisonOperator::Gt,
            threshold: 0.05,
            window_seconds: 300,
            cooldown_seconds: 600,
            webhook_url: None,
            enabled: true,
            created_at: Timestamp::try_from(100.0).unwrap(),
            updated_at: Timestamp::try_from(100.0).unwrap(),
            last_fired_at: last_fired_at.map(|value| Timestamp::try_from(value).unwrap()),
        }
    }

    fn summary(error_rate: f64) -> ServiceSummary {
        ServiceSummary {
            service_name: "checkout".to_string(),
            span_count: 100,
            error_count: 10,
            error_rate,
            p50_ms: 10.0,
            p95_ms: 20.0,
            p99_ms: 30.0,
            avg_ms: 12.0,
            first_seen: 100.0,
            last_seen: 200.0,
        }
    }

    #[test]
    fn fires_once_then_cooldown_suppresses() {
        let now = Timestamp::try_from(1_000.0).unwrap();
        let first = evaluate_rule(&rule(None), &[summary(0.10)], now);
        assert_eq!(first.len(), 1);
        assert!(first[0].matched);
        assert!(first[0].fired);

        let second = evaluate_rule(&rule(Some(1_000.0)), &[summary(0.10)], now);
        assert!(second[0].matched);
        assert!(!second[0].fired);
        assert_eq!(second[0].suppressed_reason.as_deref(), Some("cooldown"));
    }

    #[test]
    fn threshold_is_strict_for_gt() {
        let now = Timestamp::try_from(1_000.0).unwrap();
        let result = evaluate_rule(&rule(None), &[summary(0.05)], now);
        assert!(!result[0].matched);
        assert!(!result[0].fired);
    }
}
