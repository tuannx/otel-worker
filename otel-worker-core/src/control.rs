//! P3 control-plane contracts: dashboards, alert rules/events, and the
//! deterministic service-map projection.
//!
//! These types deliberately contain no adapter or SQL code. SQL lives behind
//! the Store trait; delivery (Worker fetch) lives in the Worker adapter.

use crate::data::util::Timestamp;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A dashboard as returned by the API. `config` is the parsed JSON document;
/// the database stores its canonical encoding and `config_hash`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Dashboard {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub config: serde_json::Value,
    pub config_hash: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Create/update payload for a dashboard. Supplying `id` makes import and
/// re-import idempotent; omitting it derives an ID from tenant + name.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DashboardInput {
    pub id: Option<String>,
    pub name: String,
    pub config: serde_json::Value,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DashboardImport {
    pub dashboards: Vec<DashboardInput>,
}

/// Alert metrics are a closed allowlist. There is intentionally no arbitrary
/// SQL or expression field in a rule.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AlertMetric {
    ErrorRate,
    P95Ms,
    SpanCount,
}

impl AlertMetric {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ErrorRate => "error_rate",
            Self::P95Ms => "p95_ms",
            Self::SpanCount => "span_count",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "error_rate" => Some(Self::ErrorRate),
            "p95_ms" => Some(Self::P95Ms),
            "span_count" => Some(Self::SpanCount),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub enum ComparisonOperator {
    #[serde(rename = ">")]
    Gt,
    #[serde(rename = ">=")]
    Gte,
    #[serde(rename = "<")]
    Lt,
    #[serde(rename = "<=")]
    Lte,
    #[serde(rename = "==")]
    Eq,
}

impl ComparisonOperator {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Gt => ">",
            Self::Gte => ">=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Eq => "==",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            ">" => Some(Self::Gt),
            ">=" => Some(Self::Gte),
            "<" => Some(Self::Lt),
            "<=" => Some(Self::Lte),
            "==" => Some(Self::Eq),
            _ => None,
        }
    }

    pub fn matches(&self, observed: f64, threshold: f64) -> bool {
        match self {
            Self::Gt => observed > threshold,
            Self::Gte => observed >= threshold,
            Self::Lt => observed < threshold,
            Self::Lte => observed <= threshold,
            // Equality is exact by design: rules are evaluated on stored,
            // deterministic aggregates and tests pin the observed value.
            Self::Eq => observed == threshold,
        }
    }
}

/// A stored alert rule. Timestamps are Unix seconds as fractional values,
/// matching the existing telemetry tables.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AlertRule {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub service_name: Option<String>,
    pub metric: AlertMetric,
    pub operator: ComparisonOperator,
    pub threshold: f64,
    pub window_seconds: u64,
    pub cooldown_seconds: u64,
    pub webhook_url: Option<String>,
    pub enabled: bool,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub last_fired_at: Option<Timestamp>,
}

/// Create/update payload for an alert rule.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AlertRuleInput {
    pub id: Option<String>,
    pub name: String,
    pub service_name: Option<String>,
    pub metric: AlertMetric,
    pub operator: ComparisonOperator,
    pub threshold: f64,
    pub window_seconds: u64,
    pub cooldown_seconds: u64,
    pub webhook_url: Option<String>,
    pub enabled: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AlertEvent {
    pub id: String,
    pub rule_id: String,
    pub tenant_id: String,
    pub service_name: String,
    pub metric: AlertMetric,
    pub operator: ComparisonOperator,
    pub threshold: f64,
    pub observed_value: f64,
    pub window_seconds: u64,
    pub fired_at: Timestamp,
    pub status: String,
    pub webhook_url: Option<String>,
    pub delivery_status: String,
    pub delivery_error: Option<String>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct AlertEvaluationResult {
    pub rule_id: String,
    pub service_name: String,
    pub metric: AlertMetric,
    pub observed_value: Option<f64>,
    pub matched: bool,
    pub fired: bool,
    pub suppressed_reason: Option<String>,
    pub event: Option<AlertEvent>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ServiceMapNode {
    pub service_name: String,
    pub span_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub p95_ms: f64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ServiceMapEdge {
    pub source: String,
    pub target: String,
    pub call_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub avg_duration_ms: f64,
    pub p95_duration_ms: f64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ServiceMap {
    pub nodes: Vec<ServiceMapNode>,
    pub edges: Vec<ServiceMapEdge>,
}

/// Canonical JSON encoding: object keys are recursively sorted and array
/// order is preserved. Dashboard hashes are calculated over this encoding.
pub fn canonical_json(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|(a, _), (b, _)| a.cmp(b));
            let inner: Vec<String> = entries
                .into_iter()
                .map(|(key, value)| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_string()),
                        canonical_json(value)
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        serde_json::Value::Array(values) => {
            let inner: Vec<String> = values.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        _ => serde_json::to_string(value).unwrap_or_else(|_| "null".to_string()),
    }
}

fn fnv1a_128(bytes: &[u8]) -> (u64, u64) {
    // Two FNV-1a lanes with different offset bases. This is a stable content
    // identifier, not a cryptographic digest.
    let mut a: u64 = 0xcbf29ce484222325;
    let mut b: u64 = 0x84222325cbf29ce4;
    for byte in bytes {
        a ^= *byte as u64;
        a = a.wrapping_mul(0x100000001b3);
        b = b.wrapping_add(*byte as u64);
        b ^= b >> 29;
        b = b.wrapping_mul(0x880355f21e6d1965);
    }
    (a, b)
}

pub fn dashboard_config_hash(config: &serde_json::Value) -> String {
    let canonical = canonical_json(config);
    let (a, b) = fnv1a_128(canonical.as_bytes());
    format!("{a:016x}{b:016x}")
}

pub fn stable_id(prefix: &str, parts: &[&str]) -> String {
    let joined = parts.join("\u{1f}");
    let (a, b) = fnv1a_128(joined.as_bytes());
    format!("{prefix}-{a:016x}{b:016x}")
}

pub fn slug_id(prefix: &str, tenant_id: &str, name: &str) -> String {
    let slug: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect::<String>()
        .split('-')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-");
    if slug.is_empty() {
        stable_id(prefix, &[tenant_id, name])
    } else {
        format!("{prefix}-{slug}")
    }
}

/// Build a service map from span rows. An edge exists only when a child span's
/// parent is present in the same trace; a missing parent is an external/root
/// boundary and does not fabricate a service node.
pub fn build_service_map(rows: &[crate::query::ServiceSpanRow]) -> ServiceMap {
    let summaries = crate::query::summarize_services(rows);
    let nodes: Vec<ServiceMapNode> = summaries
        .into_iter()
        .map(|summary| ServiceMapNode {
            service_name: summary.service_name,
            span_count: summary.span_count,
            error_count: summary.error_count,
            error_rate: summary.error_rate,
            p95_ms: summary.p95_ms,
        })
        .collect();

    let span_index: BTreeMap<(String, String), &crate::query::ServiceSpanRow> = rows
        .iter()
        .map(|row| {
            (
                (
                    row.trace_id.as_inner().to_string(),
                    row.span_id.as_inner().to_string(),
                ),
                row,
            )
        })
        .collect();

    let mut edge_rows: BTreeMap<(String, String), Vec<&crate::query::ServiceSpanRow>> =
        BTreeMap::new();
    for row in rows {
        let Some(parent_span_id) = &row.parent_span_id else {
            continue;
        };
        let key = (
            row.trace_id.as_inner().to_string(),
            parent_span_id.as_inner().to_string(),
        );
        let Some(parent) = span_index.get(&key) else {
            continue;
        };
        if parent.service_name == row.service_name {
            continue;
        }
        edge_rows
            .entry((parent.service_name.clone(), row.service_name.clone()))
            .or_default()
            .push(row);
    }

    let edges = edge_rows
        .into_iter()
        .map(|((source, target), children)| {
            let mut durations: Vec<f64> = children.iter().map(|row| row.duration_ms()).collect();
            durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
            let error_count = children.iter().filter(|row| row.is_error != 0).count() as u64;
            ServiceMapEdge {
                source,
                target,
                call_count: children.len() as u64,
                error_count,
                error_rate: if children.is_empty() {
                    0.0
                } else {
                    error_count as f64 / children.len() as f64
                },
                avg_duration_ms: if durations.is_empty() {
                    0.0
                } else {
                    durations.iter().sum::<f64>() / durations.len() as f64
                },
                p95_duration_ms: crate::query::percentile(&durations, 95.0),
            }
        })
        .collect();

    ServiceMap { nodes, edges }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::models::HexEncodedId;
    use crate::query::ServiceSpanRow;

    fn row(
        trace: &str,
        span: &str,
        parent: Option<&str>,
        service: &str,
        start: f64,
        duration_ms: f64,
        is_error: i64,
    ) -> ServiceSpanRow {
        let start_time = Timestamp::try_from(start).unwrap();
        let end_time = Timestamp::try_from(start + duration_ms / 1000.0).unwrap();
        ServiceSpanRow {
            trace_id: HexEncodedId::new(trace).unwrap(),
            span_id: HexEncodedId::new(span).unwrap(),
            parent_span_id: parent.map(|value| HexEncodedId::new(value).unwrap()),
            service_name: service.to_string(),
            name: "op".to_string(),
            start_time,
            end_time,
            is_error,
        }
    }

    #[test]
    fn dashboard_hash_ignores_object_key_order() {
        let a: serde_json::Value = serde_json::from_str(r#"{"b":2,"a":{"y":1,"x":2}}"#).unwrap();
        let b: serde_json::Value = serde_json::from_str(r#"{"a":{"x":2,"y":1},"b":2}"#).unwrap();
        assert_eq!(dashboard_config_hash(&a), dashboard_config_hash(&b));
        assert_eq!(canonical_json(&a), canonical_json(&b));
    }

    #[test]
    fn service_map_uses_parent_child_edges_only() {
        let trace = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let rows = vec![
            row(trace, "0000000000000001", None, "web", 100.0, 100.0, 0),
            row(
                trace,
                "0000000000000002",
                Some("0000000000000001"),
                "checkout",
                100.01,
                40.0,
                1,
            ),
            row(
                trace,
                "0000000000000003",
                Some("0000000000000999"),
                "payments",
                100.02,
                10.0,
                0,
            ),
        ];
        let map = build_service_map(&rows);
        assert_eq!(map.nodes.len(), 3);
        assert_eq!(map.edges.len(), 1);
        assert_eq!(map.edges[0].source, "web");
        assert_eq!(map.edges[0].target, "checkout");
        assert_eq!(map.edges[0].call_count, 1);
        assert_eq!(map.edges[0].error_count, 1);
        assert_eq!(map.edges[0].error_rate, 1.0);
    }
}
