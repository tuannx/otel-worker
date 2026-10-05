use crate::data::util::Timestamp;

/// SqlBuilder allows a store to build SQL queries.
///
/// Any query functions that are required on the [`super::Store`] trait should
/// have a equivalent defined on this builder.
///
/// Note: that this does not set any parameters. It is up to the implementation
/// to set these parameters is a safe way, to prevent SQL injection. Some
/// parameters can be provided as part of the query. For example, a query that
/// includes sorting needs to be set in the query and cannot be provided through
/// a parameters. In this a set of allowed values can be provided and thus no
/// sql injection is possible.
///
/// Future improvements: Currently this builder only supports a single dialect,
/// the sqlite dialect. We might also be able to support configurable table
/// names. For this reason all functions have a reference to `self` and all
/// return a owned string.
#[derive(Default)]
pub struct SqlBuilder {}

impl SqlBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Retrieve a single span by trace_id and span_id.
    ///
    /// Query parameters:
    /// - $1: trace_id
    /// - $2: span_id
    pub fn span_get(&self) -> String {
        String::from("SELECT * FROM spans WHERE trace_id=$1 AND span_id=$2")
    }

    /// Retrieve all spans for a given trace_id.
    ///
    /// Query parameters:
    /// - $1: trace_id
    pub fn span_list_by_trace(&self) -> String {
        String::from("SELECT * FROM spans WHERE trace_id=$1")
    }

    /// Create a new span.
    ///
    /// Query parameters:
    /// - $1: trace_id
    /// - $2: span_id
    /// - $3: parent_span_id
    /// - $4: name
    /// - $5: kind
    /// - $6: start_time
    /// - $7: end_time
    /// - $8: inner
    pub fn span_create(&self) -> String {
        String::from(
            "
            INSERT INTO spans
            (
                trace_id,
                span_id,
                parent_span_id,
                name,
                kind,
                start_time,
                end_time,
                tenant_id,
                service_name,
                inner
            )
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING *",
        )
    }

    /// Raw rows for service/operation summaries and the P3 service map.
    /// Percentiles are computed in Rust (core::query) so D1, libsql and Basin
    /// agree on the definition.
    pub fn spans_service_rows(&self) -> String {
        String::from(
            "SELECT trace_id, span_id, parent_span_id, service_name, name, start_time, end_time, \
             CASE WHEN json_extract(inner, '$.status.code') = 2 THEN 1 ELSE 0 END AS is_error \
             FROM spans ORDER BY start_time ASC",
        )
    }

    /// Get a list of all the traces. (currently limited to 20, sorted by most
    /// recent [`end_time`]). Leave limit as None to use the default of 20.
    ///
    /// If `time` is `Some`, returns only traces that happened before the
    /// `time` as specified by the trace's `end_time`.
    ///
    /// Query parameters: None
    pub fn traces_list(&self, limit: Option<u32>, time: Option<Timestamp>) -> String {
        let limit = limit.unwrap_or(20);

        let where_clause = if let Some(time) = time {
            format!("WHERE end_time <= {}", time.fractional())
        } else {
            String::new()
        };

        format!(
            "
            SELECT trace_id, MAX(end_time) as end_time
            FROM spans
            {where_clause}
            GROUP BY trace_id
            ORDER BY end_time DESC
            LIMIT {limit}
            ",
        )
    }

    /// Delete all spans with a specific trace_id.
    ///
    /// Query parameters:
    /// - $1: trace_id
    pub fn span_delete_by_trace(&self) -> String {
        String::from("DELETE FROM spans WHERE trace_id=$1")
    }

    /// Delete a specific span.
    ///
    /// Query parameters:
    /// - $1: trace_id
    /// - $2: span_id
    pub fn span_delete(&self) -> String {
        String::from("DELETE FROM spans WHERE trace_id=$1 AND span_id=$2")
    }

    pub fn log_create(&self) -> String {
        String::from(
            "INSERT INTO logs
            (tenant_id, service_name, trace_id, span_id, severity_number, severity_text, body, timestamp, attributes, resource_attributes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            RETURNING *",
        )
    }

    pub fn logs_list_by_trace(&self) -> String {
        String::from("SELECT * FROM logs WHERE trace_id=$1 ORDER BY timestamp ASC")
    }

    pub fn logs_list(&self, limit: Option<u32>) -> String {
        let limit = limit.unwrap_or(100);
        format!("SELECT * FROM logs ORDER BY timestamp DESC LIMIT {limit}")
    }

    pub fn metric_create(&self) -> String {
        String::from(
            "INSERT INTO metric_samples
            (tenant_id, service_name, metric_name, kind, timestamp, value, attributes, resource_attributes)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING *",
        )
    }

    pub fn metrics_list(&self, limit: Option<u32>) -> String {
        let limit = limit.unwrap_or(100);
        format!("SELECT * FROM metric_samples ORDER BY timestamp DESC LIMIT {limit}")
    }

    pub fn api_key_get(&self) -> String {
        String::from("SELECT * FROM api_keys WHERE key_hash=$1 AND revoked_at IS NULL")
    }
}

impl SqlBuilder {
    pub fn dashboard_upsert(&self) -> String {
        String::from(
            "INSERT INTO dashboards (id, tenant_id, name, config, config_hash, created_at, updated_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7)
             ON CONFLICT(id) DO UPDATE SET
               tenant_id = excluded.tenant_id,
               name = excluded.name,
               config = excluded.config,
               config_hash = excluded.config_hash,
               updated_at = excluded.updated_at
             RETURNING *",
        )
    }

    pub fn dashboards_list(&self) -> String {
        String::from("SELECT * FROM dashboards WHERE tenant_id=$1 ORDER BY name ASC, id ASC")
    }

    pub fn dashboard_get(&self) -> String {
        String::from("SELECT * FROM dashboards WHERE tenant_id=$1 AND id=$2")
    }

    pub fn dashboard_delete(&self) -> String {
        String::from("DELETE FROM dashboards WHERE tenant_id=$1 AND id=$2")
    }

    pub fn alert_rule_upsert(&self) -> String {
        String::from(
            "INSERT INTO alert_rules (
                id, tenant_id, name, service_name, metric, operator, threshold,
                window_seconds, cooldown_seconds, webhook_url, enabled,
                created_at, updated_at, last_fired_at
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
             ON CONFLICT(id) DO UPDATE SET
               tenant_id = excluded.tenant_id,
               name = excluded.name,
               service_name = excluded.service_name,
               metric = excluded.metric,
               operator = excluded.operator,
               threshold = excluded.threshold,
               window_seconds = excluded.window_seconds,
               cooldown_seconds = excluded.cooldown_seconds,
               webhook_url = excluded.webhook_url,
               enabled = excluded.enabled,
               updated_at = excluded.updated_at
             RETURNING *",
        )
    }

    pub fn alert_rules_list(&self) -> String {
        String::from("SELECT * FROM alert_rules WHERE tenant_id=$1 ORDER BY name ASC, id ASC")
    }

    pub fn alert_rule_get(&self) -> String {
        String::from("SELECT * FROM alert_rules WHERE tenant_id=$1 AND id=$2")
    }

    pub fn alert_rule_delete(&self) -> String {
        String::from("DELETE FROM alert_rules WHERE tenant_id=$1 AND id=$2")
    }

    pub fn alert_rule_mark_fired(&self) -> String {
        String::from(
            "WITH input(tenant_id, rule_id, fired_at) AS (SELECT $1, $2, $3) UPDATE alert_rules SET last_fired_at=(SELECT fired_at FROM input), updated_at=(SELECT fired_at FROM input) WHERE tenant_id=(SELECT tenant_id FROM input) AND id=(SELECT rule_id FROM input)",
        )
    }

    pub fn alert_event_create(&self) -> String {
        String::from(
            "INSERT INTO alert_events (
                id, rule_id, tenant_id, service_name, metric, operator, threshold,
                observed_value, window_seconds, fired_at, status, webhook_url,
                delivery_status, delivery_error
             ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
             RETURNING *",
        )
    }

    pub fn alert_events_list(&self, limit: Option<u32>) -> String {
        let limit = limit.unwrap_or(100);
        format!(
            "SELECT * FROM alert_events WHERE tenant_id=$1 ORDER BY fired_at DESC LIMIT {limit}"
        )
    }

    pub fn alert_event_get(&self) -> String {
        String::from("SELECT * FROM alert_events WHERE id=$1")
    }

    pub fn alert_event_update_delivery(&self) -> String {
        String::from(
            "WITH input(event_id, delivery_status, delivery_error) AS (SELECT $1, $2, $3) UPDATE alert_events SET delivery_status=(SELECT delivery_status FROM input), delivery_error=(SELECT delivery_error FROM input) WHERE id=(SELECT event_id FROM input)",
        )
    }
}
