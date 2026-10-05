use anyhow::Context;
use async_trait::async_trait;
use libsql::{params, Builder, Connection};
use otel_worker_core::data::models::{HexEncodedId, Span};
use otel_worker_core::data::sql::SqlBuilder;
use otel_worker_core::data::util::Timestamp;
use otel_worker_core::data::{DbError, Result, Store, Transaction};
use std::fmt::Display;
use std::path::Path;
use std::sync::Arc;
use tracing::trace;
use util::RowsExt;

mod migrations;
mod util;

#[cfg(test)]
mod tests;

pub enum DataPath<'a> {
    InMemory,
    File(&'a Path),
}

impl<'a> DataPath<'a> {
    pub fn as_path(&self) -> &'a Path {
        match self {
            DataPath::InMemory => Path::new(":memory:"),
            DataPath::File(path) => path,
        }
    }
}

impl Display for DataPath<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataPath::InMemory => write!(f, ":memory:"),
            DataPath::File(path) => f.write_fmt(format_args!("{}", path.display())),
        }
    }
}

#[derive(Clone)]
pub struct LibsqlStore {
    connection: Connection,
    sql_builder: Arc<SqlBuilder>,
}

impl LibsqlStore {
    pub async fn open(path: DataPath<'_>) -> Result<Self, anyhow::Error> {
        trace!(%path, "Opening Libsql database");

        // Not sure if we need this database object, but for now we just drop
        // it.
        let database = Builder::new_local(path.as_path())
            .build()
            .await
            .context("failed to build libSQL database object")?;

        let mut connection = database
            .connect()
            .context("failed to connect to libSQL database")?;

        Self::initialize_connection(&mut connection).await?;

        let sql_builder = Arc::new(SqlBuilder::new());

        Ok(LibsqlStore {
            connection,
            sql_builder,
        })
    }

    pub async fn in_memory() -> Result<Self, anyhow::Error> {
        Self::open(DataPath::InMemory).await
    }

    pub async fn file(db_path: &Path) -> Result<Self, anyhow::Error> {
        Self::open(DataPath::File(db_path)).await
    }

    /// This function will execute a few PRAGMA statements to set the database
    /// connection. This should run before any other queries are executed.
    async fn initialize_connection(connection: &mut Connection) -> Result<()> {
        connection
            .query(
                "PRAGMA journal_mode = WAL;
                PRAGMA busy_timeout = 5000;
                PRAGMA cache_size = 2000;
                PRAGMA foreign_keys = ON;
                PRAGMA journal_size_limit = 27103364;
                PRAGMA mmap_size = 134217728;
                PRAGMA synchronous = NORMAL;
                PRAGMA temp_store = memory;",
                (),
            )
            .await
            .map_err(|err| DbError::InternalError(err.to_string()))?;

        Ok(())
    }
}

#[async_trait]
impl Store for LibsqlStore {
    async fn start_readonly_transaction(&self) -> Result<Transaction> {
        Ok(Transaction {})
    }
    async fn start_readwrite_transaction(&self) -> Result<Transaction> {
        Ok(Transaction {})
    }

    async fn commit_transaction(&self, _tx: Transaction) -> Result<(), DbError> {
        Ok(())
    }
    async fn rollback_transaction(&self, _tx: Transaction) -> Result<(), DbError> {
        Ok(())
    }

    async fn span_get(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
        span_id: &HexEncodedId,
    ) -> Result<Span> {
        let span = self
            .connection
            .query(&self.sql_builder.span_get(), (trace_id, span_id))
            .await?
            .fetch_one()
            .await?;

        Ok(span)
    }

    async fn span_list_by_trace(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Vec<Span>> {
        let spans = self
            .connection
            .query(&self.sql_builder.span_list_by_trace(), params!(trace_id))
            .await?
            .fetch_all()
            .await?;

        Ok(spans)
    }

    async fn span_create(&self, _tx: &Transaction, span: Span) -> Result<Span> {
        let span = self
            .connection
            .query(
                &self.sql_builder.span_create(),
                params!(
                    span.trace_id,
                    span.span_id,
                    span.parent_span_id,
                    span.name,
                    span.kind,
                    span.start_time,
                    span.end_time,
                    span.tenant_id,
                    span.service_name,
                    span.inner,
                ),
            )
            .await?
            .fetch_one()
            .await?;

        Ok(span)
    }

    /// Get a list of all the traces. (currently limited to 20, sorted by most
    /// recent [`end_time`])
    ///
    /// Note that a trace is a computed value, so not all properties are
    /// present. To get all the data, use the [`Self::span_list_by_trace`] fn.
    async fn traces_list(
        &self,
        _tx: &Transaction,
        limit: Option<u32>, // Future improvement could hold sort fields, limits, etc
        time: Option<Timestamp>,
    ) -> Result<Vec<otel_worker_core::data::models::Trace>> {
        let traces = self
            .connection
            .query(&self.sql_builder.traces_list(limit, time), ())
            .await?
            .fetch_all()
            .await?;

        Ok(traces)
    }

    /// Delete all spans with a specific trace_id.
    async fn span_delete_by_trace(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Option<u64>> {
        let rows_affected = self
            .connection
            .execute(&self.sql_builder.span_delete_by_trace(), params!(trace_id))
            .await?;

        Ok(Some(rows_affected))
    }

    /// Delete a single span.
    async fn span_delete(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
        span_id: &HexEncodedId,
    ) -> Result<Option<u64>> {
        let rows_affected = self
            .connection
            .execute(&self.sql_builder.span_delete(), params!(trace_id, span_id))
            .await?;

        Ok(Some(rows_affected))
    }

    async fn log_create(
        &self,
        _tx: &Transaction,
        log: otel_worker_core::data::models::LogRecord,
    ) -> Result<otel_worker_core::data::models::LogRecord> {
        let log = self
            .connection
            .query(
                &self.sql_builder.log_create(),
                params!(
                    log.tenant_id,
                    log.service_name,
                    log.trace_id,
                    log.span_id,
                    log.severity_number,
                    log.severity_text,
                    log.body,
                    log.timestamp,
                    log.attributes,
                    log.resource_attributes
                ),
            )
            .await?
            .fetch_one()
            .await?;

        Ok(log)
    }

    async fn logs_list_by_trace(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Vec<otel_worker_core::data::models::LogRecord>> {
        let logs = self
            .connection
            .query(&self.sql_builder.logs_list_by_trace(), params!(trace_id))
            .await?
            .fetch_all()
            .await?;

        Ok(logs)
    }

    async fn logs_list(
        &self,
        _tx: &Transaction,
        limit: Option<u32>,
    ) -> Result<Vec<otel_worker_core::data::models::LogRecord>> {
        let logs = self
            .connection
            .query(&self.sql_builder.logs_list(limit), ())
            .await?
            .fetch_all()
            .await?;

        Ok(logs)
    }

    async fn metric_create(
        &self,
        _tx: &Transaction,
        sample: otel_worker_core::data::models::MetricSample,
    ) -> Result<otel_worker_core::data::models::MetricSample> {
        let sample = self
            .connection
            .query(
                &self.sql_builder.metric_create(),
                params!(
                    sample.tenant_id,
                    sample.service_name,
                    sample.metric_name,
                    sample.kind,
                    sample.timestamp,
                    sample.value,
                    sample.attributes,
                    sample.resource_attributes
                ),
            )
            .await?
            .fetch_one()
            .await?;

        Ok(sample)
    }

    async fn metrics_list(
        &self,
        _tx: &Transaction,
        limit: Option<u32>,
    ) -> Result<Vec<otel_worker_core::data::models::MetricSample>> {
        let samples = self
            .connection
            .query(&self.sql_builder.metrics_list(limit), ())
            .await?
            .fetch_all()
            .await?;

        Ok(samples)
    }

    async fn spans_service_rows(
        &self,
        _tx: &Transaction,
    ) -> Result<Vec<otel_worker_core::query::ServiceSpanRow>> {
        let rows = self
            .connection
            .query(&self.sql_builder.spans_service_rows(), ())
            .await?
            .fetch_all()
            .await?;

        Ok(rows)
    }

    async fn dashboard_upsert(
        &self,
        _tx: &Transaction,
        dashboard: otel_worker_core::data::models::DashboardRecord,
    ) -> Result<otel_worker_core::data::models::DashboardRecord> {
        let saved = self
            .connection
            .query(
                &self.sql_builder.dashboard_upsert(),
                params!(
                    dashboard.id,
                    dashboard.tenant_id,
                    dashboard.name,
                    dashboard.config,
                    dashboard.config_hash,
                    dashboard.created_at,
                    dashboard.updated_at
                ),
            )
            .await?
            .fetch_one()
            .await?;
        Ok(saved)
    }

    async fn dashboards_list(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
    ) -> Result<Vec<otel_worker_core::data::models::DashboardRecord>> {
        let rows = self
            .connection
            .query(
                &self.sql_builder.dashboards_list(),
                params!(tenant_id.to_string()),
            )
            .await?
            .fetch_all()
            .await?;
        Ok(rows)
    }

    async fn dashboard_get(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<otel_worker_core::data::models::DashboardRecord>> {
        let row = self
            .connection
            .query(
                &self.sql_builder.dashboard_get(),
                params!(tenant_id.to_string(), id.to_string()),
            )
            .await?
            .fetch_optional()
            .await?;
        Ok(row)
    }

    async fn dashboard_delete(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<u64>> {
        let rows = self
            .connection
            .execute(
                &self.sql_builder.dashboard_delete(),
                params!(tenant_id.to_string(), id.to_string()),
            )
            .await?;
        Ok(Some(rows))
    }

    async fn alert_rule_upsert(
        &self,
        _tx: &Transaction,
        rule: otel_worker_core::data::models::AlertRuleRecord,
    ) -> Result<otel_worker_core::data::models::AlertRuleRecord> {
        let saved = self
            .connection
            .query(
                &self.sql_builder.alert_rule_upsert(),
                params!(
                    rule.id,
                    rule.tenant_id,
                    rule.name,
                    rule.service_name,
                    rule.metric,
                    rule.operator,
                    rule.threshold,
                    rule.window_seconds,
                    rule.cooldown_seconds,
                    rule.webhook_url,
                    rule.enabled,
                    rule.created_at,
                    rule.updated_at,
                    rule.last_fired_at
                ),
            )
            .await?
            .fetch_one()
            .await?;
        Ok(saved)
    }

    async fn alert_rules_list(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
    ) -> Result<Vec<otel_worker_core::data::models::AlertRuleRecord>> {
        let rows = self
            .connection
            .query(
                &self.sql_builder.alert_rules_list(),
                params!(tenant_id.to_string()),
            )
            .await?
            .fetch_all()
            .await?;
        Ok(rows)
    }

    async fn alert_rule_get(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<otel_worker_core::data::models::AlertRuleRecord>> {
        let row = self
            .connection
            .query(
                &self.sql_builder.alert_rule_get(),
                params!(tenant_id.to_string(), id.to_string()),
            )
            .await?
            .fetch_optional()
            .await?;
        Ok(row)
    }

    async fn alert_rule_delete(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<u64>> {
        let rows = self
            .connection
            .execute(
                &self.sql_builder.alert_rule_delete(),
                params!(tenant_id.to_string(), id.to_string()),
            )
            .await?;
        Ok(Some(rows))
    }

    async fn alert_rule_mark_fired(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
        fired_at: Timestamp,
    ) -> Result<otel_worker_core::data::models::AlertRuleRecord> {
        let rows = self
            .connection
            .execute(
                &self.sql_builder.alert_rule_mark_fired(),
                params!(tenant_id.to_string(), id.to_string(), fired_at),
            )
            .await?;
        if rows == 0 {
            return Err(otel_worker_core::data::DbError::NotFound);
        }
        let saved = self
            .connection
            .query(
                &self.sql_builder.alert_rule_get(),
                params!(tenant_id.to_string(), id.to_string()),
            )
            .await?
            .fetch_one()
            .await?;
        Ok(saved)
    }

    async fn alert_event_create(
        &self,
        _tx: &Transaction,
        event: otel_worker_core::data::models::AlertEventRecord,
    ) -> Result<otel_worker_core::data::models::AlertEventRecord> {
        let saved = self
            .connection
            .query(
                &self.sql_builder.alert_event_create(),
                params!(
                    event.id,
                    event.rule_id,
                    event.tenant_id,
                    event.service_name,
                    event.metric,
                    event.operator,
                    event.threshold,
                    event.observed_value,
                    event.window_seconds,
                    event.fired_at,
                    event.status,
                    event.webhook_url,
                    event.delivery_status,
                    event.delivery_error
                ),
            )
            .await?
            .fetch_one()
            .await?;
        Ok(saved)
    }

    async fn alert_events_list(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        limit: Option<u32>,
    ) -> Result<Vec<otel_worker_core::data::models::AlertEventRecord>> {
        let rows = self
            .connection
            .query(
                &self.sql_builder.alert_events_list(limit),
                params!(tenant_id.to_string()),
            )
            .await?
            .fetch_all()
            .await?;
        Ok(rows)
    }

    async fn alert_event_update_delivery(
        &self,
        _tx: &Transaction,
        id: &str,
        delivery_status: &str,
        delivery_error: Option<&str>,
    ) -> Result<otel_worker_core::data::models::AlertEventRecord> {
        let rows = self
            .connection
            .execute(
                &self.sql_builder.alert_event_update_delivery(),
                params!(
                    id.to_string(),
                    delivery_status.to_string(),
                    delivery_error.map(|value| value.to_string())
                ),
            )
            .await?;
        if rows == 0 {
            return Err(otel_worker_core::data::DbError::NotFound);
        }
        let saved = self
            .connection
            .query(&self.sql_builder.alert_event_get(), params!(id.to_string()))
            .await?
            .fetch_one()
            .await?;
        Ok(saved)
    }

    async fn api_key_get(
        &self,
        key_hash: &str,
    ) -> Result<Option<otel_worker_core::data::models::ApiKey>> {
        let key = self
            .connection
            .query(
                &self.sql_builder.api_key_get(),
                params!(key_hash.to_string()),
            )
            .await?
            .fetch_optional()
            .await?;

        Ok(key)
    }
}
