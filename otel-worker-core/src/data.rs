use crate::data::models::HexEncodedId;
use crate::data::util::Timestamp;
use crate::events::ServerEvents;
use async_trait::async_trait;
use std::sync::Arc;
use thiserror::Error;

pub mod models;
pub mod sql;
pub mod util;

pub type Result<T, E = DbError> = anyhow::Result<T, E>;

pub type BoxedEvents = Arc<dyn ServerEvents>;
pub type BoxedStore = Arc<dyn Store>;

#[derive(Clone, Default, Debug)]
pub struct Transaction {}

impl Transaction {
    pub fn new() -> Self {
        Self {}
    }
}

#[derive(Debug, Error)]
pub enum DbError {
    #[error("No rows were returned")]
    NotFound,

    #[error("failed to deserialize into `T`: {0}")]
    FailedDeserialize(#[from] serde::de::value::Error),

    #[error("Internal error: {0}")]
    InternalError(String),

    #[cfg(feature = "libsql")]
    #[error("Internal database error occurred: {0}")]
    LibsqlError(#[from] libsql::Error),
}

#[async_trait]
pub trait Store: Send + Sync {
    async fn start_readonly_transaction(&self) -> Result<Transaction>;
    async fn start_readwrite_transaction(&self) -> Result<Transaction>;

    async fn commit_transaction(&self, tx: Transaction) -> Result<(), DbError>;
    async fn rollback_transaction(&self, tx: Transaction) -> Result<(), DbError>;

    async fn span_get(
        &self,
        tx: &Transaction,
        trace_id: &HexEncodedId,
        span_id: &HexEncodedId,
    ) -> Result<models::Span>;

    async fn span_list_by_trace(
        &self,
        tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Vec<models::Span>>;

    async fn span_create(&self, tx: &Transaction, span: models::Span) -> Result<models::Span>;

    /// Get a list of all the traces.
    ///
    /// Note that a trace is a computed value, so not all properties are
    /// present. To get all the data, use the [`Self::span_list_by_trace`] fn.
    async fn traces_list(
        &self,
        tx: &Transaction,
        limit: Option<u32>, // Future improvement could hold sort fields, limits, etc
        time: Option<Timestamp>,
    ) -> Result<Vec<models::Trace>>;

    /// Delete all spans with a specific trace_id.
    async fn span_delete_by_trace(
        &self,
        tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Option<u64>>;

    /// Delete a single span.
    async fn span_delete(
        &self,
        tx: &Transaction,
        trace_id: &HexEncodedId,
        span_id: &HexEncodedId,
    ) -> Result<Option<u64>>;

    // --- P1: logs / metrics / api keys -------------------------------------
    // Default implementations keep any other Store impl compiling; D1 and
    // libsql stores override them.

    async fn log_create(
        &self,
        _tx: &Transaction,
        _log: models::LogRecord,
    ) -> Result<models::LogRecord> {
        Err(DbError::InternalError("log_create not implemented".into()))
    }

    async fn logs_list_by_trace(
        &self,
        _tx: &Transaction,
        _trace_id: &HexEncodedId,
    ) -> Result<Vec<models::LogRecord>> {
        Err(DbError::InternalError(
            "logs_list_by_trace not implemented".into(),
        ))
    }

    async fn logs_list(
        &self,
        _tx: &Transaction,
        _limit: Option<u32>,
    ) -> Result<Vec<models::LogRecord>> {
        Err(DbError::InternalError("logs_list not implemented".into()))
    }

    async fn metric_create(
        &self,
        _tx: &Transaction,
        _sample: models::MetricSample,
    ) -> Result<models::MetricSample> {
        Err(DbError::InternalError(
            "metric_create not implemented".into(),
        ))
    }

    async fn metrics_list(
        &self,
        _tx: &Transaction,
        _limit: Option<u32>,
    ) -> Result<Vec<models::MetricSample>> {
        Err(DbError::InternalError(
            "metrics_list not implemented".into(),
        ))
    }

    async fn api_key_get(&self, _key_hash: &str) -> Result<Option<models::ApiKey>> {
        Ok(None)
    }

    async fn spans_service_rows(
        &self,
        _tx: &Transaction,
    ) -> Result<Vec<crate::query::ServiceSpanRow>> {
        Err(DbError::InternalError(
            "spans_service_rows not implemented".into(),
        ))
    }

    // --- P3: dashboards / alerts -------------------------------------------
    // Default implementations keep other Store impls compiling; D1 and
    // libsql override them.

    async fn dashboard_upsert(
        &self,
        _tx: &Transaction,
        _dashboard: models::DashboardRecord,
    ) -> Result<models::DashboardRecord> {
        Err(DbError::InternalError(
            "dashboard_upsert not implemented".into(),
        ))
    }

    async fn dashboards_list(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
    ) -> Result<Vec<models::DashboardRecord>> {
        Err(DbError::InternalError(
            "dashboards_list not implemented".into(),
        ))
    }

    async fn dashboard_get(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _id: &str,
    ) -> Result<Option<models::DashboardRecord>> {
        Err(DbError::InternalError(
            "dashboard_get not implemented".into(),
        ))
    }

    async fn dashboard_delete(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _id: &str,
    ) -> Result<Option<u64>> {
        Err(DbError::InternalError(
            "dashboard_delete not implemented".into(),
        ))
    }

    async fn alert_rule_upsert(
        &self,
        _tx: &Transaction,
        _rule: models::AlertRuleRecord,
    ) -> Result<models::AlertRuleRecord> {
        Err(DbError::InternalError(
            "alert_rule_upsert not implemented".into(),
        ))
    }

    async fn alert_rules_list(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
    ) -> Result<Vec<models::AlertRuleRecord>> {
        Err(DbError::InternalError(
            "alert_rules_list not implemented".into(),
        ))
    }

    async fn alert_rule_get(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _id: &str,
    ) -> Result<Option<models::AlertRuleRecord>> {
        Err(DbError::InternalError(
            "alert_rule_get not implemented".into(),
        ))
    }

    async fn alert_rule_delete(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _id: &str,
    ) -> Result<Option<u64>> {
        Err(DbError::InternalError(
            "alert_rule_delete not implemented".into(),
        ))
    }

    async fn alert_rule_mark_fired(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _id: &str,
        _fired_at: Timestamp,
    ) -> Result<models::AlertRuleRecord> {
        Err(DbError::InternalError(
            "alert_rule_mark_fired not implemented".into(),
        ))
    }

    async fn alert_event_create(
        &self,
        _tx: &Transaction,
        _event: models::AlertEventRecord,
    ) -> Result<models::AlertEventRecord> {
        Err(DbError::InternalError(
            "alert_event_create not implemented".into(),
        ))
    }

    async fn alert_events_list(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _limit: Option<u32>,
    ) -> Result<Vec<models::AlertEventRecord>> {
        Err(DbError::InternalError(
            "alert_events_list not implemented".into(),
        ))
    }

    async fn alert_event_update_delivery(
        &self,
        _tx: &Transaction,
        _id: &str,
        _delivery_status: &str,
        _delivery_error: Option<&str>,
    ) -> Result<models::AlertEventRecord> {
        Err(DbError::InternalError(
            "alert_event_update_delivery not implemented".into(),
        ))
    }

    // --- P4: model prices / tenant AI settings / genai projection ----------
    // Default implementations keep other Store impls compiling; D1 and
    // libsql override them. The settings default is the privacy-safe one:
    // content capture off.

    async fn model_prices_list(&self, _tx: &Transaction) -> Result<Vec<models::ModelPrice>> {
        Err(DbError::InternalError(
            "model_prices_list not implemented".into(),
        ))
    }

    async fn model_price_upsert(
        &self,
        _tx: &Transaction,
        _price: models::ModelPrice,
    ) -> Result<models::ModelPrice> {
        Err(DbError::InternalError(
            "model_price_upsert not implemented".into(),
        ))
    }

    async fn tenant_ai_settings_get(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
    ) -> Result<models::TenantAiSettings> {
        Ok(models::TenantAiSettings {
            tenant_id: tenant_id.to_string(),
            capture_content: false,
        })
    }

    async fn tenant_ai_settings_upsert(
        &self,
        _tx: &Transaction,
        _settings: models::TenantAiSettings,
    ) -> Result<models::TenantAiSettings> {
        Err(DbError::InternalError(
            "tenant_ai_settings_upsert not implemented".into(),
        ))
    }

    async fn genai_span_create(
        &self,
        _tx: &Transaction,
        _record: models::GenAiSpanRecord,
    ) -> Result<models::GenAiSpanRecord> {
        Err(DbError::InternalError(
            "genai_span_create not implemented".into(),
        ))
    }

    async fn genai_spans_list(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _limit: Option<u32>,
    ) -> Result<Vec<models::GenAiSpanRecord>> {
        Err(DbError::InternalError(
            "genai_spans_list not implemented".into(),
        ))
    }

    async fn genai_spans_list_by_trace(
        &self,
        _tx: &Transaction,
        _tenant_id: &str,
        _trace_id: &HexEncodedId,
    ) -> Result<Vec<models::GenAiSpanRecord>> {
        Err(DbError::InternalError(
            "genai_spans_list_by_trace not implemented".into(),
        ))
    }
}
