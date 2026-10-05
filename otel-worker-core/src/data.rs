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
}
