use axum::async_trait;
use otel_worker_core::data::models::HexEncodedId;
use otel_worker_core::data::sql::SqlBuilder;
use otel_worker_core::data::util::Timestamp;
use otel_worker_core::data::{models, DbError, Result, Store, Transaction};
use serde::Deserialize;
use std::sync::Arc;
use wasm_bindgen::JsValue;
use worker::send::SendFuture;
use worker::{D1Database, D1ResultMeta};

pub struct D1Store {
    database: Arc<D1Database>,
    sql_builder: SqlBuilder,
}

impl D1Store {
    pub fn new(database: D1Database) -> Self {
        D1Store {
            database: Arc::new(database),
            sql_builder: SqlBuilder::new(),
        }
    }

    async fn fetch_one<T>(&self, query: impl Into<String>, values: &[JsValue]) -> Result<T>
    where
        T: for<'a> Deserialize<'a>,
    {
        let prepared_statement = self
            .database
            .prepare(query)
            .bind(values)
            .map_err(|err| DbError::InternalError(err.to_string()))?; // TODO: Correct error;

        let result = prepared_statement
            .first(None)
            .await
            .map_err(|err| DbError::InternalError(err.to_string()))? // TODO: Correct error;
            .ok_or(DbError::NotFound)?;

        Ok(result)
    }

    async fn fetch_all<T>(&self, query: impl Into<String>, values: &[JsValue]) -> Result<Vec<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        let prepared_statement = self
            .database
            .prepare(query)
            .bind(values)
            .map_err(|err| DbError::InternalError(err.to_string()))?; // TODO: Correct error;

        let result = prepared_statement
            .all()
            .await
            .map_err(|err| DbError::InternalError(err.to_string()))? // TODO: Correct error
            .results()
            .map_err(|err| DbError::InternalError(err.to_string()))?; // TODO: Correct error

        Ok(result)
    }

    async fn fetch_optional<T>(
        &self,
        query: impl Into<String>,
        values: &[JsValue],
    ) -> Result<Option<T>>
    where
        T: for<'a> Deserialize<'a>,
    {
        let prepared_statement = self
            .database
            .prepare(query)
            .bind(values)
            .map_err(|err| DbError::InternalError(err.to_string()))?;

        let result = prepared_statement
            .first(None)
            .await
            .map_err(|err| DbError::InternalError(err.to_string()))?;

        Ok(result)
    }

    async fn execute(&self, query: impl Into<String>, values: &[JsValue]) -> Result<Option<u64>> {
        let prepared_statement = self
            .database
            .prepare(query)
            .bind(values)
            .map_err(|err| DbError::InternalError(err.to_string()))?;

        let results = prepared_statement
            .run()
            .await
            .map_err(|err| DbError::InternalError(err.to_string()))?;

        if let Ok(Some(D1ResultMeta {
            rows_written: Some(rows_written),
            ..
        })) = results.meta()
        {
            Ok(Some(rows_written as u64))
        } else {
            Ok(None)
        }
    }
}

#[async_trait]
impl Store for D1Store {
    async fn start_readonly_transaction(&self) -> Result<Transaction> {
        // Let's fake this for now
        Ok(Transaction::default())
    }
    async fn start_readwrite_transaction(&self) -> Result<Transaction> {
        // Let's fake this for now
        Ok(Transaction::default())
    }

    async fn commit_transaction(&self, _tx: Transaction) -> Result<(), DbError> {
        // Let's fake this for now
        Ok(())
    }
    async fn rollback_transaction(&self, _tx: Transaction) -> Result<(), DbError> {
        // Let's fake this for now
        Ok(())
    }

    async fn span_get(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
        span_id: &HexEncodedId,
    ) -> Result<models::Span> {
        SendFuture::new(async {
            self.fetch_one(
                self.sql_builder.span_get(),
                &[trace_id.into(), span_id.into()],
            )
            .await
        })
        .await
    }

    async fn span_list_by_trace(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Vec<models::Span>> {
        SendFuture::new(async {
            self.fetch_all(self.sql_builder.span_list_by_trace(), &[trace_id.into()])
                .await
        })
        .await
    }

    async fn span_create(
        &self,
        _tx: &Transaction,
        span: models::Span,
    ) -> Result<models::Span, DbError> {
        SendFuture::new(async {
            let parent_span = match span.parent_span_id {
                Some(val) => val.into_inner().into(),
                None => JsValue::null(),
            };

            self.fetch_one(
                self.sql_builder.span_create(),
                &[
                    span.trace_id.into(),
                    span.span_id.into(),
                    parent_span,
                    span.name.into(),
                    span.kind.into(),
                    span.start_time.into(),
                    span.end_time.into(),
                    span.tenant_id.into(),
                    span.service_name.into(),
                    span.inner.into(),
                ],
            )
            .await
        })
        .await
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
    ) -> Result<Vec<models::Trace>> {
        SendFuture::new(async {
            let traces = self
                .fetch_all(self.sql_builder.traces_list(limit, time), &[])
                .await?;

            Ok(traces)
        })
        .await
    }

    /// Delete all spans with a specific trace_id.
    async fn span_delete_by_trace(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Option<u64>> {
        SendFuture::new(async {
            let prepared_statement = self
                .database
                .prepare(self.sql_builder.span_delete_by_trace())
                .bind(&[trace_id.into()])
                .map_err(|err| DbError::InternalError(err.to_string()))?;

            let results = prepared_statement
                .run()
                .await
                .map_err(|err| DbError::InternalError(err.to_string()))?;

            if let Ok(Some(D1ResultMeta {
                rows_written: Some(rows_written),
                ..
            })) = results.meta()
            {
                Ok(Some(rows_written as u64))
            } else {
                Ok(None)
            }
        })
        .await
    }

    /// Delete a single span.
    async fn span_delete(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
        span_id: &HexEncodedId,
    ) -> Result<Option<u64>> {
        SendFuture::new(async {
            let prepared_statement = self
                .database
                .prepare(self.sql_builder.span_delete())
                .bind(&[trace_id.into(), span_id.into()])
                .map_err(|err| DbError::InternalError(err.to_string()))?;

            let results = prepared_statement
                .run()
                .await
                .map_err(|err| DbError::InternalError(err.to_string()))?;

            if let Ok(Some(D1ResultMeta {
                rows_written: Some(rows_written),
                ..
            })) = results.meta()
            {
                Ok(Some(rows_written as u64))
            } else {
                Ok(None)
            }
        })
        .await
    }

    async fn log_create(
        &self,
        _tx: &Transaction,
        log: models::LogRecord,
    ) -> Result<models::LogRecord> {
        SendFuture::new(async {
            let trace_id = match &log.trace_id {
                Some(v) => JsValue::from_str(v),
                None => JsValue::null(),
            };
            let span_id = match &log.span_id {
                Some(v) => JsValue::from_str(v),
                None => JsValue::null(),
            };
            self.fetch_one(
                self.sql_builder.log_create(),
                &[
                    JsValue::from_str(&log.tenant_id),
                    JsValue::from_str(&log.service_name),
                    trace_id,
                    span_id,
                    JsValue::from_f64(log.severity_number as f64),
                    JsValue::from_str(&log.severity_text),
                    JsValue::from_str(&log.body),
                    log.timestamp.into(),
                    JsValue::from_str(&log.attributes),
                    JsValue::from_str(&log.resource_attributes),
                ],
            )
            .await
        })
        .await
    }

    async fn logs_list_by_trace(
        &self,
        _tx: &Transaction,
        trace_id: &HexEncodedId,
    ) -> Result<Vec<models::LogRecord>> {
        SendFuture::new(async {
            self.fetch_all(self.sql_builder.logs_list_by_trace(), &[trace_id.into()])
                .await
        })
        .await
    }

    async fn logs_list(
        &self,
        _tx: &Transaction,
        limit: Option<u32>,
    ) -> Result<Vec<models::LogRecord>> {
        SendFuture::new(async { self.fetch_all(self.sql_builder.logs_list(limit), &[]).await })
            .await
    }

    async fn metric_create(
        &self,
        _tx: &Transaction,
        sample: models::MetricSample,
    ) -> Result<models::MetricSample> {
        SendFuture::new(async {
            self.fetch_one(
                self.sql_builder.metric_create(),
                &[
                    JsValue::from_str(&sample.tenant_id),
                    JsValue::from_str(&sample.service_name),
                    JsValue::from_str(&sample.metric_name),
                    JsValue::from_str(&sample.kind),
                    sample.timestamp.into(),
                    JsValue::from_f64(sample.value),
                    JsValue::from_str(&sample.attributes),
                    JsValue::from_str(&sample.resource_attributes),
                ],
            )
            .await
        })
        .await
    }

    async fn metrics_list(
        &self,
        _tx: &Transaction,
        limit: Option<u32>,
    ) -> Result<Vec<models::MetricSample>> {
        SendFuture::new(async {
            self.fetch_all(self.sql_builder.metrics_list(limit), &[])
                .await
        })
        .await
    }

    async fn spans_service_rows(
        &self,
        _tx: &Transaction,
    ) -> Result<Vec<otel_worker_core::query::ServiceSpanRow>> {
        SendFuture::new(async {
            self.fetch_all(self.sql_builder.spans_service_rows(), &[])
                .await
        })
        .await
    }

    async fn dashboard_upsert(
        &self,
        _tx: &Transaction,
        dashboard: models::DashboardRecord,
    ) -> Result<models::DashboardRecord> {
        SendFuture::new(async {
            self.fetch_one(
                self.sql_builder.dashboard_upsert(),
                &[
                    JsValue::from_str(&dashboard.id),
                    JsValue::from_str(&dashboard.tenant_id),
                    JsValue::from_str(&dashboard.name),
                    JsValue::from_str(&dashboard.config),
                    JsValue::from_str(&dashboard.config_hash),
                    dashboard.created_at.into(),
                    dashboard.updated_at.into(),
                ],
            )
            .await
        })
        .await
    }

    async fn dashboards_list(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
    ) -> Result<Vec<models::DashboardRecord>> {
        SendFuture::new(async {
            self.fetch_all(
                self.sql_builder.dashboards_list(),
                &[JsValue::from_str(tenant_id)],
            )
            .await
        })
        .await
    }

    async fn dashboard_get(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<models::DashboardRecord>> {
        SendFuture::new(async {
            self.fetch_optional(
                self.sql_builder.dashboard_get(),
                &[JsValue::from_str(tenant_id), JsValue::from_str(id)],
            )
            .await
        })
        .await
    }

    async fn dashboard_delete(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<u64>> {
        SendFuture::new(async {
            self.execute(
                self.sql_builder.dashboard_delete(),
                &[JsValue::from_str(tenant_id), JsValue::from_str(id)],
            )
            .await
        })
        .await
    }

    async fn alert_rule_upsert(
        &self,
        _tx: &Transaction,
        rule: models::AlertRuleRecord,
    ) -> Result<models::AlertRuleRecord> {
        SendFuture::new(async {
            let service_name = match &rule.service_name {
                Some(value) => JsValue::from_str(value),
                None => JsValue::null(),
            };
            let webhook_url = match &rule.webhook_url {
                Some(value) => JsValue::from_str(value),
                None => JsValue::null(),
            };
            let last_fired_at = match rule.last_fired_at {
                Some(value) => value.into(),
                None => JsValue::null(),
            };
            self.fetch_one(
                self.sql_builder.alert_rule_upsert(),
                &[
                    JsValue::from_str(&rule.id),
                    JsValue::from_str(&rule.tenant_id),
                    JsValue::from_str(&rule.name),
                    service_name,
                    JsValue::from_str(&rule.metric),
                    JsValue::from_str(&rule.operator),
                    JsValue::from_f64(rule.threshold),
                    JsValue::from_f64(rule.window_seconds as f64),
                    JsValue::from_f64(rule.cooldown_seconds as f64),
                    webhook_url,
                    JsValue::from_f64(rule.enabled as f64),
                    rule.created_at.into(),
                    rule.updated_at.into(),
                    last_fired_at,
                ],
            )
            .await
        })
        .await
    }

    async fn alert_rules_list(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
    ) -> Result<Vec<models::AlertRuleRecord>> {
        SendFuture::new(async {
            self.fetch_all(
                self.sql_builder.alert_rules_list(),
                &[JsValue::from_str(tenant_id)],
            )
            .await
        })
        .await
    }

    async fn alert_rule_get(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<models::AlertRuleRecord>> {
        SendFuture::new(async {
            self.fetch_optional(
                self.sql_builder.alert_rule_get(),
                &[JsValue::from_str(tenant_id), JsValue::from_str(id)],
            )
            .await
        })
        .await
    }

    async fn alert_rule_delete(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
    ) -> Result<Option<u64>> {
        SendFuture::new(async {
            self.execute(
                self.sql_builder.alert_rule_delete(),
                &[JsValue::from_str(tenant_id), JsValue::from_str(id)],
            )
            .await
        })
        .await
    }

    async fn alert_rule_mark_fired(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        id: &str,
        fired_at: Timestamp,
    ) -> Result<models::AlertRuleRecord> {
        SendFuture::new(async {
            let rows = self
                .execute(
                    self.sql_builder.alert_rule_mark_fired(),
                    &[
                        JsValue::from_str(tenant_id),
                        JsValue::from_str(id),
                        fired_at.into(),
                    ],
                )
                .await?;
            if rows.unwrap_or(0) == 0 {
                return Err(DbError::NotFound);
            }
            self.fetch_optional(
                self.sql_builder.alert_rule_get(),
                &[JsValue::from_str(tenant_id), JsValue::from_str(id)],
            )
            .await?
            .ok_or(DbError::NotFound)
        })
        .await
    }

    async fn alert_event_create(
        &self,
        _tx: &Transaction,
        event: models::AlertEventRecord,
    ) -> Result<models::AlertEventRecord> {
        SendFuture::new(async {
            let webhook_url = match &event.webhook_url {
                Some(value) => JsValue::from_str(value),
                None => JsValue::null(),
            };
            let delivery_error = match &event.delivery_error {
                Some(value) => JsValue::from_str(value),
                None => JsValue::null(),
            };
            self.fetch_one(
                self.sql_builder.alert_event_create(),
                &[
                    JsValue::from_str(&event.id),
                    JsValue::from_str(&event.rule_id),
                    JsValue::from_str(&event.tenant_id),
                    JsValue::from_str(&event.service_name),
                    JsValue::from_str(&event.metric),
                    JsValue::from_str(&event.operator),
                    JsValue::from_f64(event.threshold),
                    JsValue::from_f64(event.observed_value),
                    JsValue::from_f64(event.window_seconds as f64),
                    event.fired_at.into(),
                    JsValue::from_str(&event.status),
                    webhook_url,
                    JsValue::from_str(&event.delivery_status),
                    delivery_error,
                ],
            )
            .await
        })
        .await
    }

    async fn alert_events_list(
        &self,
        _tx: &Transaction,
        tenant_id: &str,
        limit: Option<u32>,
    ) -> Result<Vec<models::AlertEventRecord>> {
        SendFuture::new(async {
            self.fetch_all(
                self.sql_builder.alert_events_list(limit),
                &[JsValue::from_str(tenant_id)],
            )
            .await
        })
        .await
    }

    async fn alert_event_update_delivery(
        &self,
        _tx: &Transaction,
        id: &str,
        delivery_status: &str,
        delivery_error: Option<&str>,
    ) -> Result<models::AlertEventRecord> {
        SendFuture::new(async {
            let delivery_error_value = match delivery_error {
                Some(value) => JsValue::from_str(value),
                None => JsValue::null(),
            };
            let rows = self
                .execute(
                    self.sql_builder.alert_event_update_delivery(),
                    &[
                        JsValue::from_str(id),
                        JsValue::from_str(delivery_status),
                        delivery_error_value,
                    ],
                )
                .await?;
            if rows.unwrap_or(0) == 0 {
                return Err(DbError::NotFound);
            }
            self.fetch_optional(self.sql_builder.alert_event_get(), &[JsValue::from_str(id)])
                .await?
                .ok_or(DbError::NotFound)
        })
        .await
    }

    async fn api_key_get(&self, key_hash: &str) -> Result<Option<models::ApiKey>> {
        SendFuture::new(async {
            let keys: Vec<models::ApiKey> = self
                .fetch_all(
                    self.sql_builder.api_key_get(),
                    &[JsValue::from_str(key_hash)],
                )
                .await?;
            Ok(keys.into_iter().next())
        })
        .await
    }
}
