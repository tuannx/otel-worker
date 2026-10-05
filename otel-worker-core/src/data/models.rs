use crate::api;
use crate::api::models::SpanKind;
use crate::data::util::{Json, Timestamp};
use serde::de::{Error, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::str::FromStr;

/// A computed value based on the span objects that are present.
#[derive(Clone, Debug, Deserialize)]
pub struct Trace {
    pub trace_id: HexEncodedId,
}

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct Span {
    pub trace_id: HexEncodedId,
    pub span_id: HexEncodedId,
    pub parent_span_id: Option<HexEncodedId>,

    pub name: String,
    pub kind: SpanKind,

    pub start_time: Timestamp,
    pub end_time: Timestamp,

    #[serde(default = "default_tenant")]
    pub tenant_id: String,
    #[serde(default = "default_service")]
    pub service_name: String,

    pub inner: Json<api::models::Span>,
}

fn default_tenant() -> String {
    "default".to_string()
}

fn default_service() -> String {
    "unknown".to_string()
}

impl Span {
    pub fn into_inner(self) -> api::models::Span {
        self.inner.into_inner()
    }

    pub fn as_inner(&self) -> &api::models::Span {
        self.inner.as_ref()
    }
}

impl From<Span> for api::models::Span {
    fn from(value: Span) -> Self {
        value.into_inner()
    }
}

impl From<api::models::Span> for Span {
    fn from(span: api::models::Span) -> Self {
        let trace_id = span.trace_id.clone();
        let span_id = span.span_id.clone();
        let parent_span_id = span.parent_span_id.clone();
        let name = span.name.clone();
        let kind = span.kind.clone().unwrap_or(SpanKind::Unspecified);
        let start_time = span.start_time.into();
        let end_time = span.end_time.into();
        let service_name = span
            .resource_attributes
            .as_ref()
            .map(|attrs| service_name_from_resource(attrs))
            .unwrap_or_else(|| "unknown".to_string());
        let inner = Json(span);

        // these .unwrap are safe as these are guaranteed to be valid as they come from the api `Span`
        Self {
            tenant_id: "default".to_string(),
            service_name,
            trace_id: HexEncodedId::new(trace_id).unwrap(),
            span_id: HexEncodedId::new(span_id).unwrap(),
            parent_span_id: parent_span_id
                .map(|parent_span_id| HexEncodedId::new(parent_span_id).unwrap()),
            name,
            kind,
            start_time,
            end_time,
            inner,
        }
    }
}

/// One stored log record (table `logs`). Attributes are kept as JSON text,
/// matching the flat record also sent to Basin (see basin/logs.sql).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct LogRecord {
    pub tenant_id: String,
    pub service_name: String,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub severity_number: i32,
    pub severity_text: String,
    pub body: String,
    pub timestamp: Timestamp,
    pub attributes: String,
    pub resource_attributes: String,
}

/// One stored metric data point (table `metric_samples`). Histograms and
/// summaries are flattened to one sample per data point (value = sum when
/// present, otherwise count); full bucket detail stays in the raw OTLP if a
/// caller keeps it — P1 is scope/count correctness, not histogram fidelity.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct MetricSample {
    pub tenant_id: String,
    pub service_name: String,
    pub metric_name: String,
    pub kind: String,
    pub timestamp: Timestamp,
    pub value: f64,
    pub attributes: String,
    pub resource_attributes: String,
}

/// Tenant API key as stored: only the SHA-256 hex of the key (column key_hash).
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ApiKey {
    pub key_hash: String,
    pub tenant_id: String,
    pub name: String,
    pub created_at: f64,
    pub revoked_at: Option<f64>,
}

pub fn service_name_from_resource(attributes: &crate::api::models::AttributeMap) -> String {
    use crate::api::models::AttributeValue;
    match attributes.0.get("service.name") {
        Some(Some(AttributeValue::StringValue(name))) if !name.is_empty() => name.clone(),
        _ => "unknown".to_string(),
    }
}

pub fn attributes_json(attributes: &crate::api::models::AttributeMap) -> String {
    serde_json::to_string(attributes).unwrap_or_else(|_| "{}".to_string())
}

fn proto_body_to_string(body: Option<opentelemetry_proto::tonic::common::v1::AnyValue>) -> String {
    use opentelemetry_proto::tonic::common::v1::any_value::Value;
    match body.and_then(|v| v.value) {
        Some(Value::StringValue(s)) => s,
        Some(Value::BoolValue(b)) => b.to_string(),
        Some(Value::IntValue(i)) => i.to_string(),
        Some(Value::DoubleValue(d)) => d.to_string(),
        Some(other) => {
            let map: crate::api::models::AttributeValue = other.into();
            serde_json::to_string(&map).unwrap_or_default()
        }
        None => String::new(),
    }
}

impl LogRecord {
    pub fn from_collector_request(
        request: opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest,
        tenant_id: &str,
    ) -> Vec<Self> {
        let mut result = vec![];
        for resource_log in request.resource_logs {
            let resource_attributes: crate::api::models::AttributeMap = resource_log
                .resource
                .map(|r| r.attributes.into())
                .unwrap_or_default();
            let service_name = service_name_from_resource(&resource_attributes);
            let resource_json = attributes_json(&resource_attributes);
            for scope_log in resource_log.scope_logs {
                for record in scope_log.log_records {
                    let nanos = if record.time_unix_nano != 0 {
                        record.time_unix_nano
                    } else {
                        record.observed_time_unix_nano
                    };
                    let timestamp = time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)
                        .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
                        .into();
                    result.push(Self {
                        tenant_id: tenant_id.to_string(),
                        service_name: service_name.clone(),
                        trace_id: if record.trace_id.is_empty() {
                            None
                        } else {
                            Some(hex::encode(&record.trace_id))
                        },
                        span_id: if record.span_id.is_empty() {
                            None
                        } else {
                            Some(hex::encode(&record.span_id))
                        },
                        severity_number: record.severity_number,
                        severity_text: record.severity_text,
                        body: proto_body_to_string(record.body),
                        timestamp,
                        attributes: attributes_json(&record.attributes.into()),
                        resource_attributes: resource_json.clone(),
                    });
                }
            }
        }
        result
    }
}

impl MetricSample {
    pub fn from_collector_request(
        request: opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest,
        tenant_id: &str,
    ) -> Vec<Self> {
        use opentelemetry_proto::tonic::metrics::v1::metric::Data;
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;

        fn ts(nanos: u64) -> Timestamp {
            time::OffsetDateTime::from_unix_timestamp_nanos(nanos as i128)
                .unwrap_or(time::OffsetDateTime::UNIX_EPOCH)
                .into()
        }

        let mut result = vec![];
        for resource_metric in request.resource_metrics {
            let resource_attributes: crate::api::models::AttributeMap = resource_metric
                .resource
                .map(|r| r.attributes.into())
                .unwrap_or_default();
            let service_name = service_name_from_resource(&resource_attributes);
            let resource_json = attributes_json(&resource_attributes);
            for scope_metric in resource_metric.scope_metrics {
                for metric in scope_metric.metrics {
                    let push =
                        |result: &mut Vec<MetricSample>,
                         kind: &str,
                         nanos: u64,
                         value: f64,
                         attrs: crate::api::models::AttributeMap| {
                            result.push(MetricSample {
                                tenant_id: tenant_id.to_string(),
                                service_name: service_name.clone(),
                                metric_name: metric.name.clone(),
                                kind: kind.to_string(),
                                timestamp: ts(nanos),
                                value,
                                attributes: attributes_json(&attrs),
                                resource_attributes: resource_json.clone(),
                            });
                        };
                    match metric.data {
                        Some(Data::Gauge(gauge)) => {
                            for point in gauge.data_points {
                                let value = match point.value {
                                    Some(NumberValue::AsDouble(d)) => d,
                                    Some(NumberValue::AsInt(i)) => i as f64,
                                    None => continue,
                                };
                                push(
                                    &mut result,
                                    "gauge",
                                    point.time_unix_nano,
                                    value,
                                    point.attributes.into(),
                                );
                            }
                        }
                        Some(Data::Sum(sum)) => {
                            for point in sum.data_points {
                                let value = match point.value {
                                    Some(NumberValue::AsDouble(d)) => d,
                                    Some(NumberValue::AsInt(i)) => i as f64,
                                    None => continue,
                                };
                                push(
                                    &mut result,
                                    "sum",
                                    point.time_unix_nano,
                                    value,
                                    point.attributes.into(),
                                );
                            }
                        }
                        Some(Data::Histogram(hist)) => {
                            for point in hist.data_points {
                                let value = point.sum.unwrap_or(point.count as f64);
                                push(
                                    &mut result,
                                    "histogram",
                                    point.time_unix_nano,
                                    value,
                                    point.attributes.into(),
                                );
                            }
                        }
                        Some(Data::ExponentialHistogram(hist)) => {
                            for point in hist.data_points {
                                let value = point.sum.unwrap_or(point.count as f64);
                                push(
                                    &mut result,
                                    "exponential_histogram",
                                    point.time_unix_nano,
                                    value,
                                    point.attributes.into(),
                                );
                            }
                        }
                        Some(Data::Summary(summary)) => {
                            for point in summary.data_points {
                                push(
                                    &mut result,
                                    "summary",
                                    point.time_unix_nano,
                                    point.count as f64,
                                    point.attributes.into(),
                                );
                            }
                        }
                        None => {}
                    }
                }
            }
        }
        result
    }
}

#[derive(Deserialize, Default)]
#[serde(default)]
pub struct TracesListQueryParams {
    pub limit: Option<u32>,

    #[serde(with = "time::serde::rfc3339::option")]
    pub time: Option<time::OffsetDateTime>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct HexEncodedId(String);

impl HexEncodedId {
    pub fn new(input: impl Into<String>) -> Result<HexEncodedId, hex::FromHexError> {
        let id = HexEncodedId(input.into());
        id.validate()?;

        Ok(id)
    }

    pub fn validate(&self) -> Result<(), hex::FromHexError> {
        hex::decode(&self.0).map(|_| ())
    }

    pub fn into_inner(self) -> String {
        self.0
    }

    pub fn as_inner(&self) -> &str {
        &self.0
    }
}

impl From<HexEncodedId> for String {
    fn from(value: HexEncodedId) -> Self {
        value.0
    }
}

impl FromStr for HexEncodedId {
    type Err = hex::FromHexError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        HexEncodedId::new(s)
    }
}

impl Deref for HexEncodedId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<Vec<u8>> for HexEncodedId {
    fn from(value: Vec<u8>) -> Self {
        // .unwrap() is safe because we literally encode it to hex in the exact same line
        Self::new(hex::encode(value)).unwrap()
    }
}

#[cfg(feature = "libsql")]
impl From<HexEncodedId> for libsql::Value {
    fn from(value: HexEncodedId) -> Self {
        value.into_inner().into()
    }
}

#[cfg(feature = "libsql")]
impl From<&HexEncodedId> for libsql::Value {
    fn from(value: &HexEncodedId) -> Self {
        value.as_inner().into()
    }
}

#[cfg(feature = "wasm-bindgen")]
impl From<HexEncodedId> for wasm_bindgen::JsValue {
    fn from(value: HexEncodedId) -> Self {
        (&value).into()
    }
}

#[cfg(feature = "wasm-bindgen")]
impl From<&HexEncodedId> for wasm_bindgen::JsValue {
    fn from(value: &HexEncodedId) -> Self {
        wasm_bindgen::JsValue::from_str(value.as_inner())
    }
}

impl Display for HexEncodedId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl AsRef<str> for HexEncodedId {
    fn as_ref(&self) -> &str {
        &self.0
    }
}

struct HexEncodedIdVisitor;

impl<'de> Visitor<'de> for HexEncodedIdVisitor {
    type Value = HexEncodedId;

    fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
        formatter.write_str("a hex-represented string")
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        HexEncodedId::new(v).map_err(Error::custom)
    }

    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        HexEncodedId::new(v).map_err(Error::custom)
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: Error,
    {
        HexEncodedId::new(v).map_err(Error::custom)
    }
}

impl<'de> Deserialize<'de> for HexEncodedId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(HexEncodedIdVisitor)
    }
}

/// Dashboard row as stored. `config` is canonical JSON text; API handlers
/// parse it into a JSON value and verify it against `config_hash`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct DashboardRecord {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub config: String,
    pub config_hash: String,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
}

/// Alert rule row as stored. Enum-like fields stay text at the SQL boundary
/// and are validated when converted to `control::AlertRule`.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AlertRuleRecord {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub service_name: Option<String>,
    pub metric: String,
    pub operator: String,
    pub threshold: f64,
    pub window_seconds: i64,
    pub cooldown_seconds: i64,
    pub webhook_url: Option<String>,
    pub enabled: i64,
    pub created_at: Timestamp,
    pub updated_at: Timestamp,
    pub last_fired_at: Option<Timestamp>,
}

/// Alert event row as stored; enum-like fields are validated at API boundary.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AlertEventRecord {
    pub id: String,
    pub rule_id: String,
    pub tenant_id: String,
    pub service_name: String,
    pub metric: String,
    pub operator: String,
    pub threshold: f64,
    pub observed_value: f64,
    pub window_seconds: i64,
    pub fired_at: Timestamp,
    pub status: String,
    pub webhook_url: Option<String>,
    pub delivery_status: String,
    pub delivery_error: Option<String>,
}

/// Price row from the P1 `model_prices` control-plane table. Rates are USD
/// per 1M tokens; optional cache rates fall back to the input rate at ingest.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ModelPrice {
    pub provider: String,
    pub model: String,
    pub input_per_mtok: f64,
    pub output_per_mtok: f64,
    pub cache_read_per_mtok: Option<f64>,
    pub cache_creation_per_mtok: Option<f64>,
    pub effective_from: Timestamp,
}

impl ModelPrice {
    pub fn price_version(&self) -> String {
        format!(
            "{}:{}@{}",
            self.provider,
            self.model,
            self.effective_from.fractional()
        )
    }
}

/// Normalized P4 GenAI projection of one span. The raw span remains in
/// `spans`; this row is the queryable/costed view used by AI APIs and MCP.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GenAiSpanRecord {
    pub tenant_id: String,
    pub trace_id: HexEncodedId,
    pub span_id: HexEncodedId,
    pub parent_span_id: Option<HexEncodedId>,
    pub service_name: String,
    pub span_name: String,
    pub operation: String,
    pub provider: String,
    pub request_model: Option<String>,
    pub response_model: Option<String>,
    pub agent_name: Option<String>,
    pub tool_name: Option<String>,
    pub conversation_id: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_creation_tokens: Option<i64>,
    pub ttft_ms: Option<f64>,
    pub duration_ms: f64,
    pub finish_reasons: String,
    pub cost_usd: Option<f64>,
    pub price_provider: Option<String>,
    pub price_model: Option<String>,
    pub price_effective_from: Option<Timestamp>,
    pub is_error: i64,
    pub start_time: Timestamp,
    pub end_time: Timestamp,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TenantAiSettings {
    pub tenant_id: String,
    pub capture_content: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct TenantAiSettingsRecord {
    pub tenant_id: String,
    pub capture_content: i64,
}

impl From<TenantAiSettingsRecord> for TenantAiSettings {
    fn from(record: TenantAiSettingsRecord) -> Self {
        Self {
            tenant_id: record.tenant_id,
            capture_content: record.capture_content != 0,
        }
    }
}

impl From<&TenantAiSettings> for TenantAiSettingsRecord {
    fn from(settings: &TenantAiSettings) -> Self {
        Self {
            tenant_id: settings.tenant_id.clone(),
            capture_content: if settings.capture_content { 1 } else { 0 },
        }
    }
}
