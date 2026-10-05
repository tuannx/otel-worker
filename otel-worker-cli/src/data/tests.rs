use crate::data::LibsqlStore;
use otel_worker_core::api::models::{AttributeMap, SpanKind};
use otel_worker_core::data::models::{HexEncodedId, Span};
use otel_worker_core::data::Store;
use test_log::test;

/// Tests creating a span and then retrieving it using the various methods.
///
/// This test exercises the following methods:
/// - span_create()
/// - span_get()
/// - span_list_by_trace()
#[test(tokio::test)]
async fn span_successful() {
    // create store
    let store = create_test_store().await;

    // - Create span
    let tx = store
        .start_readwrite_transaction()
        .await
        .expect("unable to create transaction");

    let trace_id = HexEncodedId::new("2b76e003e3cff12e054bcd0ca6879ee4").unwrap();
    let span_id = HexEncodedId::new("a6c0ed7c2f81e7c8").unwrap();

    let now = time::OffsetDateTime::now_utc();
    let inner_span = otel_worker_core::api::models::Span {
        trace_id: trace_id.clone(),
        span_id: span_id.clone(),
        parent_span_id: None,
        name: String::from("Test span"),
        kind: Some(SpanKind::Internal),
        start_time: now,
        end_time: now,
        trace_state: Some(String::new()),
        flags: Some(0),
        scope_name: None,
        scope_version: None,
        attributes: AttributeMap::default(),
        scope_attributes: None,
        resource_attributes: None,
        status: None,
        events: vec![],
        links: vec![],
    };
    let span: Span = inner_span.into();
    let saved_span = store
        .span_create(&tx, span.clone())
        .await
        .expect("unable to create span");

    // We only are interested in the inner span, since the db model will have
    // slightly different start/end times (due to float conversion).
    assert_eq!(span.as_inner(), saved_span.as_inner());

    store
        .commit_transaction(tx)
        .await
        .expect("unable to commit transaction");

    let tx = store
        .start_readwrite_transaction()
        .await
        .expect("unable to create transaction");

    // Get previously created span
    let retrieved_span = store
        .span_get(&tx, &trace_id, &span_id)
        .await
        .expect("unable to get span");

    // Here we can assert the whole DB object, since both came from the database.
    assert_eq!(saved_span, retrieved_span);

    // List spans by trace
    let retrieved_spans = store
        .span_list_by_trace(&tx, &trace_id)
        .await
        .expect("unable to get spans");

    assert_eq!(retrieved_spans.len(), 1);
    assert_eq!(retrieved_spans[0], saved_span);

    store
        .rollback_transaction(tx)
        .await
        .expect("unable to rollback transaction");
}

/// P1: logs + metrics round-trip through the same Store trait the Worker uses.
#[test(tokio::test)]
async fn logs_and_metrics_successful() {
    use otel_worker_core::data::models::{LogRecord, MetricSample};
    use otel_worker_core::data::util::Timestamp;

    let store = create_test_store().await;
    let tx = store
        .start_readwrite_transaction()
        .await
        .expect("unable to create transaction");

    let trace_id = HexEncodedId::new("2b76e003e3cff12e054bcd0ca6879ee4").unwrap();
    let now: Timestamp = time::OffsetDateTime::now_utc().into();

    let log = LogRecord {
        tenant_id: "default".to_string(),
        service_name: "checkout".to_string(),
        trace_id: Some(trace_id.as_inner().to_string()),
        span_id: None,
        severity_number: 17,
        severity_text: "ERROR".to_string(),
        body: "payment failed".to_string(),
        timestamp: now,
        attributes: "{}".to_string(),
        resource_attributes: "{}".to_string(),
    };
    let saved_log = store
        .log_create(&tx, log.clone())
        .await
        .expect("unable to create log");
    assert_eq!(saved_log.body, "payment failed");
    assert_eq!(saved_log.severity_number, 17);

    let by_trace = store
        .logs_list_by_trace(&tx, &trace_id)
        .await
        .expect("unable to list logs by trace");
    assert_eq!(by_trace.len(), 1);
    assert_eq!(by_trace[0].service_name, "checkout");

    let all_logs = store
        .logs_list(&tx, Some(10))
        .await
        .expect("unable to list logs");
    assert_eq!(all_logs.len(), 1);

    let sample = MetricSample {
        tenant_id: "default".to_string(),
        service_name: "checkout".to_string(),
        metric_name: "http.server.duration".to_string(),
        kind: "histogram".to_string(),
        timestamp: now,
        value: 42.5,
        attributes: "{}".to_string(),
        resource_attributes: "{}".to_string(),
    };
    let saved_sample = store
        .metric_create(&tx, sample)
        .await
        .expect("unable to create metric sample");
    assert_eq!(saved_sample.value, 42.5);

    let samples = store
        .metrics_list(&tx, Some(10))
        .await
        .expect("unable to list metrics");
    assert_eq!(samples.len(), 1);
    assert_eq!(samples[0].metric_name, "http.server.duration");

    // Unknown API key resolves to None (worker auth then returns 401).
    let missing = store
        .api_key_get("deadbeef")
        .await
        .expect("unable to look up api key");
    assert!(missing.is_none());

    store
        .commit_transaction(tx)
        .await
        .expect("unable to commit transaction");
}

/// P1: proto -> model conversion counts (fixture-level gate, no DB).
#[test]
fn logs_metrics_from_proto_counts() {
    use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
    use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
    use otel_worker_core::data::models::{LogRecord, MetricSample};

    let logs_json = include_str!("fixtures/logs.json");
    let request: ExportLogsServiceRequest =
        serde_json::from_str(logs_json).expect("logs fixture must parse");
    let logs = LogRecord::from_collector_request(request, "default");
    assert_eq!(logs.len(), 2);
    assert_eq!(logs[0].service_name, "checkout");
    assert_eq!(
        logs[0].trace_id.as_deref(),
        Some("2b76e003e3cff12e054bcd0ca6879ee4")
    );

    let metrics_json = include_str!("fixtures/metrics.json");
    let request: ExportMetricsServiceRequest =
        serde_json::from_str(metrics_json).expect("metrics fixture must parse");
    let samples = MetricSample::from_collector_request(request, "default");
    assert_eq!(samples.len(), 2);
    assert!(samples.iter().any(|s| s.kind == "gauge" && s.value == 7.0));
    assert!(samples.iter().any(|s| s.kind == "histogram"));
}

/// P2: services summary from stored spans (percentiles computed in core).
#[test(tokio::test)]
async fn services_summary_from_spans() {
    use otel_worker_core::query::summarize_services;

    let store = create_test_store().await;
    let tx = store
        .start_readwrite_transaction()
        .await
        .expect("unable to create transaction");

    let trace_id = HexEncodedId::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    let now = time::OffsetDateTime::now_utc();
    for i in 0..4 {
        let span_id = HexEncodedId::new(format!("{i:016x}")).unwrap();
        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert(
            "service.name".to_string(),
            Some(otel_worker_core::api::models::AttributeValue::StringValue(
                "checkout".to_string(),
            )),
        );
        let inner_span = otel_worker_core::api::models::Span {
            trace_id: trace_id.clone(),
            span_id,
            parent_span_id: None,
            name: "GET /pay".to_string(),
            kind: Some(SpanKind::Server),
            start_time: now,
            end_time: now + time::Duration::milliseconds((i as i64 + 1) * 10),
            trace_state: Some(String::new()),
            flags: Some(0),
            scope_name: None,
            scope_version: None,
            attributes: AttributeMap::default(),
            scope_attributes: None,
            resource_attributes: Some(otel_worker_core::api::models::AttributeMap(attrs)),
            status: None,
            events: vec![],
            links: vec![],
        };
        let span: Span = inner_span.into();
        assert_eq!(span.service_name, "checkout");
        store
            .span_create(&tx, span)
            .await
            .expect("unable to create span");
    }

    let rows = store
        .spans_service_rows(&tx)
        .await
        .expect("unable to read service rows");
    assert_eq!(rows.len(), 4);

    let services = summarize_services(&rows);
    assert_eq!(services.len(), 1);
    assert_eq!(services[0].service_name, "checkout");
    assert_eq!(services[0].span_count, 4);
    assert_eq!(services[0].error_count, 0);
    // durations 10,20,30,40 ms -> p50 = 25 (linear interpolation, pinned in query.rs)
    assert!(
        (services[0].p50_ms - 25.0).abs() < 0.01,
        "p50={}",
        services[0].p50_ms
    );
    assert!((services[0].avg_ms - 25.0).abs() < 0.01);
}

/// P3: dashboards are idempotently imported and alerts fire at most once
/// inside a pinned cooldown window.
#[test(tokio::test)]
async fn p3_dashboards_alerts_and_service_map() {
    use otel_worker_core::alerting::evaluate_alerts;
    use otel_worker_core::control::{build_service_map, dashboard_config_hash, AlertRule};
    use otel_worker_core::data::models::{AlertRuleRecord, DashboardRecord};
    use otel_worker_core::data::util::Timestamp;

    let store = create_test_store().await;
    let tx = store
        .start_readwrite_transaction()
        .await
        .expect("unable to create transaction");
    let now: Timestamp = time::OffsetDateTime::now_utc().into();

    let config: serde_json::Value =
        serde_json::from_str(r#"{"version":1,"panels":[{"type":"services"}]}"#).unwrap();
    let config_text = otel_worker_core::control::canonical_json(&config);
    let dashboard = DashboardRecord {
        id: "dashboard-overview".to_string(),
        tenant_id: "default".to_string(),
        name: "Overview".to_string(),
        config: config_text,
        config_hash: dashboard_config_hash(&config),
        created_at: now,
        updated_at: now,
    };
    let saved = store
        .dashboard_upsert(&tx, dashboard.clone())
        .await
        .expect("unable to save dashboard");
    assert_eq!(saved.config_hash, dashboard.config_hash);

    // Re-importing the same id + config yields the same persisted hash.
    let saved_again = store
        .dashboard_upsert(&tx, dashboard)
        .await
        .expect("unable to re-save dashboard");
    assert_eq!(saved_again.config_hash, saved.config_hash);

    let dashboards = store
        .dashboards_list(&tx, "default")
        .await
        .expect("unable to list dashboards");
    assert_eq!(dashboards.len(), 1);

    // One web root and one erroring checkout child, inside the alert window.
    let trace_id = HexEncodedId::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    let root_id = HexEncodedId::new("0000000000000001").unwrap();
    let child_id = HexEncodedId::new("0000000000000002").unwrap();
    let start = time::OffsetDateTime::now_utc() - time::Duration::seconds(10);
    for (span_id, parent_span_id, service, name, error) in [
        (root_id.clone(), None, "web", "GET /", false),
        (
            child_id.clone(),
            Some(root_id.clone()),
            "checkout",
            "POST /pay",
            true,
        ),
    ] {
        let mut attrs = std::collections::BTreeMap::new();
        attrs.insert(
            "service.name".to_string(),
            Some(otel_worker_core::api::models::AttributeValue::StringValue(
                service.to_string(),
            )),
        );
        let inner_span = otel_worker_core::api::models::Span {
            trace_id: trace_id.clone(),
            span_id,
            parent_span_id,
            name: name.to_string(),
            kind: Some(SpanKind::Server),
            start_time: start,
            end_time: start + time::Duration::milliseconds(50),
            trace_state: Some(String::new()),
            flags: Some(0),
            scope_name: None,
            scope_version: None,
            attributes: AttributeMap::default(),
            scope_attributes: None,
            resource_attributes: Some(otel_worker_core::api::models::AttributeMap(attrs)),
            status: Some(opentelemetry_proto::tonic::trace::v1::Status {
                message: if error {
                    "failed".to_string()
                } else {
                    String::new()
                },
                code: if error { 2 } else { 1 },
            }),
            events: vec![],
            links: vec![],
        };
        let span: Span = inner_span.into();
        assert_eq!(span.service_name, service);
        store
            .span_create(&tx, span)
            .await
            .expect("unable to create P3 span");
    }

    // The stored `inner` JSON determines `is_error`; exercise the real service
    // map projection rather than hand-built rows.
    let rows = store
        .spans_service_rows(&tx)
        .await
        .expect("unable to read P3 service rows");
    assert_eq!(rows.len(), 2);
    let map = build_service_map(&rows);
    assert_eq!(map.nodes.len(), 2);
    assert_eq!(map.edges.len(), 1);
    assert_eq!(map.edges[0].source, "web");
    assert_eq!(map.edges[0].target, "checkout");
    assert_eq!(rows[1].is_error, 1);

    let rule = AlertRule {
        id: "alert-checkout-errors".to_string(),
        tenant_id: "default".to_string(),
        name: "Checkout errors".to_string(),
        service_name: Some("checkout".to_string()),
        metric: otel_worker_core::control::AlertMetric::ErrorRate,
        operator: otel_worker_core::control::ComparisonOperator::Gt,
        threshold: 0.05,
        window_seconds: 300,
        cooldown_seconds: 600,
        webhook_url: None,
        enabled: true,
        created_at: now,
        updated_at: now,
        last_fired_at: None,
    };
    store
        .alert_rule_upsert(&tx, AlertRuleRecord::from(&rule))
        .await
        .expect("unable to save alert rule");
    store
        .commit_transaction(tx)
        .await
        .expect("unable to commit P3 test data");

    let boxed_store: otel_worker_core::data::BoxedStore = std::sync::Arc::new(store.clone());
    // The checkout span is an error, so error_rate is exactly 1.0 and the
    // rule threshold (> 5%) must fire.
    let first = evaluate_alerts(&boxed_store, "default", Timestamp::now())
        .await
        .expect("first alert evaluation failed");
    let first_events: Vec<_> = first.iter().filter(|result| result.fired).collect();

    let second = evaluate_alerts(&boxed_store, "default", Timestamp::now())
        .await
        .expect("second alert evaluation failed");
    let second_fired = second.iter().filter(|result| result.fired).count();

    let tx = store
        .start_readonly_transaction()
        .await
        .expect("unable to create read transaction");
    let events = store
        .alert_events_list(&tx, "default", Some(10))
        .await
        .expect("unable to list alert events");
    // Cooldown contract: evaluating twice in the same window creates exactly
    // one persisted event for this rule; the second evaluation is suppressed.
    assert_eq!(first_events.len(), 1);
    assert_eq!(first_events[0].observed_value, Some(1.0));
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].suppressed_reason.as_deref(), Some("cooldown"));
    assert_eq!(second_fired, 0);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].rule_id, "alert-checkout-errors");
    assert_eq!(events[0].observed_value, 1.0);
}

pub async fn create_test_store() -> LibsqlStore {
    let store = LibsqlStore::in_memory()
        .await
        .expect("unable to create test store");

    store.migrate().await.expect("unable to run migrations");

    store
}

struct NoopEvents;

#[async_trait::async_trait]
impl otel_worker_core::events::ServerEvents for NoopEvents {
    async fn broadcast(&self, _msg: otel_worker_core::api::models::ServerMessage) {}
}

const P4_T0_SECONDS: i64 = 1_759_276_800;

fn kv_str(key: &str, value: &str) -> opentelemetry_proto::tonic::common::v1::KeyValue {
    use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::StringValue(value.to_string())),
        }),
    }
}

fn kv_int(key: &str, value: i64) -> opentelemetry_proto::tonic::common::v1::KeyValue {
    use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::IntValue(value)),
        }),
    }
}

fn kv_double(key: &str, value: f64) -> opentelemetry_proto::tonic::common::v1::KeyValue {
    use opentelemetry_proto::tonic::common::v1::{any_value::Value, AnyValue, KeyValue};
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(Value::DoubleValue(value)),
        }),
    }
}

fn p4_span(
    trace: &[u8; 16],
    base_seconds: i64,
    span_id: u64,
    parent: Option<u64>,
    name: &str,
    start_offset_ms: u64,
    duration_ms: u64,
    attributes: Vec<opentelemetry_proto::tonic::common::v1::KeyValue>,
    is_error: bool,
) -> opentelemetry_proto::tonic::trace::v1::Span {
    use opentelemetry_proto::tonic::trace::v1::{Span, Status};
    let start_ns = (base_seconds as u64) * 1_000_000_000 + start_offset_ms * 1_000_000;
    Span {
        trace_id: trace.to_vec(),
        span_id: span_id.to_be_bytes().to_vec(),
        trace_state: String::new(),
        parent_span_id: parent
            .map(|id| id.to_be_bytes().to_vec())
            .unwrap_or_default(),
        flags: 0,
        name: name.to_string(),
        kind: 1,
        start_time_unix_nano: start_ns,
        end_time_unix_nano: start_ns + duration_ms * 1_000_000,
        attributes,
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status: Some(Status {
            message: String::new(),
            code: if is_error { 2 } else { 1 },
        }),
    }
}

fn p4_request(
    trace: &[u8; 16],
    start_offset_seconds: i64,
    with_content: bool,
) -> opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest {
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use opentelemetry_proto::tonic::common::v1::InstrumentationScope;
    use opentelemetry_proto::tonic::resource::v1::Resource;
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans};

    let base_seconds = P4_T0_SECONDS + start_offset_seconds;

    let mut chat1_attrs = vec![
        kv_str("gen_ai.operation.name", "chat"),
        kv_str("gen_ai.provider.name", "openai"),
        kv_str("gen_ai.request.model", "gpt-x"),
        kv_str("gen_ai.response.model", "gpt-x"),
        kv_int("gen_ai.usage.input_tokens", 1000),
        kv_int("gen_ai.usage.output_tokens", 500),
        kv_int("gen_ai.usage.cache_read.input_tokens", 200),
        kv_double("gen_ai.server.time_to_first_token", 0.12),
        kv_str("gen_ai.response.finish_reasons", "stop"),
    ];
    if with_content {
        chat1_attrs.push(kv_str(
            "gen_ai.input.messages",
            "[{\"role\":\"user\",\"content\":\"leak sk-FAKEKEY1234567890 please\"}]",
        ));
    }

    let spans = vec![
        p4_span(
            trace,
            base_seconds,
            1,
            None,
            "invoke_agent",
            0,
            1000,
            vec![
                kv_str("gen_ai.operation.name", "invoke_agent"),
                kv_str("gen_ai.agent.name", "research-agent"),
                kv_str("gen_ai.conversation.id", "conv-1"),
            ],
            false,
        ),
        p4_span(
            trace,
            base_seconds,
            2,
            Some(1),
            "chat",
            10,
            400,
            chat1_attrs,
            false,
        ),
        p4_span(
            trace,
            base_seconds,
            3,
            Some(2),
            "execute_tool",
            50,
            120,
            vec![
                kv_str("gen_ai.operation.name", "execute_tool"),
                kv_str("gen_ai.tool.name", "web_search"),
            ],
            true,
        ),
        p4_span(
            trace,
            base_seconds,
            4,
            Some(1),
            "chat",
            500,
            300,
            vec![
                // Legacy alias path: provider + token names from older libs.
                kv_str("gen_ai.system", "openai"),
                kv_str("gen_ai.request.model", "gpt-x"),
                kv_int("gen_ai.usage.prompt_tokens", 2000),
                kv_int("gen_ai.usage.completion_tokens", 1000),
            ],
            false,
        ),
    ];

    ExportTraceServiceRequest {
        resource_spans: vec![ResourceSpans {
            resource: Some(Resource {
                attributes: vec![kv_str("service.name", "agent-service")],
                dropped_attributes_count: 0,
            }),
            scope_spans: vec![ScopeSpans {
                scope: Some(InstrumentationScope {
                    name: "p4-fixture".to_string(),
                    version: "1".to_string(),
                    attributes: vec![],
                    dropped_attributes_count: 0,
                }),
                spans,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// P4 gate (docs/AI-TRACE.md §5), exercised through the real ingest path
/// (Service -> libsql Store), the same handlers' projections, and the sink
/// record shapes that would be published to Basin:
/// 1. cost == tokens x price, exactly by formula;
/// 2. fake API key in prompt content appears in no store or sink record;
/// 3. replay tree + total cost from the shared projection;
/// 4. a price change applies to new spans only; old spans keep price_version.
#[test(tokio::test)]
async fn p4_genai_cost_privacy_and_replay() {
    use otel_worker_core::data::models::{ModelPrice, TenantAiSettings};
    use otel_worker_core::data::util::Timestamp;
    use otel_worker_core::genai::{
        build_agent_run, search_spans, summarize_overview, AiSearchFilter,
    };
    use otel_worker_core::service::Service;

    let store = create_test_store().await;

    // Price v1 in force before the first ingest.
    let price_v1 = ModelPrice {
        provider: "openai".to_string(),
        model: "gpt-x".to_string(),
        input_per_mtok: 2.0,
        output_per_mtok: 8.0,
        cache_read_per_mtok: Some(0.5),
        cache_creation_per_mtok: None,
        effective_from: Timestamp::try_from((P4_T0_SECONDS - 86_400) as f64).unwrap(),
    };
    let tx = store.start_readwrite_transaction().await.unwrap();
    store.model_price_upsert(&tx, price_v1).await.unwrap();
    store.commit_transaction(tx).await.unwrap();

    let boxed_store: otel_worker_core::data::BoxedStore = std::sync::Arc::new(store.clone());
    let service = Service::new(boxed_store.clone(), std::sync::Arc::new(NoopEvents));

    let trace1 = [0xaau8; 16];
    let trace1_id = HexEncodedId::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap();
    service
        .ingest_traces(p4_request(&trace1, 0, true), "default")
        .await
        .expect("first P4 ingest failed");

    let tx = store.start_readonly_transaction().await.unwrap();
    let records = store
        .genai_spans_list_by_trace(&tx, "default", &trace1_id)
        .await
        .expect("unable to list genai spans");
    assert_eq!(records.len(), 4, "invoke_agent + 2 chat + 1 tool expected");

    let by_span = |id: u64| {
        records
            .iter()
            .find(|r| r.span_id.as_inner() == format!("{id:016x}"))
            .unwrap_or_else(|| panic!("span {id} missing from projection"))
    };

    // (1) exact cost math: (in*2 + out*8 + cache_read*0.5) / 1e6
    let chat1_cost = (1000.0 * 2.0 + 500.0 * 8.0 + 200.0 * 0.5) / 1_000_000.0;
    let chat2_cost = (2000.0 * 2.0 + 1000.0 * 8.0) / 1_000_000.0;
    assert_eq!(by_span(2).cost_usd, Some(chat1_cost));
    assert_eq!(by_span(4).cost_usd, Some(chat2_cost));
    assert_eq!(
        by_span(2).price_effective_from.map(|t| t.fractional()),
        Some((P4_T0_SECONDS - 86_400) as f64)
    );
    assert_eq!(by_span(2).ttft_ms, Some(120.0));
    assert_eq!(by_span(4).provider, "openai", "legacy gen_ai.system alias");
    assert_eq!(by_span(3).operation, "execute_tool");
    assert_eq!(by_span(3).is_error, 1);
    assert_eq!(by_span(3).cost_usd, None);

    // (3) replay: tree depth + totals from the shared projection.
    let run = build_agent_run("default", trace1_id.as_inner(), &records).unwrap();
    assert_eq!(run.agent_name.as_deref(), Some("research-agent"));
    assert_eq!(run.span_count, 4);
    assert_eq!(run.total_cost_usd, chat1_cost + chat2_cost);
    let depths: Vec<u32> = run.spans.iter().map(|s| s.depth).collect();
    assert_eq!(depths, vec![0, 1, 2, 1]);
    assert_eq!(
        run.spans[1].price_version.as_deref(),
        Some(format!("openai:gpt-x@{}", (P4_T0_SECONDS - 86_400) as f64).as_str())
    );

    // (2) privacy: the fake key is in no stored span, projection row, API
    // view, or Basin-bound sink record (capture_content defaults to false).
    let stored_spans = store
        .span_list_by_trace(&tx, &trace1_id)
        .await
        .expect("unable to list stored spans");
    assert_eq!(stored_spans.len(), 4);
    for span in &stored_spans {
        let inner = serde_json::to_string(span.as_inner()).unwrap();
        assert!(
            !inner.contains("FAKEKEY"),
            "span inner leaked content: {inner}"
        );
        let basin = serde_json::to_string(&otel_worker_core::sink::span_record(
            span.as_inner(),
            "default",
        ))
        .unwrap();
        assert!(!basin.contains("FAKEKEY"), "basin span leaked: {basin}");
    }
    for record in &records {
        let basin =
            serde_json::to_string(&otel_worker_core::sink::genai_span_record(record)).unwrap();
        assert!(!basin.contains("FAKEKEY"), "basin genai leaked: {basin}");
    }
    let errors = search_spans(
        &records,
        &AiSearchFilter {
            has_error: Some(true),
            ..Default::default()
        },
    );
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].tool_name.as_deref(), Some("web_search"));

    // Overview totals: both chat spans priced under v1.
    let overview = summarize_overview(&records);
    assert_eq!(overview.totals.cost_usd, chat1_cost + chat2_cost);
    let gpt = overview
        .by_model
        .iter()
        .find(|m| m.model == "gpt-x")
        .expect("gpt-x model group missing");
    assert_eq!(gpt.cost_usd, chat1_cost + chat2_cost);
    assert_eq!(gpt.span_count, 2);
    assert_eq!(gpt.priced_span_count, 2);

    // (4) price change: spans starting after v2 takes effect use v2; the
    // already-stored spans keep their v1 cost and price_version.
    let price_v2 = ModelPrice {
        provider: "openai".to_string(),
        model: "gpt-x".to_string(),
        input_per_mtok: 4.0,
        output_per_mtok: 16.0,
        cache_read_per_mtok: Some(1.0),
        cache_creation_per_mtok: None,
        effective_from: Timestamp::try_from((P4_T0_SECONDS + 86_400) as f64).unwrap(),
    };
    let tx = store.start_readwrite_transaction().await.unwrap();
    store.model_price_upsert(&tx, price_v2).await.unwrap();
    store.commit_transaction(tx).await.unwrap();

    let trace2 = [0xbbu8; 16];
    let trace2_id = HexEncodedId::new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").unwrap();
    service
        .ingest_traces(p4_request(&trace2, 2 * 86_400, false), "default")
        .await
        .expect("second P4 ingest failed");

    let tx = store.start_readonly_transaction().await.unwrap();
    let records2 = store
        .genai_spans_list_by_trace(&tx, "default", &trace2_id)
        .await
        .expect("unable to list trace2 genai spans");
    let chat1_v2_cost = (1000.0 * 4.0 + 500.0 * 16.0 + 200.0 * 1.0) / 1_000_000.0;
    let new_chat1 = records2
        .iter()
        .find(|r| r.span_id.as_inner() == "0000000000000002")
        .unwrap();
    assert_eq!(new_chat1.cost_usd, Some(chat1_v2_cost));
    assert_eq!(
        new_chat1.price_effective_from.map(|t| t.fractional()),
        Some((P4_T0_SECONDS + 86_400) as f64)
    );

    // Old spans are untouched: still priced and versioned by v1.
    let records1_after = store
        .genai_spans_list_by_trace(&tx, "default", &trace1_id)
        .await
        .expect("unable to re-list trace1 genai spans");
    let old_chat1 = records1_after
        .iter()
        .find(|r| r.span_id.as_inner() == "0000000000000002")
        .unwrap();
    assert_eq!(old_chat1.cost_usd, Some(chat1_cost));
    assert_eq!(
        old_chat1.price_effective_from.map(|t| t.fractional()),
        Some((P4_T0_SECONDS - 86_400) as f64)
    );

    // Opt-in capture: content is stored only redacted.
    let tx = store.start_readwrite_transaction().await.unwrap();
    store
        .tenant_ai_settings_upsert(
            &tx,
            TenantAiSettings {
                tenant_id: "default".to_string(),
                capture_content: true,
            },
        )
        .await
        .unwrap();
    store.commit_transaction(tx).await.unwrap();

    let trace3 = [0xccu8; 16];
    let trace3_id = HexEncodedId::new("cccccccccccccccccccccccccccccccc").unwrap();
    service
        .ingest_traces(p4_request(&trace3, 0, true), "default")
        .await
        .expect("third P4 ingest failed");
    let tx = store.start_readonly_transaction().await.unwrap();
    let stored3 = store
        .span_list_by_trace(&tx, &trace3_id)
        .await
        .expect("unable to list trace3 spans");
    let joined: String = stored3
        .iter()
        .map(|s| serde_json::to_string(s.as_inner()).unwrap())
        .collect::<Vec<_>>()
        .join(" ");
    assert!(
        !joined.contains("FAKEKEY"),
        "opt-in storage leaked key: {joined}"
    );
    assert!(
        joined.contains("[REDACTED]"),
        "opt-in storage must keep redacted content: {joined}"
    );
}
