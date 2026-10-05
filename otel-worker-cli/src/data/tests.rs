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
