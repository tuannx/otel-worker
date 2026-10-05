use otel_worker_core::control::AlertEvent;
use otel_worker_core::data::util::Timestamp;
use otel_worker_core::data::BoxedStore;
use worker::send::SendFuture;
use worker::{Fetch, Method, Request, RequestInit};

/// Evaluate the default tenant's rules and deliver any resulting `pending`
/// events to their rule webhooks. The evaluator in core is deterministic and
/// persists events before delivery, so webhook failures are visible on the
/// event instead of silently losing the alert.
pub async fn run_scheduled_alerts(store: BoxedStore) {
    let results = match otel_worker_core::alerting::evaluate_alerts(
        &store,
        "default",
        Timestamp::now(),
    )
    .await
    {
        Ok(results) => results,
        Err(err) => {
            worker::console_error!("scheduled alert evaluation failed: {err}");
            return;
        }
    };

    for result in results {
        let Some(event) = result.event else {
            continue;
        };
        if event.delivery_status != "pending" {
            continue;
        }
        let Some(webhook_url) = event.webhook_url.clone() else {
            continue;
        };

        let (status, error) = match deliver_webhook(&webhook_url, &event).await {
            Ok(status) => (format!("delivered_http_{status}"), None),
            Err(err) => ("failed".to_string(), Some(err)),
        };

        if let Ok(tx) = store.start_readwrite_transaction().await {
            if let Err(err) = store
                .alert_event_update_delivery(&tx, &event.id, &status, error.as_deref())
                .await
            {
                worker::console_error!("failed to record alert delivery for {}: {err}", event.id);
            } else {
                let _ = store.commit_transaction(tx).await;
            }
        }
    }
}

async fn deliver_webhook(url: &str, event: &AlertEvent) -> Result<u16, String> {
    let url = url.to_string();
    let payload = serde_json::json!({
        "event": "alert.firing",
        "alert": event,
    })
    .to_string();

    SendFuture::new(async move {
        let mut init = RequestInit::new();
        init.with_method(Method::Post);
        init.with_body(Some(payload.into()));

        let mut headers = worker::Headers::new();
        headers
            .set("content-type", "application/json")
            .map_err(|err| err.to_string())?;
        init.with_headers(headers);

        let request = Request::new_with_init(&url, &init).map_err(|err| err.to_string())?;
        let response = Fetch::Request(request)
            .send()
            .await
            .map_err(|err| err.to_string())?;
        let status = response.status_code();
        if (200..300).contains(&status) {
            Ok(status)
        } else {
            Err(format!("webhook returned HTTP {status}"))
        }
    })
    .await
}
