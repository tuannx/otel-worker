use axum::async_trait;
use otel_worker_core::sink::SignalSink;
use worker::send::SendFuture;
use worker::{Fetch, Method, Request, RequestInit};

/// Dual-write to a Basin Pipelines HTTP stream endpoint.
/// Body format: a JSON array of flat records, each tagged with `signal`.
/// The underlying Workers Fetch future is !Send, so the work is wrapped in
/// `SendFuture` (safe on the single-threaded Workers runtime), matching how
/// the existing Durable Object event broadcast is handled in lib.rs.
pub struct BasinHttpSink {
    url: String,
    token: Option<String>,
}

impl BasinHttpSink {
    pub fn new(url: String, token: Option<String>) -> Self {
        Self { url, token }
    }
}

#[async_trait]
impl SignalSink for BasinHttpSink {
    async fn publish(&self, signal: &str, records: Vec<serde_json::Value>) -> Result<(), String> {
        let url = self.url.clone();
        let token = self.token.clone();
        let signal = signal.to_string();
        SendFuture::new(async move { publish_inner(url, token, &signal, records).await }).await
    }
}

async fn publish_inner(
    url: String,
    token: Option<String>,
    signal: &str,
    records: Vec<serde_json::Value>,
) -> Result<(), String> {
    let envelope: Vec<serde_json::Value> = records
        .into_iter()
        .map(|mut record| {
            if let Some(obj) = record.as_object_mut() {
                obj.insert(
                    "signal".to_string(),
                    serde_json::Value::String(signal.to_string()),
                );
            }
            record
        })
        .collect();
    let body = serde_json::to_string(&envelope).map_err(|e| e.to_string())?;

    let mut init = RequestInit::new();
    init.with_method(Method::Post);
    init.with_body(Some(body.into()));

    let mut headers = worker::Headers::new();
    headers
        .set("content-type", "application/json")
        .map_err(|e| e.to_string())?;
    if let Some(token) = &token {
        headers
            .set("authorization", &format!("Bearer {token}"))
            .map_err(|e| e.to_string())?;
    }
    init.with_headers(headers);

    let request = Request::new_with_init(&url, &init).map_err(|e| e.to_string())?;
    let mut response = Fetch::Request(request)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = response.status_code();
    if !(200..300).contains(&status) {
        let text = response.text().await.unwrap_or_default();
        return Err(format!("Basin sink HTTP {status}: {text}"));
    }
    Ok(())
}
