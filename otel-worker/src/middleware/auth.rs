use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::middleware::Next;
use axum::response::Response;
use sha2::{Digest, Sha256};

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

/// Auth accepts either:
/// - the legacy single `AUTH_TOKEN` (tenant `default`), or
/// - a per-tenant API key whose SHA-256 hex is stored in the `api_keys`
///   table (see migrations/20251005_create_signals.sql).
/// On success, the resolved tenant is injected as `x-tenant-id` so ingest
/// handlers can stamp records without re-resolving the key.
pub async fn auth_middleware(
    expected_token: String,
    store: otel_worker_core::data::BoxedStore,
    mut request: Request<Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    let auth_header = request
        .headers()
        .get("Authorization")
        .and_then(|header| header.to_str().ok())
        .unwrap_or("")
        .to_string();

    let Some(("Bearer", token)) = auth_header.split_once(' ') else {
        return Err(StatusCode::UNAUTHORIZED);
    };

    if token == expected_token {
        request.headers_mut().insert(
            "x-tenant-id",
            "default".parse().map_err(|_| StatusCode::UNAUTHORIZED)?,
        );
        return Ok(next.run(request).await);
    }

    // Per-tenant API key path.
    let key_hash = sha256_hex(token);
    match store.api_key_get(&key_hash).await {
        Ok(Some(key)) => {
            let tenant = key
                .tenant_id
                .parse()
                .map_err(|_| StatusCode::UNAUTHORIZED)?;
            request.headers_mut().insert("x-tenant-id", tenant);
            Ok(next.run(request).await)
        }
        _ => Err(StatusCode::UNAUTHORIZED),
    }
}
