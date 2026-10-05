-- P1: logs, metrics, tenants/api keys, model prices (control plane)
-- Applied to D1 (otel-worker/migrations) and libsql (cli) with identical content.

CREATE TABLE tenants (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at REAL NOT NULL DEFAULT (unixepoch('subsec'))
) STRICT;

INSERT OR IGNORE INTO tenants (id, name) VALUES ('default', 'Default tenant');

-- Only the SHA-256 hex of a key is stored. Plain keys are shown once at creation.
CREATE TABLE api_keys (
    key_hash TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL REFERENCES tenants(id),
    name TEXT NOT NULL,
    created_at REAL NOT NULL DEFAULT (unixepoch('subsec')),
    revoked_at REAL
) STRICT;

CREATE INDEX api_keys_tenant ON api_keys (tenant_id);

CREATE TABLE logs (
    tenant_id TEXT NOT NULL,
    service_name TEXT NOT NULL,
    trace_id TEXT,
    span_id TEXT,
    severity_number INTEGER NOT NULL,
    severity_text TEXT NOT NULL,
    body TEXT NOT NULL,
    timestamp REAL NOT NULL,
    attributes TEXT NOT NULL,
    resource_attributes TEXT NOT NULL
) STRICT;

CREATE INDEX logs_trace_id ON logs (trace_id);
CREATE INDEX logs_service_time ON logs (service_name, timestamp);
CREATE INDEX logs_tenant_time ON logs (tenant_id, timestamp);

CREATE TABLE metric_samples (
    tenant_id TEXT NOT NULL,
    service_name TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    kind TEXT NOT NULL,
    timestamp REAL NOT NULL,
    value REAL NOT NULL,
    attributes TEXT NOT NULL,
    resource_attributes TEXT NOT NULL
) STRICT;

CREATE INDEX metric_samples_service_name_time ON metric_samples (service_name, metric_name, timestamp);
CREATE INDEX metric_samples_tenant_time ON metric_samples (tenant_id, timestamp);

-- Control plane for AI-trace cost (used in P4). Prices are per 1M tokens (USD),
-- seeded/edited by the tenant; history is kept by effective_from.
CREATE TABLE model_prices (
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    input_per_mtok REAL NOT NULL,
    output_per_mtok REAL NOT NULL,
    cache_read_per_mtok REAL,
    cache_creation_per_mtok REAL,
    effective_from REAL NOT NULL,
    PRIMARY KEY (provider, model, effective_from)
) STRICT;
