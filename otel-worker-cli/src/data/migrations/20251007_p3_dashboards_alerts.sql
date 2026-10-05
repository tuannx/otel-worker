-- P3: control-plane objects for dashboards and alerting.
-- Applied identically to D1 (otel-worker/migrations) and libsql (otel-worker-cli).

CREATE TABLE dashboards (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    config TEXT NOT NULL,
    config_hash TEXT NOT NULL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
) STRICT;

CREATE INDEX dashboards_tenant_name ON dashboards (tenant_id, name);

CREATE TABLE alert_rules (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    name TEXT NOT NULL,
    service_name TEXT,
    metric TEXT NOT NULL,
    operator TEXT NOT NULL,
    threshold REAL NOT NULL,
    window_seconds INTEGER NOT NULL,
    cooldown_seconds INTEGER NOT NULL,
    webhook_url TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    last_fired_at REAL
) STRICT;

CREATE INDEX alert_rules_tenant_enabled ON alert_rules (tenant_id, enabled);

CREATE TABLE alert_events (
    id TEXT PRIMARY KEY,
    rule_id TEXT NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    tenant_id TEXT NOT NULL DEFAULT 'default',
    service_name TEXT NOT NULL,
    metric TEXT NOT NULL,
    operator TEXT NOT NULL,
    threshold REAL NOT NULL,
    observed_value REAL NOT NULL,
    window_seconds INTEGER NOT NULL,
    fired_at REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'firing',
    webhook_url TEXT,
    delivery_status TEXT NOT NULL DEFAULT 'not_configured',
    delivery_error TEXT
) STRICT;

CREATE INDEX alert_events_tenant_time ON alert_events (tenant_id, fired_at DESC);
CREATE INDEX alert_events_rule_time ON alert_events (rule_id, fired_at DESC);
