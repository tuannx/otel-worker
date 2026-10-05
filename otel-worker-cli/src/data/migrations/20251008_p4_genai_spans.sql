-- P4: normalized GenAI span projection + per-tenant content capture opt-in.
-- model_prices already exists from P1 (20251005_create_signals.sql).

ALTER TABLE tenants ADD COLUMN capture_content INTEGER NOT NULL DEFAULT 0;

CREATE TABLE genai_spans (
    tenant_id TEXT NOT NULL,
    trace_id TEXT NOT NULL,
    span_id TEXT NOT NULL,
    parent_span_id TEXT,
    service_name TEXT NOT NULL,
    span_name TEXT NOT NULL,
    operation TEXT NOT NULL,
    provider TEXT NOT NULL,
    request_model TEXT,
    response_model TEXT,
    agent_name TEXT,
    tool_name TEXT,
    conversation_id TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cache_read_tokens INTEGER,
    cache_creation_tokens INTEGER,
    ttft_ms REAL,
    duration_ms REAL NOT NULL,
    finish_reasons TEXT NOT NULL DEFAULT '[]',
    cost_usd REAL,
    price_provider TEXT,
    price_model TEXT,
    price_effective_from REAL,
    is_error INTEGER NOT NULL DEFAULT 0,
    start_time REAL NOT NULL,
    end_time REAL NOT NULL,
    PRIMARY KEY (tenant_id, trace_id, span_id)
) STRICT;

CREATE INDEX genai_spans_tenant_time ON genai_spans (tenant_id, start_time DESC);
CREATE INDEX genai_spans_trace ON genai_spans (tenant_id, trace_id, start_time);
CREATE INDEX genai_spans_model_time ON genai_spans (tenant_id, provider, response_model, start_time DESC);
CREATE INDEX genai_spans_conversation ON genai_spans (tenant_id, conversation_id, start_time);
