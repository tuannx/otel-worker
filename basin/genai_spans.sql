-- Basin Pipeline transform: genai_spans stream -> genai_spans Iceberg sink
-- Row-level only (no aggregation in Pipelines). Field list mirrors
-- otel-worker-core/src/sink.rs `genai_span_record`. Prompt/completion content
-- is never part of this shape: ingest drops or redacts it before dual-write,
-- and only the normalized projection fields land here.
SELECT
  tenant_id,
  trace_id,
  span_id,
  parent_span_id,
  service_name,
  span_name,
  operation,
  provider,
  request_model,
  response_model,
  agent_name,
  tool_name,
  conversation_id,
  input_tokens,
  output_tokens,
  cache_read_tokens,
  cache_creation_tokens,
  ttft_ms,
  duration_ms,
  finish_reasons,
  cost_usd,
  price_version,
  is_error,
  start_ts,
  end_ts
FROM genai_spans_stream;
