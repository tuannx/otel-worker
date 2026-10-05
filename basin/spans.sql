-- Basin Pipeline transform: spans stream -> spans Iceberg sink
-- Row-level only (no aggregation in Pipelines). Field list mirrors sink.rs.
SELECT
  tenant_id,
  trace_id,
  span_id,
  parent_span_id,
  service_name,
  name,
  kind,
  start_ts,
  end_ts,
  attributes
FROM spans_stream;
