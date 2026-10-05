-- Basin Pipeline transform: logs stream -> logs Iceberg sink
SELECT
  tenant_id,
  service_name,
  trace_id,
  span_id,
  severity_number,
  severity_text,
  body,
  timestamp,
  attributes,
  resource_attributes
FROM logs_stream;
