-- Basin Pipeline transform: metric_samples stream -> metric_samples Iceberg sink
SELECT
  tenant_id,
  service_name,
  metric_name,
  kind,
  timestamp,
  value,
  attributes,
  resource_attributes
FROM metric_samples_stream;
