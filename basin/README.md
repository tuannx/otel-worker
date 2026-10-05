# Basin dual-write (P1)

Basin is the cold analytics store (replaces ClickHouse in the SigNoz model).
D1 remains the hot store + control plane. Nothing here runs until a Cloudflare
account with Basin access is chosen by the owner.

## How dual-write works in code

- Ingest Worker writes every signal to D1 first (source of truth for hot queries).
- If `BASIN_PIPELINE_URL` is set (Worker secret/var), the Worker also POSTs
  NDJSON batches to that Basin Pipelines HTTP stream endpoint. If it is not
  set, dual-write is disabled and ingest behaves exactly as before.
- Sink failures never fail ingest: the D1 write has already committed; the
  sink result is logged. (At-least-once on the D1 side, best-effort on Basin
  for P1; reconciliation = count comparison, see gate in docs/PLAN.md.)

## Resources to create (owner runs, account of their choice)

```sh
# from repo root, with wrangler logged into the chosen account
npx wrangler basin pipelines setup   # if first time in the account
```

Create one HTTP stream + pipeline per signal, each landing in its own
Iceberg table in Basin Catalog on R2. Schemas are the flat records emitted
by the Worker (see `otel-worker-core/src/sink.rs`):

- `spans`       — tenant_id, trace_id, span_id, parent_span_id, service_name,
                  name, kind, start_ts, end_ts, attributes (JSON string)
- `logs`        — tenant_id, service_name, trace_id, span_id, severity_number,
                  severity_text, body, timestamp, attributes, resource_attributes
- `metric_samples` — tenant_id, service_name, metric_name, kind, timestamp,
                  value, attributes, resource_attributes

Partitioning: by day on the timestamp column, secondary `service_name`.
Retention: Basin Catalog snapshot expiration + R2 lifecycle (set at creation).

## Pipeline transforms

Pipelines SQL is row-level only (no GROUP BY). Keep transforms 1:1: select the
stream fields through unchanged; all aggregation happens later in Basin SQL
at query time (P2). The files `spans.sql`, `logs.sql`, `metric_samples.sql`
in this directory hold the per-signal transform statements to paste into the
pipeline definition once stream/sink names exist in the account.

## Gate for this layer (cannot pass until account exists)

1. Send the fixtures in `examples/send-signals/`.
2. D1 counts == fixture counts.
3. Basin SQL counts == fixture counts (after pipeline flush).
Until then this layer's verdict is `warn`, not `pass`.
