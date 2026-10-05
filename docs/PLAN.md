# PLAN — SigNoz/Datadog clone trên Cloudflare

> Codename tạm: `cf-signoz` (chưa chốt tên sản phẩm — cần Xuan chốt)
> Repo: fork `tuannx/otel-worker` ← `fiberplane/otel-worker` (MIT OR Apache-2.0, 49★, updated 2025-11-05)
> Ngày lập plan: 2026-10-05 (America/Chicago)
> Verdict tổng cho hướng đi: **warn** — đi được, nhưng *chưa chuẩn* nếu giữ nguyên kiến trúc gốc. Chuẩn khi theo kiến trúc lai ở §3.

---

## 1. Audit bản gốc (evidence)

| Claim | Evidence |
|---|---|
| Chỉ ingest traces, chưa phải observability platform | `otel-worker/README.md`: OTLP/HTTP `/v1/traces`, JSON + protobuf, **gRPC không hỗ trợ**; không có `/v1/logs`, `/v1/metrics` |
| Store là D1, 1 bảng spans | `otel-worker/migrations/20240708_create_spans.sql`: `spans(trace_id, span_id, parent_span_id, name, kind, start_time, end_time, inner TEXT)`; index chỉ trên `(trace_id, span_id)` và `(trace_id, parent_span_id)` — không index time/service, không TTL |
| Auth 1 token tĩnh | `otel-worker/src/lib.rs` + `middleware/auth.rs`: `AUTH_TOKEN` secret duy nhất, Bearer. Không tenant, không API key theo service |
| Không có UI | Surface hiện có: HTTP API lấy traces, WebSocket realtime (`/api/ws`, Durable Object `WebSocketHibernationServer`), CLI (`otel-worker-cli`) + MCP server (`cargo cli mcp`) |
| Core nhỏ, fork sạch được | `otel-worker-core` ~904 LOC (api/otel/service/data). Rust + worker-rs, `compatibility_date = 2024-07-30` |
| Cần Workers Paid khi deploy | README gốc: worker dùng Durable Objects → paid account |

**Kết luận audit:** bản gốc là *OTLP trace collector tối thiểu chạy ở edge*, không phải SigNoz clone. Giữ nó làm ingestion core là hợp lý; coi D1 là storage chính cho analytics là sai và sẽ nghẽn (query theo time/service = scan, `inner TEXT` blob không query được theo attribute).

## 2. Đối chiếu SigNoz / Datadog → Cloudflare

SigNoz (nguồn: signoz.io/docs, repo SigNoz): OTel-native, 3 signals (traces/logs/metrics) chung 1 ClickHouse store, correlation trace↔log, query builder, dashboards, alerts, service map, LLM observability trên `gen_ai.*`. Datadog thêm: proprietary agent, continuous profiling, RUM, synthetics, security — **ngoài scope clone này**.

### Port được (MVP → V1)
- [x] OTLP ingest traces (đã có)
- [ ] OTLP ingest logs + metrics (`/v1/logs`, `/v1/metrics`)
- [ ] Trace explorer: flamegraph/Gantt, lọc service/time/error, trace detail theo `trace_id`
- [ ] Log explorer correlate bằng `trace_id` / `span_id`
- [ ] Metrics: RED metrics tự suy từ spans (rate/errors/duration, p50/p95/p99), custom metric samples
- [ ] Service map suy từ parent/child + `peer.service` / http attributes
- [ ] Dashboards + query builder (compile xuống SQL, allowlist function/operator)
- [ ] Alerts: rule trên metrics/traces/logs → webhook (Slack/email/Telegram qua Worker)
- [ ] MCP tools cho agent: `search_traces`, `get_trace`, `search_logs`, `service_health`, `llm_cost` (bản gốc đã có khung MCP — mở rộng, không làm lại)
- [ ] AI trace (xem `docs/AI-TRACE.md`)

### Không port / nói thẳng là không làm
- Datadog agent, eBPF, continuous profiler, RUM session replay, synthetics network — không có tương đương sạch trên Workers.
- ClickHouse tự host — thay bằng Basin (Iceberg/R2) + Basin SQL, xem §3.
- gRPC OTLP ở Workers: giữ HTTP/protobuf + HTTP/JSON. Nếu cần gRPC thật thì phải có collector ngoài edge (tách riêng, không hứa ở MVP).

## 3. Kiến trúc chuẩn (điểm quyết định)

**Nguyên tắc: Basin không thay mọi thứ. Basin thay ClickHouse cho analytics lạnh; trace lookup theo ID cần hot store riêng.**

```
OTLP/HTTP client (app, agent, LLM)
        │
        ▼
┌─ Ingest Worker (Rust, fork này) ─────────────────────┐
│ auth: API key/tenant (hash lưu D1, cache KV/Cache API)│
│ validate → sample → redact (prompt body OFF mặc định)  │
│ enrich gen_ai: token cost từ bảng model_prices        │
└──────┬──────────────────────┬─────────────────────────┘
       │ hot write            │ stream write (binding)
       ▼                      ▼
  D1 (hot, TTL ngắn)     Basin Pipelines ──► Iceberg trên R2 (Basin Catalog)
  spans/logs gần đây     spans · logs · metric_samples · genai_spans
  lookup trace_id        partition: day, service_name
       │                      │
       │                      ▼ Basin SQL (serverless)
       │                 aggregates: p95, error rate,
       │                 token/cost, service map edges
       ▼                      │
  Query API (Worker) ◄────────┘  kết quả aggregate cache vào D1
       │
       ├── UI (Workers Static Assets / Pages): Services, Traces, Logs,
       │    Metrics, AI/LLM, Service Map, Dashboards, Alerts
       ├── Alert evaluator (Cron Trigger + Queues → webhook)
       └── MCP server (mở rộng otel-worker-cli)
Control plane (D1): tenants, api_keys, services, dashboards,
                    alert_rules, alert_history, model_prices
```

Lý do lai, không Basin-only (evidence):
- Cloudflare thông báo Basin GA 2026-10-01: Pipelines ingest từ Workers/HTTP/Logpush → Iceberg/R2; Catalog quản lý compaction/snapshot expiration; SQL hỗ trợ joins, window functions, >190 functions (`developers.cloudflare.com/changelog/post/2026-10-01-basin-ga/`).
- Basin SQL là query engine analytical theo batch/scan trên Iceberg — hợp aggregate theo time/service, **không** hợp point-lookup 1 `trace_id` cần <200ms và flamegraph đầy đủ ngay sau ingest. Nên giữ D1 hot (TTL 24h–7d, cleanup bằng Cron) + ghi Basin cho retention dài (30–90d+) và analytics.
- Workers traces native của Cloudflare chỉ giữ 7 ngày (`developers.cloudflare.com/workers/observability/traces/`) — không dùng làm system of record cho sản phẩm này.

### Schema nháp (Iceberg, Basin)
- `spans`: `tenant_id, trace_id, span_id, parent_span_id, service_name, name, kind, status_code, start_ts, end_ts, duration_ns, attributes MAP/VARIANT (JSON string giai đoạn 1), gen_ai_* cột tách riêng`
- `logs`: `tenant_id, trace_id, span_id, service_name, severity, body, ts, attributes`
- `metric_samples`: `tenant_id, service_name, metric_name, ts, value, attributes_hash, attributes`
- `genai_spans` (view/bảng tách): `model, provider, operation, input_tokens, output_tokens, cache_tokens, cost_usd, ttft_ms, finish_reasons, session_id, agent_name, tool_name`

Partition: `days(ts)`, thứ cấp `service_name`. Retention: Basin Catalog snapshot expiration + lifecycle R2.

### Sampling / cost gate
- Head sampling theo tenant (mặc định traces 100% ở MVP vì volume tự dùng; AI/error traces luôn giữ 100%).
- Redaction trước khi ghi Basin: drop `gen_ai.input.messages` / `gen_ai.output.messages` trừ khi tenant bật opt-in; truncate attribute > 8KB; hash API key, không lưu key thô.

## 4. Roadmap theo gate (deterministic, không LLM judge)

Mỗi phase xong khi gate của nó pass bằng lệnh/test lặp lại được. Verdict mỗi phase: `pass | warn | block`.

### P0 — Fork + plan (hôm nay)
Gate: fork `tuannx/otel-worker` tồn tại; tài liệu này + `AI-TRACE.md` + `FEATURE-MATRIX.md` trong repo; nêu rõ blocker toolchain.
Hiện trạng: fork xong; Rust toolchain **chưa có** trong môi trường agent (`cargo: command not found`) → build verify để ở P1, không giả pass.

### P1 — Ingest đủ 3 signals + Basin sink
- Thêm `/v1/logs`, `/v1/metrics` (OTLP/HTTP JSON+protobuf), API key theo tenant, dual-write: D1 hot + Basin Pipelines HTTP sink (bật khi đặt `BASIN_PIPELINE_URL`; tắt = hành vi cũ).
- Gate: gửi fixture trace/log/metric bằng script trong `examples/` → (a) D1 đếm đúng số span/log/sample, (b) Basin SQL đếm đúng (±0) sau khi pipeline flush, (c) trace lỗi + AI span giữ 100% dù sampling <100%.
- Trạng thái 2026-10-05: code xong trên branch `feat/p1-logs-metrics-basin` — `cargo test -p otel-worker-core -p otel-worker-cli` pass (10 tests, gồm 2 test P1: round-trip logs/metrics qua libsql + đếm proto→model từ fixture), `cargo check -p otel-worker --target wasm32-unknown-unknown` pass. Gate (a) ở mức store/fixture: pass. Gate (b) Basin SQL: **chưa chạy được** — cần Cloudflare account của Xuan để tạo Basin Pipeline thật (verdict phần Basin: warn).

### P2 — Query API + UI MVP
- Services list (rate/error/p95 từ Basin SQL, cache D1), trace detail flamegraph (hot D1, fallback Basin), log explorer lọc theo `trace_id`.
- Gate: e2e — ingest 1 trace 5 spans có 1 span lỗi + 3 log cùng `trace_id` → UI/API trả đúng cây span, đúng log, p95 tính tay khớp query.
- Trạng thái 2026-10-05: xong trên branch `feat/p2-query-ui`. Query API: `GET /v1/services`, `/v1/services/:name/operations`, `/v1/metrics/summary` (percentile pinned trong `otel-worker-core/src/query.rs`, tính trên hot store; Basin SQL thay vào cùng contract ở phase sau). spans có cột `service_name`/`tenant_id` riêng (migration 20251006). UI MVP ở `ui/index.html`, Worker serve tại `/ui`: Services → Operations, Traces → Gantt theo parent/child + logs của trace, Logs, Metrics; token nhập trong browser (sessionStorage). Evidence: `cargo test` core 8/8 + cli 4/4 (gồm test p50=25ms tính tay trên durations 10/20/30/40ms), wasm check pass, JS UI qua `node --check`. Nợ: chưa chạy `wrangler dev` e2e trên D1 local (cùng nợ hạ tầng như P1).

### P3 — Dashboards, alerts, service map
- Query builder → Basin SQL allowlist; alert evaluator bằng Cron (mỗi 1–5 phút) → webhook; service map từ edges.
- Gate: rule `error_rate > 5% trong 5m` bắn đúng 1 alert vào webhook test, không bắn lại trong cooldown; dashboard JSON import/export lặp lại cho cùng hash.
- Trạng thái 2026-10-05: code xong trên branch `feat/p3-dashboards-alerts-map`. Control plane ở D1/libsql (migration `20251007_p3_dashboards_alerts.sql`): `dashboards`, `alert_rules`, `alert_events`; dashboard config được canonical hoá (sort key đệ quy) và gắn `config_hash`, import cùng id/config lặp lại cho cùng hash. API: CRUD/import dashboards, CRUD alert rules, list alert events, `POST /v1/alerts/evaluate`, `GET /v1/service-map`. Alert metrics là allowlist đóng (`error_rate`, `p95_ms`, `span_count`) — không nhận SQL/expression tự do; Basin SQL sẽ thay tầng aggregate khi có account thật. Evaluator dùng chung cho HTTP evaluate và Worker Cron (`*/5 * * * *`); event được persist trước webhook, trạng thái delivery ghi lại trên event. Service map dựng edge từ parent/child cùng trace (không bịa node khi thiếu parent). UI có thêm Map (SVG + bảng edges), Dashboards (tạo/import/export/render panels), Alerts (tạo rule, evaluate now, events). Evidence: test `p3_dashboards_alerts_and_service_map` — dashboard re-import cùng hash, map có đúng edge `web -> checkout`, rule `error_rate > 0.05` với observed `1.0` bắn đúng 1 event và lần evaluate thứ hai bị suppress bởi cooldown. Nợ thật: chưa deploy Worker Cron/webhook lên Cloudflare account của Xuan và chưa chạy Basin SQL.

### P4 — AI trace
Xem `docs/AI-TRACE.md`. Gate ở đó.
- Trạng thái 2026-10-05: code xong trên branch `feat/p4-ai-trace`. Core mới `otel-worker-core/src/genai.rs` (không import adapter): alias mapping cho `gen_ai.*` (provider `gen_ai.provider.name` → `gen_ai.system`, token `input_tokens` → `prompt_tokens`, …), privacy sanitize ở ingest (drop content mặc định; opt-in vẫn redact API-key/email/card + truncate 8.000 ký tự), chọn giá theo `effective_from` tại thời điểm span và tính cost = tokens × giá, lưu `price_version` kèm span. Projection `genai_spans` (migration `20251008_p4_genai_spans.sql` cho D1 + libsql) ghi cùng transaction với span; Basin dual-write thêm signal `genai_spans` (xem `basin/genai_spans.sql`). API: `GET /v1/ai/overview`, `/v1/ai/runs`, `/v1/ai/runs/:trace_id`, `/v1/ai/tools`, `/v1/ai/search`, `GET|POST /v1/ai/prices`, `GET|PUT /v1/ai/settings`. ApiClient + MCP dùng chung các endpoint này: tools `llm_cost_by_model`, `agent_run`, `tool_failures`, `search_ai_traces` (cộng `get_trace` cũ). UI thêm tab AI: totals, cost theo model, agent runs + replay timeline theo depth, tool health, search, form giá, toggle `capture_content`. Evidence: test `p4_genai_cost_privacy_and_replay` (libsql, qua `Service::ingest_traces` thật) — fixture `invoke_agent` + 2 `chat` + 1 `execute_tool` lỗi: cost chat đúng phép nhân (0.0061 + 0.012 USD), chuỗi `sk-FAKEKEY…` trong `gen_ai.input.messages` không xuất hiện ở span đã lưu, projection hay sink records; replay depths `[0,1,2,1]` và tổng cost khớp; đổi giá v2 → span mới dùng v2, span cũ giữ cost/`price_effective_from` v1; bật `capture_content` → nội dung lưu chỉ còn dạng `[REDACTED]`. Unit tests `genai::*` phủ mapping alias, redaction, chọn giá. `cargo test` core 20/20 + cli 6/6, wasm check pass, UI JS qua `node --check`. Nợ thật (chung P1–P3): chưa chạy Basin SQL/deploy thật trên Cloudflare account của Xuan nên gate Basin/deploy của AI-TRACE §5 vẫn ở mức store/fixture, verdict warn cho phần đó.

### P5 — Hardening
- Retention/compaction bật, rate limit theo tenant, load test (k6/vegeta qua HTTP fixture), chi phí R2/Basin đo thật 7 ngày.
- Gate: báo cáo cost/1M spans kèm số đo, không ước lượng miệng; p95 ingest latency có số.

## 5. Rủi ro / blocker thật
1. **Toolchain**: môi trường build hiện chưa có Rust; CI (GitHub Actions) là nơi verify chính cho tới khi cài `rustup` + `worker-build`.
2. **Basin mới GA (2026-10-01)**: giới hạn/pricing Pipelines/SQL phải đọc live từ docs Cloudflare trước khi hứa SLA; không chốt số từ bài báo.
3. **GenAI semconv chưa stable** (SigNoz docs ghi rõ mọi `gen_ai.*` đang ở trạng thái Development) → pin version instrumentation, có lớp map attribute ở ingest để đổi tên không vỡ dashboard.
4. **Một front nữa**: dự án này cộng vào hàng đợi đang mở (ArcadeAttest hạn 2026-10-12, Nexus deploy chờ Cloudflare token). Thứ tự ưu tiên do Xuan chốt; plan này không tự xếp trên các hạn đó.
5. **Deere/IP**: nếu đưa telemetry từ hệ thống liên quan công việc vào đây thì dừng — chỉ dùng dữ liệu dự án cá nhân/demo cho tới khi ranh giới IP rõ.

## 6. Việc cần Xuan chốt (1 lần, gọn)
1. Tên sản phẩm/repo: giữ `tuannx/otel-worker` (fork) hay tạo repo mới tên riêng (vd `cf-signoz`) rồi chuyển fork sang?
2. Cloudflare account dùng để tạo Basin/R2/D1 thật ở P1 — dùng account nào, đã có Workers Paid chưa?
3. Ưu tiên: P1 làm ngay, hay xếp sau ArcadeAttest (12/10)?
