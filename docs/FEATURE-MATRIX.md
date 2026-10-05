# Feature Matrix — gốc vs SigNoz/Datadog vs dự án này

Trạng thái: `có` / `kế hoạch Pn` / `không làm`. Claim nào chưa có evidence chạy thật thì để ở cột kế hoạch, không ghi là có.

| Tính năng | fiberplane/otel-worker (gốc) | SigNoz | Datadog | Dự án này |
|---|---|---|---|---|
| OTLP traces HTTP (JSON/protobuf) | có | có | có | có (giữ nguyên) |
| OTLP gRPC | không | có | có | không làm (HTTP là đủ cho Workers; cần gRPC thì collector riêng) |
| OTLP logs | không | có | có | P1 |
| OTLP metrics | không | có | có | P1 |
| Trace explorer + flamegraph | API thô + WS realtime, không UI | có | có | P2 |
| Log ↔ trace correlation | không | có | có | P2 |
| RED metrics từ spans (rate/err/p95) | không | có | có | P2 (Basin SQL) |
| Service map | không | có | có | P3 |
| Dashboards / query builder | không | có | có | P3 |
| Alerts → webhook | không | có | có | P3 (Cron + Queues) |
| Multi-tenant + API key/service | không (1 token tĩnh) | có | có | P1 |
| LLM / AI trace (`gen_ai.*`, token cost) | không | có | có (LLM Obs.) | P4 — xem `docs/AI-TRACE.md` |
| MCP cho agent truy vấn telemetry | có (khung, qua CLI) | có (mcp server chính thức) | — | mở rộng ở P2/P4 |
| Storage analytics | D1 (1 bảng spans) | ClickHouse | proprietary | Basin: Iceberg/R2 + Basin SQL; D1 chỉ hot + control plane |
| Retention dài + compaction tự động | không (không TTL) | ClickHouse TTL | có | P1/P5: D1 TTL ngắn + Basin Catalog snapshot expiration |
| Continuous profiling / eBPF / RUM replay | không | không | có | **không làm** — không có tương đương sạch trên Workers |
| Pricing theo host/custom metric | n/a | không (theo GB/samples) | có (phức tạp) | không áp dụng; tự host trên Cloudflare của chính tenant |

Ghi chú trung thực: gọi đây là "SigNoz/Datadog clone" chỉ đúng ở lớp *OTel-native traces/logs/metrics + dashboards/alerts/LLM observability*. Lớp agent/profiling/RUM của Datadog nằm ngoài định nghĩa clone này.
