# AI Trace Plan — LLM / Agent observability

Mục tiêu: làm phần mà SigNoz gọi là LLM observability, trên cùng pipeline OTLP của repo này, để trace được cả agent (nhiều bước, gọi tool) chứ không chỉ 1 request HTTP.

## 1. Chuẩn dữ liệu: OpenTelemetry GenAI semantic conventions

- Nguồn chuẩn: OTel GenAI semconv (`gen_ai.*`). **Lưu ý độ ổn định:** theo tài liệu SigNoz, toàn bộ `gen_ai.*` hiện vẫn ở trạng thái *Development* và đã chuyển sang repo riêng; các thư viện instrumentation áp dụng không đồng đều. → Ở ingest phải có **attribute mapping layer**: chuẩn hoá tên cũ/mới về 1 tên nội bộ, pin version thư viện khi demo.
- Không dùng SDK riêng của vendor nào. Chấp nhận span từ OpenLLMetry, OpenLIT, OpenInference, OTel contrib, hoặc span tự instrument.

### Span hierarchy chuẩn cho 1 lượt agent
```
invoke_agent            (gen_ai.operation.name = invoke_agent)
├─ chat                 (gen_ai.operation.name = chat)        ← mỗi vòng gọi model
│   ├─ execute_tool     (gen_ai.operation.name = execute_tool)
│   └─ chat             (vòng tiếp theo)
├─ embeddings / create_agent / execute_tool ...
└─ (eval span — phase sau, không thuộc MVP)
```

### Attribute bắt buộc trên span `chat`
| Attribute | Ý nghĩa |
|---|---|
| `gen_ai.request.model` | model yêu cầu |
| `gen_ai.response.model` | model thực trả (nếu khác) |
| `gen_ai.provider.name` (và alias cũ `gen_ai.system`) | openai / anthropic / workers-ai / ... |
| `gen_ai.usage.input_tokens`, `gen_ai.usage.output_tokens` | token thật từ provider, không ước lượng |
| `gen_ai.usage.cache_creation.input_tokens`, `gen_ai.usage.cache_read.input_tokens` | nếu provider có |
| `gen_ai.server.time_to_first_token` (TTFT) | cho streaming |
| `gen_ai.response.finish_reasons` | mảng, không stringify object |
| `gen_ai.conversation.id` / session attribute tương đương | gom theo phiên agent |

Trên span `execute_tool`: tên tool, thời gian chạy, lỗi. Trên `invoke_agent`: tên agent, tổng token/cost của cả lượt (rollup ở query layer, không ghi đè span gốc).

## 2. Cost: token → tiền ở ingest

OTel chỉ ghi token, không ghi tiền (giá thay đổi theo hợp đồng). Giống cách SigNoz làm:

1. Bảng `model_prices` ở D1 control plane: `(provider, model, input_per_mtok, output_per_mtok, cache_read_per_mtok, cache_creation_per_mtok, effective_from)`. Giá seed từ bảng giá public của provider tại ngày cấu hình — ghi ngày cạnh giá, không coi là hằng số.
2. Ingest Worker tra giá (cache theo model, TTL ngắn) → tính `gen_ai.usage.cost` (USD) → ghi kèm span vào D1 hot + Basin `genai_spans`.
3. Đổi giá = sửa bảng, không sửa code; cost lịch sử giữ nguyên theo giá tại thời điểm ingest (ghi `price_version` kèm span để truy vết).

## 3. Privacy gate (bắt buộc, không phải tuỳ chọn cho đẹp)

- `gen_ai.input.messages` / `gen_ai.output.messages` (nội dung prompt/completion): **mặc định DROP ở ingest**. Chỉ ghi khi tenant bật `capture_content = true` trong D1, và vẫn truncate + redact theo regex cấu hình (API key, email, số thẻ dạng chuẩn).
- Không bao giờ ghi API key của provider; chỉ ghi model/provider/tokens.
- Rationale: prompt có thể chứa dữ liệu khách hàng/dữ liệu công việc. Lưu mặc định là tự tạo rủi ro không cần thiết — phần lớn câu hỏi vận hành (cost, latency, error, tool nào hỏng) không cần nội dung prompt.

## 4. Màn hình AI (UI + MCP)

1. **LLM overview**: cost theo model/provider/service/tenant; input/output/cache tokens; TTFT p50/p95; duration p95; error rate; top agent theo cost.
2. **Agent run replay**: 1 `trace_id` = 1 lượt agent; timeline `invoke_agent → chat → tool`, bấm từng bước xem token/cost/lỗi (không xem nội dung trừ khi đã opt-in).
3. **Tool health**: tool nào chậm/hỏng nhất theo `execute_tool`.
4. **MCP tools** (cho agent khác truy vấn, nối khung MCP sẵn có của `otel-worker-cli`): `llm_cost_by_model(range)`, `agent_run(trace_id)`, `tool_failures(service, range)`, `search_traces(has_error, gen_ai.model)`.

## 5. Gate cho phase AI (P4)

Verdict chỉ pass khi cả 4 điểm đo được trên fixture lặp lại:

1. Gửi fixture agent 3 span (`invoke_agent` + 2 `chat`, token biết trước) → cost trả về = token × giá trong `model_prices` (sai số 0, vì phép nhân xác định).
2. Fixture có `gen_ai.input.messages` chứa chuỗi giả dạng API key, tenant chưa opt-in → chuỗi đó **không xuất hiện** trong D1, Basin, hay API trả về (grep cả 3 nơi).
3. `agent_run(trace_id)` qua MCP và qua UI/API cho cùng cây span, cùng tổng cost.
4. Đổi 1 giá trong `model_prices`, gửi lại fixture → cost mới áp dụng cho span mới, span cũ giữ `price_version` cũ.

## 6. Ngoài scope phase này
- Eval chất lượng câu trả lời (LLM-as-judge) — nếu làm, làm deterministic trước (schema/score lưu như span riêng), không đưa LLM judge vào gate.
- Prompt versioning / dataset quản lý như Langfuse — cân nhắc sau khi P4 pass.

## 7. Trạng thái triển khai (2026-10-05, branch `feat/p4-ai-trace`)

Đã làm đúng spec ở trên:

- **Mapping layer**: `otel-worker-core/src/genai.rs` đọc mọi attribute qua danh sách alias (tên hiện tại trước, alias cũ sau): provider `gen_ai.provider.name`/`gen_ai.system`; tokens `gen_ai.usage.input_tokens`/`prompt_tokens`, `output_tokens`/`completion_tokens`, cache read/creation; TTFT `gen_ai.server.time_to_first_token` (giây → ms); `gen_ai.response.finish_reasons` giữ dạng mảng JSON; conversation `gen_ai.conversation.id`/`gen_ai.session.id`/`session.id`. Thiếu `operation` thì suy luận: có tool name → `execute_tool`, có model/token → `chat`, có agent name → `invoke_agent`.
- **Cost ở ingest**: chọn price có `effective_from` mới nhất không sau thời điểm span (ưu tiên khớp provider, fallback khớp model); cache rate trống thì dùng input rate; kết quả lưu vào `genai_spans.cost_usd` kèm `price_provider/price_model/price_effective_from` (= `price_version` dạng `provider:model@effective_from`). Đổi giá chỉ áp cho span bắt đầu sau thời điểm hiệu lực; span đã lưu không bị tính lại.
- **Privacy gate**: sanitize chạy trong `Service::ingest_traces` trước mọi lần ghi (D1 + Basin). Mặc định drop `gen_ai.input.messages`, `gen_ai.output.messages`, `gen_ai.prompt`, `gen_ai.completion`, `llm.prompts`, `llm.completions`. Khi tenant bật `capture_content` (`PUT /v1/ai/settings`), nội dung vẫn qua redaction xác định (prefix API key kiểu `sk-`/`ghp_`/`AKIA…`, email, dãy số dạng thẻ) và truncate 8.000 ký tự.
- **API/UI/MCP dùng chung một projection**: `GET /v1/ai/overview` (totals + cost theo provider/model + theo operation), `/v1/ai/runs`, `/v1/ai/runs/:trace_id` (replay: cây span theo depth, token/cost/TTFT/`price_version` từng bước), `/v1/ai/tools`, `/v1/ai/search`, `GET|POST /v1/ai/prices`, `GET|PUT /v1/ai/settings`. MCP (otel-worker-cli) gọi đúng các endpoint này qua ApiClient: `llm_cost_by_model`, `agent_run`, `tool_failures`, `search_ai_traces`. UI tab AI hiển thị cùng số liệu và form quản lý giá + toggle capture.

Gate §5 ở mức store/fixture: **pass** — test `p4_genai_cost_privacy_and_replay` (ingest thật qua Service + libsql) chứng minh cả 4 điểm: cost bằng đúng phép nhân tokens × giá (sai số 0 theo công thức), chuỗi giả API key không xuất hiện trong span đã lưu/projection/sink records (cả khi chưa opt-in lẫn khi đã opt-in), replay cho đúng cây `[0,1,2,1]` và tổng cost, đổi giá chỉ áp cho span mới. Phần chưa chạy được: đối chiếu Basin SQL và deploy thật trên Cloudflare account (chung nợ P1–P3) → verdict tổng của phase vẫn là `warn` cho tới khi gate hạ tầng đó chạy.
