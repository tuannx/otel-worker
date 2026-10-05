//! P4 GenAI projection: attribute mapping, privacy sanitization, and cost
//! enrichment for spans carrying OpenTelemetry GenAI semantic conventions.
//!
//! The `gen_ai.*` conventions are still in Development and instrumentation
//! libraries disagree on names, so every read goes through an alias list:
//! the current name first, then the legacy alias. The raw span stays in the
//! `spans` table (after sanitization); this module builds the normalized,
//! queryable `genai_spans` row plus the deterministic projections the AI
//! APIs, UI, and MCP tools share. Nothing here talks to a database or the
//! network; the Store trait owns persistence and HTTP handlers stay thin.

use crate::api::models::{AttributeValue, Span};
use crate::data::models::{GenAiSpanRecord, ModelPrice};
use crate::data::util::Timestamp;
use serde::{Deserialize, Serialize};

// --- Attribute names (current name first, legacy aliases after) -------------

pub const OPERATION_KEYS: &[&str] = &["gen_ai.operation.name"];
pub const PROVIDER_KEYS: &[&str] = &["gen_ai.provider.name", "gen_ai.system"];
pub const REQUEST_MODEL_KEYS: &[&str] = &["gen_ai.request.model"];
pub const RESPONSE_MODEL_KEYS: &[&str] = &["gen_ai.response.model"];
pub const INPUT_TOKEN_KEYS: &[&str] = &["gen_ai.usage.input_tokens", "gen_ai.usage.prompt_tokens"];
pub const OUTPUT_TOKEN_KEYS: &[&str] = &[
    "gen_ai.usage.output_tokens",
    "gen_ai.usage.completion_tokens",
];
pub const CACHE_READ_TOKEN_KEYS: &[&str] = &[
    "gen_ai.usage.cache_read.input_tokens",
    "gen_ai.usage.cached_tokens",
];
pub const CACHE_CREATION_TOKEN_KEYS: &[&str] = &["gen_ai.usage.cache_creation.input_tokens"];
/// OTel records time-to-first-token in seconds; stored/projected as ms.
pub const TTFT_KEYS: &[&str] = &["gen_ai.server.time_to_first_token"];
pub const FINISH_REASON_KEYS: &[&str] = &["gen_ai.response.finish_reasons"];
pub const CONVERSATION_KEYS: &[&str] =
    &["gen_ai.conversation.id", "gen_ai.session.id", "session.id"];
pub const AGENT_NAME_KEYS: &[&str] = &["gen_ai.agent.name"];
pub const TOOL_NAME_KEYS: &[&str] = &["gen_ai.tool.name"];

/// Prompt/completion content attributes. Dropped at ingest unless the tenant
/// explicitly opted in via `capture_content`; even then they are redacted and
/// truncated before anything is stored.
pub const CONTENT_ATTRIBUTE_KEYS: &[&str] = &[
    "gen_ai.input.messages",
    "gen_ai.output.messages",
    "gen_ai.prompt",
    "gen_ai.completion",
    "llm.prompts",
    "llm.completions",
];

pub const MAX_CONTENT_CHARS: usize = 8000;

// --- Attribute reads ---------------------------------------------------------

fn attr<'a>(span: &'a Span, keys: &[&str]) -> Option<&'a AttributeValue> {
    keys.iter()
        .find_map(|key| span.attributes.0.get(*key).and_then(|value| value.as_ref()))
}

pub fn attr_string(span: &Span, keys: &[&str]) -> Option<String> {
    match attr(span, keys) {
        Some(AttributeValue::StringValue(value)) if !value.is_empty() => Some(value.clone()),
        _ => None,
    }
}

pub fn attr_i64(span: &Span, keys: &[&str]) -> Option<i64> {
    match attr(span, keys) {
        Some(AttributeValue::IntValue(value)) => Some(*value),
        Some(AttributeValue::DoubleValue(value)) => Some(*value as i64),
        Some(AttributeValue::StringValue(value)) => value
            .parse::<i64>()
            .ok()
            .or_else(|| value.parse::<f64>().ok().map(|v| v as i64)),
        _ => None,
    }
}

pub fn attr_f64(span: &Span, keys: &[&str]) -> Option<f64> {
    match attr(span, keys) {
        Some(AttributeValue::IntValue(value)) => Some(*value as f64),
        Some(AttributeValue::DoubleValue(value)) => Some(*value),
        Some(AttributeValue::StringValue(value)) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn finish_reasons(span: &Span) -> String {
    let reasons: Vec<String> = match attr(span, FINISH_REASON_KEYS) {
        Some(AttributeValue::ArrayValue(values)) => values
            .iter()
            .filter_map(|value| match value {
                AttributeValue::StringValue(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        Some(AttributeValue::StringValue(value)) if !value.is_empty() => vec![value.clone()],
        _ => Vec::new(),
    };
    serde_json::to_string(&reasons).unwrap_or_else(|_| "[]".to_string())
}

/// A span belongs to the GenAI projection when it carries any GenAI signal:
/// an operation, a model, token usage, an agent, or a tool name.
pub fn is_genai_span(span: &Span) -> bool {
    attr_string(span, OPERATION_KEYS).is_some()
        || attr_string(span, REQUEST_MODEL_KEYS).is_some()
        || attr_string(span, RESPONSE_MODEL_KEYS).is_some()
        || attr_i64(span, INPUT_TOKEN_KEYS).is_some()
        || attr_i64(span, OUTPUT_TOKEN_KEYS).is_some()
        || attr_string(span, AGENT_NAME_KEYS).is_some()
        || attr_string(span, TOOL_NAME_KEYS).is_some()
}

fn infer_operation(span: &Span) -> String {
    if let Some(operation) = attr_string(span, OPERATION_KEYS) {
        return operation;
    }
    if attr_string(span, TOOL_NAME_KEYS).is_some() {
        return "execute_tool".to_string();
    }
    if attr_string(span, REQUEST_MODEL_KEYS).is_some()
        || attr_string(span, RESPONSE_MODEL_KEYS).is_some()
        || attr_i64(span, INPUT_TOKEN_KEYS).is_some()
        || attr_i64(span, OUTPUT_TOKEN_KEYS).is_some()
    {
        return "chat".to_string();
    }
    if attr_string(span, AGENT_NAME_KEYS).is_some() {
        return "invoke_agent".to_string();
    }
    "unknown".to_string()
}

fn is_error(span: &Span) -> bool {
    use opentelemetry_proto::tonic::trace::v1::status::StatusCode;
    span.status
        .as_ref()
        .map(|status| status.code() == StatusCode::Error)
        .unwrap_or(false)
}

// --- Privacy: redaction + ingest-time sanitization ---------------------------

const SECRET_PREFIXES: &[&str] = &[
    "sk-proj-",
    "sk-",
    "ghp_",
    "gho_",
    "ghu_",
    "ghs_",
    "github_pat_",
    "xoxb-",
    "xoxp-",
    "AIza",
];

fn is_secret_tail_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_'
}

fn starts_with_at(chars: &[char], at: usize, prefix: &str) -> bool {
    let prefix_chars: Vec<char> = prefix.chars().collect();
    chars.len() - at >= prefix_chars.len() && chars[at..at + prefix_chars.len()] == prefix_chars[..]
}

/// Deterministic secret redaction (no regex): known API-key prefixes, email
/// local parts, and card-like digit runs are replaced with stable markers.
/// Anything not matching a pattern is returned unchanged.
pub fn redact_secrets(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        // API-key-like tokens: prefix + at least 8 secret characters.
        let mut consumed = false;
        for prefix in SECRET_PREFIXES {
            if starts_with_at(&chars, i, prefix) {
                let prefix_len = prefix.chars().count();
                let tail_len = chars[i + prefix_len..]
                    .iter()
                    .take_while(|c| is_secret_tail_char(**c))
                    .count();
                if tail_len >= 8 {
                    out.push_str("[REDACTED]");
                    i += prefix_len + tail_len;
                    consumed = true;
                    break;
                }
            }
        }
        if consumed {
            continue;
        }
        // AWS access key ids: AKIA + 16 upper/digit characters.
        if starts_with_at(&chars, i, "AKIA") {
            let tail: String = chars[i + 4..]
                .iter()
                .take(16)
                .filter(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
                .collect();
            if tail.len() == 16 {
                out.push_str("[REDACTED]");
                i += 20;
                continue;
            }
        }
        // Email addresses: keep the domain, mask the local part.
        if chars[i] == '@' && !out.is_empty() {
            let domain_end = chars[i + 1..]
                .iter()
                .position(|c| !(c.is_ascii_alphanumeric() || *c == '.' || *c == '-' || *c == '_'))
                .map(|offset| i + 1 + offset)
                .unwrap_or(chars.len());
            let domain: String = chars[i + 1..domain_end].iter().collect();
            if domain.contains('.') {
                // Remove the local part already emitted.
                let local_len = out
                    .chars()
                    .rev()
                    .take_while(|c| {
                        c.is_ascii_alphanumeric()
                            || *c == '.'
                            || *c == '-'
                            || *c == '_'
                            || *c == '+'
                    })
                    .count();
                if local_len > 0 {
                    let byte_len: usize = out
                        .chars()
                        .take(out.chars().count() - local_len)
                        .map(|c| c.len_utf8())
                        .sum();
                    out.truncate(byte_len);
                    out.push_str("***@");
                    out.push_str(&domain);
                    i = domain_end;
                    continue;
                }
            }
        }
        // Card-like digit runs: 13+ digits allowing space/dash separators.
        if chars[i].is_ascii_digit() {
            let mut j = i;
            let mut digits = 0;
            while j < chars.len()
                && (chars[j].is_ascii_digit() || chars[j] == ' ' || chars[j] == '-')
            {
                if chars[j].is_ascii_digit() {
                    digits += 1;
                }
                j += 1;
            }
            // A run must end on a digit to count as a card candidate.
            let mut end = j;
            while end > i && !chars[end - 1].is_ascii_digit() {
                end -= 1;
            }
            if digits >= 13 && end > i {
                out.push_str("[REDACTED_CARD]");
                i = end;
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

fn truncate_content(text: &str) -> String {
    if text.chars().count() <= MAX_CONTENT_CHARS {
        return text.to_string();
    }
    let mut truncated: String = text.chars().take(MAX_CONTENT_CHARS).collect();
    truncated.push_str("[TRUNCATED]");
    truncated
}

fn sanitize_value(value: &AttributeValue) -> AttributeValue {
    match value {
        AttributeValue::StringValue(text) => {
            AttributeValue::StringValue(truncate_content(&redact_secrets(text)))
        }
        other => {
            // Structured message payloads are flattened to redacted JSON text;
            // the queryable projection never needed their inner shape.
            let text = serde_json::to_string(other).unwrap_or_default();
            AttributeValue::StringValue(truncate_content(&redact_secrets(&text)))
        }
    }
}

/// Apply the tenant privacy policy to a span before it is written anywhere
/// (D1 hot store and the Basin dual-write both consume the sanitized span).
pub fn sanitize_span(span: &mut Span, capture_content: bool) {
    for key in CONTENT_ATTRIBUTE_KEYS {
        match span.attributes.0.get_mut(*key) {
            Some(Some(value)) if capture_content => {
                *value = sanitize_value(value);
            }
            Some(slot) => *slot = None,
            None => {}
        }
    }
}

// --- Cost --------------------------------------------------------------------

/// Pick the price in force at span time: model must match, the newest
/// `effective_from` not after the span wins, and a provider match beats a
/// provider-agnostic fallback at the same timestamp.
pub fn select_price<'a>(
    prices: &'a [ModelPrice],
    provider: &str,
    model: &str,
    at: Timestamp,
) -> Option<&'a ModelPrice> {
    prices
        .iter()
        .filter(|price| {
            price.model.eq_ignore_ascii_case(model)
                && price.effective_from.fractional() <= at.fractional()
        })
        .max_by(|a, b| {
            let a_provider = a.provider.eq_ignore_ascii_case(provider);
            let b_provider = b.provider.eq_ignore_ascii_case(provider);
            a_provider
                .cmp(&b_provider)
                .then_with(|| a.effective_from.cmp(&b.effective_from))
                .then_with(|| a.provider.cmp(&b.provider))
        })
}

/// USD cost of one span: tokens x per-Mtok rates / 1e6. Missing cache rates
/// fall back to the input rate (documented P1 control-plane policy).
pub fn compute_cost(
    price: &ModelPrice,
    input_tokens: i64,
    output_tokens: i64,
    cache_read_tokens: i64,
    cache_creation_tokens: i64,
) -> f64 {
    let cache_read_rate = price.cache_read_per_mtok.unwrap_or(price.input_per_mtok);
    let cache_creation_rate = price
        .cache_creation_per_mtok
        .unwrap_or(price.input_per_mtok);
    (input_tokens as f64 * price.input_per_mtok
        + output_tokens as f64 * price.output_per_mtok
        + cache_read_tokens as f64 * cache_read_rate
        + cache_creation_tokens as f64 * cache_creation_rate)
        / 1_000_000.0
}

/// Build the normalized projection row for one (already sanitized) span.
/// Returns `None` for spans without any GenAI signal.
pub fn build_genai_record(
    span: &Span,
    tenant_id: &str,
    prices: &[ModelPrice],
) -> Option<GenAiSpanRecord> {
    if !is_genai_span(span) {
        return None;
    }

    let provider = attr_string(span, PROVIDER_KEYS).unwrap_or_else(|| "unknown".to_string());
    let request_model = attr_string(span, REQUEST_MODEL_KEYS);
    let response_model = attr_string(span, RESPONSE_MODEL_KEYS);
    let priced_model = response_model.clone().or_else(|| request_model.clone());

    let input_tokens = attr_i64(span, INPUT_TOKEN_KEYS);
    let output_tokens = attr_i64(span, OUTPUT_TOKEN_KEYS);
    let cache_read_tokens = attr_i64(span, CACHE_READ_TOKEN_KEYS);
    let cache_creation_tokens = attr_i64(span, CACHE_CREATION_TOKEN_KEYS);
    let has_tokens = input_tokens.is_some()
        || output_tokens.is_some()
        || cache_read_tokens.is_some()
        || cache_creation_tokens.is_some();

    let start_time: Timestamp = span.start_time.into();
    let end_time: Timestamp = span.end_time.into();

    let (cost_usd, price_provider, price_model, price_effective_from) =
        match (&priced_model, has_tokens) {
            (Some(model), true) => match select_price(prices, &provider, model, start_time) {
                Some(price) => (
                    Some(compute_cost(
                        price,
                        input_tokens.unwrap_or(0),
                        output_tokens.unwrap_or(0),
                        cache_read_tokens.unwrap_or(0),
                        cache_creation_tokens.unwrap_or(0),
                    )),
                    Some(price.provider.clone()),
                    Some(price.model.clone()),
                    Some(price.effective_from),
                ),
                None => (None, None, None, None),
            },
            _ => (None, None, None, None),
        };

    let service_name = span
        .resource_attributes
        .as_ref()
        .map(crate::data::models::service_name_from_resource)
        .unwrap_or_else(|| "unknown".to_string());

    Some(GenAiSpanRecord {
        tenant_id: tenant_id.to_string(),
        trace_id: span.trace_id.clone(),
        span_id: span.span_id.clone(),
        parent_span_id: span.parent_span_id.clone(),
        service_name,
        span_name: span.name.clone(),
        operation: infer_operation(span),
        provider,
        request_model,
        response_model,
        agent_name: attr_string(span, AGENT_NAME_KEYS),
        tool_name: attr_string(span, TOOL_NAME_KEYS),
        conversation_id: attr_string(span, CONVERSATION_KEYS),
        input_tokens,
        output_tokens,
        cache_read_tokens,
        cache_creation_tokens,
        ttft_ms: attr_f64(span, TTFT_KEYS).map(|seconds| seconds * 1000.0),
        duration_ms: (span.end_time.unix_timestamp_nanos() - span.start_time.unix_timestamp_nanos())
            as f64
            / 1_000_000.0,
        finish_reasons: finish_reasons(span),
        cost_usd,
        price_provider,
        price_model,
        price_effective_from,
        is_error: if is_error(span) { 1 } else { 0 },
        start_time,
        end_time,
    })
}

// --- Projections shared by API / UI / MCP -------------------------------------

/// API-facing view of one projected span. `depth` is the replay-tree depth
/// (0 outside a run). `price_version` pins the exact price row that produced
/// `cost_usd`, so a later price change never rewrites history.
#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct GenAiSpanView {
    pub tenant_id: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: Option<String>,
    pub service_name: String,
    pub span_name: String,
    pub operation: String,
    pub provider: String,
    pub request_model: Option<String>,
    pub response_model: Option<String>,
    pub agent_name: Option<String>,
    pub tool_name: Option<String>,
    pub conversation_id: Option<String>,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cache_read_tokens: Option<i64>,
    pub cache_creation_tokens: Option<i64>,
    pub ttft_ms: Option<f64>,
    pub duration_ms: f64,
    pub finish_reasons: String,
    pub cost_usd: Option<f64>,
    pub price_version: Option<String>,
    pub is_error: bool,
    pub start_time: f64,
    pub end_time: f64,
    pub depth: u32,
}

impl GenAiSpanView {
    pub fn from_record(record: &GenAiSpanRecord, depth: u32) -> Self {
        let price_version = match (
            &record.price_provider,
            &record.price_model,
            &record.price_effective_from,
        ) {
            (Some(provider), Some(model), Some(effective_from)) => Some(format!(
                "{}:{}@{}",
                provider,
                model,
                effective_from.fractional()
            )),
            _ => None,
        };
        Self {
            tenant_id: record.tenant_id.clone(),
            trace_id: record.trace_id.as_inner().to_string(),
            span_id: record.span_id.as_inner().to_string(),
            parent_span_id: record
                .parent_span_id
                .as_ref()
                .map(|id| id.as_inner().to_string()),
            service_name: record.service_name.clone(),
            span_name: record.span_name.clone(),
            operation: record.operation.clone(),
            provider: record.provider.clone(),
            request_model: record.request_model.clone(),
            response_model: record.response_model.clone(),
            agent_name: record.agent_name.clone(),
            tool_name: record.tool_name.clone(),
            conversation_id: record.conversation_id.clone(),
            input_tokens: record.input_tokens,
            output_tokens: record.output_tokens,
            cache_read_tokens: record.cache_read_tokens,
            cache_creation_tokens: record.cache_creation_tokens,
            ttft_ms: record.ttft_ms,
            duration_ms: record.duration_ms,
            finish_reasons: record.finish_reasons.clone(),
            cost_usd: record.cost_usd,
            price_version,
            is_error: record.is_error != 0,
            start_time: record.start_time.fractional(),
            end_time: record.end_time.fractional(),
            depth,
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, PartialEq, Serialize)]
pub struct AiTotals {
    pub span_count: u64,
    pub trace_count: u64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_creation_tokens: i64,
    pub total_tokens: i64,
    pub cost_usd: f64,
    pub priced_span_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AiModelCost {
    pub provider: String,
    pub model: String,
    pub span_count: u64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_creation_tokens: i64,
    pub total_tokens: i64,
    pub cost_usd: f64,
    pub priced_span_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub avg_ttft_ms: Option<f64>,
    pub p95_duration_ms: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AiOperationCount {
    pub operation: String,
    pub span_count: u64,
    pub error_count: u64,
    pub cost_usd: f64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AiOverview {
    pub totals: AiTotals,
    pub by_model: Vec<AiModelCost>,
    pub by_operation: Vec<AiOperationCount>,
}

fn token_sum(records: &[&GenAiSpanRecord], pick: fn(&GenAiSpanRecord) -> Option<i64>) -> i64 {
    records.iter().map(|r| pick(r).unwrap_or(0)).sum()
}

pub fn summarize_overview(records: &[GenAiSpanRecord]) -> AiOverview {
    let refs: Vec<&GenAiSpanRecord> = records.iter().collect();
    let input = token_sum(&refs, |r| r.input_tokens);
    let output = token_sum(&refs, |r| r.output_tokens);
    let cache_read = token_sum(&refs, |r| r.cache_read_tokens);
    let cache_creation = token_sum(&refs, |r| r.cache_creation_tokens);
    let error_count = records.iter().filter(|r| r.is_error != 0).count() as u64;
    let traces: std::collections::BTreeSet<&str> =
        records.iter().map(|r| r.trace_id.as_inner()).collect();

    let totals = AiTotals {
        span_count: records.len() as u64,
        trace_count: traces.len() as u64,
        input_tokens: input,
        output_tokens: output,
        cache_read_tokens: cache_read,
        cache_creation_tokens: cache_creation,
        total_tokens: input + output + cache_read + cache_creation,
        cost_usd: records.iter().filter_map(|r| r.cost_usd).sum(),
        priced_span_count: records.iter().filter(|r| r.cost_usd.is_some()).count() as u64,
        error_count,
        error_rate: if records.is_empty() {
            0.0
        } else {
            error_count as f64 / records.len() as f64
        },
    };

    let mut by_model_map: std::collections::BTreeMap<(String, String), Vec<&GenAiSpanRecord>> =
        std::collections::BTreeMap::new();
    for record in records {
        let model = record
            .response_model
            .clone()
            .or_else(|| record.request_model.clone())
            .unwrap_or_else(|| "unknown".to_string());
        by_model_map
            .entry((record.provider.clone(), model))
            .or_default()
            .push(record);
    }
    let mut by_model: Vec<AiModelCost> = by_model_map
        .into_iter()
        .map(|((provider, model), rows)| {
            let input = token_sum(&rows, |r| r.input_tokens);
            let output = token_sum(&rows, |r| r.output_tokens);
            let cache_read = token_sum(&rows, |r| r.cache_read_tokens);
            let cache_creation = token_sum(&rows, |r| r.cache_creation_tokens);
            let errors = rows.iter().filter(|r| r.is_error != 0).count() as u64;
            let ttfts: Vec<f64> = rows.iter().filter_map(|r| r.ttft_ms).collect();
            let mut durations: Vec<f64> = rows.iter().map(|r| r.duration_ms).collect();
            durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
            AiModelCost {
                provider,
                model,
                span_count: rows.len() as u64,
                input_tokens: input,
                output_tokens: output,
                cache_read_tokens: cache_read,
                cache_creation_tokens: cache_creation,
                total_tokens: input + output + cache_read + cache_creation,
                cost_usd: rows.iter().filter_map(|r| r.cost_usd).sum(),
                priced_span_count: rows.iter().filter(|r| r.cost_usd.is_some()).count() as u64,
                error_count: errors,
                error_rate: errors as f64 / rows.len() as f64,
                avg_ttft_ms: if ttfts.is_empty() {
                    None
                } else {
                    Some(ttfts.iter().sum::<f64>() / ttfts.len() as f64)
                },
                p95_duration_ms: crate::query::percentile(&durations, 95.0),
            }
        })
        .collect();
    by_model.sort_by(|a, b| {
        b.cost_usd
            .partial_cmp(&a.cost_usd)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.provider.cmp(&b.provider))
            .then_with(|| a.model.cmp(&b.model))
    });

    let mut by_operation_map: std::collections::BTreeMap<&str, Vec<&GenAiSpanRecord>> =
        std::collections::BTreeMap::new();
    for record in records {
        by_operation_map
            .entry(record.operation.as_str())
            .or_default()
            .push(record);
    }
    let by_operation = by_operation_map
        .into_iter()
        .map(|(operation, rows)| AiOperationCount {
            operation: operation.to_string(),
            span_count: rows.len() as u64,
            error_count: rows.iter().filter(|r| r.is_error != 0).count() as u64,
            cost_usd: rows.iter().filter_map(|r| r.cost_usd).sum(),
        })
        .collect();

    AiOverview {
        totals,
        by_model,
        by_operation,
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentRunSummary {
    pub trace_id: String,
    pub agent_name: Option<String>,
    pub service_name: String,
    pub conversation_id: Option<String>,
    pub started_at: f64,
    pub duration_ms: f64,
    pub span_count: u64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub total_cost_usd: f64,
    pub error_count: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct AgentRun {
    pub tenant_id: String,
    pub trace_id: String,
    pub agent_name: Option<String>,
    pub service_name: String,
    pub conversation_id: Option<String>,
    pub started_at: f64,
    pub ended_at: f64,
    pub duration_ms: f64,
    pub span_count: u64,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cache_read_tokens: i64,
    pub cache_creation_tokens: i64,
    pub total_cost_usd: f64,
    pub priced_span_count: u64,
    pub error_count: u64,
    pub spans: Vec<GenAiSpanView>,
}

fn sorted_run_records<'a>(
    records: &'a [GenAiSpanRecord],
    trace_id: &str,
) -> Vec<&'a GenAiSpanRecord> {
    let mut rows: Vec<&GenAiSpanRecord> = records
        .iter()
        .filter(|r| r.trace_id.as_inner() == trace_id)
        .collect();
    rows.sort_by(|a, b| {
        a.start_time
            .fractional()
            .partial_cmp(&b.start_time.fractional())
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.span_id.as_inner().cmp(b.span_id.as_inner()))
    });
    rows
}

fn depth_of(
    record: &GenAiSpanRecord,
    by_span: &std::collections::BTreeMap<String, &GenAiSpanRecord>,
) -> u32 {
    let mut depth = 0;
    let mut current = record;
    let mut guard = 0;
    while let Some(parent_id) = &current.parent_span_id {
        guard += 1;
        if guard > 1024 {
            break;
        }
        match by_span.get(parent_id.as_inner()) {
            Some(parent) => {
                depth += 1;
                current = parent;
            }
            None => break,
        }
    }
    depth
}

/// Build one agent run (replay) from projected spans of a single trace.
/// Returns `None` when the trace has no GenAI spans.
pub fn build_agent_run(
    tenant_id: &str,
    trace_id: &str,
    records: &[GenAiSpanRecord],
) -> Option<AgentRun> {
    let rows = sorted_run_records(records, trace_id);
    if rows.is_empty() {
        return None;
    }
    let by_span: std::collections::BTreeMap<String, &GenAiSpanRecord> = rows
        .iter()
        .map(|r| (r.span_id.as_inner().to_string(), *r))
        .collect();
    let spans: Vec<GenAiSpanView> = rows
        .iter()
        .map(|r| GenAiSpanView::from_record(r, depth_of(r, &by_span)))
        .collect();

    let refs: Vec<&GenAiSpanRecord> = rows.clone();
    let root = rows[0];
    let started_at = rows
        .iter()
        .map(|r| r.start_time.fractional())
        .fold(f64::INFINITY, f64::min);
    let ended_at = rows
        .iter()
        .map(|r| r.end_time.fractional())
        .fold(f64::NEG_INFINITY, f64::max);

    Some(AgentRun {
        tenant_id: tenant_id.to_string(),
        trace_id: trace_id.to_string(),
        agent_name: rows
            .iter()
            .filter(|r| r.operation == "invoke_agent")
            .find_map(|r| r.agent_name.clone())
            .or_else(|| rows.iter().find_map(|r| r.agent_name.clone())),
        service_name: root.service_name.clone(),
        conversation_id: rows.iter().find_map(|r| r.conversation_id.clone()),
        started_at,
        ended_at,
        duration_ms: (ended_at - started_at) * 1000.0,
        span_count: rows.len() as u64,
        input_tokens: token_sum(&refs, |r| r.input_tokens),
        output_tokens: token_sum(&refs, |r| r.output_tokens),
        cache_read_tokens: token_sum(&refs, |r| r.cache_read_tokens),
        cache_creation_tokens: token_sum(&refs, |r| r.cache_creation_tokens),
        total_cost_usd: rows.iter().filter_map(|r| r.cost_usd).sum(),
        priced_span_count: rows.iter().filter(|r| r.cost_usd.is_some()).count() as u64,
        error_count: rows.iter().filter(|r| r.is_error != 0).count() as u64,
        spans,
    })
}

/// One summary row per trace that contains GenAI spans, newest first.
pub fn summarize_runs(records: &[GenAiSpanRecord]) -> Vec<AgentRunSummary> {
    let mut by_trace: std::collections::BTreeMap<&str, Vec<&GenAiSpanRecord>> =
        std::collections::BTreeMap::new();
    for record in records {
        by_trace
            .entry(record.trace_id.as_inner())
            .or_default()
            .push(record);
    }
    let mut runs: Vec<AgentRunSummary> = by_trace
        .into_iter()
        .filter_map(|(trace_id, rows)| {
            let run = build_agent_run(
                rows.first().map(|r| r.tenant_id.as_str()).unwrap_or(""),
                trace_id,
                &rows.iter().map(|r| (*r).clone()).collect::<Vec<_>>(),
            )?;
            Some(AgentRunSummary {
                trace_id: run.trace_id,
                agent_name: run.agent_name,
                service_name: run.service_name,
                conversation_id: run.conversation_id,
                started_at: run.started_at,
                duration_ms: run.duration_ms,
                span_count: run.span_count,
                input_tokens: run.input_tokens,
                output_tokens: run.output_tokens,
                total_cost_usd: run.total_cost_usd,
                error_count: run.error_count,
            })
        })
        .collect();
    runs.sort_by(|a, b| {
        b.started_at
            .partial_cmp(&a.started_at)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.trace_id.cmp(&b.trace_id))
    });
    runs
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct ToolHealth {
    pub tool_name: String,
    pub call_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub avg_duration_ms: f64,
    pub p95_duration_ms: f64,
}

/// Tool health from `execute_tool` spans, worst offenders first.
pub fn summarize_tools(records: &[GenAiSpanRecord]) -> Vec<ToolHealth> {
    let mut by_tool: std::collections::BTreeMap<String, Vec<&GenAiSpanRecord>> =
        std::collections::BTreeMap::new();
    for record in records.iter().filter(|r| r.operation == "execute_tool") {
        let name = record
            .tool_name
            .clone()
            .unwrap_or_else(|| record.span_name.clone());
        by_tool.entry(name).or_default().push(record);
    }
    let mut tools: Vec<ToolHealth> = by_tool
        .into_iter()
        .map(|(tool_name, rows)| {
            let errors = rows.iter().filter(|r| r.is_error != 0).count() as u64;
            let mut durations: Vec<f64> = rows.iter().map(|r| r.duration_ms).collect();
            durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
            ToolHealth {
                tool_name,
                call_count: rows.len() as u64,
                error_count: errors,
                error_rate: errors as f64 / rows.len() as f64,
                avg_duration_ms: durations.iter().sum::<f64>() / rows.len() as f64,
                p95_duration_ms: crate::query::percentile(&durations, 95.0),
            }
        })
        .collect();
    tools.sort_by(|a, b| {
        b.error_count
            .cmp(&a.error_count)
            .then_with(|| b.call_count.cmp(&a.call_count))
            .then_with(|| a.tool_name.cmp(&b.tool_name))
    });
    tools
}

#[derive(Clone, Debug, Default)]
pub struct AiSearchFilter {
    pub query: Option<String>,
    pub operation: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub conversation_id: Option<String>,
    pub has_error: Option<bool>,
    pub limit: usize,
}

fn contains(haystack: &str, needle: &str) -> bool {
    haystack.to_lowercase().contains(&needle.to_lowercase())
}

/// Deterministic substring/equality filtering over projected spans, in the
/// order given (callers pass newest-first store rows).
pub fn search_spans(records: &[GenAiSpanRecord], filter: &AiSearchFilter) -> Vec<GenAiSpanView> {
    records
        .iter()
        .filter(|record| {
            if let Some(operation) = &filter.operation {
                if record.operation != *operation {
                    return false;
                }
            }
            if let Some(provider) = &filter.provider {
                if !contains(&record.provider, provider) {
                    return false;
                }
            }
            if let Some(model) = &filter.model {
                let matches = record
                    .response_model
                    .as_deref()
                    .map(|m| contains(m, model))
                    .unwrap_or(false)
                    || record
                        .request_model
                        .as_deref()
                        .map(|m| contains(m, model))
                        .unwrap_or(false);
                if !matches {
                    return false;
                }
            }
            if let Some(conversation_id) = &filter.conversation_id {
                if record.conversation_id.as_deref() != Some(conversation_id.as_str()) {
                    return false;
                }
            }
            if let Some(has_error) = filter.has_error {
                if (record.is_error != 0) != has_error {
                    return false;
                }
            }
            if let Some(query) = &filter.query {
                let fields = [
                    record.span_name.as_str(),
                    record.operation.as_str(),
                    record.provider.as_str(),
                    record.agent_name.as_deref().unwrap_or(""),
                    record.tool_name.as_deref().unwrap_or(""),
                    record.response_model.as_deref().unwrap_or(""),
                    record.request_model.as_deref().unwrap_or(""),
                ];
                if !fields.iter().any(|field| contains(field, query)) {
                    return false;
                }
            }
            true
        })
        .take(if filter.limit == 0 {
            usize::MAX
        } else {
            filter.limit
        })
        .map(|record| GenAiSpanView::from_record(record, 0))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::models::{AttributeMap, SpanKind};
    use crate::data::models::HexEncodedId;
    use std::collections::BTreeMap;

    fn span_with(attrs: Vec<(&str, AttributeValue)>) -> Span {
        let mut map = BTreeMap::new();
        for (key, value) in attrs {
            map.insert(key.to_string(), Some(value));
        }
        let start = time::OffsetDateTime::from_unix_timestamp(1_759_276_800).unwrap();
        Span {
            trace_id: HexEncodedId::new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa").unwrap(),
            span_id: HexEncodedId::new("0000000000000001").unwrap(),
            parent_span_id: None,
            name: "chat".to_string(),
            trace_state: None,
            flags: None,
            kind: Some(SpanKind::Client),
            scope_name: None,
            scope_version: None,
            start_time: start,
            end_time: start + time::Duration::milliseconds(250),
            attributes: AttributeMap(map),
            scope_attributes: None,
            resource_attributes: None,
            status: None,
            events: vec![],
            links: vec![],
        }
    }

    fn price(effective_from: f64, input: f64, output: f64) -> ModelPrice {
        ModelPrice {
            provider: "openai".to_string(),
            model: "gpt-x".to_string(),
            input_per_mtok: input,
            output_per_mtok: output,
            cache_read_per_mtok: Some(0.5),
            cache_creation_per_mtok: None,
            effective_from: Timestamp::try_from(effective_from).unwrap(),
        }
    }

    #[test]
    fn mapping_uses_current_names_and_legacy_aliases() {
        let current = span_with(vec![
            (
                "gen_ai.operation.name",
                AttributeValue::StringValue("chat".into()),
            ),
            (
                "gen_ai.provider.name",
                AttributeValue::StringValue("openai".into()),
            ),
            (
                "gen_ai.request.model",
                AttributeValue::StringValue("gpt-x".into()),
            ),
            ("gen_ai.usage.input_tokens", AttributeValue::IntValue(1000)),
            ("gen_ai.usage.output_tokens", AttributeValue::IntValue(500)),
        ]);
        let record = build_genai_record(&current, "default", &[]).unwrap();
        assert_eq!(record.provider, "openai");
        assert_eq!(record.operation, "chat");
        assert_eq!(record.input_tokens, Some(1000));

        // Legacy instrumentation: gen_ai.system + prompt/completion token names.
        let legacy = span_with(vec![
            (
                "gen_ai.system",
                AttributeValue::StringValue("anthropic".into()),
            ),
            (
                "gen_ai.request.model",
                AttributeValue::StringValue("claude-x".into()),
            ),
            ("gen_ai.usage.prompt_tokens", AttributeValue::IntValue(42)),
            (
                "gen_ai.usage.completion_tokens",
                AttributeValue::IntValue(7),
            ),
        ]);
        let record = build_genai_record(&legacy, "default", &[]).unwrap();
        assert_eq!(record.provider, "anthropic");
        assert_eq!(record.operation, "chat");
        assert_eq!(record.input_tokens, Some(42));
        assert_eq!(record.output_tokens, Some(7));
    }

    #[test]
    fn non_genai_spans_are_not_projected() {
        let plain = span_with(vec![(
            "http.route",
            AttributeValue::StringValue("/pay".into()),
        )]);
        assert!(build_genai_record(&plain, "default", &[]).is_none());
    }

    #[test]
    fn cost_is_exact_token_times_price() {
        let span = span_with(vec![
            (
                "gen_ai.operation.name",
                AttributeValue::StringValue("chat".into()),
            ),
            (
                "gen_ai.provider.name",
                AttributeValue::StringValue("openai".into()),
            ),
            (
                "gen_ai.response.model",
                AttributeValue::StringValue("gpt-x".into()),
            ),
            ("gen_ai.usage.input_tokens", AttributeValue::IntValue(1000)),
            ("gen_ai.usage.output_tokens", AttributeValue::IntValue(500)),
            (
                "gen_ai.usage.cache_read.input_tokens",
                AttributeValue::IntValue(200),
            ),
        ]);
        let prices = vec![price(1_759_000_000.0, 2.0, 8.0)];
        let record = build_genai_record(&span, "default", &prices).unwrap();
        // (1000*2.0 + 500*8.0 + 200*0.5) / 1e6 = 0.0061 exactly by formula.
        let expected = (1000.0 * 2.0 + 500.0 * 8.0 + 200.0 * 0.5) / 1_000_000.0;
        assert_eq!(record.cost_usd, Some(expected));
        assert_eq!(record.price_provider.as_deref(), Some("openai"));
        assert_eq!(
            record.price_effective_from.map(|t| t.fractional()),
            Some(1_759_000_000.0)
        );
    }

    #[test]
    fn price_change_applies_only_to_newer_spans() {
        let prices = vec![
            price(1_759_000_000.0, 2.0, 8.0),
            price(1_759_500_000.0, 4.0, 16.0),
        ];
        let at = Timestamp::try_from(1_759_276_800.0).unwrap();
        let chosen = select_price(&prices, "openai", "gpt-x", at).unwrap();
        assert_eq!(chosen.input_per_mtok, 2.0);

        let later = Timestamp::try_from(1_759_600_000.0).unwrap();
        let chosen = select_price(&prices, "openai", "gpt-x", later).unwrap();
        assert_eq!(chosen.input_per_mtok, 4.0);

        // No price in force yet -> no cost, never an invented number.
        let early = Timestamp::try_from(1_758_000_000.0).unwrap();
        assert!(select_price(&prices, "openai", "gpt-x", early).is_none());
    }

    #[test]
    fn privacy_drops_content_by_default() {
        let mut span = span_with(vec![
            (
                "gen_ai.operation.name",
                AttributeValue::StringValue("chat".into()),
            ),
            (
                "gen_ai.input.messages",
                AttributeValue::StringValue(
                    "[{\"role\":\"user\",\"content\":\"key sk-FAKEKEY1234567890\"}]".into(),
                ),
            ),
        ]);
        sanitize_span(&mut span, false);
        let stored = serde_json::to_string(&span.attributes).unwrap();
        assert!(!stored.contains("sk-FAKEKEY"));
        assert!(!stored.contains("FAKEKEY"));
    }

    #[test]
    fn privacy_opt_in_still_redacts_and_truncates() {
        let mut span = span_with(vec![
            (
                "gen_ai.operation.name",
                AttributeValue::StringValue("chat".into()),
            ),
            (
                "gen_ai.input.messages",
                AttributeValue::StringValue(
                    "contact me at jane.doe@example.com with key sk-FAKEKEY1234567890".into(),
                ),
            ),
        ]);
        sanitize_span(&mut span, true);
        let stored = serde_json::to_string(&span.attributes).unwrap();
        assert!(!stored.contains("sk-FAKEKEY1234567890"), "stored: {stored}");
        assert!(!stored.contains("jane.doe@"), "stored: {stored}");
        assert!(stored.contains("[REDACTED]"), "stored: {stored}");
        assert!(stored.contains("***@example.com"), "stored: {stored}");

        let long = "a".repeat(MAX_CONTENT_CHARS + 500);
        let mut span = span_with(vec![(
            "gen_ai.output.messages",
            AttributeValue::StringValue(long),
        )]);
        sanitize_span(&mut span, true);
        let stored = serde_json::to_string(&span.attributes).unwrap();
        assert!(stored.contains("[TRUNCATED]"));
        assert!(stored.chars().count() < MAX_CONTENT_CHARS + 100);
    }

    #[test]
    fn redact_handles_cards_and_aws_keys() {
        let text = redact_secrets("card 4111 1111 1111 1111 and AKIAIOSFODNN7EXAMPLE end");
        assert!(!text.contains("4111"), "text: {text}");
        assert!(!text.contains("AKIAIOSFODNN7EXAMPLE"), "text: {text}");
        assert!(text.contains("[REDACTED_CARD]"));
        // Ordinary numbers and words are untouched.
        assert_eq!(
            redact_secrets("order 12345 total 99.5"),
            "order 12345 total 99.5"
        );
    }

    #[test]
    fn run_replay_builds_depth_and_total_cost() {
        let prices = vec![price(1_759_000_000.0, 2.0, 8.0)];
        let mut root = span_with(vec![
            (
                "gen_ai.operation.name",
                AttributeValue::StringValue("invoke_agent".into()),
            ),
            (
                "gen_ai.agent.name",
                AttributeValue::StringValue("researcher".into()),
            ),
        ]);
        root.name = "invoke_agent".to_string();
        let mut child = span_with(vec![
            (
                "gen_ai.operation.name",
                AttributeValue::StringValue("chat".into()),
            ),
            (
                "gen_ai.provider.name",
                AttributeValue::StringValue("openai".into()),
            ),
            (
                "gen_ai.response.model",
                AttributeValue::StringValue("gpt-x".into()),
            ),
            ("gen_ai.usage.input_tokens", AttributeValue::IntValue(1000)),
            ("gen_ai.usage.output_tokens", AttributeValue::IntValue(500)),
        ]);
        child.span_id = HexEncodedId::new("0000000000000002").unwrap();
        child.parent_span_id = Some(root.span_id.clone());

        let records = vec![
            build_genai_record(&root, "default", &prices).unwrap(),
            build_genai_record(&child, "default", &prices).unwrap(),
        ];
        let run = build_agent_run("default", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", &records).unwrap();
        assert_eq!(run.span_count, 2);
        assert_eq!(run.agent_name.as_deref(), Some("researcher"));
        assert_eq!(run.spans[0].depth, 0);
        assert_eq!(run.spans[1].depth, 1);
        let expected = (1000.0 * 2.0 + 500.0 * 8.0) / 1_000_000.0;
        assert_eq!(run.total_cost_usd, expected);
        assert_eq!(
            run.spans[1].price_version.as_deref(),
            Some("openai:gpt-x@1759000000")
        );
    }
}
