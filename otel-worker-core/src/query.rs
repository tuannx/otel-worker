//! Deterministic query math for P2 (services / operations / metrics).
//! Percentile definition is pinned here so D1, libsql and (later) Basin SQL
//! results are compared against the same contract: linear interpolation on
//! the sorted sample, index = p/100 * (n-1) — same as numpy default.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, PartialEq)]
pub struct ServiceSpanRow {
    pub service_name: String,
    pub name: String,
    pub start_time: crate::data::util::Timestamp,
    pub end_time: crate::data::util::Timestamp,
    pub is_error: i64,
}

impl ServiceSpanRow {
    pub fn duration_ms(&self) -> f64 {
        (self.end_time.fractional() - self.start_time.fractional()) * 1000.0
    }
}

pub fn percentile(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    if sorted.len() == 1 {
        return sorted[0];
    }
    let rank = p / 100.0 * (sorted.len() - 1) as f64;
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    if lo == hi {
        return sorted[lo];
    }
    sorted[lo] + (sorted[hi] - sorted[lo]) * (rank - lo as f64)
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct ServiceSummary {
    pub service_name: String,
    pub span_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub p50_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub avg_ms: f64,
    pub first_seen: f64,
    pub last_seen: f64,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct OperationSummary {
    pub service_name: String,
    pub name: String,
    pub span_count: u64,
    pub error_count: u64,
    pub error_rate: f64,
    pub p95_ms: f64,
    pub avg_ms: f64,
}

fn summarize_durations(durations: &mut Vec<f64>) -> (f64, f64, f64, f64) {
    durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let avg = if durations.is_empty() {
        0.0
    } else {
        durations.iter().sum::<f64>() / durations.len() as f64
    };
    (
        percentile(durations, 50.0),
        percentile(durations, 95.0),
        percentile(durations, 99.0),
        avg,
    )
}

pub fn summarize_services(rows: &[ServiceSpanRow]) -> Vec<ServiceSummary> {
    let mut by_service: std::collections::BTreeMap<&str, Vec<&ServiceSpanRow>> =
        std::collections::BTreeMap::new();
    for row in rows {
        by_service.entry(&row.service_name).or_default().push(row);
    }
    by_service
        .into_iter()
        .map(|(service_name, rows)| {
            let mut durations: Vec<f64> = rows.iter().map(|r| r.duration_ms()).collect();
            let error_count = rows.iter().filter(|r| r.is_error != 0).count() as u64;
            let (p50_ms, p95_ms, p99_ms, avg_ms) = summarize_durations(&mut durations);
            ServiceSummary {
                service_name: service_name.to_string(),
                span_count: rows.len() as u64,
                error_count,
                error_rate: if rows.is_empty() {
                    0.0
                } else {
                    error_count as f64 / rows.len() as f64
                },
                p50_ms,
                p95_ms,
                p99_ms,
                avg_ms,
                first_seen: rows
                    .iter()
                    .map(|r| r.start_time.fractional())
                    .fold(f64::INFINITY, f64::min),
                last_seen: rows
                    .iter()
                    .map(|r| r.end_time.fractional())
                    .fold(f64::NEG_INFINITY, f64::max),
            }
        })
        .collect()
}

pub fn summarize_operations(rows: &[ServiceSpanRow]) -> Vec<OperationSummary> {
    let mut by_op: std::collections::BTreeMap<(&str, &str), Vec<&ServiceSpanRow>> =
        std::collections::BTreeMap::new();
    for row in rows {
        by_op
            .entry((&row.service_name, &row.name))
            .or_default()
            .push(row);
    }
    by_op
        .into_iter()
        .map(|((service_name, name), rows)| {
            let mut durations: Vec<f64> = rows.iter().map(|r| r.duration_ms()).collect();
            let error_count = rows.iter().filter(|r| r.is_error != 0).count() as u64;
            let (_, p95_ms, _, avg_ms) = summarize_durations(&mut durations);
            OperationSummary {
                service_name: service_name.to_string(),
                name: name.to_string(),
                span_count: rows.len() as u64,
                error_count,
                error_rate: if rows.is_empty() {
                    0.0
                } else {
                    error_count as f64 / rows.len() as f64
                },
                p95_ms,
                avg_ms,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_pinned_definition() {
        let v: Vec<f64> = (1..=100).map(|i| i as f64).collect();
        assert_eq!(percentile(&v, 50.0), 50.5);
        assert!((percentile(&v, 95.0) - 95.05).abs() < 1e-9);
        assert_eq!(percentile(&[], 95.0), 0.0);
        assert_eq!(percentile(&[7.0], 99.0), 7.0);
    }
}
