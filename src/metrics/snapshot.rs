//! The registry as values: the same series the page prints, in the same
//! order, with the same names and labels.

use super::gauges::{GaugeProvider, MemberGaugeProvider, SHARD_GAUGES};
use super::registry::{
    read, read_with_member, HistogramReading, Reading, BUCKETS, ERROR_CODES, SCAN_COUNTERS,
};
use super::render::DURATION;
use super::routes::REQUEST_ROUTES;
use crate::pb::{HistogramBucketSample, HistogramSample, MetricKind, MetricLabel, MetricSample};

/// The registry as values: the same counters, gauges, and histograms
/// [`super::render()`] prints, in the same order, with the same names and
/// labels, so a dashboard and a scraper never disagree.
pub fn snapshot(process: &str, gauges: &[GaugeProvider]) -> crate::pb::MetricsSnapshot {
    snapshot_reading(process, &read(gauges))
}

/// The registry as values with member gauges sampled too.
pub fn snapshot_with_member(
    process: &str,
    gauges: &[GaugeProvider],
    members: &[MemberGaugeProvider],
) -> crate::pb::MetricsSnapshot {
    snapshot_reading(process, &read_with_member(gauges, members))
}

/// The snapshot for one [`Reading`].
pub fn snapshot_reading(process: &str, reading: &Reading) -> crate::pb::MetricsSnapshot {
    let mut samples = Vec::new();
    let mut histograms = Vec::new();
    request_samples(reading, &mut samples, &mut histograms);
    scan_samples(reading, &mut samples);
    gauge_samples(reading, &mut samples);
    crate::pb::MetricsSnapshot {
        unix_ms: crate::diagnostics::unix_ms(),
        process: process.to_string(),
        samples,
        histograms,
    }
}

fn label(name: &str, value: &str) -> MetricLabel {
    MetricLabel {
        name: name.to_string(),
        value: value.to_string(),
    }
}
fn counter(name: &str, labels: Vec<MetricLabel>, value: u64) -> MetricSample {
    MetricSample {
        name: name.to_string(),
        labels,
        kind: MetricKind::Counter as i32,
        value: value as f64,
    }
}
fn gauge(name: &str, labels: Vec<MetricLabel>, value: u64) -> MetricSample {
    MetricSample {
        name: name.to_string(),
        labels,
        kind: MetricKind::Gauge as i32,
        value: value as f64,
    }
}
fn histogram(labels: Vec<MetricLabel>, h: &HistogramReading) -> HistogramSample {
    let mut cumulative = 0u64;
    let mut buckets = Vec::with_capacity(BUCKETS.len());
    for (i, (_, le)) in BUCKETS.iter().enumerate() {
        cumulative += h.buckets[i];
        buckets.push(HistogramBucketSample {
            le: if *le == u64::MAX {
                f64::INFINITY
            } else {
                *le as f64 / 1e9
            },
            cumulative_count: cumulative,
        });
    }
    HistogramSample {
        name: DURATION.to_string(),
        labels,
        buckets,
        sum: h.sum_ns as f64 / 1e9,
        count: cumulative,
    }
}

/// The per-route series: arrivals, in-flight, latency histograms, and
/// errors.
fn request_samples(
    reading: &Reading,
    samples: &mut Vec<MetricSample>,
    histograms: &mut Vec<HistogramSample>,
) {
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        samples.push(counter(
            "turbovec_requests_total",
            vec![label("rpc", name)],
            reading.requests[i],
        ));
    }
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        samples.push(gauge(
            "turbovec_requests_in_flight",
            vec![label("rpc", name)],
            reading.in_flight[i],
        ));
    }
    for (i, (_, name, streaming)) in REQUEST_ROUTES.iter().enumerate() {
        if *streaming {
            histograms.push(histogram(
                vec![label("rpc", name), label("phase", "first_response")],
                &reading.first_response[i],
            ));
            histograms.push(histogram(
                vec![label("rpc", name), label("phase", "complete")],
                &reading.complete[i],
            ));
        } else {
            histograms.push(histogram(vec![label("rpc", name)], &reading.complete[i]));
        }
    }
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        for (c, code) in ERROR_CODES.iter().enumerate() {
            samples.push(counter(
                "turbovec_request_errors_total",
                vec![label("rpc", name), label("code", code)],
                reading.errors[i][c],
            ));
        }
    }
}

/// The process-wide scan and ingest counters.
fn scan_samples(reading: &Reading, samples: &mut Vec<MetricSample>) {
    for ((name, _, _), value) in SCAN_COUNTERS.iter().zip(reading.scan) {
        samples.push(counter(name, Vec::new(), value));
    }
    samples.push(counter(
        "turbovec_scan_batches_total",
        Vec::new(),
        reading.batches,
    ));
    samples.push(counter(
        "turbovec_scan_batched_jobs_total",
        Vec::new(),
        reading.batched_jobs,
    ));
}

/// The shard gauges, then the member's series when the process serves one.
fn gauge_samples(reading: &Reading, samples: &mut Vec<MetricSample>) {
    for (name, _, read) in SHARD_GAUGES {
        for shard in &reading.shards {
            samples.push(gauge(
                name,
                vec![label("slot_offset", &shard.slot_offset.to_string())],
                read(shard),
            ));
        }
    }
    let Some(member) = reading.members.first() else {
        return;
    };
    for (name, kind, _, value) in member.scalars() {
        samples.push(if kind == "counter" {
            counter(name, Vec::new(), value)
        } else {
            gauge(name, Vec::new(), value)
        });
    }
    if member.peer_rejections.is_empty() {
        samples.push(counter("raft_peer_rejections_total", Vec::new(), 0));
    }
    for rejection in &member.peer_rejections {
        samples.push(counter(
            "raft_peer_rejections_total",
            vec![
                label("peer", &rejection.peer.to_string()),
                label("action", &rejection.action),
                label("code", &rejection.code),
            ],
            rejection.count,
        ));
    }
}
