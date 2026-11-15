//! The exposition page: one reading rendered as Prometheus text.

use super::gauges::{GaugeProvider, MemberGaugeProvider, MemberGauges, ShardGauges, SHARD_GAUGES};
use super::registry::{
    read, read_with_member, HistogramReading, Reading, BUCKETS, ERROR_CODES, SCAN_COUNTERS,
};
use super::routes::REQUEST_ROUTES;

fn write_sample_head(out: &mut String, name: &str, labels: &str) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        out.push_str(labels);
        out.push('}');
    }
    out.push(' ');
}

/// Append one metric line. Public so gauge providers in other modules
/// render values the same way counters are rendered (Prometheus wants
/// consistent float formatting; integers print as integers).
pub fn write_metric(out: &mut String, name: &str, labels: &str, value: u64) {
    write_sample_head(out, name, labels);
    out.push_str(&value.to_string());
    out.push('\n');
}

/// Append one metric line whose value is a duration in seconds,
/// rendered as an exact decimal from the nanosecond total (no float
/// arithmetic, so `_sum` never shows a rounding tail); always carries
/// a decimal point so the value reads as a float.
pub fn write_metric_seconds(out: &mut String, name: &str, labels: &str, ns: u64) {
    write_sample_head(out, name, labels);
    out.push_str(&seconds_text(ns));
    out.push('\n');
}

/// `ns` nanoseconds as decimal seconds: `1_500_000` renders `0.0015`,
/// `0` renders `0.0`.
pub(super) fn seconds_text(ns: u64) -> String {
    let mut text = format!("{}.{:09}", ns / 1_000_000_000, ns % 1_000_000_000);
    while text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.push('0');
    }
    text
}

fn header(out: &mut String, name: &str, kind: &str, help: &str) {
    out.push_str("# HELP ");
    out.push_str(name);
    out.push(' ');
    out.push_str(help);
    out.push_str("\n# TYPE ");
    out.push_str(name);
    out.push(' ');
    out.push_str(kind);
    out.push('\n');
}

pub(super) const DURATION: &str = "turbovec_request_duration_seconds";

/// One histogram's exposition rows under `labels` (the `rpc` and, for
/// a streaming phase, `phase` labels): cumulative buckets, `_sum`,
/// `_count`. `_count` is the `+Inf` bucket by construction.
pub(super) fn write_histogram(out: &mut String, labels: &str, histogram: &HistogramReading) {
    let mut cumulative = 0u64;
    for (i, (le, _)) in BUCKETS.iter().enumerate() {
        cumulative += histogram.buckets[i];
        write_metric(
            out,
            &format!("{DURATION}_bucket"),
            &format!("{labels},le=\"{le}\""),
            cumulative,
        );
    }
    write_metric_seconds(out, &format!("{DURATION}_sum"), labels, histogram.sum_ns);
    write_metric(out, &format!("{DURATION}_count"), labels, cumulative);
}

/// Render the whole exposition page: the process-wide counters, then
/// every gauge provider in order. One [`read`], then [`render_reading`].
pub fn render(gauges: &[GaugeProvider]) -> String {
    render_reading(&read(gauges))
}

/// Render the whole exposition page with member gauges sampled too.
pub fn render_with_member(gauges: &[GaugeProvider], members: &[MemberGaugeProvider]) -> String {
    render_reading(&read_with_member(gauges, members))
}

/// The exposition page for one [`Reading`].
pub fn render_reading(reading: &Reading) -> String {
    let mut page = String::with_capacity(96 * 1024);
    write_request_rows(&mut page, reading);
    write_scan_rows(&mut page, reading);
    write_shard_gauges(&mut page, &reading.shards);
    write_member_gauges(&mut page, &reading.members);
    page
}

/// The per-route rows: arrivals, in-flight, latency histograms, and errors.
fn write_request_rows(out: &mut String, reading: &Reading) {
    header(
        out,
        "turbovec_requests_total",
        "counter",
        "Requests served, by RPC route (counted at arrival).",
    );
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        write_metric(
            out,
            "turbovec_requests_total",
            &format!("rpc=\"{name}\""),
            reading.requests[i],
        );
    }

    header(
        out,
        "turbovec_requests_in_flight",
        "gauge",
        "Requests inside a handler or with an open response stream, by RPC route.",
    );
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        write_metric(
            out,
            "turbovec_requests_in_flight",
            &format!("rpc=\"{name}\""),
            reading.in_flight[i],
        );
    }

    header(
        out,
        DURATION,
        "histogram",
        "Request latency in seconds from arrival, by RPC route; streaming routes \
         report phase=\"first_response\" (first message on the wire) and \
         phase=\"complete\" (stream ended).",
    );
    for (i, (_, name, streaming)) in REQUEST_ROUTES.iter().enumerate() {
        if *streaming {
            write_histogram(
                out,
                &format!("rpc=\"{name}\",phase=\"first_response\""),
                &reading.first_response[i],
            );
            write_histogram(
                out,
                &format!("rpc=\"{name}\",phase=\"complete\""),
                &reading.complete[i],
            );
        } else {
            write_histogram(out, &format!("rpc=\"{name}\""), &reading.complete[i]);
        }
    }

    header(
        out,
        "turbovec_request_errors_total",
        "counter",
        "Requests that ended in a gRPC error, by RPC route and canonical status code.",
    );
    for (i, (_, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        for (c, code) in ERROR_CODES.iter().enumerate() {
            write_metric(
                out,
                "turbovec_request_errors_total",
                &format!("rpc=\"{name}\",code=\"{code}\""),
                reading.errors[i][c],
            );
        }
    }
}

/// The process-wide scan and ingest counters.
fn write_scan_rows(out: &mut String, reading: &Reading) {
    for ((name, help, _), value) in SCAN_COUNTERS.iter().zip(reading.scan) {
        header(out, name, "counter", help);
        write_metric(out, name, "", value);
    }

    header(
        out,
        "turbovec_scan_batches_total",
        "counter",
        "Batched kernel passes (coalesced scans).",
    );
    write_metric(out, "turbovec_scan_batches_total", "", reading.batches);
    header(
        out,
        "turbovec_scan_batched_jobs_total",
        "counter",
        "Scan jobs that rode a batched pass.",
    );
    write_metric(
        out,
        "turbovec_scan_batched_jobs_total",
        "",
        reading.batched_jobs,
    );
}

/// The shard gauge rows: every shard's sample of one metric under a single
/// `# TYPE` header.
fn write_shard_gauges(out: &mut String, samples: &[ShardGauges]) {
    if samples.is_empty() {
        return;
    }
    for (name, help, read) in SHARD_GAUGES {
        header(out, name, "gauge", help);
        for sample in samples {
            write_metric(
                out,
                name,
                &format!("slot_offset=\"{}\"", sample.slot_offset),
                read(sample),
            );
        }
    }
}

/// The member gauge rows for one reading: at most one member sample,
/// because a process serves at most one member.
fn write_member_gauges(out: &mut String, members: &[MemberGauges]) {
    let Some(sample) = members.first() else {
        return;
    };
    for (name, kind, help, value) in sample.scalars() {
        header(out, name, kind, help);
        write_metric(out, name, "", value);
    }
    header(
        out,
        "raft_peer_rejections_total",
        "counter",
        "Peer messages rejected, by peer node id, RPC and status code.",
    );
    for rejection in &sample.peer_rejections {
        write_metric(
            out,
            "raft_peer_rejections_total",
            &format!(
                "peer=\"{}\",action=\"{}\",code=\"{}\"",
                rejection.peer, rejection.action, rejection.code
            ),
            rejection.count,
        );
    }
    if sample.peer_rejections.is_empty() {
        write_metric(out, "raft_peer_rejections_total", "", 0);
    }
}
