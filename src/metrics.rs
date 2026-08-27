//! Process-wide metrics and the Prometheus text-format exporter
//! (`docs/metrics.md`).
//!
//! Hand-rolled on purpose. The exposition format is a few lines of
//! `name{label="value"} 123\n` text and the scrape protocol is one GET,
//! so neither a metrics framework nor an HTTP framework earns a place
//! in the serving binary for it — the same argument that keeps CEL and
//! regex crates out (`docs/cel-filters.md`).
//!
//! The design splits along lifetime lines:
//!
//! - **Counters** are process-wide statics, incremented at the few
//!   places the engine already counts things (`record_scan`,
//!   `inc_request`, `add_ingested`). Monotone, cheap (one relaxed
//!   atomic add), and always on — whether or not an exporter serves
//!   them.
//! - **Gauges** are read at SCRAPE time from live shard state, through
//!   closures the binary hands to [`serve`]. Nothing has to remember
//!   to update a gauge on every mutation, so a gauge can never go
//!   stale — it is the state, sampled.

use std::sync::atomic::{AtomicU64, Ordering};

/// One RPC route the exporter counts. The set is fixed at compile time
/// (`REQUEST_ROUTES`), which is what lets the counters be plain statics
/// with no label registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    SearchShard,
    StreamSearch,
    BrowseShard,
    HybridShard,
    ShardLegs,
    Bm25Query,
    TermStats,
    VectorRescore,
    Bm25Rescore,
    FetchValues,
    GetDocuments,
    AddDocuments,
    AddVectors,
    IngestMapped,
}

/// Route names as they appear in the `rpc` label, parallel to the
/// counter table.
const REQUEST_ROUTES: [(Route, &str); 14] = [
    (Route::SearchShard, "search_shard"),
    (Route::StreamSearch, "stream_search"),
    (Route::BrowseShard, "browse_shard"),
    (Route::HybridShard, "hybrid_shard"),
    (Route::ShardLegs, "shard_legs"),
    (Route::Bm25Query, "bm25_query"),
    (Route::TermStats, "term_stats"),
    (Route::VectorRescore, "vector_rescore"),
    (Route::Bm25Rescore, "bm25_rescore"),
    (Route::FetchValues, "fetch_values"),
    (Route::GetDocuments, "get_documents"),
    (Route::AddDocuments, "add_documents"),
    (Route::AddVectors, "add_vectors"),
    (Route::IngestMapped, "ingest_mapped"),
];

// A `const` here is the repeat-element initializer for the array
// below, not a shared value: each array slot gets its OWN atomic (the
// interior-mutability lint's hazard — accidentally sharing one — is
// exactly what the repeat semantics avoid).
#[allow(clippy::declare_interior_mutable_const)]
const ZERO: AtomicU64 = AtomicU64::new(0);
static REQUESTS: [AtomicU64; REQUEST_ROUTES.len()] = [ZERO; REQUEST_ROUTES.len()];

static SCAN_CHUNK_CALLS: AtomicU64 = AtomicU64::new(0);
static SCAN_CANDIDATES: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOORS_OFFERED: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOORS_PUBLISHED: AtomicU64 = AtomicU64::new(0);
static SCAN_FLOOR_UPDATES_APPLIED: AtomicU64 = AtomicU64::new(0);
static DOCUMENTS_ADDED: AtomicU64 = AtomicU64::new(0);
static VECTORS_ADDED: AtomicU64 = AtomicU64::new(0);

/// Count one served request on `route`, at the top of its handler —
/// arrivals, not successes, so a shard erroring under load is visible
/// as traffic rather than invisible as silence.
pub fn inc_request(route: Route) {
    let i = REQUEST_ROUTES
        .iter()
        .position(|&(r, _)| r == route)
        .expect("every Route has a table row");
    REQUESTS[i].fetch_add(1, Ordering::Relaxed);
}

/// Fold one completed vector scan's stats into the process totals.
/// Called where the scan produces its `ScanOutcome`, so every route
/// through the scheduler (batched or solo) is counted once.
pub fn record_scan(stats: &crate::chunked::ScanStats) {
    SCAN_CHUNK_CALLS.fetch_add(u64::from(stats.chunk_calls), Ordering::Relaxed);
    SCAN_CANDIDATES.fetch_add(stats.candidates_collected, Ordering::Relaxed);
    SCAN_FLOORS_OFFERED.fetch_add(stats.floors_offered, Ordering::Relaxed);
    SCAN_FLOORS_PUBLISHED.fetch_add(stats.floors_published, Ordering::Relaxed);
    SCAN_FLOOR_UPDATES_APPLIED.fetch_add(stats.floor_updates_applied, Ordering::Relaxed);
}

/// Count ingested items: `documents` from AddDocuments streams,
/// `vectors` from AddVectors batches.
pub fn add_ingested(documents: u64, vectors: u64) {
    if documents > 0 {
        DOCUMENTS_ADDED.fetch_add(documents, Ordering::Relaxed);
    }
    if vectors > 0 {
        VECTORS_ADDED.fetch_add(vectors, Ordering::Relaxed);
    }
}

/// One shard's gauges, sampled at SCRAPE time from live state, so a
/// gauge can never go stale and never needs an update site.
#[derive(Debug, Clone)]
pub struct ShardGauges {
    /// The shard's identity label value: its slot offset, the shard's
    /// name in the global id space.
    pub slot_offset: u64,
    /// Vectors in the shard's index.
    pub vectors: u64,
    /// Documents in the shard's postings.
    pub documents: u64,
    /// The BM25 statistics epoch (advances on every mutation).
    pub stats_epoch: u64,
}

/// A live-state gauge sampler for one shard. Returns VALUES rather
/// than rendering text so [`render`] can group all shards' samples of
/// one metric under a single `# TYPE` header, as the exposition
/// format requires.
pub type GaugeProvider = Box<dyn Fn() -> ShardGauges + Send + Sync>;

/// Append one metric line. Public so gauge providers in other modules
/// render values the same way counters are rendered (Prometheus wants
/// consistent float formatting; integers print as integers).
pub fn write_metric(out: &mut String, name: &str, labels: &str, value: u64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        out.push_str(labels);
        out.push('}');
    }
    out.push(' ');
    out.push_str(&value.to_string());
    out.push('\n');
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

/// Render the whole exposition page: the process-wide counters, then
/// every gauge provider in order.
pub fn render(gauges: &[GaugeProvider]) -> String {
    let mut out = String::with_capacity(4096);

    header(
        &mut out,
        "turbovec_requests_total",
        "counter",
        "Requests served, by RPC route (counted at arrival).",
    );
    for (i, (_, name)) in REQUEST_ROUTES.iter().enumerate() {
        write_metric(
            &mut out,
            "turbovec_requests_total",
            &format!("rpc=\"{name}\""),
            REQUESTS[i].load(Ordering::Relaxed),
        );
    }

    for (name, help, counter) in [
        (
            "turbovec_scan_chunk_calls_total",
            "Per-chunk kernel calls made by vector scans.",
            &SCAN_CHUNK_CALLS,
        ),
        (
            "turbovec_scan_candidates_total",
            "Real candidates collected by vector scans (floor sharing's savings show here).",
            &SCAN_CANDIDATES,
        ),
        (
            "turbovec_scan_floors_offered_total",
            "Floors the scan offered to publish (its own behavior, knob-independent).",
            &SCAN_FLOORS_OFFERED,
        ),
        (
            "turbovec_scan_floors_published_total",
            "Floors actually put on the wire (what the floor knobs move).",
            &SCAN_FLOORS_PUBLISHED,
        ),
        (
            "turbovec_scan_floor_updates_applied_total",
            "Chunks that ran under a coordinator-pushed floor.",
            &SCAN_FLOOR_UPDATES_APPLIED,
        ),
        (
            "turbovec_documents_added_total",
            "Documents ingested over AddDocuments streams.",
            &DOCUMENTS_ADDED,
        ),
        (
            "turbovec_vectors_added_total",
            "Vectors ingested over AddVectors batches.",
            &VECTORS_ADDED,
        ),
    ] {
        header(&mut out, name, "counter", help);
        write_metric(&mut out, name, "", counter.load(Ordering::Relaxed));
    }

    let (batches, jobs) = crate::node::scan_batch_counters();
    header(
        &mut out,
        "turbovec_scan_batches_total",
        "counter",
        "Batched kernel passes (coalesced scans).",
    );
    write_metric(&mut out, "turbovec_scan_batches_total", "", batches);
    header(
        &mut out,
        "turbovec_scan_batched_jobs_total",
        "counter",
        "Scan jobs that rode a batched pass.",
    );
    write_metric(&mut out, "turbovec_scan_batched_jobs_total", "", jobs);

    if !gauges.is_empty() {
        let samples: Vec<ShardGauges> = gauges.iter().map(|g| g()).collect();
        for (name, help, read) in [
            (
                "turbovec_shard_vectors",
                "Vectors in the shard's index.",
                (|s| s.vectors) as fn(&ShardGauges) -> u64,
            ),
            (
                "turbovec_shard_documents",
                "Documents in the shard's postings.",
                |s| s.documents,
            ),
            (
                "turbovec_shard_stats_epoch",
                "BM25 statistics epoch (advances on every mutation).",
                |s| s.stats_epoch,
            ),
        ] {
            header(&mut out, name, "gauge", help);
            for sample in &samples {
                write_metric(
                    &mut out,
                    name,
                    &format!("slot_offset=\"{}\"", sample.slot_offset),
                    read(sample),
                );
            }
        }
    }
    out
}

/// Serve `render` over HTTP on `listener`, forever. The server answers
/// every request on the socket with the metrics page — it parses
/// nothing beyond draining the request head, because the only client
/// is a scraper and the only resource is the page. Bind the listener
/// to a trusted interface; there is no auth here (there is none
/// anywhere in the engine yet — the ops doc is explicit about it).
pub async fn serve(listener: tokio::net::TcpListener, gauges: Vec<GaugeProvider>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let gauges = std::sync::Arc::new(gauges);
    loop {
        let Ok((mut socket, _)) = listener.accept().await else {
            continue;
        };
        let gauges = gauges.clone();
        tokio::spawn(async move {
            // Drain the request head (up to a bound; a scraper's GET is
            // tiny) so the peer never sees a reset before our response.
            let mut buf = [0u8; 4096];
            let mut head = Vec::new();
            loop {
                match socket.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => {
                        head.extend_from_slice(&buf[..n]);
                        if head.windows(4).any(|w| w == b"\r\n\r\n") || head.len() > 16 * 1024 {
                            break;
                        }
                    }
                }
            }
            let body = render(&gauges);
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4; \
                 charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(response.as_bytes()).await;
            let _ = socket.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The page is valid exposition text for what this module emits:
    /// every non-comment line is `name[{labels}] value` with a numeric
    /// value, every metric has HELP and TYPE, and gauges render after
    /// counters in provider order.
    #[test]
    fn page_shape_is_exposition_text() {
        inc_request(Route::SearchShard);
        inc_request(Route::SearchShard);
        add_ingested(3, 7);
        let gauges: Vec<GaugeProvider> = vec![
            Box::new(|| ShardGauges {
                slot_offset: 0,
                vectors: 42,
                documents: 17,
                stats_epoch: 3,
            }),
            Box::new(|| ShardGauges {
                slot_offset: 1000,
                vectors: 7,
                documents: 7,
                stats_epoch: 1,
            }),
        ];
        let page = render(&gauges);
        assert!(page.contains("turbovec_requests_total{rpc=\"search_shard\"} 2"));
        assert!(page.contains("turbovec_documents_added_total 3"));
        assert!(page.contains("turbovec_vectors_added_total 7"));
        assert!(page.contains("turbovec_shard_vectors{slot_offset=\"0\"} 42"));
        assert!(page.contains("turbovec_shard_vectors{slot_offset=\"1000\"} 7"));
        assert!(page.contains("turbovec_shard_documents{slot_offset=\"0\"} 17"));
        // Grouping: both shards' samples of one metric sit under ONE
        // TYPE header, as the exposition format requires.
        assert_eq!(page.matches("# TYPE turbovec_shard_vectors").count(), 1);
        for line in page.lines() {
            if line.starts_with('#') {
                continue;
            }
            let (name, value) = line.rsplit_once(' ').expect("name value");
            assert!(value.parse::<u64>().is_ok(), "numeric value in {line:?}");
            let bare = name.split('{').next().unwrap();
            assert!(
                page.contains(&format!("# TYPE {bare} ")),
                "{bare} has a TYPE line"
            );
        }
    }

    /// Every route increments its own row and only its own row.
    #[test]
    fn routes_count_independently() {
        let before = render(&[]);
        let count = |page: &str, rpc: &str| -> u64 {
            page.lines()
                .find(|l| l.contains(&format!("rpc=\"{rpc}\"")))
                .and_then(|l| l.rsplit_once(' '))
                .and_then(|(_, v)| v.parse().ok())
                .unwrap()
        };
        inc_request(Route::Bm25Query);
        let after = render(&[]);
        assert_eq!(
            count(&after, "bm25_query"),
            count(&before, "bm25_query") + 1
        );
        assert_eq!(count(&after, "term_stats"), count(&before, "term_stats"));
    }
}
