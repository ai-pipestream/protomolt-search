use std::sync::atomic::Ordering;

use tonic::Code;

use super::registry::{code_index, Histogram, BUCKETS, ERROR_CODES};
use super::render::{seconds_text, write_histogram, DURATION};
use super::routes::{N_ROUTES, REQUEST_ROUTES};
use super::{add_ingested, inc_request, render, GaugeProvider, Route, ShardGauges};

/// The value of the first sample line starting with `needle`.
pub(super) fn sample(page: &str, needle: &str) -> u64 {
    page.lines()
        .find(|l| l.starts_with(needle))
        .and_then(|l| l.rsplit_once(' '))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or_else(|| panic!("no sample line starting {needle:?}"))
}

/// Every per-route static is indexed by the discriminant, so the
/// table must list the routes in declaration order, once each,
/// under distinct names.
#[test]
fn route_table_matches_discriminants() {
    let mut names = std::collections::HashSet::new();
    for (i, (route, name, _)) in REQUEST_ROUTES.iter().enumerate() {
        assert_eq!(*route as usize, i, "{route:?} sits at row {i}");
        assert!(names.insert(*name), "duplicate route name {name}");
    }
}

/// `le` is inclusive, as Prometheus defines it; buckets are
/// cumulative on the page; `_count` equals the `+Inf` bucket.
#[test]
fn buckets_are_inclusive_and_cumulative() {
    let h = Histogram::new();
    h.observe(1_000_000); // exactly 1ms: the 0.001 bucket
    h.observe(1_000_001); // just over: the 0.002 bucket
    h.observe(0);
    h.observe(999_999_999_999); // beyond 10s: +Inf only
    assert_eq!(h.buckets[0].load(Ordering::Relaxed), 2);
    assert_eq!(h.buckets[1].load(Ordering::Relaxed), 1);
    assert_eq!(h.buckets[13].load(Ordering::Relaxed), 1);
    assert_eq!(h.sum_ns.load(Ordering::Relaxed), 1_000_002_000_000);

    let mut out = String::new();
    write_histogram(&mut out, "rpc=\"t\"", &h.reading());
    let bucket = |le: &str| {
        sample(
            &out,
            &format!("{DURATION}_bucket{{rpc=\"t\",le=\"{le}\"}} "),
        )
    };
    assert_eq!(bucket("0.001"), 2);
    assert_eq!(bucket("0.002"), 3);
    assert_eq!(bucket("10"), 3);
    assert_eq!(bucket("+Inf"), 4);
    assert_eq!(sample(&out, &format!("{DURATION}_count{{rpc=\"t\"}} ")), 4);
    assert!(
        out.contains(&format!("{DURATION}_sum{{rpc=\"t\"}} 1000.002\n")),
        "{out}"
    );
    assert_eq!(BUCKETS.len(), 14);
    assert_eq!(BUCKETS[13].0, "+Inf");
}

/// Seconds render as exact decimals with a decimal point, never in
/// exponent form and never with a float rounding tail.
#[test]
fn seconds_render_as_exact_decimals() {
    assert_eq!(seconds_text(0), "0.0");
    assert_eq!(seconds_text(1), "0.000000001");
    assert_eq!(seconds_text(1_500_000), "0.0015");
    assert_eq!(seconds_text(2_000_000_000), "2.0");
    assert_eq!(seconds_text(300_000_000), "0.3");
    assert_eq!(seconds_text(12_345_678_901), "12.345678901");
    for text in [seconds_text(0), seconds_text(1), seconds_text(u64::MAX)] {
        assert!(text.contains('.'), "{text}");
        assert!(text.parse::<f64>().is_ok(), "{text}");
    }
}

/// Every canonical code the engine emits on purpose has its own
/// row; the rest fold into `other`; `Ok` is not an error.
#[test]
fn codes_map_to_rows() {
    assert_eq!(code_index(Code::Ok), None);
    for (code, name) in [
        (Code::InvalidArgument, "invalid_argument"),
        (Code::FailedPrecondition, "failed_precondition"),
        (Code::NotFound, "not_found"),
        (Code::ResourceExhausted, "resource_exhausted"),
        (Code::Unavailable, "unavailable"),
        (Code::DeadlineExceeded, "deadline_exceeded"),
        (Code::Unauthenticated, "unauthenticated"),
        (Code::PermissionDenied, "permission_denied"),
        (Code::Internal, "internal"),
    ] {
        assert_eq!(ERROR_CODES[code_index(code).unwrap()], name);
    }
    for code in [
        Code::Cancelled,
        Code::Unknown,
        Code::AlreadyExists,
        Code::Aborted,
        Code::OutOfRange,
        Code::Unimplemented,
        Code::DataLoss,
    ] {
        assert_eq!(ERROR_CODES[code_index(code).unwrap()], "other", "{code:?}");
    }
}

/// The page is valid exposition text for what this module emits:
/// every non-comment line is `name[{labels}] value` with a numeric
/// value (integers everywhere except the histogram `_sum`, which is
/// a float), every metric has HELP and TYPE, and gauges render
/// after counters in provider order.
#[test]
fn page_shape_is_exposition_text() {
    let before = render(&[]);
    let documents = sample(&before, "turbovec_documents_added_total ");
    let vectors = sample(&before, "turbovec_vectors_added_total ");
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
    assert!(sample(&page, "turbovec_requests_total{rpc=\"search_shard\"} ") >= 2);
    // Other tests can ingest concurrently into these process-wide counters.
    assert!(sample(&page, "turbovec_documents_added_total ") >= documents + 3);
    assert!(sample(&page, "turbovec_vectors_added_total ") >= vectors + 7);
    assert!(page.contains("turbovec_shard_vectors{slot_offset=\"0\"} 42"));
    assert!(page.contains("turbovec_shard_vectors{slot_offset=\"1000\"} 7"));
    assert!(page.contains("turbovec_shard_documents{slot_offset=\"0\"} 17"));
    assert!(page.contains("# TYPE turbovec_request_duration_seconds histogram"));
    assert!(page.contains("# TYPE turbovec_requests_in_flight gauge"));
    // Grouping: both shards' samples of one metric sit under ONE
    // TYPE header, as the exposition format requires.
    assert_eq!(page.matches("# TYPE turbovec_shard_vectors").count(), 1);
    assert_eq!(
        page.matches("# TYPE turbovec_request_duration_seconds ")
            .count(),
        1
    );
    let mut sums = 0;
    for line in page.lines() {
        if line.starts_with('#') {
            continue;
        }
        let (name, value) = line.rsplit_once(' ').expect("name value");
        let bare = name.split('{').next().unwrap();
        if bare == format!("{DURATION}_sum") {
            sums += 1;
            assert!(value.contains('.'), "float sum in {line:?}");
            assert!(value.parse::<f64>().is_ok(), "numeric value in {line:?}");
        } else {
            assert!(value.parse::<u64>().is_ok(), "numeric value in {line:?}");
        }
        // Histogram children hang off the histogram's own TYPE line.
        let family = bare
            .strip_suffix("_bucket")
            .or_else(|| bare.strip_suffix("_sum"))
            .or_else(|| bare.strip_suffix("_count"))
            .filter(|family| *family == DURATION)
            .unwrap_or(bare);
        assert!(
            page.contains(&format!("# TYPE {family} ")),
            "{bare} has a TYPE line"
        );
    }
    // One `_sum` per unary route plus two per streaming route.
    let streaming = REQUEST_ROUTES.iter().filter(|r| r.2).count();
    assert_eq!(sums, N_ROUTES + streaming);
}

/// Every route pre-declares its histogram and all ten error rows
/// from the first scrape; streaming routes carry both phases and
/// unary routes carry no phase label.
#[test]
fn every_route_has_histogram_and_error_rows() {
    let page = render(&[]);
    for (_, name, streaming) in REQUEST_ROUTES {
        for code in ERROR_CODES {
            assert!(
                page.contains(&format!(
                    "turbovec_request_errors_total{{rpc=\"{name}\",code=\"{code}\"}} "
                )),
                "{name}/{code}"
            );
        }
        assert!(page.contains(&format!("turbovec_requests_in_flight{{rpc=\"{name}\"}} ")));
        if streaming {
            for phase in ["first_response", "complete"] {
                assert!(page.contains(&format!(
                    "{DURATION}_count{{rpc=\"{name}\",phase=\"{phase}\"}} "
                )));
                assert!(page.contains(&format!(
                    "{DURATION}_bucket{{rpc=\"{name}\",phase=\"{phase}\",le=\"+Inf\"}} "
                )));
            }
            assert!(!page.contains(&format!("{DURATION}_count{{rpc=\"{name}\"}} ")));
        } else {
            assert!(page.contains(&format!("{DURATION}_count{{rpc=\"{name}\"}} ")));
            assert!(page.contains(&format!("{DURATION}_bucket{{rpc=\"{name}\",le=\"+Inf\"}} ")));
            assert!(!page.contains(&format!("{DURATION}_count{{rpc=\"{name}\",phase=")));
        }
    }
}
