use std::collections::HashMap;

use super::{CountMinSketch, HeavyHitters, HyperLogLog};

// ---- Count-min sketch (mirrors CountMinSketchTest) ----

/// A Zipf-ish stream: term i occurs ~1/i of the lead term's count.
fn skewed_stream(
    sketch: &mut CountMinSketch,
    distinct: usize,
    lead_count: u64,
) -> Vec<(String, u64)> {
    let mut truth = Vec::new();
    for i in 1..=distinct {
        let term = format!("term-{i}");
        let count = (lead_count / i as u64).max(1);
        truth.push((term.clone(), count));
        for _ in 0..count {
            sketch.add(&term, 1);
        }
    }
    truth
}

#[test]
fn cms_estimates_never_undershoot_and_stay_close_for_heavy_terms() {
    let mut sketch = CountMinSketch::new();
    let truth = skewed_stream(&mut sketch, 2_000, 10_000);

    for (term, count) in &truth {
        assert!(
            sketch.estimate(term) >= *count,
            "estimate of {term} undershoots"
        );
    }
    // Heavy terms sit far above the collision noise floor; their
    // estimates should be within a few percent of the truth.
    for (i, (_, count)) in truth.iter().enumerate().take(20) {
        let ratio = sketch.estimate(&format!("term-{}", i + 1)) as f64 / *count as f64;
        assert!(
            (1.0..=1.05).contains(&ratio),
            "relative error of term-{} out of band: {ratio}",
            i + 1
        );
    }
    // A term that never occurred still gets the one-sided estimate of
    // the noise floor, not a negative number.
    assert_eq!(sketch.estimate("never-seen"), 0);
}

#[test]
fn cms_merge_combines_as_if_one_sketch_saw_everything() {
    let mut left = CountMinSketch::new();
    let mut right = CountMinSketch::new();
    let mut all = CountMinSketch::new();
    for i in 0..500u64 {
        left.add(&format!("term-{i}"), i + 1);
        right.add(&format!("term-{i}"), 2 * (i + 1));
        all.add(&format!("term-{i}"), 3 * (i + 1));
    }
    left.merge(&right).unwrap();

    for i in 0..500 {
        assert_eq!(
            left.estimate(&format!("term-{i}")),
            all.estimate(&format!("term-{i}"))
        );
    }
    assert_eq!(left.total_count(), all.total_count());
}

#[test]
fn cms_snapshot_round_trip_restores_identical_estimates() {
    let mut sketch = CountMinSketch::new();
    let truth = skewed_stream(&mut sketch, 200, 5_000);

    let restored = CountMinSketch::from_bytes(
        sketch.depth(),
        sketch.width(),
        &sketch.to_bytes(),
        sketch.total_count(),
    )
    .unwrap();

    assert_eq!(restored.total_count(), sketch.total_count());
    for (term, _) in &truth {
        assert_eq!(restored.estimate(term), sketch.estimate(term));
    }
}

// ---- HyperLogLog (mirrors HyperLogLogTest) ----

fn hll_filled(prefix: &str, distinct: usize) -> HyperLogLog {
    let mut sketch = HyperLogLog::new();
    for i in 0..distinct {
        sketch.add(&format!("{prefix}{i}"));
    }
    sketch
}

#[test]
fn hll_empty_sketch_estimates_zero() {
    assert_eq!(HyperLogLog::new().estimate(), 0.0);
}

#[test]
fn hll_cardinality_at_10k_distinct() {
    let estimate = hll_filled("term-", 10_000).estimate();
    assert!((0.95..=1.05).contains(&(estimate / 10_000.0)), "{estimate}");
}

#[test]
fn hll_cardinality_at_100k_distinct() {
    let estimate = hll_filled("term-", 100_000).estimate();
    assert!(
        (0.95..=1.05).contains(&(estimate / 100_000.0)),
        "{estimate}"
    );
}

#[test]
fn hll_duplicates_do_not_count() {
    let mut sketch = HyperLogLog::new();
    for i in 0..1_000 {
        sketch.add(&format!("term-{}", i % 100));
    }
    assert!((0.90..=1.10).contains(&(sketch.estimate() / 100.0)));
}

#[test]
fn hll_merge_estimates_the_union() {
    // 60k + 60k distinct terms with a 20k overlap: union is 100k.
    let mut left = hll_filled("term-", 60_000);
    let mut right = HyperLogLog::new();
    for i in 40_000..100_000 {
        right.add(&format!("term-{i}"));
    }

    left.merge(&right).unwrap();

    assert!((0.95..=1.05).contains(&(left.estimate() / 100_000.0)));
}

#[test]
fn hll_snapshot_round_trip_restores_identical_estimate() {
    let sketch = hll_filled("term-", 10_000);
    let restored = HyperLogLog::from_bytes(sketch.precision(), &sketch.to_bytes()).unwrap();
    assert_eq!(restored.estimate(), sketch.estimate());
}

// ---- Heavy hitters (mirrors HeavyHittersTest) ----

#[test]
fn hh_true_heavy_hitters_survive_the_tail() {
    let mut hitters = HeavyHitters::new(16);
    let heavy: Vec<String> = (0..10).map(|h| format!("heavy-{h}")).collect();
    for round in 0..1_000 {
        for term in &heavy {
            hitters.add(term, 1);
        }
        hitters.add(&format!("tail-{round}"), 1);
    }

    let snapshot = hitters.snapshot();
    assert_eq!(snapshot.len(), 16);
    let counts: HashMap<&str, u64> = snapshot
        .iter()
        .map(|e| (e.term.as_str(), e.count))
        .collect();
    for term in &heavy {
        // Space-saving overestimates by at most the minimum count in
        // the table; at 1000 true occurrences the heavy terms are
        // near-exact.
        let count = *counts.get(term.as_str()).expect("heavy term survives");
        assert!((1_000..=1_050).contains(&count), "{term} at {count}");
    }
    // Ordered by count descending: the heavy block leads.
    for entry in snapshot.iter().take(heavy.len()) {
        assert!(entry.count >= 1_000);
    }
}

#[test]
fn hh_tracked_terms_accumulate_exactly() {
    let mut hitters = HeavyHitters::new(4);
    hitters.add("alpha", 3);
    hitters.add("alpha", 4);
    hitters.add("beta", 2);

    assert_eq!(hitters.estimate("alpha"), 7);
    assert_eq!(hitters.estimate("beta"), 2);
    assert_eq!(hitters.estimate("gamma"), 0);
    let terms: Vec<String> = hitters.snapshot().into_iter().map(|e| e.term).collect();
    assert_eq!(terms, ["alpha", "beta"]);
}

#[test]
fn hh_restore_rebuilds_the_list() {
    let mut hitters = HeavyHitters::new(8);
    hitters.add("alpha", 5);
    let snapshot = hitters.snapshot();

    let mut restored = HeavyHitters::new(8);
    restored.restore(snapshot.clone());

    assert_eq!(restored.estimate("alpha"), 5);
    assert_eq!(restored.snapshot(), snapshot);
}
