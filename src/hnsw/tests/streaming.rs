//! The streaming search path: emission, stop, raised floors, early first
//! flush, and graceful finish.

use super::*;
use crate::vector::{VectorStreamEndReason, VectorStreamSummary};

fn streaming_refuses_without_a_depth(fx: &Fixture) {
    let (dim, queries, index) = (fx.dim, &fx.queries[..], &fx.index);
    // Without a depth the provider refuses.
    let error = index
        .search_streaming_controlled(
            &queries[..dim],
            VectorSearchOptions::new(),
            &mut |_| VectorStreamControl::Continue,
            &mut || VectorStreamControl::Continue,
        )
        .unwrap_err();
    assert!(error.to_string().contains("candidate depth"), "{error}");
}

fn emission_is_allowed_exact_and_covers_the_top_k(
    fx: &Fixture,
    allow: &[bool],
    options: VectorSearchOptions<'_>,
) {
    let (dim, vectors, queries, index) = (fx.dim, &fx.vectors[..], &fx.queries[..], &fx.index);
    // Every emitted row is allowed, exactly scored, and the emission covers
    // the beam's top-k.
    let mut emitted: Vec<(usize, i64, f32)> = Vec::new();
    let mut batches = 0;
    let mut polls = 0;
    let summary = index
        .search_streaming_controlled(
            queries,
            options,
            &mut |batch| {
                batches += 1;
                assert_eq!(batch.scores.len(), batch.slots.len());
                assert!(!batch.scores.is_empty());
                for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                    emitted.push((batch.query_index, slot, score));
                }
                VectorStreamControl::Continue
            },
            &mut || {
                polls += 1;
                VectorStreamControl::Continue
            },
        )
        .unwrap();
    assert!(summary.completed);
    assert_eq!(summary.query_count, 3);
    assert_eq!(summary.emitted, emitted.len());
    assert!(summary.units_scanned >= emitted.len());
    assert!(batches > 3 && polls > 3, "{batches} batches, {polls} polls");
    for &(q, slot, score) in &emitted {
        assert!(slot >= 0 && allow[slot as usize]);
        let exact: f32 = vectors[slot as usize * dim..][..dim]
            .iter()
            .zip(&queries[q * dim..(q + 1) * dim])
            .map(|(a, b)| a * b)
            .sum();
        assert!((score - exact).abs() <= 1.0e-5);
    }
    for q in 0..3 {
        let seen: Vec<i64> = emitted.iter().filter(|e| e.0 == q).map(|e| e.1).collect();
        let mut sorted = seen.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), seen.len(), "query {q} emitted a row twice");
        let top = index
            .search(
                &queries[q * dim..(q + 1) * dim],
                10,
                VectorSearchOptions::new()
                    .with_mask(allow)
                    .with_candidate_depth(64),
            )
            .unwrap();
        for slot in top.slots {
            assert!(
                seen.contains(&slot),
                "query {q}: top row {slot} never emitted"
            );
        }
    }
}

fn stops_end_the_stream_uncompleted(fx: &Fixture, options: VectorSearchOptions<'_>) {
    let (queries, index) = (&fx.queries[..], &fx.index);
    // Stop from the sink ends the stream uncompleted after one batch.
    let mut seen = 0;
    let summary = index
        .search_streaming_controlled(
            queries,
            options,
            &mut |batch| {
                seen += batch.slots.len();
                VectorStreamControl::Stop
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert!(!summary.completed);
    assert_eq!(summary.emitted, seen);
    assert!(seen > 0 && seen <= 64);

    // Stop from the control ends it too, with nothing more emitted.
    let mut after_stop = 0;
    let stopped = std::cell::Cell::new(false);
    let summary = index
        .search_streaming_controlled(
            queries,
            options,
            &mut |batch| {
                if stopped.get() {
                    after_stop += batch.slots.len();
                }
                VectorStreamControl::Continue
            },
            &mut || {
                stopped.set(true);
                VectorStreamControl::Stop
            },
        )
        .unwrap();
    assert!(!summary.completed);
    assert_eq!(after_stop, 0);
}

fn raised_and_initial_floors_gate_emission(
    fx: &Fixture,
    allow: &[bool],
    options: VectorSearchOptions<'_>,
) {
    let (dim, queries, index) = (fx.dim, &fx.queries[..], &fx.index);
    // A floor raised by the control gates what follows, and a floor raised
    // by the sink carries into the next query.
    let plain = index
        .search(queries, 1, VectorSearchOptions::new().with_mask(allow))
        .unwrap();
    let raise_to = plain.scores[1] - 1.0e-3; // just under query 1's best
    let mut polls = 0;
    let mut per_query: Vec<Vec<f32>> = vec![Vec::new(); 3];
    let summary = index
        .search_streaming_controlled(
            queries,
            options,
            &mut |batch| {
                per_query[batch.query_index].extend_from_slice(batch.scores);
                if batch.query_index == 0 {
                    VectorStreamControl::RaiseFloor(raise_to)
                } else {
                    VectorStreamControl::Continue
                }
            },
            &mut || {
                polls += 1;
                if polls == 2 {
                    VectorStreamControl::RaiseFloor(-0.5)
                } else {
                    VectorStreamControl::Continue
                }
            },
        )
        .unwrap();
    assert!(summary.completed);
    // Query 0's first batch raised the floor to just under query 1's best,
    // so query 1 emits its best row (and anything tied) and nothing below.
    assert!(
        per_query[1].iter().all(|&s| s >= raise_to),
        "{:?}",
        per_query[1]
    );
    assert!(per_query[1].contains(&plain.scores[1]));
    assert!(per_query[2].iter().all(|&s| s >= raise_to));
    assert!(per_query[0].iter().all(|&s| s >= -0.5));
    // An initial floor from the options behaves like a raised one.
    let mut below = 0;
    index
        .search_streaming_controlled(
            &queries[..dim],
            options.with_minimum_score(0.5),
            &mut |batch| {
                below += batch.scores.iter().filter(|s| **s < 0.5).count();
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert_eq!(below, 0);
}

#[test]
fn streaming_emits_scored_rows_and_honors_stop_and_raised_floors() {
    let fx = Fixture::new(3000, 3, 16, 0x1618, BuildMode::Parallel);
    let allow: Vec<bool> = (0..3000).map(|i| i % 2 == 1).collect();
    streaming_refuses_without_a_depth(&fx);
    let options = VectorSearchOptions::new()
        .with_mask(&allow)
        .with_candidate_depth(64);
    emission_is_allowed_exact_and_covers_the_top_k(&fx, &allow, options);
    stops_end_the_stream_uncompleted(&fx, options);
    raised_and_initial_floors_gate_emission(&fx, &allow, options);
}

/// Full streaming walk baseline at ef = 256.
fn full_streaming_walk(fx: &Fixture) -> VectorStreamSummary {
    let (index, q) = (&fx.index, &fx.queries[..fx.dim]);
    let full = index
        .search_streaming_controlled(
            q,
            VectorSearchOptions::new().with_candidate_depth(256),
            &mut |_| VectorStreamControl::Continue,
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert!(full.completed);
    assert_eq!(full.end_reason, VectorStreamEndReason::WalkConverged);
    assert!(full.units_scanned > 150);
    full
}

fn the_first_flush_arrives_on_hop_two(fx: &Fixture) {
    let (index, q) = (&fx.index, &fx.queries[..fx.dim]);
    // 1. Early first-flush: a batch must arrive on hop 2 even when fewer than
    //    64 rows have been scored so far.
    let hop_polls = std::cell::Cell::new(0usize);
    let mut first_batch_poll = None;
    index
        .search_streaming_controlled(
            q,
            VectorSearchOptions::new().with_candidate_depth(256),
            &mut |batch| {
                if first_batch_poll.is_none() && !batch.slots.is_empty() {
                    first_batch_poll = Some(hop_polls.get());
                }
                VectorStreamControl::Continue
            },
            &mut || {
                hop_polls.set(hop_polls.get() + 1);
                VectorStreamControl::Continue
            },
        )
        .unwrap();
    assert_eq!(
        first_batch_poll,
        Some(2),
        "early first-flush must emit after 2 layer-0 hops without waiting for 64 rows"
    );
}

fn a_graceful_finish_flushes_buffered_rows(fx: &Fixture, full: VectorStreamSummary) {
    let (index, q) = (&fx.index, &fx.queries[..fx.dim]);
    // 2. Graceful Finish (deadline = false): stops at the next hop, flushes
    //    buffered candidates >= floor, marks completed = true with
    //    FinishRequested, and scans strictly fewer rows than the full walk.
    let floor = 0.1f32;
    let mut polls = 0usize;
    let mut finished_scores = Vec::new();
    let finished = index
        .search_streaming_controlled(
            q,
            VectorSearchOptions::new()
                .with_candidate_depth(256)
                .with_minimum_score(floor),
            &mut |batch| {
                finished_scores.extend_from_slice(batch.scores);
                VectorStreamControl::Continue
            },
            &mut || {
                polls += 1;
                if polls >= 4 {
                    VectorStreamControl::Finish { deadline: false }
                } else {
                    VectorStreamControl::Continue
                }
            },
        )
        .unwrap();
    assert!(finished.completed);
    assert_eq!(finished.end_reason, VectorStreamEndReason::FinishRequested);
    assert!(finished.units_scanned < full.units_scanned / 2);
    assert!(!finished_scores.is_empty());
    assert!(finished_scores.iter().all(|&s| s >= floor));
    assert_eq!(finished.emitted, finished_scores.len());
}

fn a_deadline_finish_and_a_hard_stop_report_their_reasons(fx: &Fixture) {
    let (index, q) = (&fx.index, &fx.queries[..fx.dim]);
    // 3. Graceful Finish (deadline = true): reports Deadline.
    let mut polls = 0usize;
    let deadline_fin = index
        .search_streaming_controlled(
            q,
            VectorSearchOptions::new().with_candidate_depth(256),
            &mut |_| VectorStreamControl::Continue,
            &mut || {
                polls += 1;
                if polls >= 3 {
                    VectorStreamControl::Finish { deadline: true }
                } else {
                    VectorStreamControl::Continue
                }
            },
        )
        .unwrap();
    assert!(deadline_fin.completed);
    assert_eq!(deadline_fin.end_reason, VectorStreamEndReason::Deadline);

    // 4. Hard Stop at hop 1 (before early first-flush): drops buffered rows,
    //    completed = false, Abandoned.
    let mut polls = 0usize;
    let mut stopped_emitted = 0usize;
    let stopped = index
        .search_streaming_controlled(
            q,
            VectorSearchOptions::new().with_candidate_depth(256),
            &mut |batch| {
                stopped_emitted += batch.slots.len();
                VectorStreamControl::Continue
            },
            &mut || {
                polls += 1;
                if polls >= 1 {
                    VectorStreamControl::Stop
                } else {
                    VectorStreamControl::Continue
                }
            },
        )
        .unwrap();
    assert!(!stopped.completed);
    assert_eq!(stopped.end_reason, VectorStreamEndReason::Abandoned);
    assert_eq!(stopped_emitted, 0);
}

fn enumerate_ignores_finish(fx: &Fixture) {
    let (index, q) = (&fx.index, &fx.queries[..fx.dim]);
    // 5. Enumerate regime ignores Finish so small filtered subsets stay exact.
    let mut tiny_mask = vec![false; 1600];
    for slot in &mut tiny_mask[..12] {
        *slot = true;
    }
    let enum_fin = index
        .search_streaming_controlled(
            q,
            VectorSearchOptions::new()
                .with_candidate_depth(64)
                .with_mask(&tiny_mask)
                .with_enumerate_max_rows(64),
            &mut |_| VectorStreamControl::Continue,
            &mut || VectorStreamControl::Finish { deadline: false },
        )
        .unwrap();
    assert!(enum_fin.completed);
    assert_eq!(enum_fin.end_reason, VectorStreamEndReason::Exhausted);
    assert_eq!(enum_fin.emitted, 12);
}

#[test]
fn early_first_flush_and_graceful_finish_preserve_buffered_rows() {
    let fx = Fixture::new(1600, 1, 24, 0x9100, BuildMode::FixedOrder);
    let full = full_streaming_walk(&fx);
    the_first_flush_arrives_on_hop_two(&fx);
    a_graceful_finish_flushes_buffered_rows(&fx, full);
    a_deadline_finish_and_a_hard_stop_report_their_reasons(&fx);
    enumerate_ignores_finish(&fx);
}
