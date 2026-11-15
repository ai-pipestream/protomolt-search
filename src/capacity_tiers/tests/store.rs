//! The observation store: bounds, cohorts, incarnations, and retries.

use super::*;

#[test]
fn checked_arithmetic_and_store_bounds_hold_state_intact() {
    // Epoch exhaustion refuses before mutating: the report is not
    // inserted, and a later exact retry of a stored report still
    // works.
    let mut store = store_a();
    store.epoch = u64::MAX;
    let fresh = obs(9, &P3_S6, 60_000, 10_000_000_000, 0, T - 1000);
    let count_before = store.observations().len();
    assert_eq!(
        store.ingest(fresh).unwrap_err(),
        "report_shard: the observation epoch is exhausted"
    );
    assert_eq!(store.observations().len(), count_before);
    let retry = obs(7, &K1_S6, 1_000_000, 268_435_456, 3_000_000, T - 5000);
    assert_eq!(
        store.ingest(retry).expect("retry"),
        IngestOutcome::Unchanged
    );
    assert_eq!(
        store.commit_expiry(W1 + COHORT, 1000).unwrap_err(),
        "report_shard: the observation epoch is exhausted"
    );
    assert_eq!(
        store.observations().len(),
        count_before,
        "expiry changed nothing"
    );

    // The incarnation registry is bounded.
    let mut tight = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 1, committed_view_a())
        .expect("valid cohort config");
    tight.register_incarnation("krick-1", inc(0x0a)).unwrap();
    assert_eq!(
        tight.register_incarnation("pi5v1", inc(0x0b)).unwrap_err(),
        "report_shard: the incarnation registry holds the bound of 1 nodes"
    );

    // Retained accounting charges allocation capacity, not length:
    // a short string with a large capacity defeats a tight bound.
    let mut over = obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0);
    over.partition.column = {
        let mut s = String::with_capacity(4096);
        s.push_str("key_bucket");
        s
    };
    assert!(over.heap_bytes() >= 2 * 4096);
    let mut tight_bytes = ObservationStore::new(COHORT, 0, 1000, 100, 64, committed_view_a())
        .expect("valid cohort config");
    tight_bytes
        .register_incarnation("krick-1", inc(0x0a))
        .unwrap();
    assert!(tight_bytes
        .ingest(obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0))
        .unwrap_err()
        .contains("bound of 100 retained bytes"));
}

#[test]
fn cohort_alignment_rules() {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();

    // E10: a 660-second, off-grid window refuses by name.
    let mut misaligned = obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0);
    misaligned.window_start_unix_ms = T - 660_000;
    misaligned.window_end_unix_ms = T - 60_000;
    assert_eq!(
        store.ingest(misaligned).unwrap_err(),
        "report_shard: window [1788868140000, 1788868740000) is not a cohort window (length 600000 ms, phase 0)"
    );

    let mut inverted = obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0);
    inverted.window_start_unix_ms = W1;
    inverted.window_end_unix_ms = W0;
    assert!(store.ingest(inverted).unwrap_err().contains("inverted"));

    assert!(ObservationStore::new((1 << 40) + 1, 0, 10, 1000, 64, committed_view_a()).is_err());

    let store = store_a();
    let snapshot = snapshot_a(&store, 118);
    assert_eq!(snapshot.current_cohort().unwrap(), (W0, W1));
    let mut mid = snapshot_a(&store, 118);
    mid.planning_instant_unix_ms = W0 + 100_000;
    assert_eq!(mid.current_cohort().unwrap(), (W0 - COHORT, W0));
}

#[test]
fn restart_supersedes_the_old_incarnation() {
    let mut store = store_a();
    let epoch_before = store.epoch();
    store.register_incarnation("krick-1", inc(0x1a)).unwrap();
    assert_eq!(
        store.epoch(),
        epoch_before + 1,
        "the drop advanced the epoch once"
    );
    assert_eq!(
        store.observations().len(),
        3,
        "both krick-1 reports dropped"
    );

    let ghost = obs(7, &K1_S6, 1_000_000, 268_435_456, 3_000_000, T - 5000);
    let err = store.ingest(ghost).unwrap_err();
    assert!(
        err.contains("report_shard: node krick-1 incarnation"),
        "{err}"
    );
    assert!(err.contains("is superseded by"), "{err}");

    let mut fresh = obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0);
    fresh.reporter.process_incarnation = inc(0x1a);
    assert_eq!(store.ingest(fresh).expect("ingest"), IngestOutcome::Landed);
    let mut s7_owner = obs(7, &K1_S7, 500_000, 134_217_728, 0, 0);
    s7_owner.reporter.process_incarnation = inc(0x1a);
    assert_eq!(
        store.ingest(s7_owner).expect("ingest"),
        IngestOutcome::Landed
    );
    let snapshot = snapshot_a(&store, 118);
    let response = plan_tiers(&snapshot).expect("plan");
    assert_eq!(response.placements[0].rate_nanos, 0);
    assert_eq!(
        response.placements[0].classified_tier.as_deref(),
        Some("archive")
    );
}

#[test]
fn exact_and_conflicting_retries() {
    let mut store = store_a();
    let digest_before = store.observation_set_digest();
    let epoch_before = store.epoch();

    let retry = obs(7, &K1_S6, 1_000_000, 268_435_456, 3_000_000, T - 5000);
    assert_eq!(
        store.ingest(retry).expect("ingest"),
        IngestOutcome::Unchanged
    );
    assert_eq!(store.epoch(), epoch_before);
    assert_eq!(store.observation_set_digest(), digest_before);

    let mut conflict = obs(7, &K1_S6, 1_000_000, 268_435_456, 2_999_999, T - 5000);
    conflict.scan_bytes = 0;
    conflict.queue_wait_p50_us = 0;
    conflict.queue_wait_p99_us = 0;
    conflict.samples = 0;
    assert_eq!(
        store.ingest(conflict).unwrap_err(),
        "report_shard: observation for (key_bucket=7, gen 9, L4, s6, gen 3) window [1788868200000, 1788868800000) from krick-1/0000000000000000000000000000000a conflicts with the stored report"
    );

    let mut old = obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0);
    old.window_start_unix_ms = W0 - COHORT;
    old.window_end_unix_ms = W0;
    assert_eq!(
        store.ingest(old).expect("ingest"),
        IngestOutcome::Superseded
    );
    assert_eq!(store.epoch(), epoch_before);
}

#[test]
fn committed_identity_is_equality_checked_both_directions() {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    let base = || obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0);

    let mut ahead = base();
    ahead.shard.source_generation = 4;
    assert_eq!(
        store.ingest(ahead).unwrap_err(),
        "report_shard: shard s6 source generation 4 is not the committed 3"
    );
    let mut behind_epoch = base();
    behind_epoch.shard.ownership_epoch = 4;
    assert!(store
        .ingest(behind_epoch)
        .unwrap_err()
        .contains("ownership epoch 4 is not the committed 5"));
    let mut replaced = base();
    replaced.shard.storage_incarnation = inc(0xaa);
    assert!(store
        .ingest(replaced)
        .unwrap_err()
        .contains("is not the committed"));
    let mut wrong_leaf = base();
    wrong_leaf.leaf = "L7".to_string();
    assert!(store
        .ingest(wrong_leaf)
        .unwrap_err()
        .contains("covers no rows in leaf L7"));
    let mut wrong_shard = base();
    wrong_shard.shard.shard = "s9".to_string();
    assert!(store
        .ingest(wrong_shard)
        .unwrap_err()
        .contains("not in the committed topology"));
    let mut wrong_gen = base();
    wrong_gen.topology_generation = 10;
    assert!(store
        .ingest(wrong_gen)
        .unwrap_err()
        .contains("topology_generation 10 is not the committed 9"));

    let mut store2 = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    assert!(store2
        .ingest(base())
        .unwrap_err()
        .contains("has not registered an incarnation"));
}

#[test]
fn store_bounds_and_impossible_states() {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    let mut no_bytes = obs(7, &K1_S6, 1_000_000, 0, 0, 0);
    assert!(store
        .ingest(no_bytes.clone())
        .unwrap_err()
        .contains("0 resident bytes"));
    no_bytes.rows = 0;
    no_bytes.scans_observed = 5;
    assert!(store
        .ingest(no_bytes)
        .unwrap_err()
        .contains("scans of 0 resident bytes"));

    let mut tight = ObservationStore::new(COHORT, 0, 1, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    tight.register_incarnation("krick-1", inc(0x0a)).unwrap();
    tight.register_incarnation("pi5v1", inc(0x0b)).unwrap();
    tight
        .ingest(obs(7, &K1_S6, 1_000_000, 268_435_456, 0, 0))
        .expect("first record");
    assert_eq!(
        tight
            .ingest(obs(7, &P1_S6, 1_000_000, 268_435_456, 0, 0))
            .unwrap_err(),
        "report_shard: the observation store holds the bound of 1 records"
    );
    let mut newer = obs(7, &K1_S6, 1_000_000, 268_435_456, 1, 0);
    newer.last_scanned_unix_ms = W1 - 1000;
    newer.window_start_unix_ms = W1;
    newer.window_end_unix_ms = W1 + COHORT;
    assert_eq!(
        tight.ingest(newer).expect("supersede at the record bound"),
        IngestOutcome::Landed
    );

    let mut store = store_a();
    let dropped = store.commit_expiry(W1 + COHORT, 1000).expect("expiry");
    assert_eq!(dropped, 5);
    assert_eq!(store.observations().len(), 0);
    assert_eq!(store.commit_expiry(W1 + COHORT, 1000).expect("expiry"), 0);
}
