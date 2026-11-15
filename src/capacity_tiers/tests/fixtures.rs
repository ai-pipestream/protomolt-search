//! The pinned example fixtures: committed views, reports, stores, and inputs.

use super::*;

/// One committed copy of the example fixtures: the node and its
/// incarnations, and the shard installation the copy belongs to.
pub(super) struct CopySpec {
    pub(super) node: &'static str,
    pub(super) proc_inc: u8,
    pub(super) stor_inc: u8,
    pub(super) domain: &'static str,
    pub(super) leaf: &'static str,
    pub(super) shard: &'static str,
    pub(super) source_generation: u64,
    pub(super) ownership_epoch: u64,
}

pub(super) const fn copy_s6(
    node: &'static str,
    proc_inc: u8,
    stor_inc: u8,
    domain: &'static str,
) -> CopySpec {
    CopySpec {
        node,
        proc_inc,
        stor_inc,
        domain,
        leaf: "L4",
        shard: "s6",
        source_generation: 3,
        ownership_epoch: 5,
    }
}

pub(super) const fn copy_s7(
    node: &'static str,
    proc_inc: u8,
    stor_inc: u8,
    domain: &'static str,
) -> CopySpec {
    CopySpec {
        node,
        proc_inc,
        stor_inc,
        domain,
        leaf: "L7",
        shard: "s7",
        source_generation: 3,
        ownership_epoch: 2,
    }
}

pub(super) const K1_S6: CopySpec = copy_s6("krick-1", 0x0a, 0xa1, "krick");
pub(super) const P1_S6: CopySpec = copy_s6("pi5v1", 0x0b, 0xb1, "pi5-west");
pub(super) const P3_S6: CopySpec = copy_s6("pi5v3", 0x0c, 0xc1, "pi5-east");
pub(super) const P1_S7: CopySpec = copy_s7("pi5v1", 0x0b, 0xb2, "pi5-west");
pub(super) const K1_S7: CopySpec = copy_s7("krick-1", 0x0a, 0xa2, "krick");

pub(super) fn committed_copy(spec: &CopySpec, coverage: [u8; 32]) -> CommittedCopy {
    CommittedCopy {
        node_id: spec.node.to_string(),
        storage_incarnation: inc(spec.stor_inc),
        failure_domain: spec.domain.to_string(),
        complete: true,
        coverage_digest: coverage,
    }
}

/// One report from `spec`'s node about `bucket` in the copy's shard and
/// leaf, over the fixture cohort window.
pub(super) fn obs(
    bucket: u64,
    spec: &CopySpec,
    rows: u64,
    bytes: u64,
    scans: u64,
    last: u64,
) -> PartitionObservation {
    PartitionObservation {
        partition: partition(bucket),
        reporter: ReporterIdentity {
            node_id: spec.node.to_string(),
            process_incarnation: inc(spec.proc_inc),
        },
        shard: ShardRef {
            shard: spec.shard.to_string(),
            source_generation: spec.source_generation,
            storage_incarnation: inc(spec.stor_inc),
            ownership_epoch: spec.ownership_epoch,
        },
        topology_generation: 9,
        leaf: spec.leaf.to_string(),
        rows,
        resident_bytes: bytes,
        scans_observed: scans,
        scan_bytes: if scans > 0 { 786_432_000_000 } else { 0 },
        queue_wait_p50_us: if scans > 0 { 1200 } else { 0 },
        queue_wait_p99_us: if scans > 0 { 9400 } else { 0 },
        samples: if scans > 0 { 3000 } else { 0 },
        last_scanned_unix_ms: last,
        window_start_unix_ms: W0,
        window_end_unix_ms: W1,
    }
}

pub(super) fn fixture_a_reports() -> Vec<PartitionObservation> {
    vec![
        obs(7, &K1_S6, 1_000_000, 268_435_456, 3_000_000, T - 5000),
        obs(7, &P1_S6, 1_000_000, 268_435_456, 0, 0),
        obs(7, &P3_S6, 1_000_000, 268_435_456, 0, 0),
        obs(7, &P1_S7, 500_000, 134_217_728, 0, T - 3_600_000),
        obs(7, &K1_S7, 500_000, 134_217_728, 0, 0),
    ]
}

pub(super) fn committed_view_a() -> CommittedView {
    let mut shards = BTreeMap::new();
    shards.insert(
        "s6".to_string(),
        CommittedShard {
            source_generation: 3,
            ownership_epoch: 5,
            leaves: BTreeMap::from([(
                "L4".to_string(),
                LeafCoverage {
                    pool: vec![
                        "krick-1".to_string(),
                        "pi5v1".to_string(),
                        "pi5v3".to_string(),
                    ],
                    owner_node: "krick-1".to_string(),
                    copies: vec![
                        committed_copy(&K1_S6, cov_l4()),
                        committed_copy(&P1_S6, cov_l4()),
                        committed_copy(&P3_S6, cov_l4()),
                    ],
                },
            )]),
        },
    );
    shards.insert(
        "s7".to_string(),
        CommittedShard {
            source_generation: 3,
            ownership_epoch: 2,
            leaves: BTreeMap::from([(
                "L7".to_string(),
                LeafCoverage {
                    pool: vec!["krick-1".to_string(), "pi5v1".to_string()],
                    owner_node: "pi5v1".to_string(),
                    copies: vec![
                        committed_copy(&P1_S7, cov_l7()),
                        committed_copy(&K1_S7, cov_l7()),
                    ],
                },
            )]),
        },
    );
    CommittedView {
        workspace: "ws-court".to_string(),
        collection: "cases".to_string(),
        derived_fingerprint: decl_fp(),
        topology_generation: 9,
        shards,
    }
}

pub(super) fn nodes_map() -> BTreeMap<String, NodeRecord> {
    let server = |domain: &str, total: u64| NodeRecord {
        residency: NodeResidency::Server,
        eligible: true,
        failure_domain: domain.to_string(),
        capacity: NodeCapacity {
            total_bytes: total,
            resident_bytes: 0,
        },
    };
    BTreeMap::from([
        ("krick-1".to_string(), server("krick", 26_843_545_600)),
        ("pi5v1".to_string(), server("pi5-west", 16_106_127_360)),
        ("pi5v3".to_string(), server("pi5-east", 0)),
    ])
}

pub(super) fn store_a() -> ObservationStore {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_a())
        .expect("valid cohort config");
    store.register_incarnation("krick-1", inc(0x0a)).unwrap();
    store.register_incarnation("pi5v1", inc(0x0b)).unwrap();
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    for report in fixture_a_reports() {
        assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
    }
    store
}

pub(super) fn input_a(store: &ObservationStore, epoch: u64) -> TierSnapshotInput {
    TierSnapshotInput {
        planning_instant_unix_ms: T,
        max_moves: 16,
        max_observation_age_ms: 600_000,
        clock_skew_bound_ms: 5_000,
        authority_id: "control-0".to_string(),
        authority_incarnation: inc(0x01),
        control_revision: 41,
        policy_revision: 7,
        policy: policy_p(),
        observation_epoch: epoch,
        topology_generation: 9,
        placement_tree_digest: tree_digest(),
        workspace: "ws-court".to_string(),
        collection: "cases".to_string(),
        derived_fingerprint: decl_fp(),
        provider_geometry_digest: prov_digest(),
        cohort_length_ms: COHORT,
        cohort_phase_unix_ms: 0,
        nodes: nodes_map(),
        observations: store.observations().into_iter().cloned().collect(),
        fragment_requests: vec![req(7, "L4"), req(7, "L7")],
        committed: committed_view_a(),
        current_incarnations: store.current_incarnations().clone(),
    }
}

pub(super) fn snapshot_a(store: &ObservationStore, epoch: u64) -> TierSnapshot {
    TierSnapshot::validated(input_a(store, epoch)).expect("fixture A validates")
}

pub(super) fn fixture_m_reports() -> Vec<PartitionObservation> {
    vec![
        obs(5, &P3_S6, 80_000, 21_474_836_480, 0, T - 7_200_000),
        obs(6, &P3_S6, 72_000, 19_327_352_832, 0, T - 10_800_000),
    ]
}

pub(super) fn committed_view_m() -> CommittedView {
    let mut shards = BTreeMap::new();
    shards.insert(
        "s6".to_string(),
        CommittedShard {
            source_generation: 3,
            ownership_epoch: 5,
            leaves: BTreeMap::from([(
                "L4".to_string(),
                LeafCoverage {
                    pool: vec![
                        "krick-1".to_string(),
                        "pi5v1".to_string(),
                        "pi5v3".to_string(),
                    ],
                    owner_node: "pi5v3".to_string(),
                    copies: vec![committed_copy(&P3_S6, cov_l4())],
                },
            )]),
        },
    );
    CommittedView {
        workspace: "ws-court".to_string(),
        collection: "cases".to_string(),
        derived_fingerprint: decl_fp(),
        topology_generation: 9,
        shards,
    }
}

pub(super) fn store_m() -> ObservationStore {
    let mut store = ObservationStore::new(COHORT, 0, 1000, 1 << 20, 64, committed_view_m())
        .expect("valid cohort config");
    store.register_incarnation("pi5v3", inc(0x0c)).unwrap();
    for report in fixture_m_reports() {
        assert_eq!(store.ingest(report).expect("ingest"), IngestOutcome::Landed);
    }
    store
}

pub(super) fn input_m(store: &ObservationStore) -> TierSnapshotInput {
    TierSnapshotInput {
        observation_epoch: 119,
        fragment_requests: vec![req(5, "L4"), req(6, "L4")],
        committed: committed_view_m(),
        observations: store.observations().into_iter().cloned().collect(),
        current_incarnations: store.current_incarnations().clone(),
        ..input_a(&store_a(), 119)
    }
}

pub(super) fn snapshot_m(store: &ObservationStore) -> TierSnapshot {
    TierSnapshot::validated(input_m(store)).expect("fixture M validates")
}
