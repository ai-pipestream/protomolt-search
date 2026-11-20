use prost::Message;

use super::*;
use crate::pb::{
    DenseExecutionMode as Mode, DenseExecutionShape as Shape, DenseFilterStrategy as Strategy,
    DenseScorePrecision as Precision, DenseTechnique as Technique,
};

fn point(k: u32, candidates: u32, recall: u32) -> DensePolicyPoint {
    DensePolicyPoint {
        k,
        filter_selectivity_ppm_min: 500_000,
        filter_selectivity_ppm_max: UNFILTERED_PPM,
        candidates,
        measured_recall_ppm: recall,
        enumerate_max_rows: Some(200),
        mode: Mode::Balanced as i32,
        technique: Technique::HnswWalk as i32,
        selection_precision: Precision::Fp32 as i32,
        output_precision: Precision::Fp32 as i32,
        filter_strategy: Strategy::EnumerateSmall as i32,
        shape: Shape::Unary as i32,
        approximate_p50_us: 10,
        approximate_p99_us: 20,
        exhaustive_p50_us: 100,
        exhaustive_p99_us: 200,
        approximate_qps: 1000.0,
        exhaustive_qps: 100.0,
        ..Default::default()
    }
}

fn document(points: Vec<DensePolicyPoint>) -> DenseExecutionPolicyDocument {
    DenseExecutionPolicyDocument {
        format_version: FORMAT_VERSION,
        policy_id: "graph-v2".into(),
        embedding_model: "test".into(),
        provider_backend: "hnsw".into(),
        scoring_fingerprint: "fp".into(),
        corpus_generation: 3,
        corpus_rows: 1000,
        deleted_docs: 0,
        corpus_fingerprint: "corpus-fp-v1".into(),
        dimensions: 16,
        measured_queries: 32,
        target_recall_ppm: 950_000,
        points,
        hardware_fingerprint: "hw".into(),
        node_count: 1,
        hosts: Vec::new(),
    }
}

fn key(k: u32, depth: u32, selectivity: u32) -> RequestKey {
    RequestKey {
        k,
        candidate_depth: depth,
        filter_selectivity_ppm: selectivity,
        output_precision: Precision::Fp32,
        shape: Shape::Unary,
    }
}

#[test]
fn round_trips_through_bytes_and_fingerprints_them() {
    let doc = document(vec![point(10, 40, 990_000)]);
    let bytes = doc.encode_to_vec();
    let policy = DenseExecutionPolicy::decode(&bytes).unwrap();
    assert_eq!(policy.fingerprint(), turbo_sha256::sha256(&bytes).to_hex());
    let dir = std::env::temp_dir().join(format!("dense-policy-v2-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    policy.save(&path).unwrap();
    let loaded = DenseExecutionPolicy::load(&path).unwrap();
    assert_eq!(loaded.document(), &doc);
    assert_eq!(loaded.fingerprint(), policy.fingerprint());

    let next_seq = SAVE_SEQ.load(Ordering::Relaxed);
    let collision = dir.join(format!(
        ".policy.binpb.tmp.{}.{next_seq}",
        std::process::id()
    ));
    std::fs::write(&collision, b"keep-leftover").unwrap();
    assert!(policy.save(&path).is_err());
    assert_eq!(std::fs::read(&collision).unwrap(), b"keep-leftover");
    std::fs::remove_file(&collision).unwrap();
    assert!(policy
        .save(&dir.join("missing-parent").join("policy.binpb"))
        .is_err());
    std::fs::remove_dir_all(dir).unwrap();
}

/// One way to break a valid document.
type Mutation = Box<dyn Fn(&mut DenseExecutionPolicyDocument)>;

fn malformed_header_cases() -> Vec<(Mutation, &'static str)> {
    vec![
        (Box::new(|d| d.format_version = 1), "format_version"),
        (Box::new(|d| d.policy_id.clear()), "policy_id"),
        (Box::new(|d| d.policy_id = "   ".into()), "policy_id"),
        (
            Box::new(|d| d.corpus_fingerprint = "\t ".into()),
            "corpus_fingerprint",
        ),
        (
            Box::new(|d| d.hardware_fingerprint = "  ".into()),
            "hardware_fingerprint",
        ),
        (
            Box::new(|d| d.hosts = vec![local_host_profile()]),
            "hardware_fingerprint",
        ),
        (Box::new(|d| d.points[0].approximate_qps = 0.0), "qps"),
        (Box::new(|d| d.deleted_docs = 1001), "deleted_docs"),
        (Box::new(|d| d.target_recall_ppm = 0), "target_recall_ppm"),
        (Box::new(|d| d.points.clear()), "at least one point"),
    ]
}

fn malformed_staged_and_shape_cases() -> Vec<(Mutation, &'static str)> {
    vec![
        (
            Box::new(|d| {
                d.points[0].technique = Technique::BitplaneStaged as i32;
                d.points[0].filter_strategy = Strategy::Default as i32;
                d.points[0].shape = Shape::Streaming as i32;
                d.points[0].enumerate_max_rows = None;
            }),
            "UNARY-only",
        ),
        (
            Box::new(|d| {
                d.points[0].technique = Technique::BitplaneStaged as i32;
                d.points[0].filter_strategy = Strategy::Speed as i32;
                d.points[0].enumerate_max_rows = None;
            }),
            "BITPLANE_STAGED filter_strategy",
        ),
        (
            Box::new(|d| {
                d.points[0].technique = Technique::BitplaneStaged as i32;
                d.points[0].filter_strategy = Strategy::Default as i32;
                d.points[0].enumerate_max_rows = None;
                d.points[0].patience_saturation_ppm = Some(990_000);
                d.points[0].patience_hops = Some(10);
            }),
            "BITPLANE_STAGED point must not set",
        ),
        (
            Box::new(|d| {
                d.points[0].technique = Technique::BitplaneStaged as i32;
                d.points[0].filter_strategy = Strategy::Default as i32;
                d.points[0].enumerate_max_rows = None;
                d.points[0].floor_greediness_ppm = Some(950_000);
                d.points[0].floor_clamp_min = Some(8);
            }),
            "BITPLANE_STAGED point must not set",
        ),
        (
            Box::new(|d| d.points[0].finish_settle_us = Some(250)),
            "UNARY point must not set finish_settle_us",
        ),
    ]
}

fn malformed_point_cases() -> Vec<(Mutation, &'static str)> {
    vec![
        (
            Box::new(|d| d.points[0].mode = Mode::Exhaustive as i32),
            "mode",
        ),
        (Box::new(|d| d.points[0].technique = 0), "technique"),
        (Box::new(|d| d.points[0].shape = 0), "shape"),
        (
            Box::new(|d| d.points[0].filter_strategy = 0),
            "filter_strategy",
        ),
        (Box::new(|d| d.points[0].candidates = 5), "candidates"),
        (Box::new(|d| d.points[0].candidates = 1001), "corpus_rows"),
        (
            Box::new(|d| d.points[0].measured_recall_ppm = 0),
            "measured_recall_ppm",
        ),
        (
            Box::new(|d| d.points[0].enumerate_max_rows = None),
            "enumerate_max_rows",
        ),
        (
            Box::new(|d| d.points[0].enumerate_max_rows = Some(1001)),
            "enumerate_max_rows",
        ),
        (
            Box::new(|d| {
                d.points[0].filter_selectivity_ppm_min = UNFILTERED_PPM;
                d.points[0].filter_selectivity_ppm_max = UNFILTERED_PPM;
            }),
            "unfiltered point must not set enumerate_max_rows",
        ),
        (
            Box::new(|d| d.points[0].filter_selectivity_ppm_min = 0),
            "selectivity band",
        ),
        (
            Box::new(|d| d.points[0].patience_hops = Some(3)),
            "patience",
        ),
        (
            Box::new(|d| d.points.push(d.points[0])),
            "duplicate point key",
        ),
        (
            Box::new(|d| {
                let mut overlap = point(10, 80, 995_000);
                overlap.filter_selectivity_ppm_min = 100_000;
                overlap.filter_selectivity_ppm_max = 600_000;
                d.points.push(overlap);
            }),
            "overlapping selectivity bands",
        ),
    ]
}

#[test]
fn malformed_documents_refuse_by_name() {
    let v1 = b"format_version = 1\npolicy_id = \"old\"\n";
    assert!(DenseExecutionPolicy::decode(v1).is_err());
    for (mutate, needle) in malformed_header_cases()
        .into_iter()
        .chain(malformed_staged_and_shape_cases())
        .chain(malformed_point_cases())
    {
        let mut doc = document(vec![point(10, 40, 990_000)]);
        mutate(&mut doc);
        let error = DenseExecutionPolicy::from_document(doc).unwrap_err();
        assert!(error.contains(needle), "{needle}: {error}");
    }
}

#[test]
fn identity_mismatches_name_the_field() {
    let policy =
        DenseExecutionPolicy::from_document(document(vec![point(10, 40, 990_000)])).unwrap();
    let live = LiveIdentity {
        provider_backend: "hnsw".into(),
        scoring_fingerprint: "fp".into(),
        corpus_generation: 3,
        corpus_rows: 1000,
        deleted_docs: 0,
        dimensions: 16,
        corpus_fingerprint: "corpus-fp-v1".into(),
    };
    policy.verify_identity(&live).unwrap();
    for (mutate, field) in [
        (
            Box::new(|l: &mut LiveIdentity| l.corpus_generation = 4) as Box<dyn Fn(&mut _)>,
            "corpus_generation",
        ),
        (
            Box::new(|l: &mut LiveIdentity| l.corpus_rows = 1),
            "corpus_rows",
        ),
        (
            Box::new(|l: &mut LiveIdentity| l.deleted_docs = 1),
            "deleted_docs",
        ),
        (
            Box::new(|l: &mut LiveIdentity| l.corpus_fingerprint = "corpus-fp-v2".into()),
            "corpus_fingerprint",
        ),
        (
            Box::new(|l: &mut LiveIdentity| l.scoring_fingerprint = "x".into()),
            "scoring_fingerprint",
        ),
    ] {
        let mut changed = live.clone();
        mutate(&mut changed);
        let error = policy.verify_identity(&changed).unwrap_err();
        assert!(error.contains(field), "{error}");
    }
}

#[test]
fn qualification_binds_every_key_field_and_the_recall_target() {
    let mut fast = point(10, 20, 960_000);
    fast.mode = Mode::Fast as i32;
    let policy = DenseExecutionPolicy::from_document(document(vec![
        point(10, 40, 990_000),
        fast,
        point(10, 80, 900_000),
    ]))
    .unwrap();
    let Qualification::Qualified(hit) = policy.qualify(key(10, 0, UNFILTERED_PPM)) else {
        panic!("qualifies")
    };
    assert_eq!(
        (hit.point.candidates, hit.point.mode),
        (20, Mode::Fast as i32)
    );
    let Qualification::Qualified(hit) = policy.qualify(key(10, 40, 600_000)) else {
        panic!("named depth")
    };
    assert_eq!(hit.point.candidates, 40);
    for unqualified in [
        key(10, 80, UNFILTERED_PPM),
        key(10, 30, UNFILTERED_PPM),
        key(10, 0, 100_000),
        key(5, 0, UNFILTERED_PPM),
        key(0, 0, UNFILTERED_PPM),
        RequestKey {
            output_precision: Precision::Quantized,
            ..key(10, 0, UNFILTERED_PPM)
        },
        RequestKey {
            shape: Shape::Streaming,
            ..key(10, 0, UNFILTERED_PPM)
        },
    ] {
        assert!(
            matches!(policy.qualify(unqualified), Qualification::Unqualified(_)),
            "{unqualified:?}"
        );
    }
}

#[test]
fn a_point_that_meets_recall_but_measured_slower_routes_to_exhaustive() {
    let mut slow = point(10, 40, 990_000);
    slow.approximate_p50_us = 150;
    slow.approximate_p99_us = 300;
    let mut fewer_but_slower = point(10, 20, 990_000);
    fewer_but_slower.approximate_p50_us = 60;
    fewer_but_slower.approximate_p99_us = 90;
    let mut deeper_but_faster = point(10, 80, 990_000);
    deeper_but_faster.approximate_p50_us = 15;
    deeper_but_faster.approximate_p99_us = 30;
    let policy = DenseExecutionPolicy::from_document(document(vec![slow])).unwrap();
    let Qualification::Unqualified(reason) = policy.qualify(key(10, 0, UNFILTERED_PPM)) else {
        panic!("slower than exhaustive")
    };
    assert!(reason.contains("faster"), "{reason}");
    let policy =
        DenseExecutionPolicy::from_document(document(vec![fewer_but_slower, deeper_but_faster]))
            .unwrap();
    let Qualification::Qualified(hit) = policy.qualify(key(10, 0, UNFILTERED_PPM)) else {
        panic!("qualifies")
    };
    assert_eq!(
        hit.point.candidates, 80,
        "fastest p50 (15 us) wins over fewer candidates (20 at 60 us)"
    );
}

#[test]
fn concurrent_slot_installs_keep_serving_and_persisted_fingerprints_identical() {
    use std::sync::Arc;
    let dir = std::env::temp_dir().join(format!("dense-slot-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    let slot = Arc::new(PolicySlot::new(None, Some(path.clone())));
    std::thread::scope(|scope| {
        for depth in [20u32, 40, 60, 80] {
            let slot = Arc::clone(&slot);
            scope.spawn(move || {
                for i in 0..8u32 {
                    let mut doc = document(vec![point(10, depth + i, 990_000)]);
                    doc.policy_id = format!("slot-{depth}-{i}");
                    let policy = DenseExecutionPolicy::from_document(doc).unwrap();
                    slot.install(policy).unwrap();
                }
            });
        }
    });
    let serving = slot.current().unwrap();
    let reloaded = DenseExecutionPolicy::load(&path).unwrap();
    assert_eq!(serving.fingerprint(), reloaded.fingerprint());
    assert_eq!(serving.document(), reloaded.document());
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn selectivity_is_floored_and_whole_when_unfiltered() {
    assert_eq!(selectivity_ppm(5, 10), 500_000);
    assert_eq!(selectivity_ppm(1, 10_000_000), 1);
    assert_eq!(selectivity_ppm(999_999, 1_000_000), 999_999);
    assert_eq!(selectivity_ppm(0, 10), 0);
    assert_eq!(selectivity_ppm(10, 10), UNFILTERED_PPM);
    assert_eq!(selectivity_ppm(11, 10), UNFILTERED_PPM);
    assert_eq!(selectivity_ppm(5, 0), UNFILTERED_PPM);
}
