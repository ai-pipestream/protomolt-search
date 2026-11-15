//! Tests for the dense execution policy document, identity, and qualification.

use super::*;

const TEXT: &str = r#"
format_version = 1
policy_id = "court-ann-v1"
embedding_model = "bge-m3"
corpus_generation = 7
corpus_rows = 1000
dimensions = 64
provider_backend = "fake-ann"
scoring_fingerprint = "fake-ann:v1"
measured_queries = 32

[[points]]
k = 10
filter_selectivity_ppm_min = 1000000
filter_selectivity_ppm_max = 1000000
candidates = 100
measured_recall_ppm = 990000

[[points]]
k = 10
filter_selectivity_ppm_min = 100000
filter_selectivity_ppm_max = 500000
candidates = 200
measured_recall_ppm = 970000

[[points]]
k = 10
filter_selectivity_ppm_min = 100000
filter_selectivity_ppm_max = 500000
candidates = 400
measured_recall_ppm = 995000
"#;

fn live() -> LiveIdentity {
    LiveIdentity {
        provider_backend: "fake-ann".into(),
        scoring_fingerprint: "fake-ann:v1".into(),
        corpus_generation: 7,
        corpus_rows: 1000,
        dimensions: 64,
    }
}

#[test]
fn the_document_round_trips_and_the_fingerprint_is_the_bytes() {
    let policy = DenseExecutionPolicy::parse(TEXT).unwrap();
    assert_eq!(policy.points().len(), 3);
    let text = policy.to_toml();
    let again = DenseExecutionPolicy::parse(&text).unwrap();
    assert_eq!(again.points(), policy.points());
    assert_eq!(again.identity(), policy.identity());
    assert_eq!(again.policy_id(), "court-ann-v1");
    assert_eq!(
        again.fingerprint(),
        crate::sha256::hex_digest(text.as_bytes())
    );
    // The saved file is the same document.
    let dir = std::env::temp_dir().join(format!("dense-policy-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.toml");
    policy.save(&path).unwrap();
    let loaded = DenseExecutionPolicy::load(&path).unwrap();
    assert_eq!(loaded.fingerprint(), again.fingerprint());
    assert_eq!(loaded.points(), policy.points());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_key_qualifies_only_at_an_exact_point() {
    let policy = DenseExecutionPolicy::parse(TEXT).unwrap();
    policy.verify_identity(&live()).unwrap();
    // Unfiltered, one depth measured: the depth need not be named.
    let hit = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 0,
            filter_selectivity_ppm: UNFILTERED_PPM,
        })
        .unwrap();
    assert_eq!(hit.point.candidates, 100);
    assert_eq!(hit.point.measured_recall_ppm, 990_000);
    assert_eq!(hit.policy_fingerprint, policy.fingerprint());
    // A named depth must be a measured one.
    let miss = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 150,
            filter_selectivity_ppm: UNFILTERED_PPM,
        })
        .unwrap_err();
    assert!(miss.contains("not at the requested 150"), "{miss}");
    // Inside the filtered band, two depths were measured: ambiguous
    // without selection_k, exact with it.
    let ambiguous = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 0,
            filter_selectivity_ppm: 250_000,
        })
        .unwrap_err();
    assert!(ambiguous.contains("[200, 400]"), "{ambiguous}");
    let deep = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 400,
            filter_selectivity_ppm: 500_000,
        })
        .unwrap();
    assert_eq!(deep.point.measured_recall_ppm, 995_000);
    // Outside every band, or at an unmeasured k: refused by name.
    let outside = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 0,
            filter_selectivity_ppm: 50_000,
        })
        .unwrap_err();
    assert!(outside.contains("50000 ppm"), "{outside}");
    let other_k = policy
        .qualify(RequestKey {
            k: 11,
            candidate_depth: 0,
            filter_selectivity_ppm: UNFILTERED_PPM,
        })
        .unwrap_err();
    assert!(
        other_k.contains("k=11") && other_k.contains("[10]"),
        "{other_k}"
    );
}

#[test]
fn identity_mismatches_name_the_field() {
    let policy = DenseExecutionPolicy::parse(TEXT).unwrap();
    let mut rows = live();
    rows.corpus_rows = 1001;
    assert!(policy
        .verify_identity(&rows)
        .unwrap_err()
        .contains("corpus_rows 1000 does not match live 1001"));
    let mut generation = live();
    generation.corpus_generation = 8;
    assert!(policy
        .verify_identity(&generation)
        .unwrap_err()
        .contains("corpus_generation"));
    let mut backend = live();
    backend.provider_backend = "embedded-turbovec".into();
    assert!(policy
        .verify_identity(&backend)
        .unwrap_err()
        .contains("provider_backend \"fake-ann\" does not match live \"embedded-turbovec\""));
    let mut fingerprint = live();
    fingerprint.scoring_fingerprint = "other".into();
    assert!(policy
        .verify_identity(&fingerprint)
        .unwrap_err()
        .contains("scoring_fingerprint"));
    let mut dims = live();
    dims.dimensions = 65;
    assert!(policy
        .verify_identity(&dims)
        .unwrap_err()
        .contains("dimensions"));
}

#[test]
fn malformed_documents_are_refused_by_name() {
    let overlap = TEXT.replace("candidates = 400", "candidates = 200");
    assert!(DenseExecutionPolicy::parse(&overlap)
        .unwrap_err()
        .contains("overlap on selectivity"));
    let shallow = TEXT.replace("candidates = 100", "candidates = 9");
    assert!(DenseExecutionPolicy::parse(&shallow)
        .unwrap_err()
        .contains("invalid candidate depth 9"));
    let unknown = format!("{TEXT}\nnprobe = 4\n");
    assert!(DenseExecutionPolicy::parse(&unknown)
        .unwrap_err()
        .contains("nprobe"));
    let quoted = TEXT.replace("court-ann-v1", "court\\\"ann");
    assert!(DenseExecutionPolicy::parse(&quoted).is_err());
    let version = TEXT.replace("format_version = 1", "format_version = 2");
    assert!(DenseExecutionPolicy::parse(&version)
        .unwrap_err()
        .contains("format_version 2"));
}

/// A filtered point carries the enumerate threshold it was measured
/// under through the document form; an unfiltered point has no mask to
/// apply one to, and a threshold above the corpus is not a measurement.
#[test]
fn a_filtered_point_carries_its_enumerate_threshold() {
    let text = TEXT.replace(
        "candidates = 200\nmeasured_recall_ppm = 970000\n",
        "candidates = 200\nmeasured_recall_ppm = 970000\nenumerate_max_rows = 64\n",
    );
    assert_ne!(text, TEXT);
    let policy = DenseExecutionPolicy::parse(&text).unwrap();
    let thresholds: Vec<Option<u32>> = policy
        .points()
        .iter()
        .map(|point| point.enumerate_max_rows)
        .collect();
    assert_eq!(thresholds, [None, Some(64), None]);
    let again = DenseExecutionPolicy::parse(&policy.to_toml()).unwrap();
    assert_eq!(again.points(), policy.points());
    let hit = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 200,
            filter_selectivity_ppm: 250_000,
        })
        .unwrap();
    assert_eq!(hit.point.enumerate_max_rows, Some(64));

    let unfiltered = TEXT.replace(
        "candidates = 100\nmeasured_recall_ppm = 990000\n",
        "candidates = 100\nmeasured_recall_ppm = 990000\nenumerate_max_rows = 64\n",
    );
    let refused = DenseExecutionPolicy::parse(&unfiltered).unwrap_err();
    assert!(refused.contains("no mask to enumerate"), "{refused}");
    let above = text.replace("enumerate_max_rows = 64", "enumerate_max_rows = 1001");
    let refused = DenseExecutionPolicy::parse(&above).unwrap_err();
    assert!(
        refused.contains("above the corpus's 1000 rows"),
        "{refused}"
    );
}

#[test]
fn selectivity_is_floored_parts_per_million() {
    assert_eq!(selectivity_ppm(0, 1000), 0);
    assert_eq!(selectivity_ppm(1, 3_000_000), 1);
    assert_eq!(selectivity_ppm(250, 1000), 250_000);
    assert_eq!(selectivity_ppm(1000, 1000), UNFILTERED_PPM);
    assert_eq!(selectivity_ppm(5, 0), UNFILTERED_PPM);
}

#[test]
fn a_point_carries_patience_parameters_and_validates_pairs() {
    let text = TEXT.replace(
        "candidates = 100\nmeasured_recall_ppm = 990000\n",
        "candidates = 100\nmeasured_recall_ppm = 990000\npatience_saturation_ppm = 990000\npatience_hops = 15\n",
    );
    let policy = DenseExecutionPolicy::parse(&text).unwrap();
    let again = DenseExecutionPolicy::parse(&policy.to_toml()).unwrap();
    assert_eq!(again.points(), policy.points());
    let hit = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 0,
            filter_selectivity_ppm: UNFILTERED_PPM,
        })
        .unwrap();
    assert_eq!(hit.point.patience_saturation_ppm, Some(990_000));
    assert_eq!(hit.point.patience_hops, Some(15));

    let half = TEXT.replace(
        "candidates = 100\nmeasured_recall_ppm = 990000\n",
        "candidates = 100\nmeasured_recall_ppm = 990000\npatience_saturation_ppm = 990000\n",
    );
    assert!(DenseExecutionPolicy::parse(&half)
        .unwrap_err()
        .contains("patience_saturation_ppm and patience_hops together"));

    let zero_hops = text.replace("patience_hops = 15", "patience_hops = 0");
    assert!(DenseExecutionPolicy::parse(&zero_hops)
        .unwrap_err()
        .contains("patience_hops must be positive"));

    let bad_ppm = text.replace(
        "patience_saturation_ppm = 990000",
        "patience_saturation_ppm = 1000001",
    );
    assert!(DenseExecutionPolicy::parse(&bad_ppm)
        .unwrap_err()
        .contains("patience_saturation_ppm 1000001 outside 1..=1000000"));
}

#[test]
fn a_point_carries_finish_settle_us_and_validates_bounds() {
    let text = TEXT.replace(
        "candidates = 100\nmeasured_recall_ppm = 990000\n",
        "candidates = 100\nmeasured_recall_ppm = 990000\nfinish_settle_us = 250\n",
    );
    let policy = DenseExecutionPolicy::parse(&text).unwrap();
    let again = DenseExecutionPolicy::parse(&policy.to_toml()).unwrap();
    assert_eq!(again.points(), policy.points());
    let hit = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 0,
            filter_selectivity_ppm: UNFILTERED_PPM,
        })
        .unwrap();
    assert_eq!(hit.point.finish_settle_us, Some(250));

    let zero = text.replace("finish_settle_us = 250", "finish_settle_us = 0");
    assert!(DenseExecutionPolicy::parse(&zero)
        .unwrap_err()
        .contains("finish_settle_us 0 outside"));
    let huge = text.replace("finish_settle_us = 250", "finish_settle_us = 60000001");
    assert!(DenseExecutionPolicy::parse(&huge)
        .unwrap_err()
        .contains("finish_settle_us 60000001 outside"));
}

#[test]
fn a_point_carries_floor_prune_parameters_and_validates_pairs() {
    let text = TEXT.replace(
        "candidates = 100\nmeasured_recall_ppm = 990000\n",
        "candidates = 100\nmeasured_recall_ppm = 990000\nfloor_greediness_ppm = 0\nfloor_clamp_min = 8\n",
    );
    let policy = DenseExecutionPolicy::parse(&text).unwrap();
    let again = DenseExecutionPolicy::parse(&policy.to_toml()).unwrap();
    assert_eq!(again.points(), policy.points());
    let hit = policy
        .qualify(RequestKey {
            k: 10,
            candidate_depth: 0,
            filter_selectivity_ppm: UNFILTERED_PPM,
        })
        .unwrap();
    assert_eq!(hit.point.floor_greediness_ppm, Some(0));
    assert_eq!(hit.point.floor_clamp_min, Some(8));

    let half = TEXT.replace(
        "candidates = 100\nmeasured_recall_ppm = 990000\n",
        "candidates = 100\nmeasured_recall_ppm = 990000\nfloor_greediness_ppm = 500000\n",
    );
    assert!(DenseExecutionPolicy::parse(&half)
        .unwrap_err()
        .contains("floor_greediness_ppm and floor_clamp_min together"));

    let bad_ppm = text.replace("floor_greediness_ppm = 0", "floor_greediness_ppm = 1000001");
    assert!(DenseExecutionPolicy::parse(&bad_ppm)
        .unwrap_err()
        .contains("floor_greediness_ppm 1000001 outside 0..=1000000"));

    let huge_clamp = text.replace("floor_clamp_min = 8", "floor_clamp_min = 1001");
    assert!(DenseExecutionPolicy::parse(&huge_clamp)
        .unwrap_err()
        .contains("floor_clamp_min 1001 above the corpus's 1000 rows"));
}
