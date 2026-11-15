use super::*;

const PROFILE: &str = r#"
format_version = 1
profile_id = "court-held-out-v1"
embedding_model = "minilm-static-256"
corpus_generation = 7
corpus_rows = 1000000
dimensions = 256
provider_backend = "embedded-turbovec"
scoring_fingerprint = "score-abc"
measured_queries = 128

[[points]]
k = 10000
target_recall_ppm = 990000
candidates = 12078

[[points]]
k = 10000
target_recall_ppm = 1000000
candidates = 17265
"#;

/// The 100k / k=10,000 ladder from the challenge report, as a v2 file.
const MEASURED: &str = r#"
format_version = 2
profile_id = "challenge-100k-k10000"
embedding_model = "synthetic-64"
corpus_generation = 3
corpus_rows = 100000
dimensions = 64
provider_backend = "embedded-turbovec"
scoring_fingerprint = "score-abc"
measured_queries = 16
default_target_recall_ppm = 990000

[[measurements]]
k = 10000
candidates = 10000
queries = 16
mean_recall_ppm = 891631
min_recall_ppm = 854800
p50_total_ms = 41.5
p50_selection_ms = 30.25
p50_rerank_ms = 9.0

[[measurements]]
k = 10000
candidates = 20000
queries = 16
mean_recall_ppm = 995543
min_recall_ppm = 991700
p50_total_ms = 52.0
p50_selection_ms = 33.0
p50_rerank_ms = 17.5

[[measurements]]
k = 10000
candidates = 30000
queries = 16
mean_recall_ppm = 999962
min_recall_ppm = 999800
p50_total_ms = 63.0
p50_selection_ms = 36.0
p50_rerank_ms = 25.0

[[measurements]]
k = 10000
candidates = 50000
queries = 16
mean_recall_ppm = 1000000
min_recall_ppm = 1000000
p50_total_ms = 90.0
p50_selection_ms = 42.0
p50_rerank_ms = 46.0

[[points]]
k = 10000
target_recall_ppm = 950000
candidates = 20000

[[points]]
k = 10000
target_recall_ppm = 990000
candidates = 20000

[[points]]
k = 10000
target_recall_ppm = 1000000
candidates = 50000
"#;

fn ladder() -> Vec<ProfileMeasurement> {
    DenseQualityProfile::parse(MEASURED)
        .unwrap()
        .measurements()
        .to_vec()
}

#[test]
fn exact_resolution_never_interpolates_or_clamps() {
    let profile = DenseQualityProfile::parse(PROFILE).unwrap();
    let point = profile.resolve(10_000, 990_000, "", 20_000).unwrap();
    assert_eq!(point.selection_k, 12_078);
    assert_eq!(point.corpus_generation, 7);
    assert_eq!(profile.measured_queries(), 128);
    assert!(profile.resolve(1_000, 990_000, "", 0).is_err());
    assert!(profile.resolve(10_000, 990_000, "", 12_000).is_err());
    assert!(profile
        .resolve(10_000, 990_000, "wrong-fingerprint", 0)
        .is_err());
}

#[test]
fn duplicate_points_are_rejected() {
    let duplicate =
        format!("{PROFILE}\n[[points]]\nk=10000\ntarget_recall_ppm=990000\ncandidates=13000\n");
    assert!(DenseQualityProfile::parse(&duplicate)
        .unwrap_err()
        .contains("duplicate"));
}

#[test]
fn a_version_1_file_loads_unchanged_and_carries_no_default() {
    let profile = DenseQualityProfile::parse(PROFILE).unwrap();
    assert_eq!(profile.default_target_recall_ppm(), None);
    assert!(profile.measurements().is_empty());
    assert_eq!(profile.points().len(), 2);
    assert_eq!(
        profile.fingerprint(),
        crate::sha256::hex_digest(PROFILE.as_bytes())
    );
    // The new fields are not smuggled into the old version.
    let with_default = PROFILE.replace(
        "measured_queries = 128",
        "measured_queries = 128\ndefault_target_recall_ppm = 990000",
    );
    let error = DenseQualityProfile::parse(&with_default).unwrap_err();
    assert!(
        error.contains("format_version 1 carries neither"),
        "{error}"
    );
    let error =
        DenseQualityProfile::parse(&PROFILE.replace("format_version = 1", "format_version = 3"))
            .unwrap_err();
    assert!(error.contains("format_version 3 is unsupported"), "{error}");
    let error =
        DenseQualityProfile::parse(&format!("{PROFILE}\nexpansion_factor = 2.0\n")).unwrap_err();
    assert!(error.contains("unknown field"), "{error}");
}

#[test]
fn a_version_2_file_carries_its_evidence_and_default() {
    let profile = DenseQualityProfile::parse(MEASURED).unwrap();
    assert_eq!(profile.default_target_recall_ppm(), Some(990_000));
    assert_eq!(profile.measurements().len(), 4);
    assert_eq!(profile.measurements()[1].p50_rerank_ms, 17.5);
    assert_eq!(
        profile.resolve(10_000, 990_000, "", 0).unwrap().selection_k,
        20_000
    );
    assert_eq!(
        profile
            .resolve(10_000, 1_000_000, "", 0)
            .unwrap()
            .selection_k,
        50_000
    );
    assert!(profile.resolve(10_000, 999_000, "", 0).is_err());
}

#[test]
fn choose_points_takes_the_smallest_depth_whose_worst_query_meets_the_target() {
    let chosen =
        choose_points(&ladder(), &[1_000_000, 950_000, 999_000, 990_000, 990_000]).unwrap();
    assert_eq!(
        chosen.points,
        vec![
            ProfilePoint {
                k: 10_000,
                target_recall_ppm: 950_000,
                candidates: 20_000
            },
            ProfilePoint {
                k: 10_000,
                target_recall_ppm: 990_000,
                candidates: 20_000
            },
            ProfilePoint {
                k: 10_000,
                target_recall_ppm: 999_000,
                candidates: 30_000
            },
            ProfilePoint {
                k: 10_000,
                target_recall_ppm: 1_000_000,
                candidates: 50_000
            },
        ]
    );
    assert!(chosen.unmet.is_empty());

    // The mean at 20,000 (99.55%) clears 99.5%; the worst query (99.17%)
    // does not, so the target moves to the next rung.
    let chosen = choose_points(&ladder(), &[995_000]).unwrap();
    assert_eq!(chosen.points[0].candidates, 30_000);

    // A target no rung's worst query reaches is reported, not invented.
    let shallow: Vec<ProfileMeasurement> = ladder().into_iter().take(2).collect();
    let chosen = choose_points(&shallow, &[990_000, 999_000]).unwrap();
    assert_eq!(chosen.points.len(), 1);
    assert_eq!(
        chosen.unmet,
        vec![UnmetTarget {
            k: 10_000,
            target_recall_ppm: 999_000,
            best_min_recall_ppm: 991_700,
            best_candidates: 20_000,
        }]
    );
    assert!(choose_points(&shallow, &[0]).is_err());
    assert!(choose_points(&shallow, &[1_000_001]).is_err());
}

#[test]
fn an_unjustified_point_is_refused_by_name() {
    // A depth the ladder never measured.
    let error = DenseQualityProfile::parse(&MEASURED.replace(
        "target_recall_ppm = 950000\ncandidates = 20000",
        "target_recall_ppm = 950000\ncandidates = 15000",
    ))
    .unwrap_err();
    assert!(
        error.contains("candidates=15000 is not justified"),
        "{error}"
    );
    // A measured depth that promises more than its worst query delivered.
    let error = DenseQualityProfile::parse(&MEASURED.replace(
        "target_recall_ppm = 1000000\ncandidates = 50000",
        "target_recall_ppm = 1000000\ncandidates = 30000",
    ))
    .unwrap_err();
    assert!(
        error.contains("candidates=30000 claims more than was measured: min_recall_ppm=999800"),
        "{error}"
    );
    // A default that resolves nothing.
    let error = DenseQualityProfile::parse(&MEASURED.replace(
        "default_target_recall_ppm = 990000",
        "default_target_recall_ppm = 999000",
    ))
    .unwrap_err();
    assert!(
        error.contains("default_target_recall_ppm 999000 names no point"),
        "{error}"
    );
    // Malformed rungs.
    for (from, to, needle) in [
        (
            "min_recall_ppm = 991700",
            "min_recall_ppm = 999999",
            "minimum above the mean",
        ),
        (
            "p50_rerank_ms = 17.5",
            "p50_rerank_ms = -1.0",
            "p50_rerank_ms",
        ),
        (
            "p50_rerank_ms = 17.5",
            "p50_rerank_ms = inf",
            "p50_rerank_ms",
        ),
        (
            "queries = 16\nmean_recall_ppm = 995543",
            "queries = 0\nmean_recall_ppm = 995543",
            "measured no queries",
        ),
    ] {
        let error = DenseQualityProfile::parse(&MEASURED.replacen(from, to, 1)).unwrap_err();
        assert!(error.contains(needle), "{from} -> {to}: {error}");
    }
    let duplicate = format!(
        "{MEASURED}\n[[measurements]]\nk = 10000\ncandidates = 20000\nqueries = 16\n\
         mean_recall_ppm = 995543\nmin_recall_ppm = 991700\np50_total_ms = 1.0\n\
         p50_selection_ms = 1.0\np50_rerank_ms = 1.0\n"
    );
    assert!(DenseQualityProfile::parse(&duplicate)
        .unwrap_err()
        .contains("duplicate measurement"));
}

#[test]
fn the_document_round_trips_and_the_fingerprint_is_the_bytes() {
    let parsed = DenseQualityProfile::parse(MEASURED).unwrap();
    let text = parsed.to_toml();
    let again = DenseQualityProfile::parse(&text).unwrap();
    assert_eq!(again.to_toml(), text);
    assert_eq!(again.measurements(), parsed.measurements());
    assert_eq!(again.points(), parsed.points());
    assert_eq!(again.identity(), parsed.identity());
    assert_eq!(again.default_target_recall_ppm(), Some(990_000));
    assert_eq!(
        again.fingerprint(),
        crate::sha256::hex_digest(text.as_bytes())
    );

    let chosen = choose_points(parsed.measurements(), &[950_000, 990_000, 1_000_000]).unwrap();
    let built = DenseQualityProfile::from_measurements(
        parsed.identity().clone(),
        16,
        Some(990_000),
        parsed.measurements().to_vec(),
        chosen.points,
    )
    .unwrap();
    assert_eq!(built.to_toml(), text);
    assert_eq!(built.fingerprint(), again.fingerprint());
    let dir = std::env::temp_dir().join(format!("dense-quality-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("profile.toml");
    built.save(&path).unwrap();
    let loaded = DenseQualityProfile::load(&path).unwrap();
    assert_eq!(loaded.fingerprint(), built.fingerprint());
    assert_eq!(
        loaded
            .resolve(10_000, 990_000, built.fingerprint(), 0)
            .unwrap(),
        built.resolve(10_000, 990_000, "", 0).unwrap()
    );
    let _ = std::fs::remove_dir_all(dir);

    // Points that the ladder does not justify are refused at build time
    // too, so the tool cannot write what the server would refuse.
    let error = DenseQualityProfile::from_measurements(
        parsed.identity().clone(),
        16,
        None,
        parsed.measurements().to_vec(),
        vec![ProfilePoint {
            k: 10_000,
            target_recall_ppm: 1_000_000,
            candidates: 20_000,
        }],
    )
    .unwrap_err();
    assert!(error.contains("claims more than was measured"), "{error}");
}
