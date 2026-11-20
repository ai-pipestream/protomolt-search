//! Tests for filter selectivity and version-1 TOML policy refusal.

use super::*;

const RETIRED_V1_TOML: &str = r#"
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
"#;

#[test]
fn selectivity_is_floored_parts_per_million() {
    assert_eq!(selectivity_ppm(0, 1000), 0);
    assert_eq!(selectivity_ppm(1, 3_000_000), 1);
    assert_eq!(selectivity_ppm(250, 1000), 250_000);
    assert_eq!(selectivity_ppm(1000, 1000), UNFILTERED_PPM);
    assert_eq!(selectivity_ppm(5, 0), UNFILTERED_PPM);
}

#[test]
fn version_one_toml_policies_are_refused_at_load() {
    let dir = std::env::temp_dir().join(format!("dense-policy-v1-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.toml");
    std::fs::write(&path, RETIRED_V1_TOML).unwrap();
    let refused = DenseExecutionPolicy::load(&path).unwrap_err();
    assert!(
        refused.contains("version-1 TOML schema is not read") && refused.contains("version-2"),
        "{refused}"
    );
    let missing = DenseExecutionPolicy::load(&dir.join("missing.binpb")).unwrap_err();
    assert!(missing.contains("read dense execution policy"), "{missing}");
    let _ = std::fs::remove_dir_all(&dir);
}
