//! Forked latency and thread-CPU probe of one dense search on the
//! embedded TurboVec scan and on the HNSW graph, with receipt validation.
//!
//! Each fork builds the same seeded 2,000-row index and times
//! `search_unfiltered` at k = 10, rotating through 16 corpus queries.
//! `cargo test` runs the smoke shape (2 forks, 2 warmup, 5 measured
//! iterations); set `COLLAB_JMH_*` (see [`crate::collab_harness::jmh`])
//! for a full sweep and `COLLAB_JMH_RECEIPT_DIR` to keep the receipts.
//! The test asserts the receipt schema and internal consistency, never a
//! latency bound: timings vary by host, receipts are the evidence.

use serde_json::{json, Value};

use pipestream_search::harness::{fit_calibration, seeded_index, unit_vectors};
use pipestream_search::hnsw::HNSW;
use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};

use crate::collab_harness::jmh::{
    default_receipt_dir, Bench, RemoveOnDrop, TrialConfig, FORK_ENV, SCHEMA,
};

const DIM: usize = 64;
const ROWS: usize = 2_000;
const QUERIES: usize = 16;
const K: usize = 10;
const TEST_NAME: &str = "dense_and_hnsw_search_receipts";

struct Probe {
    index: VectorIndex,
    queries: Vec<f32>,
}

fn probe(backend: &str) -> Probe {
    let corpus = unit_vectors(ROWS, DIM, 0xD3_5E00);
    let mut index = if backend == HNSW {
        VectorIndex::create(HNSW, DIM, 8).unwrap()
    } else {
        let (shift, scale) = fit_calibration(DIM, 4, &corpus);
        seeded_index(DIM, 4, &shift, &scale)
    };
    index.add(&corpus, DIM).unwrap();
    index.prepare().unwrap();
    // The probe must time the path it names: a linked graph for HNSW
    // (an unlinked store answers by exhaustive enumeration instead).
    assert_eq!(index.descriptor().backend_kind, backend);
    assert!(
        backend != HNSW || index.is_graph_linked(),
        "HNSW graph not built"
    );
    let queries = corpus[..QUERIES * DIM].to_vec();
    let check = index.search_unfiltered(&queries[..DIM], K);
    assert_eq!(check.indices_for_query(0).len(), K);
    Probe { index, queries }
}

fn search_op() -> impl FnMut(&Probe) -> usize {
    let mut next = 0;
    move |probe: &Probe| {
        next = (next + 1) % QUERIES;
        let query = &probe.queries[next * DIM..(next + 1) * DIM];
        probe
            .index
            .search_unfiltered(query, K)
            .indices_for_query(0)
            .len()
    }
}

fn number(value: &Value, path: &[&str]) -> f64 {
    let leaf = path.iter().fold(value, |v, key| &v[*key]);
    leaf.as_f64()
        .unwrap_or_else(|| panic!("receipt field {path:?} is not a number"))
}

/// Schema and consistency of one receipt produced under `config`.
fn validate(receipt: &Value, config: &TrialConfig) {
    assert_eq!(receipt["schema"], SCHEMA);
    let provenance = &receipt["provenance"];
    let sha = provenance["git_sha"].as_str().expect("git_sha");
    assert!(
        sha.len() == 40 && sha.bytes().all(|b| b.is_ascii_hexdigit()),
        "{sha}"
    );
    assert!(
        provenance["git_dirty"].is_boolean(),
        "git_dirty is recorded"
    );
    assert!(provenance["cpu_model"].is_string() || provenance["cpu_model"].is_null());
    assert!(provenance["rustflags"].is_string() || provenance["rustflags"].is_null());
    assert!(number(provenance, &["available_parallelism"]) >= 1.0);
    assert!(provenance["debug_assertions"].is_boolean());
    assert_eq!(receipt["config"]["forks"], config.forks);
    assert_eq!(receipt["config"]["iterations"], config.iterations);
    let forks = receipt["forks"].as_array().expect("forks");
    assert_eq!(forks.len(), config.forks);
    for fork in forks {
        assert!(fork["batch_ops"].as_u64().unwrap() >= 1);
        for key in ["wall_ns_per_op", "cpu_ns_per_op"] {
            let samples = fork[key].as_array().unwrap();
            assert_eq!(samples.len(), config.iterations);
            assert!(samples.iter().all(|s| s.as_f64().unwrap() > 0.0));
        }
    }
    for clock in ["wall_ns_per_op", "cpu_ns_per_op"] {
        let stat = |key| number(receipt, &[clock, key]);
        assert_eq!(stat("n") as usize, config.forks * config.iterations);
        assert!(stat("p50") <= stat("p95") && stat("p95") <= stat("p99"));
        assert!(stat("ci99_low") <= stat("mean") && stat("mean") <= stat("ci99_high"));
        assert!(stat("stddev") >= 0.0);
    }
    let ratio = number(receipt, &["cpu_to_wall_ratio"]);
    assert!(
        ratio > 0.0 && ratio <= 1.05,
        "one thread cannot outrun the wall: {ratio}"
    );
}

#[test]
fn dense_and_hnsw_search_receipts() {
    let config = TrialConfig::from_env(TrialConfig::smoke());
    let keep = std::env::var_os("COLLAB_JMH_RECEIPT_DIR").is_some();
    // The parent owns the temporary receipt directory and removes it on
    // every exit, panics included; forked children never touch it.
    let parent = std::env::var_os(FORK_ENV).is_none();
    let _remove = (parent && !keep).then(|| RemoveOnDrop(default_receipt_dir()));
    let dir = default_receipt_dir().join("dense-probe");
    let module = module_path!().split_once("::").expect("crate prefix").1;
    let bench = |name: &str| Bench {
        name: name.into(),
        test_path: format!("{module}::{TEST_NAME}"),
        config: config.clone(),
        receipt_dir: dir.clone(),
    };
    let turbovec = bench("dense-turbovec").run(|| probe(EMBEDDED_TURBOVEC), search_op());
    let hnsw = bench("dense-hnsw").run(|| probe(HNSW), search_op());
    let (Some((turbovec_path, turbovec)), Some((hnsw_path, hnsw))) = (turbovec, hnsw) else {
        return; // A forked child: its samples are written.
    };
    for (path, receipt) in [(&turbovec_path, &turbovec), (&hnsw_path, &hnsw)] {
        validate(receipt, &config);
        let on_disk: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(&on_disk, receipt, "the written receipt is the returned one");
    }
    let p50 = |r: &Value| number(r, &["wall_ns_per_op", "p50"]);
    let comparison = json!({
        "schema": SCHEMA,
        "comparison": ["dense-hnsw", "dense-turbovec"],
        "hnsw_over_turbovec_wall_p50": p50(&hnsw) / p50(&turbovec),
        "receipts": [turbovec_path, hnsw_path],
    });
    let path = dir.join(format!("dense-comparison-{}.json", std::process::id()));
    std::fs::write(&path, serde_json::to_string_pretty(&comparison).unwrap()).unwrap();
    eprintln!("dense probe receipts: {}", path.display());
}
