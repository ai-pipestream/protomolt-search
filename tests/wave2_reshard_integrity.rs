//! Reshard inputs that disagree, or that lost their transplanted analysis,
//! are refused before any child is published.

mod common;

use std::path::{Path, PathBuf};
use std::sync::Arc;

use common::start_empty_node;
use pipestream_search::analyzer::{body_spec, SessionLayers, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::derived::{spec_from_config, Declaration, DerivedColumnConfig};
use pipestream_search::node::{Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::wal::{wal_record, FlushMarker, LoggedAddDocuments, LoggedAddVectors};
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, AnalysisSpec, FlushRequest, IntegerValue,
};
use pipestream_search::placement::{PlacementNodeConfig, PlacementTreeConfig};
use pipestream_search::postings::AnalyzedDoc;
use pipestream_search::reshard;
use pipestream_search::wal::{self, WalManifest, WalWriter};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

const DIM: usize = 16;
const BITS: usize = 4;

/// A scratch directory removed when the test ends, pass or fail.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("wave2-reshard-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl std::ops::Deref for TempDir {
    type Target = Path;
    fn deref(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn derived(expression: &str) -> Vec<DerivedColumnConfig> {
    vec![DerivedColumnConfig {
        name: "bucket".into(),
        kind: "i64".into(),
        expression: expression.into(),
        disclosure: "inputs".into(),
    }]
}

/// A flushed full-history generation holding one vector at `id`, written
/// under `declaration` (empty for none).
fn generation(dir: &Path, name: &str, id: u64, declaration: &[DerivedColumnConfig]) -> PathBuf {
    let (shift, scale) = common::fit_calibration(DIM, BITS, &common::unit_vectors(8, DIM, 7));
    let fingerprint = if declaration.is_empty() {
        String::new()
    } else {
        Declaration::compile(&spec_from_config(declaration).unwrap())
            .unwrap()
            .fingerprint()
            .to_string()
    };
    let mut manifest = WalManifest {
        collection: String::new(),
        dim: DIM as u32,
        vector_backend: String::new(),
        vector_config_format: String::new(),
        vector_config_payload: Vec::new(),
        slot_offset: id,
        generation: 0,
        bucket_bits: 2,
        bucket_count: 4,
        preexisting_vectors: 0,
        preexisting_documents: 0,
        format_version: wal::FORMAT_VERSION,
        derived_fingerprint: fingerprint,
        derived: declaration.to_vec(),
        columns: Some(wal::ColumnTable {
            fields: vec!["body".into()],
            integers: vec!["placement".into(), "bucket".into()],
            ..Default::default()
        }),
    };
    manifest.set_backend_config(
        pipestream_search::vector::embedded_turbovec_config(BITS, &shift, &scale).unwrap(),
    );
    let root = dir.join(name);
    let mut writer = WalWriter::create(&root, manifest).unwrap();
    writer
        .append(wal_record::Op::AddVectors(LoggedAddVectors {
            first_id: id,
            batch: Some(AddVectorsRequest {
                vectors: common::unit_vectors(1, DIM, id + 1),
                dim: DIM as u32,
                materialized_rows: Vec::new(),
            }),
            stable_routing_keys: Vec::new(),
        }))
        .unwrap();
    // One document already derived under this log's declaration.
    let mut row = document(id as usize);
    if !declaration.is_empty() {
        row.integers.push(IntegerValue {
            field: "bucket".into(),
            value: 7,
        });
        row.derived_fingerprint = wal::read_manifest(&wal::gen_dir(&root, 0))
            .unwrap()
            .derived_fingerprint;
    }
    writer
        .append(wal_record::Op::AddDocuments(LoggedAddDocuments {
            first_id: id,
            documents: vec![row],
            ..Default::default()
        }))
        .unwrap();
    writer
        .append(wal_record::Op::Flush(FlushMarker {}))
        .unwrap();
    writer.flush().unwrap();
    wal::gen_dir(&root, 0)
}

type Batch<'a, 'b> = [(&'a str, Option<&'b AnalysisSpec>, SessionLayers)];

/// The native analyzer under the body spec when `working`, as a serving
/// sidecar with a default analysis would answer; otherwise one that
/// refuses any call.
fn analyzer(working: bool) -> impl FnMut(&Batch<'_, '_>) -> Result<Vec<AnalyzedDoc>, String> {
    let handle = tokio::runtime::Handle::try_current().ok();
    move |docs| {
        if !working {
            return Err("this run analyzes nothing".to_string());
        }
        let spec = body_spec();
        let docs: Vec<_> = docs
            .iter()
            .map(|(text, given, layers)| (*text, Some(given.unwrap_or(&spec)), *layers))
            .collect();
        let handle = handle
            .as_ref()
            .expect("a working analyzer runs under tokio");
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                pipestream_search::analyzer::analyze_batch_streams(
                    NATIVE_ANALYSIS_BACKEND,
                    &docs,
                    1,
                )
                .await
                .map_err(|e| e.to_string())
            })
        })
    }
}

fn merge(dir: &Path, out: &str, gens: &[PathBuf]) -> Result<reshard::ReshardOutput, String> {
    reshard::merge(gens, &dir.join(out), None, true, None, &mut analyzer(false))
}

#[test]
fn merge_refuses_inputs_under_different_derived_declarations_in_either_order() {
    let dir = TempDir::new("merge-derived");
    let year = generation(&dir, "year.wal", 0, &derived("calendar.year(placement)"));
    let month = generation(&dir, "month.wal", 1, &derived("calendar.month(placement)"));
    let plain = generation(&dir, "plain.wal", 2, &[]);

    // The fixture is sound: inputs under one declaration merge.
    let same = generation(&dir, "same.wal", 3, &derived("calendar.year(placement)"));
    merge(&dir, "ok", &[year.clone(), same]).unwrap();

    for (out, pair) in [
        ("year-month", [year.clone(), month.clone()]),
        ("month-year", [month.clone(), year.clone()]),
        ("year-plain", [year.clone(), plain.clone()]),
        ("plain-year", [plain.clone(), year.clone()]),
    ] {
        let err = match merge(&dir, out, &pair) {
            Ok(_) => panic!("{out}: a merge across derived declarations was published"),
            Err(err) => err,
        };
        assert!(err.contains("must share one declaration"), "{out}: {err}");
        assert!(
            !dir.join(out).join("shard-0.tv").exists(),
            "{out}: the refused merge left an image"
        );
    }
}

/// The declared-against-declared and declared-against-none pairs, both
/// ways round; `run` is one multi-input entry point over the pair.
fn refuses_mixed_declarations(tag: &str, run: impl Fn(&Path, &[PathBuf]) -> Result<(), String>) {
    let dir = TempDir::new(tag);
    let year = generation(&dir, "year.wal", 0, &derived("calendar.year(placement)"));
    let month = generation(&dir, "month.wal", 1, &derived("calendar.month(placement)"));
    let plain = generation(&dir, "plain.wal", 2, &[]);
    for (out, pair) in [
        ("year-month", [year.clone(), month.clone()]),
        ("month-year", [month, year.clone()]),
        ("year-plain", [year.clone(), plain.clone()]),
        ("plain-year", [plain, year]),
    ] {
        let err = match run(&dir.join(out), &pair) {
            Ok(()) => panic!("{tag} {out}: children were published across declarations"),
            Err(err) => err,
        };
        assert!(
            err.contains("must share one declaration"),
            "{tag} {out}: {err}"
        );
    }
}

#[test]
fn a_hash_split_refuses_inputs_under_different_derived_declarations() {
    refuses_mixed_declarations("split-logs", |out, gens| {
        reshard::split_logs(gens, 1, out, 0, 1 << 20, true, None, &mut analyzer(false)).map(|_| ())
    });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tree_split_refuses_inputs_under_different_derived_declarations() {
    refuses_mixed_declarations("split-tree", |out, gens| {
        let options = reshard::TreeSplitOptions::default();
        let mut analyze = analyzer(true);
        reshard::split_placement_tree_logs(
            gens,
            &one_leaf(),
            out,
            &[0],
            None,
            options,
            &mut analyze,
        )
        .map(|_| ())
    });
}

fn document(i: usize) -> AddDocumentsRequest {
    AddDocumentsRequest {
        text: format!("opinion {i} about search and retrieval"),
        analysis: Some(body_spec()),
        integers: vec![IntegerValue {
            field: "placement".into(),
            value: 1_700_000_000_000 + i as i64,
        }],
        ..Default::default()
    }
}

/// A sealed, flushed segmented shard of `n` aligned rows written under
/// `derived`; its generation.
async fn sealed_source(dir: &Path, n: usize, derived: Option<Arc<Declaration>>) -> PathBuf {
    let index_path = dir.join("source.tv");
    let (addr, handle) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::Segments,
        seal_tail_docs: 8,
        wal: true,
        wal_buckets: 4,
        integer_fields: vec!["placement".into()],
        derived,
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (shift, scale) = common::fit_calibration(DIM, BITS, &common::unit_vectors(8, DIM, 7));
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, BITS, &shift, &scale,
        ))
        .await
        .unwrap();
    let vectors = common::unit_vectors(n, DIM, 0x5EED);
    for (block, rows) in (0..n).collect::<Vec<_>>().chunks(4).enumerate() {
        let (tx, rx) = mpsc::channel(rows.len());
        for &i in rows {
            tx.send(document(i)).await.unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = block * 4 * DIM;
        let (tx, rx) = mpsc::channel(1);
        tx.send(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: vectors[start..start + rows.len() * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
    handle.abort();
    let (_, gen) = wal::latest_gen(&wal::wal_dir(&index_path))
        .unwrap()
        .unwrap();
    gen
}

fn one_leaf() -> PlacementTreeConfig {
    PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 0,
        nodes: vec![PlacementNodeConfig {
            name: "all".into(),
            shards: 1,
            ..Default::default()
        }],
    }
}

/// A from-segments split into one child with one spill bucket, under an
/// analyzer that works: a build that leaves transplant mode succeeds
/// with whatever that analyzer returns.
fn split_from_segments(gen: &Path, out: &Path) -> Result<reshard::TreeReshardOutput, String> {
    reshard::split_placement_tree_logs(
        std::slice::from_ref(&gen.to_path_buf()),
        &one_leaf(),
        out,
        &[0],
        None,
        reshard::TreeSplitOptions {
            source: reshard::TreeRowSource::Segments,
            spill_bucket_bits: Some(0),
            ..Default::default()
        },
        &mut analyzer(true),
    )
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_from_segments_split_refuses_a_spill_bucket_whose_analysis_sidecar_is_empty() {
    let dir = TempDir::new("sidecar");
    let gen = sealed_source(&dir, 24, None).await;
    let source_generation = wal::read_manifest(&gen).unwrap().generation;

    // The fixture is sound: the same split with its sidecar intact
    // transplants every row.
    let intact = split_from_segments(&gen, &dir.join("intact")).unwrap();
    assert_eq!(intact.transplanted_rows, 24);

    // The spill's one sidecar path leads to a file that keeps nothing,
    // so the populated bucket reads back a zero-byte sidecar.
    let out = dir.join("lost");
    let spill = wal::gen_dir(&out.join("spill").join("c00000"), source_generation);
    std::fs::create_dir_all(&spill).unwrap();
    std::os::unix::fs::symlink("/dev/null", spill.join("analysis-00000.bin")).unwrap();
    let err = match split_from_segments(&gen, &out) {
        Ok(_) => panic!("a child was published without its transplanted analysis"),
        Err(err) => err,
    };
    assert!(
        err.contains("analysis sidecar is missing or truncated"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_from_segments_split_refuses_segments_whose_declaration_differs_from_the_log() {
    let dir = TempDir::new("segment-declaration");
    let compile = |expression: &str| {
        Declaration::compile(&spec_from_config(&derived(expression)).unwrap()).unwrap()
    };
    let sealed_under = Arc::new(compile("calendar.year(placement)"));
    let gen = sealed_source(&dir, 24, Some(sealed_under)).await;

    // The sealed rows hold years; the log's manifest now names the same
    // column as a month, as it would after its shard was redeclared.
    let mut manifest = wal::read_manifest(&gen).unwrap();
    manifest.derived_fingerprint = compile("calendar.month(placement)")
        .fingerprint()
        .to_string();
    manifest.derived = derived("calendar.month(placement)");
    wal::write_manifest(&gen, &manifest).unwrap();

    let err = match split_from_segments(&gen, &dir.join("restamped")) {
        Ok(_) => panic!("rows derived as years were published under the month declaration"),
        Err(err) => err,
    };
    assert!(
        err.contains("derived-column declaration") && err.contains("log manifest"),
        "{err}"
    );
}
