//! Regression coverage for `FieldTranspose`, segment-sourced resharding, and
//! segment-sourced compaction when segments contain leading, middle, or
//! trailing empty or all-filtered rows.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use pipestream_search::analyzer::{
    analyze_batch_streams, uax29_body_spec, NATIVE_ANALYSIS_BACKEND,
};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::{
    embedded_backend_request, fit_calibration, start_empty_node, start_opened_node, unit_vectors,
};
use pipestream_search::live_docs::LiveDocs;
use pipestream_search::mapping::derive_plan;
use pipestream_search::node::{segments_root, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::*;
use pipestream_search::placement::*;
use pipestream_search::postings::{
    AnalyzedDoc, AnalyzedField, Bm25Reader, Bm25Store, SpillBuilder, StoredBinding,
};
use pipestream_search::reshard::{self, PartitionSpec, SegmentCompactionOptions};
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog};
use prost::Message;
use tokio_stream::iter as stream_iter;
use tonic::Request;

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
const SEAL_ROWS: usize = 8;
const TOTAL_ROWS: usize = 24;
const SEED: u64 = 0xE071_5107;

type RowSpec = (&'static str, AnalyzedField, AnalyzedField);
type ServerHandle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("wave2_empty_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn empty_row(text: &'static str) -> RowSpec {
    let body = AnalyzedField {
        positions: Some(Vec::new()),
        sentences: Some(Vec::new()),
        ..Default::default()
    };
    (text, body, AnalyzedField::default())
}

fn full_body(tag: &str, scale: f32) -> AnalyzedField {
    AnalyzedField {
        terms: vec![
            ("alpha".into(), 1, vec![(0, 5)]),
            (format!("tok_{tag}"), 2, vec![(6, 10), (11, 15)]),
        ],
        length: 3,
        positions: Some(vec![vec![0], vec![1, 3]]),
        sentences: Some(vec![(0, 16)]),
        pos_lengths: Some(vec![vec![1], vec![2, 1]]),
        splade: vec![("alpha".into(), 42), (format!("sp_{tag}"), 19)],
        splade_scale_bits: scale.to_bits(),
    }
}

fn full_title(tag: &str) -> AnalyzedField {
    AnalyzedField {
        terms: vec![(format!("head_{tag}"), 1, vec![(0, 4)])],
        length: 1,
        ..Default::default()
    }
}

fn assert_transpose_case(dir: &Path, name: &str, docs: &[RowSpec]) {
    let any_lattice = docs.iter().any(|(_, b, _)| b.pos_lengths.is_some());
    let sp = dir.join(format!("{name}_s.bm25"));
    let bp = dir.join(format!("{name}_b.bm25"));
    let mut store = Bm25Store::with_fields(&["body", "title"])
        .with_positions(&["body"])
        .with_sentences(&["body"]);
    let mut spill = SpillBuilder::create_with_fields(&dir.join(name), &["body", "title"])
        .unwrap()
        .with_position_fields(&["body"])
        .with_sentence_fields(&["body"]);
    for (slot, (text, body, title)) in docs.iter().enumerate() {
        let doc = AnalyzedDoc {
            fields: vec![body.clone(), title.clone()],
            ..Default::default()
        };
        store.add_document(slot as u32, (*text).into(), doc.clone());
        spill
            .add_document_with_lineage(slot as u32, (*text).into(), doc, None)
            .unwrap();
    }
    store.save(&sp).unwrap();
    spill.finish(&bp).unwrap();
    for path in [&sp, &bp] {
        let r = Bm25Reader::open(path).unwrap();
        assert_eq!(r.next_doc_id() as usize, docs.len());
        let btx = r.field(0).transpose().unwrap();
        let ttx = r.field(1).transpose().unwrap();
        assert_eq!((btx.rows(), ttx.rows()), (docs.len(), docs.len()));
        for (slot, (_, body, title)) in docs.iter().enumerate() {
            let mut want = body.clone();
            if any_lattice && want.positions.is_some() && want.pos_lengths.is_none() {
                want.pos_lengths = Some(Vec::new());
            }
            assert_eq!(btx.field(slot as u32), Some(want), "{name}:{slot}");
            assert_eq!(ttx.field(slot as u32), Some(title.clone()));
        }
        let end = docs.len() as u32;
        assert_eq!((btx.field(end), ttx.field(end)), (None, None));
    }
}

#[test]
fn field_transpose_preserves_leading_middle_and_trailing_empty_slots() {
    let d = tempdir("direct");
    let (e0, e1) = (empty_row(""), empty_row("---"));
    let fa = ("a", full_body("a", 0.125), full_title("a"));
    let fb = ("b", full_body("b", 0.25), full_title("b"));
    let lead = [e0.clone(), e1.clone(), fa.clone(), fb.clone()];
    assert_transpose_case(&d, "lead", &lead);
    assert_transpose_case(&d, "mid", &[fa.clone(), e0.clone(), e1.clone(), fb.clone()]);
    assert_transpose_case(&d, "trail", &[fa, fb, e0.clone(), e1.clone()]);
    let (_, eb, et) = empty_row("");
    let fc = ("c", full_body("c", 0.125), et);
    let fd = ("---", eb, full_title("d"));
    let fe = ("e", full_body("e", 0.375), full_title("e"));
    assert_transpose_case(&d, "mix", &[e0, fc, fd, fe, e1]);
}

/// Every 8-row segment carries leading (`0, 1`), middle (`4`), and trailing
/// (`6, 7`) whitespace/punctuation rows that UAX-29 filters to zero tokens.
fn fixture_doc(row: usize, placement_code: Option<i64>) -> AddDocumentsRequest {
    let (body, title): (String, Option<String>) = match row % SEAL_ROWS {
        0 => ("   \t\n".into(), None),
        1 => ("--- ... !!!".into(), Some("::: ;;;".into())),
        2 => (format!("opinion {row} common beta"), None),
        3 => ("--- !!!".into(), Some(format!("caption {row} common"))),
        4 => ("\u{200b}\u{200c}   \t".into(), None),
        6 => ("((( ))) ---".into(), Some("...".into())),
        7 => ("=== *** ///".into(), Some("---".into())),
        _ => (
            format!("opinion {row} common gamma"),
            Some(format!("caption {row} delta")),
        ),
    };
    let mut integers = vec![IntegerValue {
        field: "year".into(),
        value: 2000 + (row % 6) as i64,
    }];
    if let Some(v) = placement_code {
        integers.push(IntegerValue {
            field: "placement".into(),
            value: v,
        });
    }
    let fields = title
        .into_iter()
        .map(|text| DocumentField {
            field: "title".into(),
            text,
            analysis: Some(uax29_body_spec()),
        })
        .collect();
    let grp = ["ca9", "ca2", "scotus"][row % 3].into();
    AddDocumentsRequest {
        text: body,
        analysis: Some(uax29_body_spec()),
        lineage: Some(DocLineage {
            parent_id: row as u64,
            ..Default::default()
        }),
        fields,
        facets: vec![FacetValue {
            field: "grp".into(),
            value: grp,
        }],
        integers,
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        ..Default::default()
    }
}

fn shard_node_config(index_path: PathBuf, wal: bool) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        seal_tail_docs: SEAL_ROWS as u32,
        wal,
        wal_buckets: if wal { 2 } else { 1 },
        bm25_fields: vec!["body".into(), "title".into()],
        facet_fields: vec!["grp".into()],
        integer_fields: vec!["year".into(), "placement".into()],
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        ..Default::default()
    }
}

async fn seed_segmented_node(addr: &str, placement_code: Option<i64>) {
    let vectors = unit_vectors(TOTAL_ROWS, DIM, SEED);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &vectors);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let cfg_req = embedded_backend_request(DIM, BIT_WIDTH, &shift, &scale);
    client.configure_vector_backend(cfg_req).await.unwrap();
    for block in 0..(TOTAL_ROWS / SEAL_ROWS) {
        let (start, end) = (block * SEAL_ROWS, (block + 1) * SEAL_ROWS);
        let docs: Vec<_> = (start..end)
            .map(|r| fixture_doc(r, placement_code))
            .collect();
        client.add_documents(stream_iter(docs)).await.unwrap();
        let vectors = vectors[start * DIM..end * DIM].to_vec();
        let vreq = AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors,
            dim: DIM as u32,
        };
        client.add_vectors(stream_iter([vreq])).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
}

async fn snapshot_rows(addrs: &[String]) -> (BTreeMap<u64, String>, BTreeMap<u64, u64>) {
    let (mut by_parent, mut parent_of_doc) = (BTreeMap::new(), BTreeMap::new());
    for addr in addrs {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let req = BrowseShardRequest {
            k: 1_000,
            first_page: true,
            ..Default::default()
        };
        let doc_ids = client.browse_shard(req).await.unwrap().into_inner().doc_ids;
        let fetched = client
            .get_documents(GetDocumentsRequest { doc_ids })
            .await
            .unwrap();
        for doc in fetched.into_inner().documents {
            let pid = doc.lineage.unwrap().parent_id;
            assert!(by_parent.insert(pid, doc.text).is_none(), "duplicate {pid}");
            parent_of_doc.insert(doc.doc_id, pid);
        }
    }
    (by_parent, parent_of_doc)
}

async fn lexical_sig(addrs: &[String], pmap: &BTreeMap<u64, u64>, query: &str) -> Vec<(u64, u32)> {
    let backend = Some(NATIVE_ANALYSIS_BACKEND.to_string());
    let coord = CoordinatorServiceImpl::new(addrs.to_vec()).with_bm25(backend, Default::default());
    let qf = |f: &str| QueryField {
        field: f.into(),
        weight: 1.0,
        analysis: Some(uax29_body_spec()),
        ..Default::default()
    };
    let req = Bm25SearchRequest {
        text: query.into(),
        k: 100,
        fields: vec![qf("body"), qf("title")],
        ..Default::default()
    };
    let resp = SearchService::bm25_search(&coord, Request::new(req))
        .await
        .unwrap();
    let mut hits: Vec<_> = resp
        .into_inner()
        .hits
        .into_iter()
        .map(|h| (pmap[&h.doc_id], h.score.to_bits()))
        .collect();
    hits.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    hits
}

fn placement_trees() -> (PlacementTreeConfig, PlacementTreeConfig) {
    let leaf = |n: &str, c: Option<&str>| PlacementNodeConfig {
        name: n.into(),
        cel: c.map(str::to_string),
        shards: 1,
        nodes: Vec::new(),
        children: Vec::new(),
    };
    let old = PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 4,
        nodes: vec![leaf("archive", None)],
    };
    let split = PlacementTreeConfig {
        column: "placement".into(),
        level_bits: 4,
        nodes: vec![leaf("hi", Some("year >= 2003")), leaf("lo", None)],
    };
    (old, split)
}

fn split_tree(
    gen: &Path,
    tree: &PlacementTreeConfig,
    dst: PathBuf,
    source: reshard::TreeRowSource,
    cut: reshard::SpillCut,
) -> reshard::TreeReshardOutput {
    let handle = tokio::runtime::Handle::current();
    let mut an = |docs: &[(&str, Option<&_>, _)]| {
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                analyze_batch_streams(NATIVE_ANALYSIS_BACKEND, docs, 1)
                    .await
                    .map_err(|e| e.to_string())
            })
        })
    };
    let opts = reshard::TreeSplitOptions {
        source,
        cut,
        ..Default::default()
    };
    let gens = [gen.to_path_buf()];
    reshard::split_placement_tree_logs(&gens, tree, &dst, &[0, 1_000], None, opts, &mut an).unwrap()
}

async fn serve_split_children(
    out: &reshard::TreeReshardOutput,
    tree: &PlacementTreeConfig,
) -> (Vec<String>, Vec<ServerHandle>) {
    let (mut addrs, mut handles) = (Vec::new(), Vec::new());
    for (image, child) in out.images.children.iter().zip(&out.children) {
        let mut cfg = shard_node_config(image.vector_path.clone(), false);
        cfg.slot_offset = image.slot_offset;
        cfg.placement_column = Some("placement".into());
        cfg.placement_leaf = Some(child.code);
        let pinned = PinnedLeaf::pin(tree, "placement", child.code).unwrap();
        cfg.placement_tree = Some(std::sync::Arc::new(pinned));
        let (addr, handle) = start_opened_node(cfg).await;
        addrs.push(addr);
        handles.push(handle);
    }
    (addrs, handles)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reshard_from_segments_preserves_leading_middle_and_trailing_empty_rows() {
    let dir = tempdir("reshard");
    let (old_tree, new_tree) = placement_trees();
    let archive = Placement::validate(&old_tree)
        .unwrap()
        .leaf_by_name("archive")
        .unwrap()
        .code;
    let index_path = dir.join("source.tv");
    let (addr, _src) = start_empty_node(shard_node_config(index_path.clone(), true)).await;
    seed_segmented_node(&addr, Some(archive)).await;
    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path)).unwrap();

    let src_logs = reshard::TreeRowSource::Logs;
    let by_logs = split_tree(
        &gen,
        &new_tree,
        dir.join("logs"),
        src_logs,
        reshard::SpillCut::Hash,
    );
    let (log_addrs, _lh) = serve_split_children(&by_logs, &new_tree).await;
    let (log_rows, log_map) = snapshot_rows(&log_addrs).await;

    let col_cut = reshard::SpillCut::Column {
        column: "year".into(),
        rows_per_cut: 6,
    };
    for (tag, cut) in [("seg_hash", reshard::SpillCut::Hash), ("seg_col", col_cut)] {
        let src_seg = reshard::TreeRowSource::Segments;
        let by_seg = split_tree(&gen, &new_tree, dir.join(tag), src_seg, cut);
        assert_eq!(by_seg.transplanted_rows, TOTAL_ROWS as u64);
        assert_eq!(by_seg.placed, by_logs.placed);
        let (seg_addrs, _sh) = serve_split_children(&by_seg, &new_tree).await;
        let (seg_rows, seg_map) = snapshot_rows(&seg_addrs).await;
        assert_eq!(seg_rows, log_rows);
        for q in ["common", "beta", "caption"] {
            let got = lexical_sig(&seg_addrs, &seg_map, q).await;
            assert_eq!(got, lexical_sig(&log_addrs, &log_map, q).await);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_from_segments_preserves_leading_middle_and_trailing_empty_rows() {
    let dir = tempdir("compact");
    let index_path = dir.join("walless.tv");
    let desc = include_bytes!("fixtures/vector-binding/descriptor.bin");
    let plan = derive_plan(desc, "vector_binding.Named").unwrap();
    let binding = StoredBinding {
        plan_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
        ..Default::default()
    };
    SegmentCatalog::open(segments_root(&index_path))
        .unwrap()
        .publish_binding(&binding)
        .unwrap();

    let (addr, _handle) = start_opened_node(shard_node_config(index_path.clone(), false)).await;
    seed_segmented_node(&addr, None).await;
    let addrs = std::slice::from_ref(&addr);
    let (before_rows, before_map) = snapshot_rows(addrs).await;
    assert_eq!(before_rows.len(), TOTAL_ROWS);
    let bc = lexical_sig(addrs, &before_map, "common").await;
    let bt = lexical_sig(addrs, &before_map, "caption").await;

    let set = OpenedSegmentSet::open(segments_root(&index_path)).unwrap();
    let spec = PartitionSpec {
        column: "year",
        bound: 8,
    };
    let (live, def) = (LiveDocs::default(), SegmentCompactionOptions::default());
    let offline =
        reshard::compact_segments_partitioned(&set, &live, 0, &dir.join("cuts"), spec, &def)
            .unwrap();
    assert_eq!(offline.rows_before, TOTAL_ROWS as u64);
    assert_eq!(offline.id_map.len(), TOTAL_ROWS);
    drop(set);

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let creq = CompactShardRequest {
        partition_column: "year".into(),
        tail_bound: 8,
        ..Default::default()
    };
    let resp = client.compact_shard(creq).await.unwrap().into_inner();
    assert_eq!(
        (resp.rows_before, resp.rows_after),
        (TOTAL_ROWS as u64, TOTAL_ROWS as u64)
    );
    let (after_rows, after_map) = snapshot_rows(addrs).await;
    assert_eq!(after_rows, before_rows);
    assert_eq!(lexical_sig(addrs, &after_map, "common").await, bc);
    assert_eq!(lexical_sig(addrs, &after_map, "caption").await, bt);
}
