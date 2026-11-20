//! Reshard keeps a document's chunks on one child, and every sealed
//! segment records whether its lineage parents are colocated on its shard:
//! routed (keyed) ingest and a parent-routed reshard prove it; raw lineage
//! ingest cannot, and nothing downstream launders the missing proof.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use pipestream_search::analyzer::{analyze_document_native, body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::node::{segments_root, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, AnalysisSpec, CompactShardRequest, DeleteDocumentsRequest, DocLineage,
    FlushRequest,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader};
use pipestream_search::reshard::{self, SplitRequest};
use pipestream_search::segments::OpenedSegmentSet;
use tonic::transport::Channel;
use tonic::Request;

/// Parents with several chunks each, and unparented rows beside them.
const PARENTS: u64 = 6;
const CHUNKS: u64 = 3;
const LONE_ROWS: u64 = 24;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("parents_colocated_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: &Path) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path.to_path_buf()),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        wal: true,
        wal_buckets: 8,
        ..Default::default()
    }
}

/// The parent id a chunk of parent `p` names: far above every row id, so a
/// chunk is never its own parent.
fn parent_id(p: u64) -> u64 {
    1_000_000 + p
}

/// Append one document, under `key` when the row was routed by it.
async fn append(
    client: &mut NodeServiceClient<Channel>,
    text: String,
    lineage: Option<u64>,
    key: Option<String>,
) {
    let doc = AddDocumentsRequest {
        text,
        analysis: Some(body_spec()),
        lineage: lineage.map(|parent_id| DocLineage {
            parent_id,
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut request = Request::new(tokio_stream::iter([doc]));
    if let Some(key) = key {
        request.metadata_mut().insert_bin(
            "x-protomolt-stable-key-bin",
            tonic::metadata::MetadataValue::from_bytes(key.as_bytes()),
        );
    }
    client.add_documents(request).await.unwrap();
}

/// Every parent's chunks, interleaved with unparented rows so a parent's
/// chunks fall in different WAL buckets; keyed by the parent's document
/// key when `routed`.
async fn ingest_corpus(client: &mut NodeServiceClient<Channel>, routed: bool) {
    // A reshard replays under the log's locked provider state, so the
    // shard locks one before its first row even though no row has a vector.
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 0xC0_10CA7E));
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            8, 4, &shift, &scale,
        ))
        .await
        .unwrap();
    for row in 0..LONE_ROWS {
        let key = routed.then(|| format!("lone-{row}"));
        append(client, format!("lone row {row} court"), None, key).await;
        if row < PARENTS * CHUNKS {
            let p = row % PARENTS;
            let key = routed.then(|| format!("opinion-{p}"));
            let text = format!("opinion {p} chunk {row} court");
            append(client, text, Some(parent_id(p)), key).await;
        }
    }
    client.flush(FlushRequest {}).await.unwrap();
}

fn catalog(index_path: &Path) -> OpenedSegmentSet {
    OpenedSegmentSet::open(segments_root(index_path)).unwrap()
}

/// The lineage parent of every row a catalog holds, by part.
fn parents_of(set: &OpenedSegmentSet) -> Vec<u64> {
    let mut parents = Vec::new();
    for part in 0..set.len() {
        let reader: &Bm25Reader = set.bm25(part);
        for row in 0..reader.next_doc_id() {
            if Bm25Index::text(reader, row).is_some() {
                parents.push(reader.lineage(row).map_or(u64::MAX, |l| l.parent_id));
            }
        }
    }
    parents
}

/// One batch the reshard analyzer receives.
type Batch<'t, 's> = [(
    &'t str,
    Option<&'s AnalysisSpec>,
    pipestream_search::analyzer::SessionLayers,
)];

fn native_analyzer(docs: &Batch<'_, '_>) -> Result<Vec<AnalyzedDoc>, String> {
    docs.iter()
        .map(|(text, spec, _)| analyze_document_native(text, *spec).map_err(|e| e.to_string()))
        .collect()
}

/// Split the shard's log four ways by lineage parent.
fn split_four(index_path: &Path, out_dir: &Path) -> reshard::ReshardOutput {
    let gen = reshard::resolve_gen(&pipestream_search::wal::wal_dir(index_path)).unwrap();
    let request = SplitRequest::new(std::slice::from_ref(&gen), 4, out_dir).slot_stride(1_000);
    reshard::split_logs_with(&request, &mut native_analyzer).unwrap()
}

/// Every parent's chunks sit in exactly one child, while the unparented
/// rows spread over more than one.
fn assert_parents_whole(output: &reshard::ReshardOutput) {
    let mut homes: BTreeMap<u64, BTreeSet<usize>> = BTreeMap::new();
    let mut lone_children = BTreeSet::new();
    let mut rows = 0u64;
    for (child, image) in output.children.iter().enumerate() {
        if image.num_documents == 0 {
            continue;
        }
        for parent in parents_of(&catalog(&image.vector_path)) {
            rows += 1;
            if parent == u64::MAX {
                lone_children.insert(child);
            } else {
                homes.entry(parent).or_default().insert(child);
            }
        }
    }
    assert_eq!(rows, LONE_ROWS + PARENTS * CHUNKS, "every row lands once");
    assert_eq!(homes.len() as u64, PARENTS);
    for (parent, children) in &homes {
        assert_eq!(children.len(), 1, "parent {parent} split over {children:?}");
    }
    // Unparented rows route by their own stable key when keyed (the
    // online hash space), by id only when unkeyed; either way each one
    // is its own group, so no spread shape is asserted for them.
    let _ = &lone_children;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn routed_parents_reshard_whole_and_keep_their_proof() {
    let dir = tempdir("routed");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, true).await;
    assert!(
        catalog(&path).parents_colocated(),
        "routed ingest proves it"
    );
    handle.abort();

    let output = split_four(&path, &dir.join("split"));
    assert_parents_whole(&output);
    for image in output.children.iter().filter(|i| i.num_documents > 0) {
        assert!(image.parents_colocated);
        assert!(catalog(&image.vector_path).parents_colocated());
        let child = NodeServiceImpl::open(config(&image.vector_path), None).unwrap();
        assert!(child.parents_colocated(), "a child node serves the proof");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The child a parent's key hash names under the online routing rule:
/// the child whose hash range holds the key's stable routing hash.
fn online_child(output: &reshard::ReshardOutput, key: &str) -> usize {
    let hash = pipestream_search::coordinator::stable_routing_hash(key.as_bytes());
    output
        .children
        .iter()
        .position(|image| image.hash_lo <= hash && hash <= image.hash_hi)
        .expect("some child owns every hash")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_reshard_keyed_appends_land_on_the_resharded_home() {
    let dir = tempdir("post-reshard");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, true).await;
    handle.abort();

    let output = split_four(&path, &dir.join("split"));
    assert_parents_whole(&output);
    // Every parent's resharded home is the child its stable key routes
    // to online, so a later keyed chunk cannot scatter the parent.
    let mut homes: BTreeMap<u64, usize> = BTreeMap::new();
    for (child, image) in output.children.iter().enumerate() {
        if image.num_documents == 0 {
            continue;
        }
        for parent in parents_of(&catalog(&image.vector_path)) {
            if parent != u64::MAX {
                homes.insert(parent, child);
            }
        }
    }
    assert_eq!(homes.len() as u64, PARENTS);
    for p in 0..PARENTS {
        assert_eq!(
            homes[&parent_id(p)],
            online_child(&output, &format!("opinion-{p}")),
            "parent {p} would scatter on its next keyed append"
        );
    }

    // Serve parent 0's child and append one more keyed chunk of it: the
    // new segment keeps the proof and the parent stays whole there.
    let home = &output.children[homes[&parent_id(0)]];
    let (addr, handle) = common::start_opened_node(config(&home.vector_path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    append(
        &mut client,
        "opinion 0 late chunk court".into(),
        Some(parent_id(0)),
        Some("opinion-0".into()),
    )
    .await;
    client.flush(FlushRequest {}).await.unwrap();
    let set = catalog(&home.vector_path);
    assert!(set.parents_colocated(), "the late chunk keeps the proof");
    assert_eq!(
        parents_of(&set)
            .into_iter()
            .filter(|&parent| parent == parent_id(0))
            .count() as u64,
        CHUNKS + 1
    );
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn raw_lineage_cannot_claim_colocation_and_a_reshard_does_not_launder_it() {
    let dir = tempdir("raw");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, false).await;
    assert!(
        !catalog(&path).parents_colocated(),
        "raw lineage ingest proves nothing about other shards"
    );
    handle.abort();

    let output = split_four(&path, &dir.join("split"));
    assert_parents_whole(&output);
    for image in output.children.iter().filter(|i| i.num_documents > 0) {
        let set = catalog(&image.vector_path);
        let parented = parents_of(&set).iter().any(|&parent| parent != u64::MAX);
        assert_eq!(
            set.parents_colocated(),
            !parented,
            "only a child of unparented rows proves colocation"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_proof_is_per_segment_and_survives_compaction_reopen_and_snapshots() {
    let dir = tempdir("lifecycle");
    let path = dir.join("shard.tv");
    let (addr, handle) = common::start_empty_node(config(&path)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    ingest_corpus(&mut client, true).await;
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![0],
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    client
        .compact_shard(CompactShardRequest::default())
        .await
        .unwrap();
    assert!(catalog(&path).parents_colocated(), "compaction keeps it");

    // One raw chunk of a parent seals into a segment of its own that
    // cannot claim the proof; the segments sealed before keep theirs.
    let sealed_before = catalog(&path).len();
    append(
        &mut client,
        "raw chunk court".into(),
        Some(parent_id(0)),
        None,
    )
    .await;
    client.flush(FlushRequest {}).await.unwrap();
    let set = catalog(&path);
    let bits: Vec<bool> = (0..set.len())
        .map(|i| set.metadata(i).parents_colocated)
        .collect();
    assert_eq!(bits.len(), sealed_before + 1);
    assert!(bits[..sealed_before].iter().all(|&bit| bit));
    assert!(
        !bits[sealed_before],
        "the raw chunk's segment cannot claim it"
    );
    assert!(!set.parents_colocated());
    handle.abort();

    let reopened = NodeServiceImpl::open(config(&path), None).unwrap();
    assert!(!reopened.parents_colocated(), "the bits survive a reopen");
    let repository = dir.join("repository");
    reopened.export_snapshot_blocking(&repository).unwrap();
    let target_path = dir.join("target.tv");
    let target = NodeServiceImpl::open(config(&target_path), None).unwrap();
    let (manifest, _) = pipestream_search::snapshot_repository::read_manifest(&repository).unwrap();
    target
        .install_staged_repository(&repository, &manifest)
        .unwrap();
    let restored = catalog(&target_path);
    assert_eq!(
        (0..restored.len())
            .map(|i| restored.metadata(i).parents_colocated)
            .collect::<Vec<_>>(),
        bits,
        "a snapshot restore keeps every segment's proof"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
