//! Snapshot admission and stale catalog publication preserve serving generations.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::derived::Declaration;
use pipestream_search::harness::{start_empty_node, start_opened_node};
use pipestream_search::hnsw::HNSW;
use pipestream_search::node::{segments_root, NodeConfig};
use pipestream_search::pb::install_snapshot_from_request::Source;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::*;
use pipestream_search::postings::Bm25Store;
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSource};
use pipestream_search::snapshot::{export_snapshot, install_snapshot_from};
use pipestream_search::snapshot_repository::hash_file;
use tonic::Request;

const DIM: usize = 16;
static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "snapshot-seal-{}-{}",
            std::process::id(),
            NEXT_DIR.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Node {
    config: NodeConfig,
    addr: String,
    task: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Node {
    async fn start(config: NodeConfig, text: &str, value: f32) -> Self {
        let (addr, task) = start_empty_node(config.clone()).await;
        let node = Self { config, addr, task };
        node.ingest(text, value).await;
        node
    }

    async fn client(&self) -> NodeServiceClient<tonic::transport::Channel> {
        NodeServiceClient::connect(self.addr.clone()).await.unwrap()
    }

    async fn ingest(&self, text: &str, value: f32) {
        let documents = (0..2)
            .map(|i| AddDocumentsRequest {
                text: format!("marker {text} row{i}"),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: self.config.facet_fields[0].clone(),
                    value: text.into(),
                }],
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let mut client = self.client().await;
        assert_eq!(
            client
                .add_documents(tokio_stream::iter(documents))
                .await
                .unwrap()
                .into_inner()
                .added,
            2
        );
        let mut vectors = vec![0.0; 2 * DIM];
        vectors[0] = value;
        vectors[DIM] = -value;
        assert_eq!(
            client
                .add_vectors(tokio_stream::iter([AddVectorsRequest {
                    vectors,
                    dim: DIM as u32,
                    materialized_rows: Vec::new(),
                }]))
                .await
                .unwrap()
                .into_inner()
                .added,
            2
        );
        assert_eq!(
            client
                .delete_documents(DeleteDocumentsRequest {
                    doc_ids: vec![1],
                    expected_wal_generation: None,
                })
                .await
                .unwrap()
                .into_inner()
                .deleted,
            1
        );
        assert!(
            client
                .flush(FlushRequest {})
                .await
                .unwrap()
                .into_inner()
                .written
        );
    }

    async fn restart(&mut self) {
        self.task.abort();
        let _ = (&mut self.task).await;
        (self.addr, self.task) = start_opened_node(self.config.clone()).await;
    }

    async fn install(&self, repository: &Path) -> Result<InstallSnapshotResponse, tonic::Status> {
        install_snapshot_from(
            &self.addr,
            InstallSnapshotFromRequest {
                source: Some(Source::Directory(repository.display().to_string())),
                ..Default::default()
            },
        )
        .await
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn config(root: &Path, name: &str) -> NodeConfig {
    std::fs::create_dir_all(root.join(name)).unwrap();
    NodeConfig {
        index_path: Some(root.join(name).join("index")),
        vector_backend: HNSW.into(),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        facet_fields: vec!["tag".into()],
        collection: "snapshot-fixture".into(),
        wal: true,
        seal_tail_docs: 100,
        ..Default::default()
    }
}

fn derived(expression: &str) -> Arc<Declaration> {
    Arc::new(
        Declaration::compile(&DerivedColumns {
            columns: vec![DerivedColumn {
                name: "computed".into(),
                expression: expression.into(),
                kind: MaterializeKind::I64 as i32,
                disclosure: DerivedDisclosure::Inputs as i32,
            }],
        })
        .unwrap(),
    )
}

#[derive(Debug, PartialEq)]
struct Signature {
    counts: (u64, u64, u64, u64),
    documents: Vec<(u64, String)>,
    vectors: Vec<(u64, u32)>,
    lexical: Vec<(u64, u32)>,
}

async fn signature(node: &Node) -> Signature {
    let mut client = node.client().await;
    let health = client.health(HealthRequest {}).await.unwrap().into_inner();
    let documents = client
        .get_documents(GetDocumentsRequest {
            doc_ids: vec![0, 1],
        })
        .await
        .unwrap()
        .into_inner()
        .documents
        .into_iter()
        .map(|doc| (doc.doc_id, doc.text))
        .collect();
    let mut vector = vec![0.0; DIM];
    vector[0] = 1.0;
    let vectors = client
        .exact_vector_rescore(ExactVectorRescoreRequest {
            vector,
            candidate_ids: vec![0, 1],
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .hits
        .into_iter()
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect::<Vec<_>>();
    let coordinator = CoordinatorServiceImpl::new(vec![node.addr.clone()])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    let lexical = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            text: "marker".into(),
            k: 2,
            analysis: Some(body_spec()),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner()
    .hits
    .into_iter()
    .map(|hit| (hit.doc_id, hit.score.to_bits()))
    .collect::<Vec<_>>();
    assert_eq!(vectors.len(), 1, "deleted vector must remain masked");
    assert_eq!(lexical.len(), 1, "deleted document must remain masked");
    Signature {
        counts: (
            health.num_vectors,
            health.document_slots,
            health.live_docs,
            health.deleted_docs,
        ),
        documents,
        vectors,
        lexical,
    }
}

fn persisted(root: &Path) -> BTreeMap<PathBuf, (u64, String)> {
    let mut hashes = BTreeMap::new();
    collect_hashes(root, root, &mut hashes);
    hashes
}

fn collect_hashes(root: &Path, path: &Path, hashes: &mut BTreeMap<PathBuf, (u64, String)>) {
    for entry in std::fs::read_dir(path).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            collect_hashes(root, &path, hashes);
        } else {
            hashes.insert(
                path.strip_prefix(root).unwrap().to_path_buf(),
                hash_file(&path).unwrap(),
            );
        }
    }
}

enum SchemaMismatch {
    Derived,
    Facet,
    Field,
    Positions,
}

async fn incompatible_snapshot(mismatch: SchemaMismatch) {
    let dir = Directory::new();
    let mut target_config = config(&dir.0, "target");
    let mut source_config = config(&dir.0, "source");
    match mismatch {
        SchemaMismatch::Derived => {
            target_config.derived = Some(derived("1"));
            source_config.derived = Some(derived("2"));
        }
        SchemaMismatch::Facet => source_config.facet_fields = vec!["foreign_tag".into()],
        SchemaMismatch::Field => source_config.bm25_fields.push("foreign_title".into()),
        SchemaMismatch::Positions => source_config.position_fields.push("body".into()),
    }
    let mut target = Node::start(target_config, "original", 1.0).await;
    let source = Node::start(source_config, "replacement", 2.0).await;
    let repository = dir.0.join("repository");
    let export = export_snapshot(&source.addr, &repository).await.unwrap();
    let manifest = export.manifest.unwrap();
    let health = target
        .client()
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(manifest.collection, target.config.collection);
    assert_eq!(manifest.backend_kind, health.vector_backend);
    assert_eq!(manifest.scoring_fingerprint, health.scoring_fingerprint);
    let before = signature(&target).await;
    let disk = persisted(&dir.0.join("target"));
    let error = target
        .install(&repository)
        .await
        .expect_err("incompatible snapshot must refuse");
    assert_eq!(
        persisted(&dir.0.join("target")),
        disk,
        "refusal changed durable bytes"
    );
    assert_eq!(
        signature(&target).await,
        before,
        "refusal changed serving generation"
    );
    target.restart().await;
    assert_eq!(
        signature(&target).await,
        before,
        "restart adopted rejected generation"
    );
    assert!(
        error.message().contains("derived") || error.message().contains("table"),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incompatible_derived_snapshot_preserves_disk_live_and_restart() {
    incompatible_snapshot(SchemaMismatch::Derived).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incompatible_column_snapshot_preserves_disk_live_and_restart() {
    incompatible_snapshot(SchemaMismatch::Facet).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_field_mismatch_preserves_disk_live_and_restart() {
    incompatible_snapshot(SchemaMismatch::Field).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn segment_position_mismatch_preserves_disk_live_and_restart() {
    incompatible_snapshot(SchemaMismatch::Positions).await;
}

fn copy_tail(set: &OpenedSegmentSet, root: &Path) {
    std::fs::create_dir_all(root).unwrap();
    let metadata = set.metadata(0);
    let source = SegmentCatalog::segment_dir(set.root(), &metadata.segment_id);
    for artifact in [
        &metadata.bm25,
        &metadata.vector,
        &metadata.exact_vectors,
        &metadata.live_docs,
    ] {
        assert!(!artifact.file.is_empty());
        std::fs::copy(source.join(&artifact.file), root.join(&artifact.file)).unwrap();
    }
}

fn stale_append(catalog: &SegmentCatalog, root: &Path, base: u64) -> Result<(), String> {
    let bm25 = root.join("documents.bm25");
    let vector = root.join("vector.index");
    let exact = root.join("vectors.f32");
    let live = root.join("live-docs.bin");
    catalog
        .append(SegmentSource {
            segment_id: "stale-tail",
            generation: 2,
            base_label: base,
            backend_kind: HNSW,
            vector_path: Some(&vector),
            exact_vector_path: Some(&exact),
            multi_res_path: None,
            bm25_path: &bm25,
            live_docs_path: &live,
            partition_column: None,
            parents_colocated: false,
        })
        .map(|_| ())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_catalog_cannot_republish_over_equal_row_snapshot() {
    let dir = Directory::new();
    let mut target = Node::start(config(&dir.0, "target"), "original", 1.0).await;
    let source = Node::start(config(&dir.0, "source"), "replacement", 2.0).await;
    let root = segments_root(target.config.index_path.as_ref().unwrap());
    let stale = SegmentCatalog::open(&root).unwrap();
    let pinned = stale.snapshot();
    let tail = dir.0.join("stale-input");
    copy_tail(&pinned, &tail);
    let validation = SegmentCatalog::open(dir.0.join("validation")).unwrap();
    stale_append(&validation, &tail, 0).expect("copied tail must be a valid segment");
    let mut old_view = SegmentedShard::open_catalog(
        stale.clone(),
        Bm25Store::new().with_columns(pipestream_search::postings::ColumnKind::Facet, &["tag"]),
    )
    .unwrap();
    let old_text = old_view.text(0);
    let repository = dir.0.join("repository");
    export_snapshot(&source.addr, &repository).await.unwrap();
    target.install(&repository).await.unwrap();
    let installed = SegmentCatalog::open(&root).unwrap().snapshot();
    assert_eq!(installed.metadata(0).rows, pinned.metadata(0).rows);
    assert_ne!(installed.published_manifest(), pinned.published_manifest());
    let expected = signature(&source).await;
    assert_eq!(signature(&target).await, expected);
    let disk = persisted(&dir.0.join("target"));
    assert!(
        stale_append(&stale, &tail, 2).is_err(),
        "a retained catalog appended across snapshot replacement"
    );
    assert_eq!(persisted(&dir.0.join("target")), disk);
    assert!(
        stale.commit_current(installed.epoch() + 10).is_err(),
        "a retained catalog committed across snapshot replacement"
    );
    assert_eq!(persisted(&dir.0.join("target")), disk);
    assert!(
        old_view.republish(Arc::clone(&pinned)).is_err(),
        "a retained serving view accepted obsolete catalog bytes"
    );
    assert_eq!(old_view.text(0), old_text);
    assert_eq!(persisted(&dir.0.join("target")), disk);
    assert_eq!(signature(&target).await, expected);
    target.restart().await;
    assert_eq!(signature(&target).await, expected);
}
