//! A reshard child whose documents ran ahead of its vectors seals as
//! an aligned segment plus a trailing documents-only segment, and a
//! vector-only child still declares the split's position/sentence
//! layers so it reopens under the same node configuration.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::analyzer::{self, body_spec, SessionLayers, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::node::{segments_root, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::node_service_server::NodeService;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, AnalysisSpec, FlushRequest, GetDocumentsRequest,
};
use pipestream_search::postings::AnalyzedDoc;
use pipestream_search::segments::OpenedSegmentSet;
use pipestream_search::{reshard, wal};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("reshard_mixed_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        position_fields: vec!["body".into()],
        sentence_fields: vec!["body".into()],
        wal: true,
        wal_buckets: 4,
        ..Default::default()
    }
}

type AnalyzeBatch =
    dyn FnMut(&[(&str, Option<&AnalysisSpec>, SessionLayers)]) -> Result<Vec<AnalyzedDoc>, String>;

fn replay_analyzer() -> Box<AnalyzeBatch> {
    let handle = tokio::runtime::Handle::current();
    Box::new(move |docs| {
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                let mut out = Vec::with_capacity(docs.len());
                for (text, spec, layers) in docs {
                    let analyzed = if *layers != SessionLayers::default() {
                        analyzer::analyze_batch(NATIVE_ANALYSIS_BACKEND, &[(*text, *spec, *layers)])
                            .await
                            .map(|mut batch| batch.remove(0))
                    } else {
                        analyzer::analyze_document(NATIVE_ANALYSIS_BACKEND, text, *spec).await
                    };
                    out.push(analyzed.map_err(|e| e.to_string())?);
                }
                Ok(out)
            })
        })
    })
}

async fn start_parent(
    dir: &std::path::Path,
    corpus: &[f32],
) -> (
    NodeServiceClient<tonic::transport::Channel>,
    PathBuf,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let index_path = dir.join("parent.tv");
    let (addr, handle) = common::start_empty_node(config(index_path.clone())).await;
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, corpus);
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, BIT_WIDTH, &shift, &scale,
        ))
        .await
        .unwrap();
    (client, index_path, handle)
}

async fn add_vectors(client: &mut NodeServiceClient<tonic::transport::Channel>, rows: &[f32]) {
    let (tx, rx) = mpsc::channel(8);
    let rows = rows.to_vec();
    let count = rows.len() / DIM;
    tokio::spawn(async move {
        tx.send(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: rows,
            dim: DIM as u32,
        })
        .await
        .unwrap();
    });
    let added = client
        .add_vectors(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, count);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn mixed_child_seals_aligned_then_documents_only() {
    let dir = tempdir("mixed");
    let corpus = unit_vectors(6, DIM, 0x5EED_1111);
    let (mut client, index_path, _node) = start_parent(&dir, &corpus).await;
    let texts: Vec<String> = (0..6).map(|i| format!("alpha row{i} gamma")).collect();
    // Six documents, vectors for only the first four: documents ahead.
    let (tx, rx) = mpsc::channel(8);
    let send_texts = texts.clone();
    tokio::spawn(async move {
        for text in send_texts {
            tx.send(AddDocumentsRequest {
                text,
                analysis: Some(body_spec()),
                position_fields: vec!["body".into()],
                sentence_fields: vec!["body".into()],
                ..Default::default()
            })
            .await
            .unwrap();
        }
    });
    assert_eq!(
        client
            .add_documents(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner()
            .added,
        6
    );
    add_vectors(&mut client, &corpus[..4 * DIM]).await;
    // A plain flush syncs the WAL, then refuses the misaligned tail;
    // the log is what the reshard replays either way.
    assert!(client.flush(FlushRequest {}).await.is_err());
    drop(client);

    let out_dir = dir.join("split");
    let gen = reshard::resolve_gen(&wal::wal_dir(&index_path)).unwrap();
    let req = reshard::SplitRequest::new(std::slice::from_ref(&gen), 1, &out_dir);
    let output = reshard::split_logs_with(&req, &mut *replay_analyzer()).unwrap();
    assert_eq!(output.children.len(), 1);
    let child = &output.children[0];
    assert_eq!((child.num_vectors, child.num_documents), (4, 6));

    let set = OpenedSegmentSet::open(segments_root(&child.vector_path)).unwrap();
    assert_eq!(set.len(), 2, "aligned segment plus documents-only tail");
    assert_eq!((set.metadata(0).rows, set.metadata(0).base_label), (4, 0));
    assert_eq!((set.metadata(1).rows, set.metadata(1).base_label), (2, 4));
    assert!(set.vector(0).is_some());
    assert!(set.vector(1).is_none(), "trailing part is documents-only");

    // The child reopens and serves every row's text in order.
    let reopened = NodeServiceImpl::open(config(child.vector_path.clone()), None).unwrap();
    let docs = reopened
        .get_documents(tonic::Request::new(GetDocumentsRequest {
            doc_ids: (0..6).collect(),
        }))
        .await
        .unwrap()
        .into_inner()
        .documents;
    let got: Vec<String> = docs.into_iter().map(|d| d.text).collect();
    assert_eq!(got, texts);
    let _ = std::fs::remove_dir_all(&dir);
}
