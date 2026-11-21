//! Shared multi-shard HNSW and facet-filtered HNSW cluster fixtures.

use super::*;

pub(crate) const DIM: usize = 32;
pub(crate) const N_DOCS: usize = 2200;
pub(crate) const SHARDS: usize = 2;
pub(crate) const GENERATION: u64 = 3;

pub(crate) struct Fixture {
    pub(crate) coordinator: CoordinatorServiceImpl,
    pub(crate) addrs: Vec<String>,
    pub(crate) corpus: Vec<f32>,
    pub(crate) query_vector: Vec<f32>,
    pub(crate) fingerprint: String,
    pub(crate) handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        for handle in &self.handles {
            handle.abort();
        }
    }
}

pub(crate) fn graph_index() -> VectorIndex {
    VectorIndex::create(HNSW, DIM, 8).unwrap()
}

pub(crate) async fn start_fixture() -> Fixture {
    let corpus = unit_vectors(N_DOCS, DIM, 0x4A5E_0001);
    let per_shard = N_DOCS / SHARDS;
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..SHARDS {
        let (addr, handle) = start_node(
            graph_index(),
            NodeConfig {
                slot_offset: (shard * per_shard) as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                ..Default::default()
            },
        )
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(per_shard);
        for i in 0..per_shard {
            tx.send(AddDocumentsRequest {
                text: format!("document {} about the appeal", shard * per_shard + i),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = shard * per_shard;
        // Two batches: a bulk build, then tail insertion into the live graph.
        let split = start + per_shard / 2;
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: corpus[start * DIM..split * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        vtx.send(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: corpus[split * DIM..(start + per_shard) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        // Flush builds the HNSW graph over all appended rows; the node reports
        // the provider and its config.
        client.flush(FlushRequest {}).await.unwrap();
        let backend = client
            .get_vector_backend(GetVectorBackendRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(backend.num_vectors, per_shard as u64);
        assert_eq!(backend.descriptor.unwrap().backend_kind, HNSW);
        assert_eq!(backend.config.unwrap().backend_kind, HNSW);
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    Fixture {
        coordinator,
        addrs,
        corpus,
        query_vector: unit_vectors(1, DIM, 0x4A5E_0002),
        fingerprint: graph_index().descriptor().scoring_fingerprint,
        handles,
    }
}

pub(crate) fn dense_request(vector: &[f32], k: u32, mode: DenseExecutionMode) -> QueryRequest {
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vector.to_vec(),
                    execution_mode: mode as i32,
                    score_mode: DenseScoreMode::Unspecified as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

pub(crate) async fn query(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
) -> Result<QueryResponse, tonic::Status> {
    coordinator
        .query(Request::new(req))
        .await
        .map(|r| r.into_inner())
}

/// The FP32 inner-product top `k` as `(doc id, score)`.
pub(crate) fn fp32_top(corpus: &[f32], query: &[f32], k: usize) -> Vec<(u64, f32)> {
    let mut all: Vec<(u64, f32)> = corpus
        .as_chunks::<DIM>()
        .0
        .iter()
        .enumerate()
        .map(|(id, row)| (id as u64, row.iter().zip(query).map(|(a, b)| a * b).sum()))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    all.truncate(k);
    all
}

/// A policy document through the file, as a coordinator loads it.
pub(crate) fn load_policy(tag: &str, text: &str) -> DenseExecutionPolicy {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("hnsw_policy_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.toml");
    std::fs::write(&path, text).unwrap();
    let policy = DenseExecutionPolicy::load(&path).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    policy
}

pub(crate) const FILTERED_SHARD_DOCS: usize = 7800;

/// Global doc id -> court: every even id is "wide" (half of each shard, well
/// above both the enumerate threshold and visit budget at this beam), one odd
/// id in 300 is "narrow" (26 rows a shard), one odd id in 30 is "sparse" (260
/// rows a shard, bridge-heavy when walked), the rest are "rest".
pub(crate) fn court_of(id: usize) -> &'static str {
    if id.is_multiple_of(2) {
        "wide"
    } else if id % 300 == 1 {
        "narrow"
    } else if id % 30 == 3 {
        "sparse"
    } else {
        "rest"
    }
}

/// Two graph shards, each larger than one scan chunk, whose documents carry
/// a `court` facet.
pub(crate) async fn start_filtered_fixture() -> (Fixture, Vec<String>) {
    let total = FILTERED_SHARD_DOCS * SHARDS;
    let corpus = unit_vectors(total, DIM, 0x4A5E_0055);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..SHARDS {
        let start = shard * FILTERED_SHARD_DOCS;
        let (addr, handle) = start_node(
            graph_index(),
            NodeConfig {
                slot_offset: start as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                facet_fields: vec!["court".to_string()],
                ..Default::default()
            },
        )
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        let feeder = tokio::spawn(async move {
            for id in start..start + FILTERED_SHARD_DOCS {
                tx.send(AddDocumentsRequest {
                    text: format!("opinion {id}"),
                    analysis: Some(body_spec()),
                    facets: vec![FacetValue {
                        field: "court".into(),
                        value: court_of(id).into(),
                    }],
                    ..Default::default()
                })
                .await
                .unwrap();
            }
        });
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        feeder.await.unwrap();
        let (vtx, vrx) = mpsc::channel(1);
        vtx.send(AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: corpus[start * DIM..(start + FILTERED_SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        client.flush(FlushRequest {}).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let fixture = Fixture {
        coordinator: CoordinatorServiceImpl::new(addrs.clone()),
        addrs: addrs.clone(),
        corpus,
        query_vector: Vec::new(),
        fingerprint: String::new(),
        handles,
    };
    (fixture, addrs)
}

pub(crate) fn filtered_dense_request(
    vector: &[f32],
    k: u32,
    court: &str,
    mode: DenseExecutionMode,
) -> QueryRequest {
    let dense = dense_request(vector, k, mode).selection.unwrap();
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![
                    dense,
                    SelectionQuery {
                        node: Some(selection_query::Node::Filter(FilterQuery {
                            id: "court".into(),
                            predicate: Some(filter_query::Predicate::Cel(format!(
                                r#"court == "{court}""#
                            ))),
                        })),
                    },
                ],
                scoring: None,
            })),
        }),
        ..Default::default()
    }
}
