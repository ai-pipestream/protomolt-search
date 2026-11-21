//! Shared two-shard query_api fleet and selection/streaming query builders.

use super::*;

pub(crate) const DIM: usize = 64;
pub(crate) const SHARD_DOCS: usize = 4;
pub(crate) const N_DOCS: usize = 2 * SHARD_DOCS;
pub(crate) const COURTS: [&str; 3] = ["ca9", "ca2", "scotus"];

/// Corpus: doc i's text mentions "zebra" for even i, and every doc
/// carries `year = i` for filters. Vectors are the seeded unit corpus;
/// the test query vector is doc 0's own.
pub(crate) async fn start_fleet() -> (CoordinatorServiceImpl, Vec<f32>, Vec<ShardGuard>) {
    start_fleet_with_addrs().await.0
}

pub(crate) async fn start_fleet_with_addrs() -> (
    (CoordinatorServiceImpl, Vec<f32>, Vec<ShardGuard>),
    Vec<String>,
) {
    start_fleet_with_identities(false).await
}

pub(crate) fn fixture_identity(id: u64) -> Option<pipestream_search::pb::DocumentIdentity> {
    (id != 0).then(|| pipestream_search::pb::DocumentIdentity {
        document_key: [vec![0, 255], id.to_le_bytes().to_vec()].concat(),
        version: u64::MAX - id,
        chunk_ordinal: id.is_multiple_of(2).then_some(0),
    })
}

pub(crate) async fn start_fleet_with_identities(
    identities: bool,
) -> (
    (CoordinatorServiceImpl, Vec<f32>, Vec<ShardGuard>),
    Vec<String>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0xC0FE_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut handles = vec![ShardGuard::from(mock)];
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let start = shard * SHARD_DOCS;
        let docs = (0..SHARD_DOCS)
            .map(|i| {
                let id = shard * SHARD_DOCS + i;
                let text = if id.is_multiple_of(2) {
                    format!("zebra document {id}")
                } else {
                    format!("plain document {id}")
                };
                AddDocumentsRequest {
                    text,
                    original_source: identities
                        .then(|| common::protobuf_source("fixture", &id.to_string())),
                    identity: identities.then(|| fixture_identity(id as u64)).flatten(),
                    source_chunk_ordinal: if identities {
                        fixture_identity(id as u64).and_then(|identity| identity.chunk_ordinal)
                    } else {
                        None
                    },
                    integers: vec![IntegerValue {
                        field: "year".into(),
                        value: id as i64,
                    }],
                    // A facet column and lineage for the sort and collapse
                    // tests: three courts round-robin, parents in pairs,
                    // groups in fours.
                    facets: vec![FacetValue {
                        field: "court".into(),
                        value: COURTS[id % 3].into(),
                    }],
                    lineage: Some(DocLineage {
                        parent_id: (id / 2) as u64,
                        group_id: (id / 4) as u64,
                        span_start: 0,
                        span_end: 0,
                    }),
                    ..Default::default()
                }
            })
            .collect();
        let (addr, handle) = start_shard(ShardSpec {
            config: NodeConfig {
                slot_offset: (shard * SHARD_DOCS) as u64,
                analysis_addr: Some(analysis.clone()),
                integer_fields: vec!["year".into()],
                facet_fields: vec!["court".into()],
                ..Default::default()
            },
            backend: Some(pipestream_search::harness::embedded_backend_request(
                DIM, 4, &shift, &scale,
            )),
            docs,
            vectors: corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs.clone()).with_bm25(Some(analysis), Default::default());
    ((coordinator, corpus[..DIM].to_vec(), handles), addrs)
}

pub(crate) fn lexical_leaf(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.to_string(),
                ..Default::default()
            })),
        })),
    }
}

pub(crate) fn dense_leaf(id: &str, vector: &[f32]) -> SelectionQuery {
    common::dense_selection(
        id,
        vector.to_vec(),
        DenseExecutionMode::Exhaustive as i32,
        0,
    )
}

pub(crate) fn fp32_dense_leaf(id: &str, vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                execution_mode: pipestream_search::pb::DenseExecutionMode::Balanced as i32,
                precision: pipestream_search::pb::DensePrecisionRequest::Fp32 as i32,
                ..Default::default()
            })),
        })),
    }
}

pub(crate) fn cel_filter(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

pub(crate) fn composite(
    operator: SelectionOperator,
    clauses: Vec<SelectionQuery>,
    strategy: Option<selection_score_strategy::Strategy>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: operator as i32,
            clauses,
            scoring: strategy.map(|s| SelectionScoreStrategy { strategy: Some(s) }),
        })),
    }
}

pub(crate) fn boolean(
    must: Vec<SelectionQuery>,
    should: Vec<SelectionQuery>,
    must_not: Vec<SelectionQuery>,
    minimum_should_match: u32,
    aggregate: Option<AggregateRequest>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should,
            must_not,
            minimum_should_match,
            aggregate,
        })),
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

pub(crate) async fn streamed_query(
    coordinator: &CoordinatorServiceImpl,
    query: Option<QueryRequest>,
    timeout_ms: u64,
) -> Vec<QueryStreamResponse> {
    let mut inbound = coordinator
        .query_stream(Request::new(QueryStreamRequest {
            collection: String::new(),
            query,
            timeout_ms,
        }))
        .await
        .expect("QueryStream opens")
        .into_inner();
    let mut events = Vec::new();
    while let Some(event) = inbound.next().await {
        let event = event.expect("well-formed stream");
        events.push(event);
    }
    events
}

pub(crate) fn stream_parts(
    events: &[QueryStreamResponse],
) -> (Vec<&QueryStreamRevision>, &QueryStreamCompletion) {
    let mut revisions = Vec::new();
    let mut completion = None;
    for event in events {
        match event.payload.as_ref().expect("stream event payload") {
            query_stream_response::Payload::Revision(revision) => revisions.push(revision),
            query_stream_response::Payload::Completion(done) => {
                assert!(
                    completion.replace(done).is_none(),
                    "one terminal certificate"
                );
            }
        }
    }
    let completion = completion.expect("terminal completion certificate");
    assert!(matches!(
        events.last().and_then(|event| event.payload.as_ref()),
        Some(query_stream_response::Payload::Completion(_))
    ));
    (revisions, completion)
}
