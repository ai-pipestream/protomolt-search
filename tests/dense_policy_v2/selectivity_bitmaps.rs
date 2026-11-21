//! Allocation-free bitmap selectivity parity across multi-shard, replica,
//! visibility, and filtered `AUTO` queries without materializing ID sets.

use std::sync::Arc;

use super::*;
use common::fit_calibration;
use pipestream_search::authorization::PolicyAuthority;
use pipestream_search::collections::CollectionSet;
use pipestream_search::coordinator::{membership_id_decodes, RequestFilters};
use pipestream_search::dense_policy::selectivity_ppm;
use pipestream_search::exact_vectors::ExactVectorStore;
use pipestream_search::harness::{fake_ann, seeded_index};
use pipestream_search::node::{Bm25Shard, NodeServiceImpl};
use pipestream_search::pb::{
    AccessAction, AccessPolicy, CollectionGrant, CollectionResource, CompositeSearchStrategy,
    DocumentVisibility, FacetValue, SelectionOperator,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Store, ColumnKind};
use pipestream_search::security::{PrincipalConfig, Principals};

fn selective_docs(base_doc: usize, total_docs: usize) -> Vec<AddDocumentsRequest> {
    (0..total_docs)
        .map(|i| {
            let id = base_doc + i;
            AddDocumentsRequest {
                text: format!("opinion {id}"),
                analysis: Some(body_spec()),
                facets: vec![
                    FacetValue {
                        field: "tenant".into(),
                        value: if id.is_multiple_of(2) {
                            "acme"
                        } else {
                            "other"
                        }
                        .into(),
                    },
                    FacetValue {
                        field: "court".into(),
                        value: if id.is_multiple_of(3) {
                            "ca9"
                        } else {
                            "scotus"
                        }
                        .into(),
                    },
                ],
                ..Default::default()
            }
        })
        .collect()
}

fn selectivity_band_policy(live: &LiveIdentity, bands: &[(u32, u32)]) -> DenseExecutionPolicy {
    let points = bands
        .iter()
        .map(|&(ppm, depth)| DensePolicyPoint {
            k: 2,
            filter_selectivity_ppm_min: ppm,
            filter_selectivity_ppm_max: ppm,
            candidates: depth,
            measured_recall_ppm: 990_000,
            mode: DenseExecutionMode::Balanced as i32,
            technique: DenseTechnique::HnswWalk as i32,
            selection_precision: DenseScorePrecision::Quantized as i32,
            output_precision: DenseScorePrecision::Quantized as i32,
            filter_strategy: DenseFilterStrategy::Speed as i32,
            shape: DenseExecutionShape::Unary as i32,
            approximate_p50_us: 10,
            approximate_p99_us: 20,
            exhaustive_p50_us: 100,
            exhaustive_p99_us: 200,
            approximate_qps: 1000.0,
            exhaustive_qps: 100.0,
            ..Default::default()
        })
        .collect();
    let host = local_host_profile();
    let hosts = vec![host.clone(), host];
    DenseExecutionPolicy::from_document(DenseExecutionPolicyDocument {
        format_version: pipestream_search::dense_policy::FORMAT_VERSION,
        policy_id: "selectivity-v2".into(),
        embedding_model: "test-embed".into(),
        provider_backend: live.provider_backend.clone(),
        scoring_fingerprint: live.scoring_fingerprint.clone(),
        corpus_generation: live.corpus_generation,
        corpus_rows: live.corpus_rows,
        deleted_docs: live.deleted_docs,
        corpus_fingerprint: live.corpus_fingerprint.clone(),
        dimensions: live.dimensions,
        measured_queries: 16,
        target_recall_ppm: 950_000,
        hardware_fingerprint: fleet_fingerprint(&hosts),
        node_count: 2,
        hosts,
        points,
    })
    .unwrap()
}

fn court_filtered_auto(vector: &[f32]) -> QueryRequest {
    QueryRequest {
        k: 2,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![
                    common::cel_selection("f", "court == 'ca9'"),
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "vec".into(),
                            query: Some(search_query::Query::Dense(DenseQuery {
                                vector: vector.to_vec(),
                                execution_mode: DenseExecutionMode::Auto as i32,
                                ..Default::default()
                            })),
                        })),
                    },
                ],
                scoring: None,
            })),
        }),
        ..Default::default()
    }
}

async fn spawn_selective_node(
    shard: usize,
    span: usize,
    vec_rows: usize,
    shift: &[f32],
    scale: &[f32],
    slice: &[f32],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let idx = fake_ann::fake_ann_index(seeded_index(DIM, 4, shift, scale));
    let (addr, h) = start_node(
        idx,
        NodeConfig {
            slot_offset: (shard * span) as u64,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            facet_fields: vec!["tenant".into(), "court".into()],
            ..Default::default()
        },
    )
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .add_documents(tokio_stream::iter(selective_docs(
            shard * span,
            vec_rows + 3,
        )))
        .await
        .unwrap();
    client
        .add_vectors(tokio_stream::iter([AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: slice.to_vec(),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();
    (addr, h)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_shard_replica_filtered_auto_selectivity_never_materializes_id_sets() {
    const VEC_ROWS: usize = 13;
    const SPAN: usize = 20;
    let corpus = unit_vectors(2 * VEC_ROWS, DIM, 0x4A5E_8001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut primaries = Vec::new();
    let mut replicas = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let slice = &corpus[shard * VEC_ROWS * DIM..(shard + 1) * VEC_ROWS * DIM];
        let (p_addr, ph) = spawn_selective_node(shard, SPAN, VEC_ROWS, &shift, &scale, slice).await;
        let (r_addr, rh) = spawn_selective_node(shard, SPAN, VEC_ROWS, &shift, &scale, slice).await;
        primaries.push(p_addr);
        replicas.push(Some(r_addr));
        handles.extend([ph, rh]);
    }

    let base = CoordinatorServiceImpl::new(primaries)
        .with_replicas(replicas)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    let live = base.live_dense_identity().await.unwrap();
    let vec_ids = base.vector_membership("").await.unwrap().ids;
    let flt = RequestFilters {
        tree: pipestream_search::cel::compile_filter("court == 'ca9'").unwrap(),
        ..Default::default()
    };
    let doc_ids = base.filter_membership(&flt).await.unwrap().ids;
    let expected_ppm = selectivity_ppm(
        vec_ids.intersection(&doc_ids).count() as u64,
        live.corpus_rows,
    );

    let coordinator =
        base.with_dense_execution_policy(selectivity_band_policy(&live, &[(expected_ppm, 6)]));
    let qvec = unit_vectors(1, DIM, 0x4A5E_8002);
    let before = membership_id_decodes();
    let outcome = run_query(&coordinator, court_filtered_auto(&qvec))
        .await
        .dense_execution
        .unwrap();
    assert_eq!(membership_id_decodes(), before);
    assert!(outcome.policy_qualified && outcome.candidate_depth == 6);
    assert_eq!(outcome.filter_selectivity_ppm, expected_ppm);

    for h in handles {
        h.abort();
    }
}

fn tenant_visibility_service(coordinator: CoordinatorServiceImpl) -> CollectionSet {
    let policy = AccessPolicy {
        format_version: 3,
        revision: 1,
        resources: vec![CollectionResource {
            workspace: "work".into(),
            collection: "".into(),
        }],
        grants: vec![CollectionGrant {
            principal: "reader".into(),
            workspace: "work".into(),
            collection: "".into(),
            actions: vec![AccessAction::Search as i32],
            field_permissions: None,
            document_visibility: Some(DocumentVisibility {
                filter: pipestream_search::cel::compile_filter("tenant == 'acme'").unwrap(),
            }),
        }],
    };
    CollectionSet::single(coordinator).with_principals(Arc::new(
        Principals::from_configs(&[PrincipalConfig {
            name: "reader".into(),
            token: "reader-token-0123456789012345".into(),
            ..Default::default()
        }])
        .unwrap()
        .with_authorizer(Arc::new(PolicyAuthority::new(policy).unwrap())),
    ))
}

fn local_selective_node(
    shard: usize,
    span: usize,
    vec_rows: usize,
    shift: &[f32],
    scale: &[f32],
    slice: &[f32],
) -> Arc<NodeServiceImpl> {
    let mut store = Bm25Store::new().with_columns(ColumnKind::Facet, &["tenant", "court"]);
    for row in 0..(vec_rows + 3) as u32 {
        let id = shard * span + row as usize;
        store.add_document(
            row,
            "opinion".into(),
            AnalyzedDoc::body(vec![("opinion".into(), 1, vec![(0, 7)])], 1),
        );
        store.set_facet(
            0,
            row,
            if id.is_multiple_of(2) {
                "acme"
            } else {
                "other"
            },
        );
        store.set_facet(
            1,
            row,
            if id.is_multiple_of(3) {
                "ca9"
            } else {
                "scotus"
            },
        );
    }
    let mut inner = seeded_index(DIM, 4, shift, scale);
    inner.add(slice, DIM).unwrap();
    Arc::new(
        NodeServiceImpl::new(
            Some(fake_ann::fake_ann_index(inner)),
            NodeConfig {
                slot_offset: (shard * span) as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
                facet_fields: vec!["tenant".into(), "court".into()],
                ..Default::default()
            },
        )
        .with_bm25(Some(Bm25Shard::Building(store)))
        .with_exact_vectors(Some(
            ExactVectorStore::from_values(DIM, slice.to_vec()).unwrap(),
        ))
        .unwrap(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_shard_visibility_and_filtered_auto_never_materialize_id_sets() {
    const VEC_ROWS: usize = 13;
    const SPAN: usize = 20;
    let corpus = unit_vectors(2 * VEC_ROWS, DIM, 0x4A5E_9001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let nodes: Vec<_> = (0..2)
        .map(|shard| {
            let slice = &corpus[shard * VEC_ROWS * DIM..(shard + 1) * VEC_ROWS * DIM];
            local_selective_node(shard, SPAN, VEC_ROWS, &shift, &scale, slice)
        })
        .collect();

    let base = CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    let live = base.live_dense_identity().await.unwrap();
    // 26 vector rows (0..13 and 20..33): even IDs have tenant='acme' (7 + 7 = 14 rows);
    // IDs divisible by 6 have tenant='acme' && court='ca9' ({0,6,12,24,30} = 5 rows).
    let vis_ppm = selectivity_ppm(14, live.corpus_rows);
    let flt_ppm = selectivity_ppm(5, live.corpus_rows);

    let svc = tenant_visibility_service(base.with_dense_execution_policy(selectivity_band_policy(
        &live,
        &[(vis_ppm, 4), (flt_ppm, 6)],
    )));
    let auth_req = |body: QueryRequest| {
        let mut req = Request::new(body);
        req.metadata_mut().insert(
            "authorization",
            "Bearer reader-token-0123456789012345".parse().unwrap(),
        );
        req
    };
    let qvec = unit_vectors(1, DIM, 0x4A5E_9002);
    let before = membership_id_decodes();
    let vis_out = svc
        .query(auth_req(dense_request(&qvec, 2, DenseExecutionMode::Auto)))
        .await
        .unwrap()
        .into_inner()
        .dense_execution
        .unwrap();
    let flt_out = svc
        .query(auth_req(court_filtered_auto(&qvec)))
        .await
        .unwrap()
        .into_inner()
        .dense_execution
        .unwrap();
    assert_eq!(membership_id_decodes(), before);
    assert!(vis_out.policy_qualified && vis_out.candidate_depth == 4);
    assert!(flt_out.policy_qualified && flt_out.candidate_depth == 6);
}
