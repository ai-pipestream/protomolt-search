use super::*;
use pipestream_search::{
    authorization::{Authorizer, PolicyAuthority},
    collections::CollectionSet,
    coordinator::CoordinatorServiceImpl,
    pb::search_service_server::SearchService,
    security::{PrincipalConfig, Principals},
    segments::{OpenedSegmentSet, SegmentCatalog, SegmentSource},
    stats_identity::StatsClaim,
};

async fn version(
    fixture: &Fixture,
    node: Arc<NodeServiceImpl>,
    version: u64,
    rows: Option<usize>,
) -> DocumentProjectionActivation {
    let receipt = fixture.accept(version, rows.map(|rows| source(rows, false)));
    let mut request = fixture.request(&receipt);
    if rows.is_none() {
        request
            .projection
            .as_mut()
            .unwrap()
            .expected_plan_fingerprint
            .clear();
        request.field_analysis.clear();
    }
    let candidate = node
        .stage_document_projection(fixture.catalog.clone(), request)
        .await
        .unwrap();
    publish_candidate(node, fixture.catalog.clone(), candidate)
        .await
        .unwrap()
}
fn set(fixture: &Fixture) -> Arc<OpenedSegmentSet> {
    Arc::new(
        OpenedSegmentSet::open(segments_root(fixture.config.index_path.as_ref().unwrap())).unwrap(),
    )
}
async fn cutover(
    fixture: &Fixture,
    node: Arc<NodeServiceImpl>,
    active: &DocumentProjectionActivation,
    chosen: Vec<usize>,
    name: &str,
) -> Result<DocumentMaintenanceActivation, tonic::Status> {
    let before = set(fixture);
    let source = fixture.catalog.clone();
    let expected = StatsClaim::required(active.stats_epoch, &active.stats_incarnation).unwrap();
    let name = name.to_owned();
    let scratch = fixture.root.join(format!("{name}-proof"));
    tokio::task::spawn_blocking(move || {
        let paths: Vec<_> = chosen
            .iter()
            .map(|&i| {
                let m = before.metadata(i);
                let dir = SegmentCatalog::segment_dir(before.root(), &m.segment_id);
                [
                    dir.join(&m.vector.file),
                    dir.join(&m.exact_vectors.file),
                    dir.join(&m.bm25.file),
                    dir.join(&m.live_docs.file),
                ]
            })
            .collect();
        let names: Vec<_> = chosen
            .iter()
            .enumerate()
            .map(|(i, _)| format!("{name}-{i}"))
            .collect();
        let mut base = 0;
        let sources = chosen
            .iter()
            .enumerate()
            .map(|(ordinal, &i)| {
                let m = before.metadata(i);
                let source = SegmentSource {
                    segment_id: &names[ordinal],
                    generation: before.epoch() + 1,
                    base_label: base,
                    backend_kind: &m.backend_kind,
                    vector_path: (!m.vector.file.is_empty()).then_some(paths[ordinal][0].as_path()),
                    exact_vector_path: (!m.exact_vectors.file.is_empty())
                        .then_some(paths[ordinal][1].as_path()),
                    bm25_path: &paths[ordinal][2],
                    live_docs_path: &paths[ordinal][3],
                    partition_column: None,
                };
                base += m.rows;
                source
            })
            .collect();
        node.publish_document_maintenance_blocking(
            &source,
            b"books-index",
            expected,
            before.epoch(),
            sources,
            &scratch,
            1,
        )
    })
    .await
    .unwrap()
}
fn service(node: Arc<NodeServiceImpl>, disclose: bool, visible: bool) -> CollectionSet {
    let authority: Arc<dyn Authorizer> = Arc::new(
        PolicyAuthority::new(AccessPolicy {
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
                field_permissions: Some(FieldPermissions {
                    grants: vec![FieldGrant {
                        field: "body".into(),
                        actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
                    }],
                    disclose_document_identity: disclose,
                }),
                document_visibility: (!visible).then(|| DocumentVisibility {
                    filter: pipestream_search::cel::compile_filter("key_bucket == 999u").unwrap(),
                }),
            }],
        })
        .unwrap(),
    );
    let coordinator = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    CollectionSet::single(coordinator).with_principals(Arc::new(
        Principals::from_configs(&[PrincipalConfig {
            name: "reader".into(),
            token: "reader-token-0123456789012345".into(),
            ..Default::default()
        }])
        .unwrap()
        .with_authorizer(authority),
    ))
}
async fn query(service: &CollectionSet, cursor: &str) -> Result<QueryResponse, tonic::Status> {
    let mut request = Request::new(QueryRequest {
        k: 1,
        cursor: cursor.into(),
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "lex".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "accepted".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    });
    request.metadata_mut().insert(
        "authorization",
        "Bearer reader-token-0123456789012345".parse().unwrap(),
    );
    service.query(request).await.map(|r| r.into_inner())
}

#[tokio::test]
async fn real_cutover_renumbers_rows_preserves_grants_and_rejects_old_read_claims() {
    let fixture = Fixture::new();
    let node = Arc::new(NodeServiceImpl::open(fixture.config.clone(), None, false).unwrap());
    version(&fixture, node.clone(), 1, Some(2)).await;
    let active = version(&fixture, node.clone(), 2, Some(2)).await;
    assert_active_rows(&node, &active, &[2, 3]).await;
    let held = set(&fixture);
    let allowed = service(node.clone(), true, true);
    let hidden_identity = service(node.clone(), false, true);
    let hidden_documents = service(node.clone(), true, false);
    let before = query(&allowed, "").await.unwrap();
    assert_eq!(before.hits.len(), 1);
    assert!(before.hits[0].identity.is_some());
    assert!(query(&hidden_identity, "").await.unwrap().hits[0]
        .identity
        .is_none());
    assert!(query(&hidden_documents, "").await.unwrap().hits.is_empty());
    let receipt_before = fixture.catalog.get(KEY, Some(2)).unwrap();
    let maintenance = cutover(&fixture, node.clone(), &active, vec![1], "repacked")
        .await
        .unwrap();
    assert_eq!(maintenance.accepted_sequence, active.accepted_sequence);
    assert_eq!(maintenance.source_intent_id, active.intent_id);
    assert_eq!(maintenance.live_rows, 2);
    let recovered = recover_activation(node.clone(), fixture.catalog.clone())
        .await
        .unwrap();
    assert_eq!(recovered.catalog_epoch, maintenance.catalog_epoch);
    assert_active_rows(&node, &recovered, &[0, 1]).await;
    let after = query(&allowed, "").await.unwrap();
    assert_eq!(after.hits[0].identity, before.hits[0].identity);
    assert_eq!(
        after.hits[0].score.to_bits(),
        before.hits[0].score.to_bits()
    );
    assert!(query(&hidden_identity, "").await.unwrap().hits[0]
        .identity
        .is_none());
    assert!(query(&hidden_documents, "").await.unwrap().hits.is_empty());
    assert_eq!(fixture.catalog.get(KEY, Some(2)).unwrap(), receipt_before);
    assert_eq!(
        node.fetch_values(Request::new(FetchValuesRequest {
            candidate_ids: vec![0],
            include_identities: true,
            expected_stats_epoch: active.stats_epoch,
            expected_stats_incarnation: active.stats_incarnation.clone(),
            ..Default::default()
        }))
        .await
        .unwrap_err()
        .code(),
        Code::FailedPrecondition
    );
    assert!(!before.next_cursor.is_empty());
    assert_eq!(
        query(&allowed, &before.next_cursor)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    // Opened old images remain readable after their directory entries retire.
    assert_eq!(held.bm25(1).document_identity(0).unwrap().version, 2);
    assert_eq!(
        held.exact_vectors(1)
            .unwrap()
            .row_values(0, 1)
            .unwrap()
            .len(),
        8
    );
    for m in &held.manifest().segments {
        assert!(!SegmentCatalog::segment_dir(held.root(), &m.segment_id).exists());
    }
    // Even when row positions and scores stay equal, maintenance invalidates
    // the old public cursor through its captured read version.
    assert!(!after.next_cursor.is_empty());
    let repeat = cutover(&fixture, node.clone(), &recovered, vec![0], "repeat")
        .await
        .unwrap();
    assert!(repeat.stats_epoch > maintenance.stats_epoch);
    assert_eq!(
        query(&allowed, &after.next_cursor)
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    let repeated = query(&allowed, "").await.unwrap();
    assert_eq!(repeated.hits[0].doc_id, after.hits[0].doc_id);
    assert_eq!(
        repeated.hits[0].score.to_bits(),
        after.hits[0].score.to_bits()
    );
    assert_eq!(repeated.hits[0].identity, after.hits[0].identity);
    let deleted = version(&fixture, node.clone(), 3, None).await;
    let empty = cutover(&fixture, node.clone(), &deleted, vec![], "empty")
        .await
        .unwrap();
    assert_eq!(empty.live_rows, 0);
    assert!(set(&fixture).is_empty());
    let health = node
        .health(Request::new(HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        (health.num_vectors, health.document_slots, health.dim),
        (0, 0, 8)
    );
    let updated = version(&fixture, node.clone(), 4, Some(1)).await;
    assert_active_rows(&node, &updated, &[0]).await;
    let source = fixture.catalog.clone();
    let node_copy = node.clone();
    assert!(tokio::task::spawn_blocking(
        move || node_copy.recover_document_maintenance_blocking(&source, b"books-index")
    )
    .await
    .unwrap()
    .unwrap()
    .is_none());
}

#[tokio::test]
async fn invalid_rewrites_and_stale_builds_leave_the_active_manifest_and_claim_intact() {
    let fixture = Fixture::new();
    let node = Arc::new(NodeServiceImpl::open(fixture.config.clone(), None, false).unwrap());
    let first = version(&fixture, node.clone(), 1, Some(2)).await;
    let manifest = fixture.manifest();
    assert!(cutover(&fixture, node.clone(), &first, vec![], "missing")
        .await
        .is_err());
    assert_eq!(fixture.manifest(), manifest);
    assert!(node.ingest_fence().is_none());
    assert_active_rows(&node, &first, &[0, 1]).await;
    let next = version(&fixture, node.clone(), 2, Some(1)).await;
    let manifest = fixture.manifest();
    assert_eq!(
        cutover(&fixture, node.clone(), &first, vec![1], "stale")
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    assert_eq!(fixture.manifest(), manifest);
    assert_active_rows(&node, &next, &[2]).await;
}
