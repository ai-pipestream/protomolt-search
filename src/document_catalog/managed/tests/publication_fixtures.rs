//! A managed source over chunked documents, a segmented node serving its
//! one target index, and the writes, leases and revocations the managed
//! publication tests drive.
use super::*;
use crate::node::{ManagedTarget, NodeConfig, NodeServiceImpl};
use crate::pb::{
    DocumentPublicationStatus, MappedFieldAnalysis, PrepareDocumentProjectionRequest,
    StageDocumentProjectionRequest,
};

pub(super) const INDEX: &[u8] = b"managed-index\0v1";
pub(super) const DESCRIPTOR: &[u8] =
    include_bytes!("../../../../tests/fixtures/unsigned-mapping/descriptor.bin");
pub(super) const TYPE: &str = "unsigned_mapping.Parent";

#[derive(Clone, PartialEq, Message)]
struct Parent {
    #[prost(fixed64, tag = "1")]
    id: u64,
    #[prost(message, repeated, tag = "2")]
    chunks: Vec<Chunk>,
}

#[derive(Clone, PartialEq, Message)]
struct Chunk {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(string, tag = "2")]
    body: String,
    #[prost(float, repeated, tag = "3")]
    embedding: Vec<f32>,
}

/// Version `version` of `key`, written as `operation`, with `chunks` rows.
pub(super) fn chunked(
    key: &[u8],
    operation: &[u8],
    version: u64,
    chunks: usize,
) -> AcceptDocumentRequest {
    let payload = Parent {
        id: version,
        chunks: (0..chunks)
            .map(|i| Chunk {
                id: i as u64,
                body: format!("version {version} chunk {i}"),
                embedding: vec![0.25; 8],
            })
            .collect(),
    };
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: key.to_vec(),
        operation_id: operation.to_vec(),
        expected_version: Some(version - 1),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: DESCRIPTOR.to_vec(),
            message_type: TYPE.into(),
            payload: payload.encode_to_vec(),
        })),
        ..Default::default()
    }
}

/// The segmented node serving the target index, and the target itself.
pub(super) fn target(dir: &Directory) -> ManagedTarget {
    target_named(dir, INDEX, "target-index")
}

pub(super) fn target_named(dir: &Directory, key: &[u8], path: &str) -> ManagedTarget {
    let plan = crate::mapping::derive_plan(DESCRIPTOR, TYPE).unwrap();
    let node = NodeServiceImpl::open(
        NodeConfig {
            collection: "books".into(),
            index_path: Some(dir.0.join(path)),
            wal: false,
            analysis_addr: Some("native".into()),
            unsigned_integer_fields: plan
                .fields
                .iter()
                .filter(|f| f.family == crate::pb::ColumnFamily::U64 as i32)
                .map(|f| f.name.clone())
                .collect(),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    ManagedTarget {
        node: Arc::new(node),
        index_key: key.to_vec(),
        stage: StageDocumentProjectionRequest {
            projection: Some(PrepareDocumentProjectionRequest {
                expected_plan_fingerprint: plan.fingerprint,
                max_rows: 100,
                max_bytes: 1 << 20,
                ..Default::default()
            }),
            field_analysis: vec![MappedFieldAnalysis {
                path: "chunks.body".into(),
                analysis: Some(crate::analyzer::body_spec()),
            }],
            max_staged_bytes: 16 << 20,
            ..Default::default()
        },
    }
}

/// One publication step under alice's Admin: the status of the version it
/// projected, or `None` when every target is caught up.
pub(super) async fn step(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
    target: &ManagedTarget,
) -> Option<DocumentPublicationStatus> {
    let next = {
        let admission = fixture.authority.admission("alice").unwrap();
        crate::node::next_publication_blocking(active, &admission, std::slice::from_ref(target))
            .unwrap()
    };
    match next {
        Some(next) => Some(
            crate::node::publish_next(next, std::slice::from_ref(target))
                .await
                .unwrap(),
        ),
        None => None,
    }
}

/// Live rows the target node serves.
pub(super) async fn live_rows(target: &ManagedTarget) -> u64 {
    use crate::pb::node_service_server::NodeService;
    target
        .node
        .health(tonic::Request::new(crate::pb::HealthRequest {}))
        .await
        .unwrap()
        .into_inner()
        .live_docs
}

/// Write `request` under a lease that lapses while its commit returns:
/// durable and UNCONFIRMED.
pub(super) fn lapsed_write(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
    request: &AcceptDocumentRequest,
) -> crate::pb::DocumentWriteReceipt {
    let admission = fixture
        .authority
        .leased_admission("alice", short_lease(40))
        .unwrap();
    active.arm_postcommit_pause(std::time::Duration::from_millis(80));
    match active.accept(&admission, request).unwrap() {
        Acceptance::Unconfirmed(receipt) => receipt,
        Acceptance::Accepted(receipt) => panic!("accepted past its lease: {receipt:?}"),
    }
}

/// Take Ingest from alice: the grants command at `control_revision` and
/// policy revision 1 leaves her Admin only.
pub(super) fn revoke_ingest(fixture: &Fixture, control_revision: u64) {
    let command = grants_command(
        &authority_identity(7),
        "admin-only",
        control_revision,
        1,
        vec![grant("alice", AccessAction::Admin)],
    );
    assert_eq!(
        fixture.authority.execute("alice", &command).unwrap().code,
        0
    );
}

fn append_candidate(
    catalog: &crate::segments::SegmentCatalog,
    candidate: &crate::node::StagedDocumentCandidate,
) -> Arc<crate::segments::OpenedSegmentSet> {
    let staged = candidate.segments().unwrap();
    let metadata = staged.metadata(0);
    let directory =
        crate::segments::SegmentCatalog::segment_dir(staged.root(), &metadata.segment_id);
    let artifact = |file: &str| directory.join(file);
    catalog
        .append(crate::segments::SegmentSource {
            multi_res_path: metadata
                .multi_res
                .as_ref()
                .map(|a| artifact(&a.file))
                .as_deref(),
            segment_id: &metadata.segment_id,
            generation: 1,
            base_label: 0,
            backend_kind: &metadata.backend_kind,
            vector_path: (!metadata.vector.file.is_empty())
                .then(|| artifact(&metadata.vector.file))
                .as_deref(),
            exact_vector_path: (!metadata.exact_vectors.file.is_empty())
                .then(|| artifact(&metadata.exact_vectors.file))
                .as_deref(),
            bm25_path: &artifact(&metadata.bm25.file),
            live_docs_path: &artifact(&metadata.live_docs.file),
            partition_column: None,
            parents_colocated: metadata.parents_colocated,
        })
        .unwrap()
}

/// Prepare an accepted candidate and leave its journal intent unresolved.
pub(super) async fn stage_pending_accepted(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
    target: &ManagedTarget,
) {
    let admission = fixture.authority.admission("alice").unwrap();
    let source = active.publication_source(&admission).unwrap();
    let version = active.settled_version(&admission, 1).unwrap().unwrap();
    let mut request = target.stage.clone();
    let projection = request.projection.get_or_insert_with(Default::default);
    projection.history_id = source.index_owner(&target.index_key).unwrap().history_id;
    projection.document_key = version.document_key.clone();
    projection.version = version.version;
    let candidate = target
        .node
        .stage_document_projection(source.clone(), request)
        .await
        .unwrap();
    let catalog = crate::segments::SegmentCatalog::open(crate::node::segments_root(
        &fixture.dir.0.join("target-index"),
    ))
    .unwrap();
    let before = catalog.snapshot();
    let after = append_candidate(&catalog, &candidate);
    source
        .prepare_index_publication(&target.index_key, &candidate, &before, &after)
        .unwrap();
}

/// Leave a fenced version's skip intent unresolved.
pub(super) fn stage_pending_fenced(
    fixture: &Fixture,
    active: &ActiveManagedCatalog,
    target: &ManagedTarget,
) {
    let admission = fixture.authority.admission("alice").unwrap();
    let source = active.publication_source(&admission).unwrap();
    let version = active.settled_version(&admission, 2).unwrap().unwrap();
    let catalog = crate::segments::SegmentCatalog::open(crate::node::segments_root(
        &fixture.dir.0.join("target-index"),
    ))
    .unwrap();
    let before = catalog.snapshot();
    let mut after = before.manifest().clone();
    after.epoch += 1;
    std::fs::write(
        before.root().join("segments.json"),
        serde_json::to_vec_pretty(&after).unwrap(),
    )
    .unwrap();
    let after = Arc::new(crate::segments::OpenedSegmentSet::open(before.root()).unwrap());
    source
        .prepare_fenced_skip(&target.index_key, &version, &before, &after)
        .unwrap();
}
