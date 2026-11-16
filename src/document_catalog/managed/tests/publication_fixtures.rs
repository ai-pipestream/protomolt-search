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
    let plan = crate::mapping::derive_plan(DESCRIPTOR, TYPE).unwrap();
    let node = NodeServiceImpl::open(
        NodeConfig {
            collection: "books".into(),
            index_path: Some(dir.0.join("target-index")),
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
        false,
    )
    .unwrap();
    ManagedTarget {
        node: Arc::new(node),
        index_key: INDEX.to_vec(),
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
        crate::node::next_publication(active, &admission, std::slice::from_ref(target)).unwrap()
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
