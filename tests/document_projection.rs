use pipestream_search::{document_catalog::DocumentCatalog, mapping, pb::*};
use prost::Message;
use tonic::Code;

const DESCRIPTOR: &[u8] = include_bytes!("fixtures/unsigned-mapping/descriptor.bin");
const TYPE: &str = "unsigned_mapping.Parent";

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
    #[prost(uint64, optional, tag = "4")]
    value: Option<u64>,
}
fn source(count: usize) -> ProtobufSource {
    let mut payload = Parent {
        id: 42,
        chunks: (0..count)
            .map(|n| Chunk {
                id: 90 + n as u64,
                body: format!("chunk {n}"),
                embedding: vec![0.25; 8],
                value: Some(u64::MAX - n as u64),
            })
            .collect(),
    }
    .encode_to_vec();
    // Unknown field and nonminimal varint must survive preparation exactly.
    payload.extend_from_slice(&[0xa0, 6, 0x81, 0]);
    ProtobufSource {
        descriptor_set: DESCRIPTOR.to_vec(),
        message_type: TYPE.into(),
        payload,
    }
}
fn accept(
    catalog: &DocumentCatalog,
    version: u64,
    source: Option<ProtobufSource>,
) -> DocumentWriteReceipt {
    catalog
        .accept(&AcceptDocumentRequest {
            contract_version: 1,
            document_key: b"external\0catalog-key".to_vec(),
            operation_id: version.to_be_bytes().to_vec(),
            expected_version: Some(version - 1),
            mutation: Some(match source {
                Some(source) => accept_document_request::Mutation::Source(source),
                None => accept_document_request::Mutation::Delete(true),
            }),
            ..Default::default()
        })
        .unwrap()
}
fn request(receipt: &DocumentWriteReceipt) -> PrepareDocumentProjectionRequest {
    PrepareDocumentProjectionRequest {
        history_id: receipt.history_id.clone(),
        document_key: receipt.document_key.clone(),
        version: receipt.version,
        expected_plan_fingerprint: mapping::derive_plan(DESCRIPTOR, TYPE).unwrap().fingerprint,
        max_rows: 100,
        max_bytes: 1024 * 1024,
        ..Default::default()
    }
}

#[test]
fn every_chunk_carries_the_accepted_identity_and_exact_original_source() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    let original = source(2);
    let receipt = accept(&catalog, 1, Some(original.clone()));
    let batch = catalog.prepare_projection(&request(&receipt)).unwrap();
    assert_eq!(batch.source, Some(original));
    assert_eq!(batch.history_id, receipt.history_id);
    assert_eq!(batch.accepted_sequence, receipt.accepted_sequence);
    assert!(!batch.deleted);
    assert_eq!(batch.body_path, "chunks.body");
    assert_eq!(batch.rows.len(), 2);
    for (ordinal, row) in batch.rows.iter().enumerate() {
        let document = row.document.as_ref().unwrap();
        assert_eq!(
            document.identity,
            Some(DocumentIdentity {
                document_key: receipt.document_key.clone(),
                version: 1,
                chunk_ordinal: Some(ordinal as u32),
            })
        );
        assert_eq!(document.source_chunk_ordinal, Some(ordinal as u32));
        assert!(document.original_source.is_none(), "one original per batch");
        assert_eq!(row.vector, vec![0.25; 8]);
        assert!(document
            .unsigned_integers
            .iter()
            .any(|value| value.value == u64::MAX - ordinal as u64));
    }
    assert!(!receipt.searchable);
    // A later acceptance cannot retarget a request for the original version.
    accept(&catalog, 2, Some(source(1)));
    assert_eq!(
        catalog.prepare_projection(&request(&receipt)).unwrap(),
        batch
    );
}

#[test]
fn zero_row_sources_and_deletions_are_distinct_complete_batches() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    let original = source(0);
    let first = accept(&catalog, 1, Some(original.clone()));
    let empty = catalog.prepare_projection(&request(&first)).unwrap();
    assert!(empty.rows.is_empty());
    assert!(!empty.deleted);
    assert_eq!(empty.source, Some(original));
    assert!(!empty.plan_fingerprint.is_empty());
    let deleted = accept(&catalog, 2, None);
    let mut request = request(&deleted);
    assert_eq!(
        catalog.prepare_projection(&request).unwrap_err().code(),
        Code::InvalidArgument
    );
    request.expected_plan_fingerprint.clear();
    let tombstone = catalog.prepare_projection(&request).unwrap();
    assert!(tombstone.deleted && tombstone.rows.is_empty() && tombstone.source.is_none());
    assert_eq!((tombstone.version, tombstone.accepted_sequence), (2, 2));
}

#[test]
fn preparation_refuses_limits_and_invalid_late_chunks_without_advancing_history() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    let receipt = accept(&catalog, 1, Some(source(2)));
    let mut req = request(&receipt);
    let batch = catalog.prepare_projection(&req).unwrap();
    req.max_rows = 1;
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::ResourceExhausted
    );
    req.max_rows = 2;
    req.max_bytes = batch.encoded_len() as u64;
    assert_eq!(catalog.prepare_projection(&req).unwrap(), batch);
    req.max_bytes -= 1;
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::ResourceExhausted
    );
    req.max_bytes = 1;
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::ResourceExhausted
    );
    let mut malformed = source(2);
    let mut parent = Parent::decode(malformed.payload.as_slice()).unwrap();
    parent.chunks[1].embedding.clear();
    malformed.payload = parent.encode_to_vec();
    let bad = accept(&catalog, 2, Some(malformed));
    let error = catalog.prepare_projection(&request(&bad)).unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("chunk 1"));
    assert_eq!(
        catalog
            .get(&receipt.document_key, None)
            .unwrap()
            .unwrap()
            .0
            .version,
        2
    );
    assert_eq!(
        catalog.prepare_projection(&request(&receipt)).unwrap(),
        batch
    );
}

#[test]
fn preparation_requires_the_exact_history_version_and_reviewed_schema() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    let receipt = accept(&catalog, 1, Some(source(1)));
    let mut req = request(&receipt);
    req.history_id[0] ^= 1;
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::FailedPrecondition
    );
    req.history_id = receipt.history_id;
    req.version = 0;
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::InvalidArgument
    );
    req.version = 2;
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::NotFound
    );
    req.version = 1;
    req.expected_plan_fingerprint = "another-plan".into();
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::FailedPrecondition
    );
    req.expected_plan_fingerprint.clear();
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::InvalidArgument
    );
}

#[tokio::test]
async fn embedded_preparation_does_not_ingest_or_publish_any_rows() {
    use pipestream_search::embedded::{
        EmbeddedDocumentCatalogConfig, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
    };
    let mut shard = EmbeddedShardConfig::in_memory(0);
    shard.node.collection = "books".into();
    let mut config = EmbeddedSearchConfig::single(shard);
    config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
        collection: "books".into(),
        path: None,
    });
    let engine = EmbeddedSearch::create(config).await.unwrap();
    let receipt = engine
        .accept_document(&AcceptDocumentRequest {
            contract_version: 1,
            document_key: b"key".to_vec(),
            operation_id: b"one".to_vec(),
            mutation: Some(accept_document_request::Mutation::Source(source(2))),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        engine
            .prepare_document_projection(&request(&receipt))
            .unwrap()
            .rows
            .len(),
        2
    );
    assert!(!receipt.searchable);
    assert!(engine
        .flush_all()
        .await
        .unwrap()
        .iter()
        .all(|response| !response.written));
}

#[test]
fn explicit_flat_projection_preserves_absent_chunk_ordinal_and_source_only_values() {
    let catalog = DocumentCatalog::in_memory("books").unwrap();
    let original = ProtobufSource {
        descriptor_set: DESCRIPTOR.to_vec(),
        message_type: "unsigned_mapping.Record".into(),
        payload: Chunk {
            id: 99,
            body: "flat".into(),
            embedding: vec![0.5; 8],
            value: Some(u64::MAX),
        }
        .encode_to_vec(),
    };
    let policy = IndexDefinition {
        projections: [
            (1, MappedKind::Uint64, "id", MappedRole::DocId),
            (2, MappedKind::Text, "body", MappedRole::None),
            (3, MappedKind::Vector, "semantic", MappedRole::None),
        ]
        .into_iter()
        .map(|(number, kind, name, role)| IndexProjection {
            field_numbers: vec![number],
            kind: kind as i32,
            column_name: name.into(),
            role: role as i32,
            vector_dims: if kind == MappedKind::Vector { 8 } else { 0 },
        })
        .collect(),
    };
    let fingerprint =
        mapping::derive_plan_with_definition(DESCRIPTOR, &original.message_type, Some(&policy))
            .unwrap()
            .fingerprint;
    let receipt = accept(&catalog, 1, Some(original.clone()));
    let mut req = request(&receipt);
    req.expected_plan_fingerprint = fingerprint;
    req.index_definition = Some(policy);
    let batch = catalog.prepare_projection(&req).unwrap();
    assert_eq!(batch.source, Some(original));
    assert_eq!(batch.rows.len(), 1);
    let row = batch.rows[0].document.as_ref().unwrap();
    assert_eq!(row.text, "flat");
    assert_eq!(
        row.identity.as_ref().unwrap().document_key,
        receipt.document_key
    );
    assert_eq!(row.identity.as_ref().unwrap().chunk_ordinal, None);
    assert_eq!(row.source_chunk_ordinal, None);
    assert!(!row
        .unsigned_integers
        .iter()
        .any(|value| value.value == u64::MAX));
    req.index_definition.as_mut().unwrap().projections[2].vector_dims = 4;
    assert_eq!(
        catalog.prepare_projection(&req).unwrap_err().code(),
        Code::FailedPrecondition
    );
}

#[test]
fn prepared_batch_is_reproducible_after_durable_reopen() {
    let directory = std::env::temp_dir().join(format!(
        "psearch-projection-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("catalog.redb");
    let req;
    let bytes;
    {
        let catalog = DocumentCatalog::create(&path, "books").unwrap();
        let receipt = accept(&catalog, 1, Some(source(2)));
        req = request(&receipt);
        bytes = catalog.prepare_projection(&req).unwrap().encode_to_vec();
    }
    {
        let catalog = DocumentCatalog::open(&path, "books").unwrap();
        assert_eq!(
            catalog.prepare_projection(&req).unwrap().encode_to_vec(),
            bytes
        );
    }
    std::fs::remove_dir_all(directory).unwrap();
}
