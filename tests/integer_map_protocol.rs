use pipestream_search::{
    document_contract,
    pb::{
        self,
        wal::{wal_record, LoggedAddDocuments},
    },
    wal::{self, WalManifest, WalWriter},
};
use prost::Message;

fn directory(tag: &str) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer-map-protocol-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    path
}
fn manifest() -> WalManifest {
    WalManifest {
        dim: 0,
        vector_backend: String::new(),
        vector_config_format: String::new(),
        vector_config_payload: vec![],
        bit_width: 4,
        calibration_shift: vec![],
        calibration_scale: vec![],
        collection: String::new(),
        slot_offset: 0,
        generation: 0,
        bucket_bits: 0,
        bucket_count: 1,
        preexisting_vectors: 0,
        preexisting_documents: 0,
        format_version: 6,
    }
}
fn document() -> pb::AddDocumentsRequest {
    pb::AddDocumentsRequest {
        text: "map".into(),
        map_integers: vec![pb::MapIntegerEntry {
            field: "s".into(),
            key: String::new(),
            value: i64::MIN,
        }],
        map_unsigned_integers: vec![pb::MapUnsignedIntegerEntry {
            field: "u".into(),
            key: String::new(),
            value: u64::MAX,
        }],
        ..Default::default()
    }
}
fn operation(documents: Vec<pb::AddDocumentsRequest>) -> wal_record::Op {
    wal_record::Op::AddDocuments(LoggedAddDocuments {
        first_id: 0,
        documents,
        ..Default::default()
    })
}

#[test]
fn wal_publishes_integer_map_version_before_records_and_replays_exact_bytes() {
    let dir = directory("upgrade");
    let mut writer = WalWriter::create(&dir, manifest()).unwrap();
    let mut with_identity = document();
    with_identity.original_source = Some(pb::ProtobufSource {
        descriptor_set: b"opaque".to_vec(),
        message_type: "Doc".into(),
        payload: vec![8, 0x81, 0],
    });
    with_identity.identity = Some(pb::DocumentIdentity {
        document_key: vec![0, 255],
        version: u64::MAX,
        chunk_ordinal: None,
    });
    let documents = vec![document(), with_identity];
    writer.append(operation(documents.clone())).unwrap();
    // This observation precedes Flush: old decoders must already refuse the generation.
    assert_eq!(wal::read_manifest(writer.dir()).unwrap().format_version, 7);
    writer.flush().unwrap();
    let records = wal::read_clocked_records(writer.dir(), 0).unwrap();
    let Some(wal_record::Op::AddDocuments(batch)) = &records[0].op else {
        panic!("missing rows")
    };
    assert_eq!(batch.documents, documents);
    let reopened =
        WalWriter::resume(writer.dir(), wal::read_manifest(writer.dir()).unwrap()).unwrap();
    assert_eq!(reopened.manifest().format_version, 7);
    drop(reopened);
    drop(writer);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn failed_wal_version_publication_does_not_append_a_typed_record() {
    let dir = directory("failed-upgrade");
    let mut writer = WalWriter::create(&dir, manifest()).unwrap();
    let path = wal::manifest_path(writer.dir());
    std::fs::remove_file(&path).unwrap();
    std::fs::create_dir(&path).unwrap();
    assert!(writer.append(operation(vec![document()])).is_err());
    assert_eq!(writer.high_watermark(), 0);
    assert_eq!(writer.manifest().format_version, 6);
    std::fs::remove_dir(&path).unwrap();
    wal::write_manifest(writer.dir(), &manifest()).unwrap();
    writer.flush().unwrap();
    assert!(wal::read_clocked_records(writer.dir(), 0)
        .unwrap()
        .is_empty());
    drop(writer);
    std::fs::remove_dir_all(dir).unwrap();
}

#[derive(Clone, PartialEq, Message)]
struct OldResponse {
    #[prost(uint64, tag = "1")]
    added: u64,
    #[prost(uint64, tag = "2")]
    total: u64,
    #[prost(uint64, tag = "3")]
    first_id: u64,
    #[prost(uint64, tag = "4")]
    wal_generation: u64,
}
#[derive(Clone, PartialEq, Message)]
struct OldDocument {
    #[prost(string, tag = "1")]
    text: String,
}

#[test]
fn a_legacy_row_count_cannot_acknowledge_dropped_integer_map_values() {
    let expected = document();
    let old = OldDocument::decode(expected.encode_to_vec().as_slice()).unwrap();
    let lost = pb::AddDocumentsRequest::decode(old.encode_to_vec().as_slice()).unwrap();
    assert!(lost.map_integers.is_empty());
    assert!(lost.map_unsigned_integers.is_empty());
    let response = OldResponse {
        added: 1,
        total: 1,
        first_id: 0,
        wal_generation: 7,
    };
    let decoded = pb::AddDocumentsResponse::decode(response.encode_to_vec().as_slice()).unwrap();
    assert_eq!(decoded.added, 1);
    assert!(document_contract::require_supported(
        decoded.document_contract_version,
        document_contract::required_version(&expected)
    )
    .is_err());
    assert!(document_contract::require_supported(
        1,
        document_contract::required_version(&expected)
    )
    .is_ok());
    assert_eq!(
        document_contract::required_version(&pb::AddDocumentsRequest::default()),
        0
    );
}
