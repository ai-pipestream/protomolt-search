use pipestream_search::pb::node_service_server::NodeService;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    derived::Declaration,
    document_catalog::DocumentCatalog,
    mapping,
    node::{segments_root, NodeConfig, NodeServiceImpl},
    pb::*,
};
use prost::Message;
use std::{path::PathBuf, sync::Arc};
use tonic::{Code, Request};

const DESCRIPTOR: &[u8] = include_bytes!("fixtures/unsigned-mapping/descriptor.bin");
const TYPE: &str = "unsigned_mapping.Parent";
const KEY: &[u8] = b"source\0exact-key";
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
fn source(count: usize, bad_last: bool) -> ProtobufSource {
    let mut payload = Parent {
        id: u64::MAX,
        chunks: (0..count)
            .map(|i| Chunk {
                id: i as u64,
                body: format!("accepted chunk {i}"),
                embedding: vec![0.25; if bad_last && i + 1 == count { 7 } else { 8 }],
                value: [Some(u64::MAX), None, Some(0)][i % 3],
            })
            .collect(),
    }
    .encode_to_vec();
    payload.extend_from_slice(&[0xa0, 6, 0x81, 0]);
    ProtobufSource {
        descriptor_set: DESCRIPTOR.to_vec(),
        message_type: TYPE.into(),
        payload,
    }
}
struct Fixture {
    root: PathBuf,
    config: NodeConfig,
    catalog: Arc<DocumentCatalog>,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "document-stage-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let plan = mapping::derive_plan(DESCRIPTOR, TYPE).unwrap();
        let config = NodeConfig {
            collection: "books".into(),
            index_path: Some(root.join("index")),
            wal: false,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            unsigned_integer_fields: plan
                .fields
                .iter()
                .filter(|f| f.family == ColumnFamily::U64 as i32)
                .map(|f| f.name.clone())
                .collect(),
            derived: Some(Arc::new(
                Declaration::compile(&DerivedColumns {
                    columns: vec![DerivedColumn {
                        name: "key_bucket".into(),
                        kind: MaterializeKind::U64 as i32,
                        expression: "hash.fnv64(stable_key()) % 64u".into(),
                        disclosure: DerivedDisclosure::Inputs as i32,
                    }],
                })
                .unwrap(),
            )),
            ..Default::default()
        };
        let catalog =
            Arc::new(DocumentCatalog::create(&root.join("sources.redb"), "books").unwrap());
        Self {
            root,
            config,
            catalog,
        }
    }
    fn accept(&self, version: u64, source: Option<ProtobufSource>) -> DocumentWriteReceipt {
        self.catalog
            .accept(&AcceptDocumentRequest {
                contract_version: 1,
                document_key: KEY.to_vec(),
                operation_id: version.to_be_bytes().to_vec(),
                expected_version: Some(version - 1),
                mutation: Some(source.map_or(
                    accept_document_request::Mutation::Delete(true),
                    accept_document_request::Mutation::Source,
                )),
                ..Default::default()
            })
            .unwrap()
    }
    fn request(&self, receipt: &DocumentWriteReceipt) -> StageDocumentProjectionRequest {
        StageDocumentProjectionRequest {
            projection: Some(PrepareDocumentProjectionRequest {
                history_id: receipt.history_id.clone(),
                document_key: KEY.to_vec(),
                version: receipt.version,
                expected_plan_fingerprint: mapping::derive_plan(DESCRIPTOR, TYPE)
                    .unwrap()
                    .fingerprint,
                max_rows: 100,
                max_bytes: 1024 * 1024,
                ..Default::default()
            }),
            field_analysis: vec![MappedFieldAnalysis {
                path: "chunks.body".into(),
                analysis: Some(body_spec()),
            }],
            max_staged_bytes: 16 * 1024 * 1024,
            ..Default::default()
        }
    }
    fn stages(&self) -> usize {
        std::fs::read_dir(&self.root)
            .unwrap()
            .filter(|e| {
                e.as_ref()
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".document-projection-")
            })
            .count()
    }
    fn manifest(&self) -> Option<Vec<u8>> {
        match std::fs::read(
            segments_root(self.config.index_path.as_ref().unwrap()).join("segments.json"),
        ) {
            Ok(bytes) => Some(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => panic!("read manifest: {error}"),
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn accepted_rows_are_analyzed_materialized_and_sealed_without_publication() {
    let fixture = Fixture::new();
    let original = source(3, false);
    let receipt = fixture.accept(1, Some(original.clone()));
    let node = NodeServiceImpl::open(fixture.config.clone(), None, false).unwrap();
    let before = fixture.manifest();
    let candidate = node
        .stage_document_projection(fixture.catalog.clone(), fixture.request(&receipt))
        .await
        .unwrap();
    assert_eq!(candidate.info().rows, 3);
    assert_eq!(
        candidate.info().accepted_sequence,
        receipt.accepted_sequence
    );
    assert_eq!(candidate.info().history_id, receipt.history_id);
    assert_eq!(candidate.source(), Some(&original));
    let set = candidate.segments().unwrap();
    assert_eq!(set.len(), 1);
    let store = set.bm25(0);
    let value = store.unsigned_integer_index("value").unwrap();
    let bucket = store.unsigned_integer_index("key_bucket").unwrap();
    for row in 0..3 {
        assert_eq!(
            store.protobuf_source(row).unwrap(),
            Some((original.clone(), Some(row)))
        );
        assert_eq!(
            store.document_identity(row),
            Some(DocumentIdentity {
                document_key: KEY.to_vec(),
                version: 1,
                chunk_ordinal: Some(row)
            })
        );
        assert_eq!(
            store.unsigned_integer_value(value, row),
            [Some(u64::MAX), None, Some(0)][row as usize]
        );
        assert_eq!(
            store.unsigned_integer_value(bucket, row),
            Some(pipestream_search::values::fnv1a64_bytes(KEY) % 64)
        );
    }
    assert!(set.binding().is_some());
    assert!(set.vector(0).is_some());
    assert!(set.exact_vectors(0).is_some());
    let health = node
        .health(Request::new(HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((health.num_vectors, health.bm25_docs), (0, 0));
    assert_eq!(fixture.manifest(), before);
    assert!(!receipt.searchable);
    assert_eq!(fixture.stages(), 1);
    drop(candidate);
    assert_eq!(fixture.stages(), 0);
}

#[tokio::test]
async fn late_row_failure_and_output_budget_leave_no_live_rows_or_private_files() {
    let fixture = Fixture::new();
    let receipt = fixture.accept(1, Some(source(2, true)));
    let node = NodeServiceImpl::open(fixture.config.clone(), None, false).unwrap();
    let before = fixture.manifest();
    let error = node
        .stage_document_projection(fixture.catalog.clone(), fixture.request(&receipt))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("dim"), "{error}");
    assert_eq!(fixture.manifest(), before);
    assert_eq!(fixture.stages(), 0);
    let receipt = fixture.accept(2, Some(source(1, false)));
    let mut request = fixture.request(&receipt);
    request.max_staged_bytes = 1;
    let error = node
        .stage_document_projection(fixture.catalog.clone(), request)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::ResourceExhausted, "{error}");
    assert_eq!(fixture.manifest(), before);
    assert_eq!(fixture.stages(), 0);
}

#[tokio::test]
async fn zero_row_sources_keep_binding_and_original_bytes_distinct_from_deletion() {
    let fixture = Fixture::new();
    let original = source(0, false);
    let receipt = fixture.accept(1, Some(original.clone()));
    let node = NodeServiceImpl::open(fixture.config.clone(), None, false).unwrap();
    let empty = node
        .stage_document_projection(fixture.catalog.clone(), fixture.request(&receipt))
        .await
        .unwrap();
    assert_eq!(empty.info().rows, 0);
    assert!(!empty.info().deleted);
    assert_eq!(empty.source(), Some(&original));
    assert!(empty.segments().unwrap().is_empty());
    assert!(empty.segments().unwrap().binding().is_some());
    drop(empty);
    let deleted = fixture.accept(2, None);
    let mut request = fixture.request(&deleted);
    request
        .projection
        .as_mut()
        .unwrap()
        .expected_plan_fingerprint
        .clear();
    request.field_analysis.clear();
    let deletion = node
        .stage_document_projection(fixture.catalog.clone(), request)
        .await
        .unwrap();
    assert!(deletion.info().deleted);
    assert_eq!(deletion.info().rows, 0);
    assert!(deletion.source().is_none() && deletion.segments().is_none());
    assert_eq!(fixture.stages(), 0);
}

#[tokio::test]
async fn wrong_collection_or_unpinned_analysis_cannot_build_a_candidate() {
    let fixture = Fixture::new();
    let receipt = fixture.accept(1, Some(source(1, false)));
    let mut config = fixture.config.clone();
    config.collection = "another".into();
    let node = NodeServiceImpl::open(config, None, false).unwrap();
    let error = node
        .stage_document_projection(fixture.catalog.clone(), fixture.request(&receipt))
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("collection"));
    let node = NodeServiceImpl::open(fixture.config.clone(), None, false).unwrap();
    let mut request = fixture.request(&receipt);
    request.field_analysis.clear();
    assert_eq!(
        node.stage_document_projection(fixture.catalog.clone(), request)
            .await
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    assert_eq!(fixture.stages(), 0);
}

#[tokio::test]
async fn staging_keeps_provider_calibration_and_refuses_a_changed_analysis_contract() {
    let fixture = Fixture::new();
    let receipt = fixture.accept(1, Some(source(2, false)));
    let node = NodeServiceImpl::open(fixture.config.clone(), None, false).unwrap();
    node.set_calibration(Request::new(SetCalibrationRequest {
        dim: 8,
        bit_width: 4,
        shift: vec![-1.0; 8],
        scale: vec![0.5; 8],
    }))
    .await
    .unwrap();
    let health = node
        .health(Request::new(HealthRequest {}))
        .await
        .unwrap()
        .into_inner();
    let candidate = node
        .stage_document_projection(fixture.catalog.clone(), fixture.request(&receipt))
        .await
        .unwrap();
    assert_eq!(
        candidate
            .segments()
            .unwrap()
            .vector(0)
            .unwrap()
            .descriptor()
            .scoring_fingerprint,
        health.scoring_fingerprint
    );
    let binding = candidate.segments().unwrap().binding().unwrap().clone();
    node.apply_wal_binding(Request::new(ApplyWalBindingRequest {
        collection: "books".into(),
        plan_fingerprint: binding.plan_fingerprint,
        body_path: binding.body_path,
        materialize_sha: binding.materialize_sha,
        analysis_sha: binding.analysis_sha,
        analysis_contract: binding.analysis_contract,
        vector_binding: binding.vector_binding,
        index_contract: binding.index_contract,
    }))
    .await
    .unwrap();
    node.flush_index().unwrap();
    drop(candidate);
    let before = fixture.manifest();
    let matching = node
        .stage_document_projection(fixture.catalog.clone(), fixture.request(&receipt))
        .await
        .unwrap();
    drop(matching);
    let mut changed = fixture.request(&receipt);
    changed.field_analysis[0].analysis = Some(pipestream_search::analyzer::cased_body_spec());
    let error = node
        .stage_document_projection(fixture.catalog.clone(), changed)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code(), Code::FailedPrecondition, "{error}");
    assert!(error.message().contains("durable mapping"), "{error}");
    assert_eq!(fixture.manifest(), before);
    assert_eq!(fixture.stages(), 0);
}

#[tokio::test]
async fn embedded_owner_stages_from_its_catalog_without_a_socket_or_searchable_receipt() {
    use pipestream_search::embedded::{
        EmbeddedDocumentCatalogConfig, EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig,
    };
    let fixture = Fixture::new();
    let mut config = EmbeddedSearchConfig::single(EmbeddedShardConfig {
        node: fixture.config.clone(),
        allow_missing_bm25: false,
    });
    config.document_catalog = Some(EmbeddedDocumentCatalogConfig {
        collection: "books".into(),
        path: None,
    });
    let runtime = EmbeddedSearch::create(config).await.unwrap();
    let receipt = runtime
        .accept_document(&AcceptDocumentRequest {
            contract_version: 1,
            document_key: KEY.to_vec(),
            operation_id: b"embedded-write".to_vec(),
            expected_version: Some(0),
            mutation: Some(accept_document_request::Mutation::Source(source(1, false))),
            ..Default::default()
        })
        .unwrap();
    let candidate = runtime
        .stage_document_projection(0, fixture.request(&receipt))
        .await
        .unwrap();
    assert_eq!(candidate.info().history_id, receipt.history_id);
    assert_eq!(candidate.info().rows, 1);
    assert!(!receipt.searchable);
    assert_eq!(runtime.shard_health(0).await.unwrap().num_vectors, 0);
    assert_eq!(
        runtime
            .stage_document_projection(1, fixture.request(&receipt))
            .await
            .err()
            .unwrap()
            .code(),
        Code::InvalidArgument
    );
    drop(candidate);
    assert_eq!(fixture.stages(), 0);
}

#[tokio::test]
async fn empty_sources_validate_the_configured_analyzer_before_building_artifacts() {
    let fixture = Fixture::new();
    let receipt = fixture.accept(1, Some(source(0, false)));
    let mut accepted = Vec::new();
    for address in [None, Some("native://"), Some(" native ")] {
        let mut config = fixture.config.clone();
        config.analysis_addr = address.map(str::to_owned);
        let node = NodeServiceImpl::open(config, None, false).unwrap();
        let mut request = fixture.request(&receipt);
        if address.is_some() {
            // Tokenizer 2 is a valid sidecar tokenizer, unsupported natively.
            request.field_analysis[0]
                .analysis
                .as_mut()
                .unwrap()
                .tokenizer = 2;
        }
        let result = node
            .stage_document_projection(fixture.catalog.clone(), request)
            .await;
        if result.is_ok() {
            accepted.push(address);
        }
        drop(result);
        assert_eq!(fixture.stages(), 0);
    }
    assert!(
        accepted.is_empty(),
        "empty sources accepted unsupported analyzers: {accepted:?}"
    );
}
