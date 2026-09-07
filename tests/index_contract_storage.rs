mod common;

use pipestream_search::{
    index_contract, mapping, pb,
    postings::{Bm25Reader, Bm25Store, SpillBuilder, StoredBinding},
};
use prost::Message;

fn binding() -> StoredBinding {
    let definition = pb::IndexDefinition {
        projections: vec![
            pb::IndexProjection {
                field_numbers: vec![1],
                kind: pb::MappedKind::Int64 as i32,
                column_name: "id".into(),
                role: pb::MappedRole::DocId as i32,
                vector_dims: 0,
            },
            pb::IndexProjection {
                field_numbers: vec![2],
                kind: pb::MappedKind::Text as i32,
                column_name: "body".into(),
                ..Default::default()
            },
            pb::IndexProjection {
                field_numbers: vec![3],
                kind: pb::MappedKind::Vector as i32,
                column_name: "semantic".into(),
                vector_dims: 16,
                ..Default::default()
            },
        ],
    };
    let plan = mapping::derive_plan_with_definition(
        include_bytes!("fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
        Some(&definition),
    )
    .unwrap();
    StoredBinding {
        index_contract: index_contract::from_plan(&plan).unwrap(),
        plan_fingerprint: plan.fingerprint.clone(),
        body_path: "body".into(),
        vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
        ..Default::default()
    }
}

#[test]
fn explicit_policy_survives_heap_spill_and_mapped_images_with_and_without_rows() {
    use pipestream_search::postings::AnalyzedDoc;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("index_contract_storage_{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for explicit_analysis in [false, true] {
        for rows in [0, 3] {
            let mut binding = binding();
            if explicit_analysis {
                binding.analysis_contract = pb::MappedAnalysisContract {
                    fields: vec![pb::MappedAnalysisColumn {
                        path: "body".into(),
                        name: "body".into(),
                        analysis: Some(pipestream_search::analyzer::body_spec()),
                    }],
                }
                .encode_to_vec();
                let mut hasher = pipestream_search::sha256::Sha256::new();
                hasher.update(b"protomolt.search.mapped-analysis.v1\0");
                hasher.update(&binding.analysis_contract);
                binding.analysis_sha = pipestream_search::sha256::to_hex(&hasher.finalize());
            }
            let mut heap = Bm25Store::new();
            let mut spill =
                SpillBuilder::create(&root.join(format!("spill-{explicit_analysis}-{rows}")))
                    .unwrap();
            heap.set_binding(Some(binding.clone()));
            spill.set_binding(Some(binding.clone()));
            for row in 0..rows {
                let analyzed = AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1);
                heap.add_document(row, "word".into(), analyzed.clone());
                spill
                    .add_document_with_lineage(row, "word".into(), analyzed, None)
                    .unwrap();
            }
            let heap_path = root.join("heap.bm25");
            let spill_path = root.join("spill.bm25");
            heap.save(&heap_path).unwrap();
            spill.finish(&spill_path).unwrap();
            assert_eq!(
                std::fs::read(&heap_path).unwrap(),
                std::fs::read(spill_path).unwrap()
            );
            assert_eq!(
                Bm25Store::load(&heap_path).unwrap().binding(),
                Some(&binding)
            );
            let mapped = Bm25Reader::open(&heap_path).unwrap();
            assert_eq!(mapped.binding(), Some(&binding));
            mapped.verify_integrity().unwrap();
            drop(mapped);
        }
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn malformed_contracts_refuse_at_codec_image_and_catalog_boundaries() {
    let binding = binding();
    let valid = index_contract::decode(&binding.index_contract, &binding.plan_fingerprint)
        .unwrap()
        .unwrap();
    let mut cases = Vec::new();
    for case in 0..7 {
        let mut invalid = valid.clone();
        match case {
            0 => invalid.format_version = 2,
            1 => invalid.plan_fingerprint = "0".repeat(64),
            2 => invalid.message_type.clear(),
            3 => invalid.index_definition = None,
            4 => invalid
                .index_definition
                .as_mut()
                .unwrap()
                .projections
                .reverse(),
            5 => invalid.index_definition.as_mut().unwrap().projections[0].field_numbers = vec![0],
            _ => invalid.index_definition.as_mut().unwrap().projections[2].kind = 999,
        }
        cases.push(invalid.encode_to_vec());
    }
    let mut duplicate = binding.index_contract.clone();
    duplicate.extend([8, 1]);
    cases.push(duplicate);
    let mut unknown = binding.index_contract.clone();
    unknown.extend([40, 1]);
    cases.push(unknown);
    for bytes in cases {
        assert!(index_contract::decode(&bytes, &binding.plan_fingerprint).is_err());
        let invalid = StoredBinding {
            index_contract: bytes,
            ..binding.clone()
        };
        let mut store = Bm25Store::new();
        store.set_binding(Some(invalid.clone()));
        assert!(store.write_v6_to(&mut Vec::new()).is_err());
        assert!(pipestream_search::segments::SegmentBinding::encode(&invalid).is_err());
    }
    let mut contradictory = binding.clone();
    let mut vector =
        pb::MappedVectorBinding::decode(contradictory.vector_binding.as_slice()).unwrap();
    vector.declared_dimensions += 1;
    contradictory.vector_binding = vector.encode_to_vec();
    assert!(pipestream_search::segments::SegmentBinding::encode(&contradictory).is_err());
}

#[test]
fn policy_entry_is_checked_without_the_outer_integrity_envelope() {
    let binding = binding();
    let mut store = Bm25Store::new();
    store.set_binding(Some(binding.clone()));
    let mut raw = Vec::new();
    store.write_v6_to(&mut raw).unwrap();
    let kind = raw.windows(12).position(|v| v == b"plan-binding").unwrap() + 12;
    assert_eq!(raw[kind], 14);
    let mut cursor = kind + 1;
    for _ in 0..4 {
        let len = u16::from_le_bytes(raw[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2 + len;
    }
    for _ in 0..2 {
        let len = u32::from_le_bytes(raw[cursor..cursor + 4].try_into().unwrap()) as usize;
        cursor += 4 + len;
    }
    let len = u32::from_le_bytes(raw[cursor..cursor + 4].try_into().unwrap()) as usize;
    assert_eq!(&raw[cursor + 4..cursor + 4 + len], binding.index_contract);
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("index_contract_malformed_{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("image.bm25");
    for cut in cursor..cursor + 4 + len {
        std::fs::write(&path, &raw[..cut]).unwrap();
        assert!(Bm25Store::load(&path).is_err());
        assert!(Bm25Reader::open(&path).is_err());
    }
    let mut unsupported = raw.clone();
    unsupported[cursor + 5] = 2; // contract format_version
    std::fs::write(&path, unsupported).unwrap();
    assert!(Bm25Store::load(&path).is_err());
    assert!(Bm25Reader::open(&path).is_err());
    for old_kind in [6, 12, 13] {
        let mut bad = raw.clone();
        bad[kind] = old_kind;
        std::fs::write(&path, bad).unwrap();
        assert!(Bm25Reader::open(&path).is_err());
    }
    std::fs::write(&path, raw).unwrap();
    assert_eq!(Bm25Reader::open(&path).unwrap().binding(), Some(&binding));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn empty_segment_catalog_retains_and_validates_explicit_policy() {
    use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog};
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("index_contract_catalog_{}", std::process::id()));
    let catalog = SegmentCatalog::open(&root).unwrap();
    let binding = binding();
    let snapshot = catalog.publish_binding(&binding).unwrap();
    assert!(snapshot.is_empty());
    assert_eq!(
        OpenedSegmentSet::open(&root).unwrap().binding(),
        Some(&binding)
    );
    let mut manifest = snapshot.published_manifest();
    let envelope = manifest.binding.as_mut().unwrap();
    let mut logged = pb::wal::LoggedBinding::decode(envelope.protobuf.as_slice()).unwrap();
    logged.index_contract.extend([8, 1]);
    envelope.protobuf = logged.encode_to_vec();
    envelope.sha256 = pipestream_search::sha256::hex_digest(&envelope.protobuf);
    pipestream_search::segments::write_manifest_file(&root.join("segments.json"), &manifest)
        .unwrap();
    assert!(OpenedSegmentSet::open(&root).is_err());
    drop(catalog);
    drop(snapshot);
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn empty_binding_recovers_from_synced_wal_without_an_index_image() {
    use pb::node_service_client::NodeServiceClient;
    use pipestream_search::node::{Layout, NodeConfig};
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("index_contract_replay_{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for layout in [Layout::SingleImage, Layout::Segments] {
        let path = root.join(format!("{layout:?}.tv"));
        let config = NodeConfig {
            index_path: Some(path.clone()),
            layout,
            wal: true,
            analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            integer_fields: vec!["id".into()],
            ..Default::default()
        };
        use pipestream_search::wal::{self, WalManifest, WalWriter};
        let mut writer = WalWriter::create(
            &wal::wal_dir(&path),
            WalManifest {
                collection: String::new(),
                dim: 16,
                vector_backend: String::new(),
                vector_config_format: String::new(),
                vector_config_payload: Vec::new(),
                bit_width: 4,
                calibration_shift: vec![0.0; 16],
                calibration_scale: vec![1.0; 16],
                slot_offset: 0,
                generation: 0,
                bucket_bits: 2,
                bucket_count: 4,
                preexisting_vectors: 0,
                preexisting_documents: 0,
                format_version: 5,
            },
        )
        .unwrap();
        let binding = binding();
        let request = pb::ApplyWalBindingRequest {
            plan_fingerprint: binding.plan_fingerprint.clone(),
            body_path: binding.body_path.clone(),
            vector_binding: binding.vector_binding.clone(),
            index_contract: binding.index_contract.clone(),
            ..Default::default()
        };
        let generation =
            pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path))
                .unwrap();
        let logged = pb::wal::LoggedBinding {
            plan_fingerprint: binding.plan_fingerprint.clone(),
            body_path: binding.body_path.clone(),
            vector_binding: binding.vector_binding.clone(),
            index_contract: binding.index_contract.clone(),
            ..Default::default()
        };
        let mut invalid = logged.clone();
        invalid.index_contract.extend([8, 1]);
        assert!(writer
            .append(pb::wal::wal_record::Op::Bind(invalid))
            .is_err());
        assert_eq!(wal::read_manifest(&generation).unwrap().format_version, 5);
        writer.flush().unwrap();
        assert!(wal::read_clocked_records(&generation, 0)
            .unwrap()
            .is_empty());
        writer
            .append(pb::wal::wal_record::Op::Bind(logged))
            .unwrap();
        assert_eq!(wal::read_manifest(&generation).unwrap().format_version, 6);
        // Sync only the WAL: no node Flush and no index image may mask recovery.
        writer.flush().unwrap();
        drop(writer);
        assert!(!path.exists());
        assert!(!path.with_extension("bm25").exists());
        let records = wal::read_clocked_records(&generation, 0).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].clock, 1);
        let (address, server) = common::start_opened_node(config).await;
        let mut client = NodeServiceClient::connect(address).await.unwrap();
        let acknowledged = client
            .apply_wal_binding(request.clone())
            .await
            .unwrap()
            .into_inner();
        assert!(acknowledged.already_bound);
        assert_eq!(acknowledged.index_contract, request.index_contract);
        assert_eq!(
            pipestream_search::reshard::read_generation_binding(&generation).unwrap(),
            Some(binding)
        );
        let mut missing = request;
        missing.index_contract.clear();
        assert_eq!(
            client.apply_wal_binding(missing).await.unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        drop(client);
        server.abort();
        let _ = server.await;
    }
    std::fs::remove_dir_all(root).unwrap();
}

/// A real receiver with an older response shape, to exercise the sender's
/// capability check independently of whether the receiver stored the binding.
#[derive(Clone)]
struct OldBindingReply {
    node: pipestream_search::node::NodeServiceImpl,
    omit_policy: std::sync::Arc<std::sync::atomic::AtomicBool>,
}
impl tonic::server::NamedService for OldBindingReply {
    const NAME: &'static str = "ai.protomolt.search.v1.NodeService";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for OldBindingReply
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let node = self.node.clone();
        let omit_policy = self.omit_policy.load(std::sync::atomic::Ordering::SeqCst);
        if !omit_policy || !request.uri().path().ends_with("/ApplyWalBinding") {
            return Box::pin(pb::node_service_server::NodeServiceServer::new(node).call(request));
        }
        Box::pin(async move {
            struct Unary(pipestream_search::node::NodeServiceImpl);
            impl tonic::server::UnaryService<pb::ApplyWalBindingRequest> for Unary {
                type Response = pb::ApplyWalBindingResponse;
                type Future = std::pin::Pin<
                    Box<
                        dyn std::future::Future<
                                Output = Result<tonic::Response<Self::Response>, tonic::Status>,
                            > + Send,
                    >,
                >;
                fn call(
                    &mut self,
                    request: tonic::Request<pb::ApplyWalBindingRequest>,
                ) -> Self::Future {
                    use pb::node_service_server::NodeService;
                    let node = self.0.clone();
                    Box::pin(async move {
                        let mut response = node.apply_wal_binding(request).await?;
                        response.get_mut().index_contract.clear();
                        Ok(response)
                    })
                }
            }
            Ok(
                tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                    .unary(Unary(node), request)
                    .await,
            )
        })
    }
}

#[tokio::test]
async fn replication_requires_policy_acknowledgment_and_persists_it_on_empty_receivers() {
    use pb::node_service_client::NodeServiceClient;
    use pipestream_search::{
        node::{Layout, NodeConfig, NodeServiceImpl},
        replication::{sync_once, ReplicaCursor},
    };
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("index_contract_replication_{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let config = |name: &str, layout| NodeConfig {
        index_path: Some(root.join(format!("{name}.tv"))),
        layout,
        wal: true,
        analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        integer_fields: vec!["id".into()],
        ..Default::default()
    };
    let (primary, primary_server) =
        common::start_empty_node(config("primary", Layout::SingleImage)).await;
    let mut source = NodeServiceClient::connect(primary.clone()).await.unwrap();
    let binding = binding();
    let request = pb::ApplyWalBindingRequest {
        plan_fingerprint: binding.plan_fingerprint.clone(),
        body_path: binding.body_path.clone(),
        vector_binding: binding.vector_binding.clone(),
        index_contract: binding.index_contract.clone(),
        ..Default::default()
    };
    source.apply_wal_binding(request.clone()).await.unwrap();
    source.flush(pb::FlushRequest {}).await.unwrap();

    let volatile_config = NodeConfig {
        index_path: None,
        wal: false,
        ..config("volatile", Layout::SingleImage)
    };
    let (replica, volatile_server) = common::start_empty_node(volatile_config).await;
    let error = sync_once(&ReplicaCursor {
        primary: primary.clone(),
        replica,
        ..Default::default()
    })
    .await
    .expect_err("in-memory acceptance cannot advance a durable replication cursor");
    assert!(error.contains("did not persist"), "{error}");
    volatile_server.abort();
    let _ = volatile_server.await;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_address = format!("http://{}", listener.local_addr().unwrap());
    let old_config = config("old", Layout::SingleImage);
    let old_node = NodeServiceImpl::new(None, old_config.clone());
    let omit_policy = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(true));
    let old_server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(OldBindingReply {
                node: old_node,
                omit_policy: omit_policy.clone(),
            })
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let error = sync_once(&ReplicaCursor {
        primary: primary.clone(),
        replica: old_address.clone(),
        ..Default::default()
    })
    .await
    .unwrap_err();
    assert!(
        error.contains("did not acknowledge the explicit index contract"),
        "{error}"
    );
    // Retry against the same already-bound receiver once it acknowledges the
    // policy. Its first application was accepted but never flushed. A cursor
    // may advance only after this retry makes that state durable.
    assert!(
        !pipestream_search::node::bm25_sidecar_path(old_config.index_path.as_ref().unwrap())
            .exists()
    );
    omit_policy.store(false, std::sync::atomic::Ordering::SeqCst);
    let retry = sync_once(&ReplicaCursor {
        primary: primary.clone(),
        replica: old_address,
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(retry.clock > 0);
    let persisted = Bm25Store::load(&pipestream_search::node::bm25_sidecar_path(
        old_config.index_path.as_ref().unwrap(),
    ))
    .expect(
        "successful catch-up must flush previously accepted bindings before advancing its cursor",
    );
    assert_eq!(persisted.binding(), Some(&binding));
    drop(persisted);
    old_server.abort();
    let _ = old_server.await;

    for layout in [Layout::SingleImage, Layout::Segments] {
        let config = config(&format!("target-{layout:?}"), layout);
        let (replica, server) = common::start_empty_node(config.clone()).await;
        let cursor = sync_once(&ReplicaCursor {
            primary: primary.clone(),
            replica,
            ..Default::default()
        })
        .await
        .unwrap();
        assert!(cursor.clock > 0);
        assert_eq!(sync_once(&cursor).await.unwrap(), cursor);
        // A snapshot receiver has no WAL fallback: the image/catalog itself
        // must carry the policy even when no source rows exist.
        let snapshot_config = NodeConfig {
            index_path: Some(root.join(format!("snapshot-{layout:?}.tv"))),
            wal: false,
            ..config.clone()
        };
        let (snapshot_addr, snapshot_server) =
            common::start_empty_node(snapshot_config.clone()).await;
        pipestream_search::snapshot::install_snapshot_from(
            &snapshot_addr,
            pb::InstallSnapshotFromRequest {
                source: Some(pb::install_snapshot_from_request::Source::PeerAddr(
                    cursor.replica.clone(),
                )),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        snapshot_server.abort();
        let _ = snapshot_server.await;
        let snapshot = NodeServiceImpl::open(snapshot_config, None, false).unwrap();
        use pb::node_service_server::NodeService;
        let response = snapshot
            .apply_wal_binding(tonic::Request::new(request.clone()))
            .await
            .unwrap()
            .into_inner();
        assert!(response.already_bound);
        assert_eq!(response.index_contract, binding.index_contract);
        drop(snapshot);
        server.abort();
        let _ = server.await;
        let (address, server) = common::start_opened_node(config).await;
        let mut target = NodeServiceClient::connect(address).await.unwrap();
        let response = target
            .apply_wal_binding(request.clone())
            .await
            .unwrap()
            .into_inner();
        assert!(response.already_bound);
        assert_eq!(response.index_contract, binding.index_contract);
        drop(target);
        server.abort();
        let _ = server.await;
    }
    drop(source);
    primary_server.abort();
    let _ = primary_server.await;
    std::fs::remove_dir_all(root).unwrap();
}
