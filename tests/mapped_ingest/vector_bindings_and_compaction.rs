//! Named vector binding isolation, empty-generation transfer, and compaction retention.

use super::*;

#[tokio::test]
async fn vector_names_cannot_shadow_node_columns_outside_the_mapped_plan() {
    let (analysis, mock) = start_mock_analysis().await;
    for family in 0..8 {
        let mut config = case_node_config(analysis.clone());
        let columns = match family {
            0 => &mut config.bm25_fields,
            1 => &mut config.facet_fields,
            2 => &mut config.numeric_fields,
            3 => &mut config.integer_fields,
            4 => &mut config.unsigned_integer_fields,
            5 => &mut config.map_facet_fields,
            6 => &mut config.map_numeric_fields,
            _ => &mut config.geo_fields,
        };
        columns.push("embedding".into());
        let (address, node) = start_empty_node(config).await;
        let error = send(&address, bind(), vec![]).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("vector column"),
            "family {family}: {error}"
        );
        node.abort();
        let _ = node.await;
    }
    mock.abort();
    let _ = mock.await;
}

#[tokio::test]
async fn named_vector_binding_cannot_relabel_legacy_or_populated_shards() {
    let (analysis, mock) = start_mock_analysis().await;
    for legacy in [false, true] {
        let (address, node) = start_empty_node(case_node_config(analysis.clone())).await;
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        if legacy {
            let expected = expected_binding();
            client
                .apply_wal_binding(pipestream_search::pb::ApplyWalBindingRequest {
                    plan_fingerprint: expected.plan_fingerprint,
                    body_path: expected.body_path,
                    ..Default::default()
                })
                .await
                .unwrap();
        } else {
            client
                .add_documents(tokio_stream::iter([AddDocumentsRequest {
                    text: "unmapped document".into(),
                    ..Default::default()
                }]))
                .await
                .unwrap();
        }
        expect_refusal(
            send(&address, bind(), vec![]).await,
            if legacy {
                "vector field binding"
            } else {
                "populated unbound"
            },
        );
        node.abort();
        let _ = node.await;
    }
    mock.abort();
    let _ = mock.await;
}

#[tokio::test]
async fn empty_generations_recover_and_transfer_bindings_without_source_rows() {
    use pipestream_search::{node::NodeServiceImpl, pb::node_service_server::NodeService};
    let (analysis, mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped-vector-empty-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for wal in [false, true] {
        let path = root.join(format!("segments-{wal}.tv"));
        let config = NodeConfig {
            index_path: Some(path.clone()),
            wal,
            ..case_node_config(analysis.clone())
        };
        let (address, node) = start_empty_node(config.clone()).await;
        send(&address, bind(), vec![]).await.unwrap();
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        let flushed = client
            .flush(pipestream_search::pb::FlushRequest {})
            .await
            .unwrap()
            .into_inner();
        assert!(flushed.written);
        assert_eq!(flushed.num_documents, 0);
        assert_eq!(flushed.num_vectors, 0);
        let before = pipestream_search::segments::SegmentCatalog::read_manifest(
            &pipestream_search::node::segments_root(&path),
        )
        .unwrap();
        client
            .flush(pipestream_search::pb::FlushRequest {})
            .await
            .unwrap();
        assert_eq!(
            before,
            pipestream_search::segments::SegmentCatalog::read_manifest(
                &pipestream_search::node::segments_root(&path)
            )
            .unwrap()
        );

        let target_config = NodeConfig {
            index_path: Some(root.join(format!("target-segments-{wal}.tv"))),
            wal: false,
            ..config.clone()
        };
        let (target, receiver) = start_empty_node(target_config.clone()).await;
        pipestream_search::snapshot::install_snapshot_from(
            &target,
            pipestream_search::pb::InstallSnapshotFromRequest {
                source: Some(
                    pipestream_search::pb::install_snapshot_from_request::Source::PeerAddr(address),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        node.abort();
        let _ = node.await;
        receiver.abort();
        let _ = receiver.await;

        // Neither source rows nor a provider image exist. The snapshot
        // receiver also has no WAL from which to recover the declaration.
        for recovered in [config, target_config.clone()] {
            let reopened = NodeServiceImpl::open(recovered, None).unwrap();
            let expected = expected_binding();
            let request = pipestream_search::pb::ApplyWalBindingRequest {
                plan_fingerprint: expected.plan_fingerprint,
                body_path: expected.body_path,
                vector_binding: expected.vector_binding,
                ..Default::default()
            };
            let response = reopened
                .apply_wal_binding(Request::new(request.clone()))
                .await
                .unwrap()
                .into_inner();
            assert!(response.already_bound);
            assert_eq!(response.vector_binding, request.vector_binding);
            let mut missing = request.clone();
            missing.vector_binding.clear();
            assert_eq!(
                reopened
                    .apply_wal_binding(Request::new(missing))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
            let mut invalid = request;
            invalid.vector_binding.extend([8, 1]);
            assert_eq!(
                reopened
                    .apply_wal_binding(Request::new(invalid))
                    .await
                    .unwrap_err()
                    .code(),
                tonic::Code::InvalidArgument
            );
        }
        // Metadata publication must not prevent the shard's first real
        // document from landing.
        let (target, receiver) = common::start_opened_node(target_config).await;
        seed_calibration(&target).await;
        let inserted = send(&target, bind(), vec![doc(0).encode()]).await.unwrap();
        assert_eq!(inserted.first_id, 0);
        assert_eq!(inserted.added, 1);
        let mut client = NodeServiceClient::connect(target).await.unwrap();
        client
            .flush(pipestream_search::pb::FlushRequest {})
            .await
            .unwrap();
        let health = client.health(HealthRequest {}).await.unwrap().into_inner();
        assert_eq!(health.num_vectors, 1);
        assert_eq!(health.document_slots, 1);
        receiver.abort();
        let _ = receiver.await;
    }
    mock.abort();
    let _ = mock.await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_keeps_vector_bindings_in_images_and_rewritten_logs() {
    use pipestream_search::node::segments_root;
    let (analysis, mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped-vector-compaction-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for (explicit, erase_all) in [(false, false), (true, false), (false, true), (true, true)] {
        let path = root.join(format!("segments-{explicit}-{erase_all}.tv"));
        let (address, node) = start_empty_node(NodeConfig {
            index_path: Some(path.clone()),
            wal: true,
            ..case_node_config(analysis.clone())
        })
        .await;
        seed_calibration(&address).await;
        send(
            &address,
            if explicit { explicit_bind() } else { bind() },
            (0..4).map(|i| doc(i).encode()).collect(),
        )
        .await
        .unwrap();
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        client
            .flush(pipestream_search::pb::FlushRequest {})
            .await
            .unwrap();
        // Older vector-first generations can retain this whole image
        // after rows move into segments. Compaction makes it stale.
        let set =
            pipestream_search::segments::OpenedSegmentSet::open(segments_root(&path)).unwrap();
        let first = set.metadata(0);
        std::fs::copy(
            pipestream_search::segments::SegmentCatalog::segment_dir(
                &segments_root(&path),
                &first.segment_id,
            )
            .join(&first.vector.file),
            &path,
        )
        .unwrap();
        let generation =
            pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path))
                .unwrap();
        let expected = pipestream_search::reshard::read_generation_binding(&generation)
            .unwrap()
            .unwrap();
        assert!(!expected.vector_binding.is_empty());
        client
            .delete_documents(pipestream_search::pb::DeleteDocumentsRequest {
                doc_ids: if erase_all { vec![0, 1, 2, 3] } else { vec![0] },
                ..Default::default()
            })
            .await
            .unwrap();
        let backend_before = client
            .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
            .await
            .unwrap()
            .into_inner();
        let compacted = client
            .compact_shard(pipestream_search::pb::CompactShardRequest::default())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(compacted.rows_after, if erase_all { 0 } else { 3 });
        assert_eq!(
            compacted.tombstones_reclaimed,
            if erase_all { 4 } else { 1 }
        );
        let rewritten =
            pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path))
                .unwrap();
        assert_eq!(
            pipestream_search::reshard::read_generation_binding(&rewritten).unwrap(),
            Some(expected.clone())
        );
        let set =
            pipestream_search::segments::OpenedSegmentSet::open(segments_root(&path)).unwrap();
        assert_eq!(set.is_empty(), erase_all);
        assert_eq!(set.binding(), Some(&expected));
        for part in 0..set.len() {
            assert_eq!(set.bm25(part).binding(), Some(&expected));
        }

        // Install over the real snapshot stream, then reopen without WAL:
        // only the transferred image can supply the binding on this receiver.
        let target_config = NodeConfig {
            index_path: Some(root.join(format!("snapshot-segments-{explicit}-{erase_all}.tv"))),
            wal: false,
            ..case_node_config(analysis.clone())
        };
        let (target, receiver) = start_empty_node(target_config.clone()).await;
        pipestream_search::snapshot::install_snapshot_from(
            &target,
            pipestream_search::pb::InstallSnapshotFromRequest {
                source: Some(
                    pipestream_search::pb::install_snapshot_from_request::Source::PeerAddr(
                        address.clone(),
                    ),
                ),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        receiver.abort();
        let _ = receiver.await;
        let reopened =
            pipestream_search::node::NodeServiceImpl::open(target_config.clone(), None).unwrap();
        let backend_after =
            pipestream_search::pb::node_service_server::NodeService::get_vector_backend(
                &reopened,
                Request::new(pipestream_search::pb::GetVectorBackendRequest {}),
            )
            .await
            .unwrap()
            .into_inner();
        assert_eq!(backend_after.descriptor, backend_before.descriptor);
        assert_eq!(backend_after.config, backend_before.config);
        assert_eq!(backend_after.num_vectors, if erase_all { 0 } else { 3 });
        let acknowledged =
            pipestream_search::pb::node_service_server::NodeService::apply_wal_binding(
                &reopened,
                Request::new(pipestream_search::pb::ApplyWalBindingRequest {
                    plan_fingerprint: expected.plan_fingerprint,
                    body_path: expected.body_path,
                    materialize_sha: expected.materialize_sha,
                    analysis_sha: expected.analysis_sha,
                    analysis_contract: expected.analysis_contract,
                    vector_binding: expected.vector_binding.clone(),
                    ..Default::default()
                }),
            )
            .await
            .unwrap()
            .into_inner();
        assert!(acknowledged.already_bound);
        assert_eq!(acknowledged.vector_binding, expected.vector_binding);
        drop(reopened);
        if erase_all {
            // The restored provider must accept new rows without refitting.
            // Its earlier empty image must not hide newly sealed segments
            // on the next peer snapshot install.
            let (restored, restored_handle) =
                common::start_opened_node(target_config.clone()).await;
            let inserted = send(
                &restored,
                if explicit { explicit_bind() } else { bind() },
                vec![doc(0).encode()],
            )
            .await
            .unwrap();
            assert_eq!(inserted.first_id, 0);
            let mut restored_client = NodeServiceClient::connect(restored.clone()).await.unwrap();
            restored_client
                .flush(pipestream_search::pb::FlushRequest {})
                .await
                .unwrap();
            let mut second_config = target_config;
            second_config.index_path = Some(root.join(format!("second-segments-{explicit}.tv")));
            let (second, second_handle) = start_empty_node(second_config).await;
            let installed = pipestream_search::snapshot::install_snapshot_from(
                &second,
                pipestream_search::pb::InstallSnapshotFromRequest {
                    source: Some(
                        pipestream_search::pb::install_snapshot_from_request::Source::PeerAddr(
                            restored,
                        ),
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(installed.num_vectors, 1);
            assert_eq!(installed.num_documents, 1);
            let mut second_client = NodeServiceClient::connect(second).await.unwrap();
            let backend = second_client
                .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
                .await
                .unwrap()
                .into_inner();
            assert_eq!(backend.num_vectors, 1);
            assert_eq!(backend.config, backend_before.config);
            second_handle.abort();
            restored_handle.abort();
            let _ = second_handle.await;
            let _ = restored_handle.await;
        }
        node.abort();
        let _ = node.await;
    }
    mock.abort();
    let _ = mock.await;
    std::fs::remove_dir_all(root).unwrap();
}
