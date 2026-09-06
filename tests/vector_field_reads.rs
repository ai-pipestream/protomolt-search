mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    mapping::derive_plan,
    node::{Layout, NodeConfig},
    pb::{node_service_client::NodeServiceClient, *},
    visibility::VisibilityScope,
};
use prost::Message;
use tonic::Code;

fn binding() -> MappedVectorBinding {
    derive_plan(
        include_bytes!("fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap()
    .vector_binding
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn node_vector_reads_bind_fields_filter_before_scoring_and_fence_even_empty_results() {
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("vector-field-reads-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let binding = binding();
    let vectors = common::unit_vectors(4, 16, 43021);
    let query = vectors[..16].to_vec();
    let (shift, scale) = common::fit_calibration(16, 4, &vectors);
    let visibility = DocumentVisibility {
        filter: pipestream_search::cel::compile_filter("audience == 'public'").unwrap(),
    };
    let scope = VisibilityScope::new(Some(&visibility)).unwrap();
    for layout in [Layout::SingleImage, Layout::Segments] {
        let (address, handle) = common::start_empty_node(NodeConfig {
            index_path: Some(root.join(format!("{layout:?}.tv"))),
            layout,
            slot_offset: 100,
            facet_fields: vec!["audience".into()],
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(address).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: 16,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        client
            .apply_wal_binding(ApplyWalBindingRequest {
                body_path: "body".into(),
                plan_fingerprint: binding.plan_fingerprint.clone(),
                vector_binding: binding.encode_to_vec(),
                ..Default::default()
            })
            .await
            .unwrap();
        client.flush(FlushRequest {}).await.unwrap();
        for row in 0..3 {
            client
                .add_documents(tokio_stream::iter([AddDocumentsRequest {
                    text: "word".into(),
                    analysis: Some(body_spec()),
                    facets: vec![FacetValue {
                        field: "audience".into(),
                        value: if row == 1 { "private" } else { "public" }.into(),
                    }],
                    ..Default::default()
                }]))
                .await
                .unwrap();
        }
        // The last vector has no document metadata; a restricted view cannot
        // acquire it even though the provider owns its slot.
        client
            .add_vectors(tokio_stream::iter([AddVectorsRequest {
                dim: 16,
                vectors: vectors.clone(),
            }]))
            .await
            .unwrap();
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![102],
                ..Default::default()
            })
            .await
            .unwrap();
        let membership = client
            .resolve_vector_bitmap(VectorBitmapRequest {
                field: "semantic".into(),
                visibility: Some(visibility.clone()),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(membership.vector_binding.as_ref(), Some(&binding));
        assert_eq!(membership.bits, vec![1]);
        let ids = vec![99, 100, 101, 102, 103, 100];
        let native = VectorRescoreRequest {
            vector: query.clone(),
            candidate_ids: ids.clone(),
            field: "semantic".into(),
            visibility: Some(visibility.clone()),
            expected_stats_epoch: membership.stats_epoch,
            expected_stats_incarnation: membership.stats_incarnation.clone(),
        };
        let exact = ExactVectorRescoreRequest {
            vector: query.clone(),
            candidate_ids: ids,
            field: "semantic".into(),
            visibility: Some(visibility.clone()),
            expected_stats_epoch: membership.stats_epoch,
            expected_stats_incarnation: membership.stats_incarnation.clone(),
            max_logical_bytes: 64,
        };
        let scored = client
            .vector_rescore(native.clone())
            .await
            .unwrap()
            .into_inner();
        let fp32 = client
            .exact_vector_rescore(exact.clone())
            .await
            .unwrap()
            .into_inner();
        for (hits, held, epoch, incarnation, fingerprint, known) in [
            (
                &scored.hits,
                &scored.vector_binding,
                scored.stats_epoch,
                &scored.stats_incarnation,
                &scored.visibility_fingerprint,
                &scored.visibility_columns_known,
            ),
            (
                &fp32.hits,
                &fp32.vector_binding,
                fp32.stats_epoch,
                &fp32.stats_incarnation,
                &fp32.visibility_fingerprint,
                &fp32.visibility_columns_known,
            ),
        ] {
            assert_eq!(
                hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                vec![100]
            );
            assert_eq!(held.as_ref(), Some(&binding));
            assert_eq!(epoch, membership.stats_epoch);
            assert_eq!(incarnation, &membership.stats_incarnation);
            scope.validate_echo(fingerprint, known).unwrap();
            assert_eq!(known, &vec![true]);
        }
        assert_eq!(fp32.logical_bytes, 64);
        let unrestricted = client
            .vector_rescore(VectorRescoreRequest {
                visibility: None,
                ..native.clone()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            unrestricted
                .hits
                .iter()
                .find(|hit| hit.doc_id == 100)
                .unwrap()
                .score
                .to_bits(),
            scored.hits[0].score.to_bits()
        );
        assert_eq!(unrestricted.hits.len(), 3);
        for field in ["body", "signal", "missing"] {
            assert_eq!(
                client
                    .resolve_vector_bitmap(VectorBitmapRequest {
                        field: field.into(),
                        visibility: None
                    })
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
            assert_eq!(
                client
                    .vector_rescore(VectorRescoreRequest {
                        field: field.into(),
                        candidate_ids: vec![],
                        ..native.clone()
                    })
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
            assert_eq!(
                client
                    .exact_vector_rescore(ExactVectorRescoreRequest {
                        field: field.into(),
                        candidate_ids: vec![],
                        ..exact.clone()
                    })
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
        }
        let empty = client
            .exact_vector_rescore(ExactVectorRescoreRequest {
                candidate_ids: vec![101, 102, 103],
                ..exact.clone()
            })
            .await
            .unwrap()
            .into_inner();
        assert!(empty.hits.is_empty());
        assert_eq!(empty.vector_binding.as_ref(), Some(&binding));
        assert_eq!(empty.stats_epoch, membership.stats_epoch);
        scope
            .validate_echo(
                &empty.visibility_fingerprint,
                &empty.visibility_columns_known,
            )
            .unwrap();
        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![100],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(
            client
                .vector_rescore(VectorRescoreRequest {
                    candidate_ids: vec![],
                    ..native
                })
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        assert_eq!(
            client
                .exact_vector_rescore(ExactVectorRescoreRequest {
                    candidate_ids: vec![],
                    ..exact
                })
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition
        );
        handle.abort();
        let _ = handle.await;
    }
    std::fs::remove_dir_all(root).unwrap();
}
