//! Conformance tests for the backend-neutral vector provisioning contract.

mod common;

use common::{embedded_backend_request, fit_calibration, start_empty_node, unit_vectors, DIM};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddVectorsRequest, BroadcastVectorBackendRequest, GetVectorBackendRequest,
    VectorQualityContract,
};
use pipestream_search::vector::EMBEDDED_TURBOVEC;
use pipestream_search::{node::NodeConfig, wal};

fn configured_request() -> pipestream_search::pb::ConfigureVectorBackendRequest {
    let sample = unit_vectors(512, DIM, 0xBACC_E001);
    let (shift, scale) = fit_calibration(DIM, 4, &sample);
    embedded_backend_request(DIM, 4, &shift, &scale)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn descriptor_and_opaque_config_round_trip_over_grpc() {
    let (addr, handle) = start_empty_node(Default::default()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let request = configured_request();

    let first = client
        .configure_vector_backend(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(!first.already_configured);
    let retry = client
        .configure_vector_backend(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(retry.already_configured);

    let report = client
        .get_vector_backend(GetVectorBackendRequest {})
        .await
        .unwrap()
        .into_inner();
    let descriptor = report.descriptor.unwrap();
    assert_eq!(descriptor.backend_kind, EMBEDDED_TURBOVEC);
    assert_eq!(descriptor.dim as usize, DIM);
    assert_eq!(descriptor.bits_per_dimension, 4);
    assert_eq!(
        descriptor.quality_contract,
        VectorQualityContract::ExhaustiveNativeScore as i32
    );
    assert!(!descriptor.scoring_fingerprint.is_empty());
    assert!(descriptor
        .capabilities
        .iter()
        .any(|c| c == "candidate_stream"));
    assert_eq!(report.config, request.config);

    client
        .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            vectors: unit_vectors(8, DIM, 0xBACC_E002),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();
    let error = client.configure_vector_backend(request).await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_broadcasts_provider_config_and_reports_partial_failure() {
    let (addr_a, handle_a) = start_empty_node(Default::default()).await;
    let (addr_b, handle_b) = start_empty_node(Default::default()).await;
    let (addr_c, handle_c) = start_empty_node(Default::default()).await;
    let mut occupied = NodeServiceClient::connect(addr_c.clone()).await.unwrap();
    occupied
        .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            vectors: unit_vectors(4, DIM, 0xBACC_E003),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();

    let request = configured_request();
    let coordinator = CoordinatorServiceImpl::new(vec![addr_a, addr_b, addr_c]);
    let results = coordinator
        .fanout_vector_backend(&BroadcastVectorBackendRequest {
            dim: request.dim,
            config: request.config,
        })
        .await;
    assert_eq!(results.len(), 3);
    assert!(results[0].ok && !results[0].already_configured);
    assert!(results[1].ok && !results[1].already_configured);
    assert!(!results[2].ok);

    handle_a.abort();
    handle_b.abort();
    handle_c.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn first_batch_completes_generic_wal_provider_metadata() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("pipestream_vector_manifest_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let image = dir.join("shard.vector");
    let (addr, handle) = start_empty_node(NodeConfig {
        index_path: Some(image.clone()),
        wal: true,
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    client
        .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            vectors: unit_vectors(8, DIM, 0xBACC_E004),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();
    client
        .flush(pipestream_search::pb::FlushRequest {})
        .await
        .unwrap();

    let (_, generation) = wal::latest_gen(&wal::wal_dir(&image))
        .unwrap()
        .expect("WAL generation");
    let manifest = wal::read_manifest(&generation).unwrap();
    let config = manifest.backend_config().unwrap();
    assert_eq!(config.backend_kind, EMBEDDED_TURBOVEC);
    assert!(!config.config_format.is_empty());
    assert!(!config.payload.is_empty());

    handle.abort();
    let _ = std::fs::remove_dir_all(dir);
}
