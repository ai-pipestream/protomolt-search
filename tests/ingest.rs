//! In-process coverage of the write path rules: SetCalibration lifecycle,
//! AddVectors validation, Flush persistence, and lossless search over
//! ingested data.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    AddVectorsRequest, FlushRequest, GetCalibrationRequest, SetCalibrationRequest,
};

use common::{fit_calibration, unit_vectors, DIM};

async fn push_batches(
    client: &mut NodeServiceClient<tonic::transport::Channel>,
    batches: Vec<AddVectorsRequest>,
) -> Result<turbovec_search::pb::AddVectorsResponse, tonic::Status> {
    let (tx, rx) = mpsc::channel(8);
    for batch in batches {
        tx.send(batch).await.unwrap();
    }
    drop(tx);
    Ok(client
        .add_vectors(ReceiverStream::new(rx))
        .await?
        .into_inner())
}

fn batch(vectors: Vec<f32>) -> AddVectorsRequest {
    AddVectorsRequest { vectors, dim: 0 }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn calibration_lifecycle_and_ingest_rules() {
    let (addr, handle) = common::start_empty_node(NodeConfig::default()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();

    // Fresh empty node: no calibration, no dim.
    let cal = client
        .get_calibration(GetCalibrationRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cal.dim, 0);
    assert!(cal.shift.is_empty());

    // Malformed calibrations are INVALID_ARGUMENT.
    let (shift, scale) = fit_calibration(DIM, 4, &unit_vectors(2_000, DIM, 0xCA11_0001));
    let bad = client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift[..DIM - 1].to_vec(),
            scale: scale.clone(),
        })
        .await;
    assert_eq!(bad.unwrap_err().code(), tonic::Code::InvalidArgument);

    // Happy path: seed, then re-seed idempotently, then disagree.
    let first = client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!first.already_seeded);
    let second = client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(second.already_seeded);
    let conflicting = client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.iter().map(|x| x + 1.0).collect(),
            scale: scale.clone(),
        })
        .await;
    assert_eq!(conflicting.unwrap_err().code(), tonic::Code::AlreadyExists);

    // Ingest two batches: counts and first_id are exact.
    let resp = push_batches(
        &mut client,
        vec![
            batch(unit_vectors(500, DIM, 0xA000_0001)),
            batch(unit_vectors(700, DIM, 0xA000_0002)),
        ],
    )
    .await
    .unwrap();
    assert_eq!(resp.added, 1_200);
    assert_eq!(resp.total, 1_200);
    assert_eq!(resp.first_id, 0);

    // Once vectors exist, SetCalibration is locked out entirely.
    let locked = client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await;
    assert_eq!(locked.unwrap_err().code(), tonic::Code::FailedPrecondition);

    // Wrong-dim batch and wrong-dim query are INVALID_ARGUMENT.
    let wrong_dim = push_batches(
        &mut client,
        vec![AddVectorsRequest {
            vectors: unit_vectors(10, 64, 0xA000_0003),
            dim: 64,
        }],
    )
    .await;
    assert_eq!(wrong_dim.unwrap_err().code(), tonic::Code::InvalidArgument);

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unseeded_from_scratch_add_constructs_index() {
    let (addr, handle) = common::start_empty_node(NodeConfig::default()).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();

    // No dim known and none supplied: FAILED_PRECONDITION.
    let no_dim = push_batches(
        &mut client,
        vec![AddVectorsRequest {
            vectors: unit_vectors(10, DIM, 0xB0B0_0001),
            dim: 0,
        }],
    )
    .await;
    assert_eq!(no_dim.unwrap_err().code(), tonic::Code::FailedPrecondition);

    // Supplying dim constructs an unseeded index. On the
    // explicit-calibration engine nothing fits on its own: the index
    // stays uncalibrated (plain TurboQuant, order-independent by
    // construction), and TQ+ arrives only through SetCalibration
    // before the first add — which is what keeps a fleet's shards
    // comparable on purpose instead of by first-batch accident.
    let resp = push_batches(
        &mut client,
        vec![AddVectorsRequest {
            vectors: unit_vectors(2_000, DIM, 0xB0B0_0002),
            dim: DIM as u32,
        }],
    )
    .await
    .unwrap();
    assert_eq!(resp.added, 2_000);
    let cal = client
        .get_calibration(GetCalibrationRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(cal.dim as usize, DIM);
    assert_eq!(cal.num_vectors, 2_000);
    assert!(
        cal.shift.is_empty() && cal.scale.is_empty(),
        "an unseeded index stays uncalibrated; nothing fits on its own"
    );

    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_persists_and_reload_matches() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("turbovec_flush_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("shard.tv");

    let (shift, scale) = fit_calibration(DIM, 4, &unit_vectors(2_000, DIM, 0xCA11_0002));
    let vectors = unit_vectors(1_500, DIM, 0xA100_0001);

    let (addr, handle) = common::start_empty_node(NodeConfig {
        index_path: Some(path.clone()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await
        .unwrap();
    push_batches(&mut client, vec![batch(vectors.clone())])
        .await
        .unwrap();

    let flushed = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flushed.written);
    assert_eq!(flushed.num_vectors, 1_500);
    handle.abort();

    // The reloaded index must search identically to a freshly built one.
    let loaded = turbovec::TurboQuantIndex::load(&path).unwrap();
    assert_eq!(loaded.len(), 1_500);
    let query = unit_vectors(1, DIM, 0xA100_0002);
    let fresh = {
        let mut idx =
            turbovec_search::harness::seeded_index(DIM, 4, &shift, &scale);
        idx.add(&vectors);
        idx
    };
    let a = loaded.search(&query, 10);
    let b = fresh.search(&query, 10);
    assert_eq!(a.scores, b.scores);
    assert_eq!(a.indices, b.indices);

    let _ = std::fs::remove_dir_all(&dir);
}
