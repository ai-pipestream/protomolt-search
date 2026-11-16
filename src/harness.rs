//! Shared harness: deterministic corpus generation, calibration fitting,
//! shard partitioning, and loopback server startup.
//!
//! Used by the integration tests and the `sweep` benchmark binary. Also
//! usable for real deployments: [`write_shards`] persists sharded,
//! uniformly configured vector images that the search binary loads
//! via `[[shards]]` entries in the cluster config.

use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Error as TransportError, Server};

use crate::coordinator::CoordinatorServiceImpl;
use crate::node::{NodeConfig, NodeServiceImpl};
use crate::pb::{
    BroadcastVectorBackendRequest, ConfigureVectorBackendRequest, GetVectorBackendResponse,
    VectorBackendConfig as WireVectorBackendConfig,
};
use crate::vector::{
    embedded_turbovec_config, legacy_calibration_config, VectorBackendConfig, VectorIndex,
    EMBEDDED_TURBOVEC,
};
use crate::MAX_MESSAGE_BYTES;

/// Deterministic pseudo-random unit vectors (LCG + L2 normalize), same
/// generator style as the turbovec test suite.
pub fn unit_vectors(n: usize, dim: usize, seed: u64) -> Vec<f32> {
    let mut s = seed.wrapping_add(0x9E3779B97F4A7C15);
    let mut out = vec![0.0f32; n * dim];
    for row in out.chunks_mut(dim) {
        let mut norm = 0.0f64;
        for x in row.iter_mut() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = ((s >> 33) as f64 / (1u64 << 31) as f64) - 1.0;
            *x = v as f32;
            norm += v * v;
        }
        let inv = 1.0 / (norm.sqrt() + 1e-9);
        for x in row.iter_mut() {
            *x = (*x as f64 * inv) as f32;
        }
    }
    out
}

/// Fit a TQ+ calibration on a representative sample: calibrate a
/// throwaway index with it (upstream's explicit `calibrate`, whose fit
/// is deterministic in the sample) and read out the committed pair.
///
/// Sample QUALITY is the caller's responsibility, per upstream's
/// design: ~1024 uniformly random rows lands within half a point of a
/// full-corpus fit, while the same count taken as a sorted prefix is
/// catastrophically biased. There is no warm-up threshold anymore —
/// a tiny sample fits (deterministically), it just fits noisily.
pub fn fit_calibration(dim: usize, bit_width: usize, sample: &[f32]) -> (Vec<f32>, Vec<f32>) {
    let config = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, dim, bit_width, sample)
        .expect("calibration sample must be non-empty finite rows");
    let legacy = legacy_calibration_config(&config)
        .expect("fitted backend config must decode")
        .expect("embedded backend exposes the legacy calibration view");
    (legacy.shift, legacy.scale)
}

/// An empty index committed to an externally fitted calibration pair:
/// upstream's `from_parts` with zero rows. Every index built from the
/// same pair encodes a given vector byte-identically regardless of
/// build history, which is what makes per-shard scores mergeable into
/// an exact global top-k.
pub fn seeded_index(dim: usize, bit_width: usize, shift: &[f32], scale: &[f32]) -> VectorIndex {
    let config = embedded_turbovec_config(bit_width, shift, scale)
        .expect("a fitted calibration pair is valid backend state");
    VectorIndex::from_backend_config(dim, &config)
        .expect("a fitted calibration pair is valid backend state")
}

/// Wire request that configures one shard with the shipped embedded provider
/// and an explicit calibration pair.
pub fn embedded_backend_request(
    dim: usize,
    bits_per_dimension: usize,
    shift: &[f32],
    scale: &[f32],
) -> ConfigureVectorBackendRequest {
    let config = embedded_turbovec_config(bits_per_dimension, shift, scale)
        .expect("test calibration is valid provider state");
    ConfigureVectorBackendRequest {
        dim: dim as u32,
        config: Some(WireVectorBackendConfig {
            backend_kind: config.backend_kind,
            config_format: config.config_format,
            payload: config.payload,
        }),
    }
}

/// The embedded-provider calibration pair a shard reports through
/// `GetVectorBackend`: empty when the shard has no index or its index has no
/// locked calibration.
pub fn reported_calibration(response: &GetVectorBackendResponse) -> (Vec<f32>, Vec<f32>) {
    let Some(config) = response.config.as_ref() else {
        return (Vec::new(), Vec::new());
    };
    let config = VectorBackendConfig {
        backend_kind: config.backend_kind.clone(),
        config_format: config.config_format.clone(),
        payload: config.payload.clone(),
    };
    legacy_calibration_config(&config)
        .expect("reported backend config must decode")
        .map_or_else(|| (Vec::new(), Vec::new()), |view| (view.shift, view.scale))
}

/// [`embedded_backend_request`] for every shard of `collection` (empty names
/// the coordinator's default dataset).
pub fn embedded_broadcast_request(
    dim: usize,
    bits_per_dimension: usize,
    shift: &[f32],
    scale: &[f32],
    collection: impl Into<String>,
) -> BroadcastVectorBackendRequest {
    let request = embedded_backend_request(dim, bits_per_dimension, shift, scale);
    BroadcastVectorBackendRequest {
        dim: request.dim,
        config: request.config,
        collection: collection.into(),
    }
}

/// One shard's index plus its global id base (the corpus offset of its
/// first vector; partitions are contiguous ranges).
pub struct Shard {
    pub index: VectorIndex,
    pub slot_offset: u64,
}

/// Build `n_shards` indexes over contiguous, disjoint partitions of
/// `corpus`, all committed to the same calibration — the property that
/// makes their scores mutually comparable.
///
/// Cut points are free: with one explicit global pair, codes are a
/// pure function of (row, pair), so ANY partition of the corpus is
/// bitwise consistent with the monolithic build. (The block-aligned
/// cuts the per-block-calibration chain required are gone with it.)
pub fn build_shards(
    corpus: &[f32],
    dim: usize,
    bit_width: usize,
    n_shards: usize,
    shift: &[f32],
    scale: &[f32],
) -> Vec<Shard> {
    let n = corpus.len() / dim;
    let cut = |i: usize| -> usize {
        if i >= n_shards {
            n
        } else {
            i * n / n_shards
        }
    };
    (0..n_shards)
        .map(|i| {
            let start = cut(i);
            let end = cut(i + 1).max(start);
            let mut index = seeded_index(dim, bit_width, shift, scale);
            index
                .add(&corpus[start * dim..end * dim], dim)
                .expect("generated vectors are valid");
            index.prepare().expect("backend prepare succeeds");
            Shard {
                index,
                slot_offset: start as u64,
            }
        })
        .collect()
}

/// The single-index reference: one index over the whole corpus, same
/// calibration.
pub fn build_monolithic(
    corpus: &[f32],
    dim: usize,
    bit_width: usize,
    shift: &[f32],
    scale: &[f32],
) -> VectorIndex {
    let mut index = seeded_index(dim, bit_width, shift, scale);
    index.add(corpus, dim).expect("generated vectors are valid");
    index.prepare().expect("backend prepare succeeds");
    index
}

/// Persist provider images as `<dir>/shard-<i>.vector` and print the
/// matching `[[shards]]` config entries (listen ports starting at
/// `base_port`, offsets from the partition layout) so a static cluster
/// config can be assembled by hand.
pub fn write_shards(
    shards: &[Shard],
    dir: &Path,
    base_port: u16,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    std::fs::create_dir_all(dir)?;
    let mut paths = Vec::with_capacity(shards.len());
    for (i, shard) in shards.iter().enumerate() {
        let path = dir.join(format!("shard-{i}.vector"));
        shard.index.write(&path).map_err(std::io::Error::other)?;
        println!(
            "[[shards]]\nlisten = \"0.0.0.0:{}\"\nindex = \"{}\"\nslot_offset = {}\n",
            base_port + i as u16,
            path.display(),
            shard.slot_offset
        );
        paths.push(path);
    }
    Ok(paths)
}

/// Accept stream for a tonic server with TCP_NODELAY set on every socket.
///
/// Without this, small gRPC writes (a shard's FloorUpdates, small Done
/// messages) can stall ~40ms on the Nagle/delayed-ACK interaction, which
/// dominates query latency on loopback and hurts on real networks too.
pub fn nodelay_incoming(
    listener: TcpListener,
) -> impl tokio_stream::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    use tokio_stream::StreamExt;
    TcpListenerStream::new(listener).map(|accepted| {
        accepted.inspect(|stream| {
            let _ = stream.set_nodelay(true);
        })
    })
}

/// Start a node server over a prebuilt index on 127.0.0.1:0. Returns its
/// `http://` address and the server task (abort to stop).
pub async fn start_node(
    index: VectorIndex,
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(Some(index), config, None).await
}

/// Start a node server with NO index (the from-scratch state: awaiting
/// ConfigureVectorBackend or AddVectors).
pub async fn start_empty_node(
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(None, config, None).await
}

/// Reopen a persisted shard through [`NodeServiceImpl::open`] — the
/// recovery path the serving binary and the embedded runtime take — and
/// serve it. `start_empty_node` constructs a fresh shard instead.
pub async fn start_opened_node(
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    let node = NodeServiceImpl::open(config, None).expect("reopen the shard");
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    node.spawn_floor_listener(addr);
    let diagnostics = node.diagnostics_server(MAX_MESSAGE_BYTES);
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, MAX_MESSAGE_BYTES))
            .add_service(diagnostics)
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}

/// Start an empty node with the product-owned phrase vocabulary attached.
pub async fn start_empty_phrase_node(
    config: NodeConfig,
    phrases: std::sync::Arc<crate::phrases::PhraseIndex>,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    start_node_inner(None, config, Some(phrases)).await
}

async fn start_node_inner(
    index: Option<VectorIndex>,
    config: NodeConfig,
    phrases: Option<std::sync::Arc<crate::phrases::PhraseIndex>>,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    serve_node(NodeServiceImpl::new(index, config).with_phrase_index(phrases)).await
}

/// Serve an already-built node service on 127.0.0.1:0. Returns its
/// `http://` address and the server task (abort to stop).
pub async fn serve_node(node: NodeServiceImpl) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    // The UDP floor lane shares the gRPC listener's host:port.
    node.spawn_floor_listener(addr);
    let diagnostics = node.diagnostics_server(MAX_MESSAGE_BYTES);
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, MAX_MESSAGE_BYTES))
            .add_service(diagnostics)
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}

/// Start a relay coordinator over
/// `children` on 127.0.0.1:0, its parent-facing UDP lane on the same
/// port, and return the relay handle beside the address so a test can
/// read its token registry. The children are not checked here: a test
/// that wants the startup refusal calls `check_children` itself.
pub async fn start_relay(
    children: Vec<String>,
) -> (
    String,
    crate::relay::RelayService,
    JoinHandle<Result<(), TransportError>>,
) {
    start_relay_over(CoordinatorServiceImpl::new(children)).await
}

/// [`start_relay`] over a coordinator the caller built, for a hot
/// topology the test then republishes.
pub async fn start_relay_over(
    coordinator: CoordinatorServiceImpl,
) -> (
    String,
    crate::relay::RelayService,
    JoinHandle<Result<(), TransportError>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let relay = crate::relay::RelayService::new(std::sync::Arc::new(coordinator));
    relay.spawn_floor_listener(addr);
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(relay.clone().into_server(MAX_MESSAGE_BYTES))
            .add_service(relay.diagnostics_server(MAX_MESSAGE_BYTES))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), relay, handle)
}

/// Start a coordinator server on 127.0.0.1:0.
pub async fn start_coordinator(
    node_addrs: Vec<String>,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(CoordinatorServiceImpl::into_server(
                CoordinatorServiceImpl::new(node_addrs),
                MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    (format!("http://{addr}"), handle)
}

// ---------------------------------------------------------------------------
// Mock analysis sidecar (test/dev fallback)
// ---------------------------------------------------------------------------

/// A mock `turbo.result.v1.AnalysisService`: deterministic, model-free
/// analysis with the same contract as `turbo-kserve` — whitespace tokens
/// with original-text UTF-8 spans, term vectors in first-occurrence order,
/// and a toy stemmer for the `SOURCE_STEMS` path. Used by the test suite
/// and as a deterministic loopback server when a model bundle is not needed.
pub mod mock_analysis;

/// Spawn a `turbo-kserve` analysis server binary on `port` and wait for its
/// listener. Returns the child (kill on drop) and its `http://` address.
pub fn start_sidecar(binary: &str, port: u16) -> Result<(Child, String), String> {
    start_sidecar_with_env(binary, port, &[])
}

/// Like [`start_sidecar`], with extra environment variables (for
/// example `TURBO_EMBED_MODEL` or bundle paths).
pub fn start_sidecar_with_env(
    binary: &str,
    port: u16,
    envs: &[(&str, &str)],
) -> Result<(Child, String), String> {
    if !Path::new(binary).exists() {
        return Err(format!("sidecar binary not found at {binary}"));
    }
    let mut command = Command::new(binary);
    command
        .env("PORT", port.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for (key, value) in envs {
        command.env(key, value);
    }
    let mut child = command.spawn().map_err(|e| format!("spawn sidecar: {e}"))?;
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return Ok((child, format!("http://127.0.0.1:{port}")));
        }
        if let Some(status) = child.try_wait().map_err(|e| e.to_string())? {
            return Err(format!("sidecar exited early: {status}"));
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    Err("sidecar never opened its port in 30s".to_string())
}

/// A vector provider that advertises a configured ANN contract while
/// scoring through a real exhaustive image underneath. Tests use it to
/// exercise every AUTO/ANN rule the product has without a second
/// backend: the results are exact, the contract says approximate, and
/// the coordinator must believe the contract.
pub mod fake_ann;
