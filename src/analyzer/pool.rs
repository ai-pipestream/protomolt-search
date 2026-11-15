//! Pooled channels to remote analysis endpoints: the process-wide
//! `turbo-kserve` pool and the per-runtime tonic channel table.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, Weak};

use tonic::transport::Channel;
use tonic::Status;

use super::embed::parse_embed_endpoint;
use super::spec_convert::turbo_field_config;
use super::status::inspect_remote_client_err;
use super::RemoteCallOptions;
use crate::pb::AnalysisSpec;

type AnalysisChannels = Mutex<HashMap<String, Channel>>;
type RuntimeChannelPools = Mutex<HashMap<tokio::runtime::Id, Weak<AnalysisChannels>>>;
static CHANNEL_POOLS: OnceLock<RuntimeChannelPools> = OnceLock::new();
static TURBO_REMOTE_POOL: OnceLock<Mutex<(u64, turbo_client::RemoteChannelPool)>> = OnceLock::new();

fn shared_turbo_pool() -> turbo_client::RemoteChannelPool {
    let guard = TURBO_REMOTE_POOL
        .get_or_init(|| Mutex::new((0, turbo_client::RemoteChannelPool::new())))
        .lock()
        .expect("turbo remote channel pool poisoned");
    guard.1.clone()
}

/// Monotonic generation counter of the pooled `turbo-kserve` HTTP/2 channel
/// table, incremented whenever an unhealthy channel is evicted.
pub fn turbo_pool_generation() -> u64 {
    TURBO_REMOTE_POOL
        .get_or_init(|| Mutex::new((0, turbo_client::RemoteChannelPool::new())))
        .lock()
        .expect("turbo remote channel pool poisoned")
        .0
}

/// Evicts cached `turbo-kserve` HTTP/2 channels after a transport reset,
/// `Unavailable`, `Unknown`, or `DeadlineExceeded` error so subsequent calls
/// dial a fresh connection.
pub fn evict_unhealthy_turbo_channel(addr: &str) {
    {
        let mut guard = TURBO_REMOTE_POOL
            .get_or_init(|| Mutex::new((0, turbo_client::RemoteChannelPool::new())))
            .lock()
            .expect("turbo remote channel pool poisoned");
        guard.0 = guard.0.saturating_add(1);
        guard.1 = turbo_client::RemoteChannelPool::new();
    }
    if let Ok(pool) = runtime_channel_pool() {
        if let Ok(mut map) = pool.lock() {
            map.remove(addr);
        }
    }
}

/// Build a [`turbo_client::RemoteEndpointConfig`] honoring `options` and
/// process-wide TLS/bearer settings.
pub fn remote_endpoint_config_for(
    addr: &str,
    options: &RemoteCallOptions,
) -> Result<turbo_client::RemoteEndpointConfig, Status> {
    let tls_ref = options
        .tls
        .as_ref()
        .or_else(|| crate::security::process_client_tls());
    let endpoint_uri = if tls_ref.is_some() {
        crate::security::secure_url(addr, tls_ref)
    } else {
        addr.to_string()
    };
    let priority = if options.interactive {
        turbo_client::RemotePriority::Interactive
    } else {
        turbo_client::RemotePriority::Bulk
    };
    let mut cfg = turbo_client::RemoteEndpointConfig::new(endpoint_uri).with_priority(priority);
    if let Some(token) = options
        .bearer_token
        .clone()
        .filter(|s| !s.trim().is_empty())
        .or_else(crate::security::process_client_bearer_token)
    {
        cfg = cfg.with_bearer_token(token);
    }
    if let Some(timeout) = options.timeout {
        cfg = cfg.with_timeout(timeout);
    }
    #[cfg(feature = "tls")]
    if let Some(tls) = tls_ref {
        cfg = cfg.with_tls(turbo_client::RemoteTlsConfig {
            server_ca_pem: tls.ca_pem.clone(),
            client_identity_pem: tls.identity_pem.clone(),
            domain_name: tls.domain.clone(),
        });
    }
    #[cfg(not(feature = "tls"))]
    if tls_ref.is_some() {
        return Err(Status::failed_precondition(
            "this build has no TLS support (feature `tls` is off)",
        ));
    }
    Ok(cfg)
}

/// Build a pooled [`turbo_client::RemoteClient`] honoring `options` and
/// process-wide TLS/bearer settings.
pub fn remote_client_for(
    addr: &str,
    options: &RemoteCallOptions,
) -> Result<turbo_client::RemoteClient, Status> {
    let cfg = remote_endpoint_config_for(addr, options)?;
    Ok(turbo_client::RemoteClient::with_pool(
        cfg,
        shared_turbo_pool(),
    ))
}

/// Connects and verifies a [`turbo_client::TurboClient`] against both lexical
/// analysis and remote `turbo-kserve` `ModelMetadata`, failing loudly
/// with `FAILED_PRECONDITION` at startup/bind on any fingerprint mismatch.
pub async fn connect_verified_turbo_client(
    addr: &str,
    spec: Option<&AnalysisSpec>,
    options: &RemoteCallOptions,
    handshake: turbo_client::IndexGenerationHandshake,
) -> Result<turbo_client::TurboClient, Status> {
    let (clean_addr, model_name, _) = parse_embed_endpoint(addr);
    let endpoint_cfg = remote_endpoint_config_for(&clean_addr, options)?;
    let field_cfg = turbo_field_config(spec)?;
    let handshake_model = handshake
        .bundle
        .as_ref()
        .map(|_| (model_name, String::new()));
    turbo_client::TurboClient::builder()
        .with_default_field(field_cfg)
        .with_handshake(handshake)
        .with_remote_endpoint(endpoint_cfg, handshake_model)
        .connect()
        .await
        .map_err(|e| inspect_remote_client_err(&clean_addr, e))
}

pub(super) fn runtime_channel_pool() -> Result<Arc<AnalysisChannels>, Status> {
    let handle = tokio::runtime::Handle::try_current().map_err(|_| {
        Status::failed_precondition("analysis channels require an active Tokio runtime")
    })?;
    let registry = CHANNEL_POOLS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut pools = registry.lock().expect("channel pool registry poisoned");
    pools.retain(|_, pool| pool.strong_count() != 0);
    if let Some(pool) = pools.get(&handle.id()).and_then(Weak::upgrade) {
        return Ok(pool);
    }
    let pool = Arc::new(Mutex::new(HashMap::new()));
    pools.insert(handle.id(), Arc::downgrade(&pool));
    let owner = Arc::clone(&pool);
    handle.spawn(async move {
        std::future::pending::<()>().await;
        drop(owner);
    });
    Ok(pool)
}

/// Reuse an analysis channel within the current Tokio runtime.
pub fn shared_channel(addr: &str) -> Result<Channel, Status> {
    let pool = runtime_channel_pool()?;
    let mut channels = pool.lock().expect("analysis channel pool poisoned");
    if let Some(channel) = channels.get(addr) {
        return Ok(channel.clone());
    }
    let tls = crate::security::process_client_tls();
    let url = if tls.is_some() {
        crate::security::secure_url(addr, tls)
    } else {
        addr.to_string()
    };
    let endpoint = Channel::from_shared(url)
        .map_err(|error| {
            Status::invalid_argument(format!("bad analysis address {addr:?}: {error}"))
        })?
        .tcp_nodelay(true);
    let endpoint =
        crate::security::apply_client_tls(endpoint, tls).map_err(Status::failed_precondition)?;
    let channel = endpoint.connect_lazy();
    channels.insert(addr.to_string(), channel.clone());
    Ok(channel)
}
