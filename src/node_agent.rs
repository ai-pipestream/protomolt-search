//! Node membership in the control plane (`docs/cluster-control.md`,
//! "Node lifecycle"): register, renew the lease, report every served
//! shard, and run the worker that executes the plan's actions assigned
//! to this node — `COPY_REPLICA` bootstraps a replica under `--data-dir`
//! from the primary's `StreamSnapshot` and catches its WAL tail up with
//! `replication::sync_once`; `DROP_REPLICA` removes a retired copy.
//!
//! One agent serves one collection: the shards it reports and the plane
//! it talks to belong to that collection. The serving binary starts one
//! per collection its shards name.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::Notify;
use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::node::{NodeConfig, NodeServiceImpl};
use crate::pb::cluster_control_client::ClusterControlClient;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    ClusterPlan, CompletePlacementActionRequest, GetClusterPlanRequest, HealthRequest,
    HealthResponse, InstallSnapshotFromRequest, NodeCapacity, NodeLease, PlacementAction,
    PlacementActionKind, RegisterNodeRequest, RenewNodeLeaseRequest, ReportShardRequest,
    ShardReplicaRole, ShardReplicaState,
};
use crate::replication::{sync_once, ReplicaCursor};

/// What a node needs to be a member (`--node-id`, `--control-addr`,
/// `--failure-domain`, `--data-dir`, and the listener knobs).
#[derive(Clone)]
pub struct NodeAgentConfig {
    pub node_id: String,
    /// The coordinator's ClusterControl endpoint (`http(s)://host:port`).
    pub control_addr: String,
    /// The collection this agent's shards belong to; empty for the
    /// unnamed dataset.
    pub collection: String,
    pub failure_domain: String,
    /// Where dynamically placed shards live: `<data-dir>/<shard_id>/`.
    pub data_dir: PathBuf,
    /// The address registered for the node.
    pub node_addr: String,
    /// The host part of every listener address this node advertises.
    pub advertise_host: String,
    /// Where replica listeners bind: the interface, and the first port
    /// (0 lets the OS choose; a bound port is remembered per shard).
    pub replica_listen: SocketAddr,
    /// Requested lease; 0 takes the plane's policy.
    pub lease_ms: u64,
    /// Shard report timer.
    pub report_ms: u64,
    /// Worker interval: how often the plan is read for assigned actions.
    pub reconcile_ms: u64,
    /// A replica is caught up when the primary's WAL watermark is at most
    /// this many clocks ahead of the replica's cursor.
    pub lag_bound: u64,
    /// The thread count reported as search capacity (0: half the cores).
    pub scan_parallel: usize,
    /// The configuration a NEW dynamically placed shard is opened with;
    /// `index_path`, `slot_offset`, `collection`, and `wal` are set per
    /// shard.
    pub template: NodeConfig,
    pub phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    pub allow_missing_bm25: bool,
    /// Listener TLS for placed replicas (the node listeners' material).
    pub tls: Option<crate::security::ServerTls>,
    pub max_message_bytes: usize,
}

/// The durable record of a dynamically placed shard
/// (`<data-dir>/<shard_id>/placed.toml`): what the plan said when it was
/// placed, the listener port it bound, and how far its bootstrap got —
/// so a restart resumes rather than repeats.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct PlacedShard {
    pub shard_id: String,
    pub collection: String,
    pub slot_offset: u64,
    pub hash_lo: u64,
    pub hash_hi: u64,
    pub port: u16,
    /// The shard generation the plan assigned this copy (the action's
    /// `target_generation`): what the copy reports. The WAL cursor below
    /// is a different clock, the source's log generation.
    pub source_generation: u64,
    /// The image is installed; a retry after a crash before this point
    /// installs again.
    pub installed: bool,
    /// The replication cursor after the last successful catch-up.
    pub cursor_generation: u64,
    pub cursor_clock: u64,
    /// Caught up within the lag bound at least once: reported ready.
    pub ready: bool,
    /// The action whose completion the plane acknowledged, when one did.
    pub completed_action: u64,
}

impl PlacedShard {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join("placed.toml")
    }

    pub fn load(dir: &Path) -> Result<Option<Self>, String> {
        let path = Self::path(dir);
        if !path.exists() {
            return Ok(None);
        }
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        toml::from_str(&text)
            .map(Some)
            .map_err(|e| format!("parse {}: {e}", path.display()))
    }

    pub fn write(&self, dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        let path = Self::path(dir);
        let tmp = dir.join("placed.toml.tmp");
        let text = toml::to_string(self).map_err(|e| format!("encode placed shard: {e}"))?;
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&tmp)
                .map_err(|e| format!("create {}: {e}", tmp.display()))?;
            file.write_all(text.as_bytes())
                .and_then(|()| file.sync_all())
                .map_err(|e| format!("write {}: {e}", tmp.display()))?;
        }
        std::fs::rename(&tmp, &path).map_err(|e| format!("publish {}: {e}", path.display()))?;
        crate::postings::fsync_parent(&path).map_err(|e| format!("fsync {}: {e}", path.display()))
    }
}

/// One shard this node serves and reports.
/// One child of a split, as chosen when the split started.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitChild {
    pub shard_id: String,
    pub hash_lo: u64,
    pub hash_hi: u64,
    pub slot_offset: u64,
}

/// Durable progress of one `SPLIT_SHARD` action
/// (`<data-dir>/<shard_id>.split/split.toml`): the children and their
/// ranges are fixed when the split starts, so a retry after a crash
/// resumes the same split rather than starting another.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SplitState {
    pub action_id: u64,
    pub shard_id: String,
    pub source_generation: u64,
    pub target_generation: u64,
    pub children: Vec<SplitChild>,
    /// The baseline images are built and the children placed.
    pub built: bool,
    /// The live tail cursor into the children.
    pub live: Option<crate::replication::LiveReshardState>,
    /// The source's ingest is fenced (re-applied after a restart).
    pub fenced: bool,
    pub completed: bool,
}

impl SplitState {
    pub fn path(dir: &Path) -> PathBuf {
        dir.join("split.toml")
    }

    pub fn load(dir: &Path) -> Result<Option<Self>, String> {
        let path = Self::path(dir);
        if !path.exists() {
            return Ok(None);
        }
        let text =
            std::fs::read_to_string(&path).map_err(|e| format!("read {}: {e}", path.display()))?;
        toml::from_str(&text)
            .map(Some)
            .map_err(|e| format!("parse {}: {e}", path.display()))
    }

    pub fn write(&self, dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        let text = toml::to_string(self).map_err(|e| format!("encode split state: {e}"))?;
        let tmp = dir.join("split.toml.tmp");
        std::fs::write(&tmp, text).map_err(|e| format!("write {}: {e}", tmp.display()))?;
        let path = Self::path(dir);
        std::fs::rename(&tmp, &path).map_err(|e| format!("publish {}: {e}", path.display()))?;
        crate::postings::fsync_parent(&path).map_err(|e| format!("fsync {}: {e}", path.display()))
    }
}

/// The marker a split leaves for a configured source it retired.
fn retired_marker(data_dir: &Path, shard_id: &str) -> PathBuf {
    data_dir.join("retired").join(shard_id)
}

/// Move one built image file into a placed shard's directory: a rename
/// on the same filesystem, a copy across.
fn move_image_file(from: &Path, to: &Path) -> Result<(), String> {
    if let Some(parent) = to.parent() {
        std::fs::create_dir_all(parent).map_err(|e| format!("create {}: {e}", parent.display()))?;
    }
    if std::fs::rename(from, to).is_ok() {
        return Ok(());
    }
    std::fs::copy(from, to)
        .map_err(|e| format!("copy {} to {}: {e}", from.display(), to.display()))?;
    std::fs::remove_file(from).map_err(|e| format!("remove {}: {e}", from.display()))
}

pub struct ServedShard {
    pub shard_id: String,
    pub node: NodeServiceImpl,
    /// The listener address, `http(s)://host:port`.
    pub addr: String,
    /// The hash range from the configuration, when given.
    pub hash_range: Option<(u64, u64)>,
    /// `Some` for a shard the worker placed under `--data-dir`.
    pub placed: Option<PlacedShard>,
    /// The server task of a placed shard (a configured shard's server
    /// belongs to the binary).
    server: Option<tokio::task::JoinHandle<()>>,
}

impl ServedShard {
    /// A shard from the process configuration.
    pub fn configured(
        shard_id: impl Into<String>,
        node: NodeServiceImpl,
        addr: impl Into<String>,
        hash_range: Option<(u64, u64)>,
    ) -> Self {
        ServedShard {
            shard_id: shard_id.into(),
            node,
            addr: crate::config::normalize_addr(addr.into()),
            hash_range,
            placed: None,
            server: None,
        }
    }
}

/// Counters a test or an operator reads to see what the agent did.
#[derive(Default)]
pub struct AgentStats {
    pub registrations: AtomicU64,
    pub renewals: AtomicU64,
    pub reports: AtomicU64,
    pub installs: AtomicU64,
    pub copies_completed: AtomicU64,
    /// Completions the worker held back because the source had moved
    /// since the last catch-up: it synced again first.
    pub stale_resyncs: AtomicU64,
    /// Completions the plane refused because its record of the source
    /// differed from what the copy had; retried on a later tick.
    pub completion_refusals: AtomicU64,
    pub drops_completed: AtomicU64,
    /// Splits this worker built, caught up, fenced, and completed.
    pub splits_completed: AtomicU64,
    pub unhandled_actions: AtomicU64,
}

/// A hook a test installs to interleave work with the worker.
pub type Hook = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

struct Inner {
    config: NodeAgentConfig,
    shards: Mutex<BTreeMap<String, ServedShard>>,
    lease: Mutex<Option<NodeLease>>,
    control: Mutex<Option<ClusterControlClient<Channel>>>,
    stats: AgentStats,
    flush_notify: Arc<Notify>,
    /// Actions logged as unhandled, by id, so the log says it once.
    unhandled: Mutex<BTreeSet<u64>>,
    /// Shards whose hash range is unknown, logged once each.
    unranged: Mutex<BTreeSet<String>>,
    /// Awaited right before a COPY_REPLICA completion is sent.
    before_complete: Mutex<Option<Hook>>,
}

/// The node's membership: lease, reports, and the worker.
#[derive(Clone)]
pub struct NodeAgent {
    inner: Arc<Inner>,
}

/// How many catch-up rounds a bootstrap runs before it gives the tick
/// back (the next tick continues from the persisted cursor).
const CATCH_UP_ROUNDS: u32 = 64;
/// Completion attempts per tick after the copy is caught up.
const COMPLETION_ROUNDS: u32 = 4;

impl NodeAgent {
    /// An agent over the configured shards. Placed shards under
    /// `data_dir` are reopened and served by [`Self::open_placed`].
    pub fn new(config: NodeAgentConfig, shards: Vec<ServedShard>) -> Self {
        let flush_notify = Arc::new(Notify::new());
        let shards = shards
            .into_iter()
            .filter(|shard| {
                // A configured shard a split retired stays retired across
                // a restart: its rows live in its children now, and a
                // report of it would re-register a range the children
                // already tile.
                let retired = retired_marker(&config.data_dir, &shard.shard_id).exists();
                if retired {
                    eprintln!(
                        "node {:?}: shard {:?} was retired by a split; not served or reported \
                         (remove it from the configuration and {} to forget it)",
                        config.node_id,
                        shard.shard_id,
                        retired_marker(&config.data_dir, &shard.shard_id).display()
                    );
                }
                !retired
            })
            .map(|shard| (shard.shard_id.clone(), shard))
            .collect();
        NodeAgent {
            inner: Arc::new(Inner {
                config,
                shards: Mutex::new(shards),
                lease: Mutex::new(None),
                control: Mutex::new(None),
                stats: AgentStats::default(),
                flush_notify,
                unhandled: Mutex::new(BTreeSet::new()),
                unranged: Mutex::new(BTreeSet::new()),
                before_complete: Mutex::new(None),
            }),
        }
    }

    /// The notifier configured shards should wake after a flush
    /// (`NodeServiceImpl::with_flush_notify`).
    pub fn flush_notify(&self) -> Arc<Notify> {
        Arc::clone(&self.inner.flush_notify)
    }

    pub fn stats(&self) -> &AgentStats {
        &self.inner.stats
    }

    pub fn node_id(&self) -> &str {
        &self.inner.config.node_id
    }

    /// Install a hook awaited right before each COPY_REPLICA completion.
    pub fn set_before_complete(&self, hook: Option<Hook>) {
        *self.inner.before_complete.lock().expect("hook lock") = hook;
    }

    /// The current lease, when registered.
    pub fn lease(&self) -> Option<NodeLease> {
        self.inner.lease.lock().expect("lease lock").clone()
    }

    /// The listener address of a served shard.
    pub fn shard_addr(&self, shard_id: &str) -> Option<String> {
        self.inner
            .shards
            .lock()
            .expect("shards lock")
            .get(shard_id)
            .map(|shard| shard.addr.clone())
    }

    /// The durable record of a placed shard.
    pub fn placed(&self, shard_id: &str) -> Option<PlacedShard> {
        self.inner
            .shards
            .lock()
            .expect("shards lock")
            .get(shard_id)
            .and_then(|shard| shard.placed.clone())
    }

    /// Served shard ids.
    pub fn shard_ids(&self) -> Vec<String> {
        self.inner
            .shards
            .lock()
            .expect("shards lock")
            .keys()
            .cloned()
            .collect()
    }

    fn shard_dir(&self, shard_id: &str) -> PathBuf {
        self.inner.config.data_dir.join(shard_id)
    }

    /// Reopen and serve every shard the worker placed under the data
    /// directory before (a restart resumes them where they were).
    pub async fn open_placed(&self) -> Result<Vec<String>, String> {
        let data_dir = &self.inner.config.data_dir;
        std::fs::create_dir_all(data_dir)
            .map_err(|e| format!("create data dir {}: {e}", data_dir.display()))?;
        let mut opened = Vec::new();
        let mut entries: Vec<PathBuf> = std::fs::read_dir(data_dir)
            .map_err(|e| format!("read data dir {}: {e}", data_dir.display()))?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| PlacedShard::path(path).exists())
            .collect();
        entries.sort();
        for dir in entries {
            let placed = PlacedShard::load(&dir)?.expect("checked above");
            if placed.collection != self.inner.config.collection {
                continue;
            }
            self.serve_placed(placed).await?;
            opened.push(
                dir.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            );
        }
        Ok(opened)
    }

    /// Open (or create) the placed shard's files and put them on a
    /// listener of their own; idempotent for a shard already served.
    async fn serve_placed(&self, mut placed: PlacedShard) -> Result<String, String> {
        if let Some(shard) = self
            .inner
            .shards
            .lock()
            .expect("shards lock")
            .get(&placed.shard_id)
        {
            if shard.placed.is_none() {
                return Err(format!(
                    "shard {:?} is configured statically on node {:?}; the plan cannot place \
                     a copy of it here",
                    placed.shard_id, self.inner.config.node_id
                ));
            }
            return Ok(shard.addr.clone());
        }
        let dir = self.shard_dir(&placed.shard_id);
        let mut config = self.inner.config.template.clone();
        config.index_path = Some(dir.join("shard"));
        config.slot_offset = placed.slot_offset;
        config.collection = placed.collection.clone();
        config.wal = true;
        std::fs::create_dir_all(&dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        let node = NodeServiceImpl::open(
            config,
            self.inner.config.phrase_index.clone(),
            self.inner.config.allow_missing_bm25,
        )?
        .with_flush_notify(Arc::clone(&self.inner.flush_notify));
        let base = self.inner.config.replica_listen;
        let port = if placed.port != 0 {
            placed.port
        } else if base.port() == 0 {
            0
        } else {
            let taken = self
                .inner
                .shards
                .lock()
                .expect("shards lock")
                .values()
                .filter(|shard| shard.placed.is_some())
                .count() as u16;
            base.port().saturating_add(taken)
        };
        let listener = tokio::net::TcpListener::bind(SocketAddr::new(base.ip(), port))
            .await
            .map_err(|e| {
                format!(
                    "bind replica listener {}:{port} for shard {:?}: {e}",
                    base.ip(),
                    placed.shard_id
                )
            })?;
        let bound = listener
            .local_addr()
            .map_err(|e| format!("listener addr: {e}"))?;
        placed.port = bound.port();
        placed.write(&dir)?;
        node.spawn_floor_listener(bound);
        let scheme = if self.inner.config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let addr = format!(
            "{scheme}://{}:{}",
            self.inner.config.advertise_host,
            bound.port()
        );
        let server = self.spawn_server(node.clone(), listener)?;
        eprintln!(
            "node {:?}: shard {:?} placed at {addr} ({})",
            self.inner.config.node_id,
            placed.shard_id,
            dir.display()
        );
        self.inner.shards.lock().expect("shards lock").insert(
            placed.shard_id.clone(),
            ServedShard {
                shard_id: placed.shard_id.clone(),
                node,
                addr: addr.clone(),
                hash_range: Some((placed.hash_lo, placed.hash_hi)),
                placed: Some(placed),
                server: Some(server),
            },
        );
        Ok(addr)
    }

    fn spawn_server(
        &self,
        node: NodeServiceImpl,
        listener: tokio::net::TcpListener,
    ) -> Result<tokio::task::JoinHandle<()>, String> {
        let max = self.inner.config.max_message_bytes;
        let builder = match &self.inner.config.tls {
            None => tonic::transport::Server::builder(),
            #[cfg(feature = "tls")]
            Some(tls) => tonic::transport::Server::builder()
                .tls_config(tls.server_config(true))
                .map_err(|e| format!("replica listener TLS: {e}"))?,
            #[cfg(not(feature = "tls"))]
            Some(_) => return Err("this build has no TLS support (feature `tls` is off)".into()),
        };
        let server = builder
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, max))
            .serve_with_incoming(crate::harness::nodelay_incoming(listener));
        Ok(tokio::spawn(async move {
            if let Err(e) = server.await {
                eprintln!("replica listener ended: {e}");
            }
        }))
    }

    async fn control(&self) -> Result<ClusterControlClient<Channel>, String> {
        if let Some(client) = self.inner.control.lock().expect("control lock").as_ref() {
            return Ok(client.clone());
        }
        let addr = crate::config::normalize_addr(self.inner.config.control_addr.clone());
        let endpoint =
            tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(&addr))
                .map_err(|e| format!("control address {addr}: {e}"))?
                .tcp_nodelay(true);
        let endpoint = crate::security::secure_endpoint(endpoint)?;
        let client = ClusterControlClient::new(endpoint.connect_lazy())
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
        *self.inner.control.lock().expect("control lock") = Some(client.clone());
        Ok(client)
    }

    /// Disk, memory, and thread capacity for the register/renew calls.
    pub fn capacity(&self) -> NodeCapacity {
        let (disk_bytes, used_disk_bytes) = filesystem_bytes(&self.inner.config.data_dir);
        let search_threads = if self.inner.config.scan_parallel == 0 {
            std::thread::available_parallelism()
                .map(|n| (n.get() / 2).max(1))
                .unwrap_or(1)
        } else {
            self.inner.config.scan_parallel
        } as u32;
        let rate = crate::node::scan_rate();
        NodeCapacity {
            disk_bytes,
            used_disk_bytes,
            memory_bytes: memory_bytes(),
            search_threads,
            failure_domain: self.inner.config.failure_domain.clone(),
            // The process-wide window's rate (docs/bandwidth-budget.md):
            // zero is unknown. A server process: the device residency is
            // declared by the phone transport, which does not register
            // through this agent.
            scan_bytes_per_second: rate.bytes_per_second,
            scan_rate_observed_unix_ms: rate.observed_unix_ms,
            scan_rate_samples: rate.samples,
            scan_rate_window_ms: rate.window_ms,
            residency: crate::pb::NodeResidency::Server as i32,
        }
    }

    /// `RegisterNode`: a fresh lease for this node at its address.
    pub async fn register(&self) -> Result<NodeLease, String> {
        let mut control = self.control().await?;
        let lease = control
            .register_node(RegisterNodeRequest {
                node_id: self.inner.config.node_id.clone(),
                addr: self.inner.config.node_addr.clone(),
                capacity: Some(self.capacity()),
                lease_ms: self.inner.config.lease_ms,
                collection: self.inner.config.collection.clone(),
            })
            .await
            .map_err(|e| format!("register node {:?}: {e}", self.inner.config.node_id))?
            .into_inner();
        *self.inner.lease.lock().expect("lease lock") = Some(lease.clone());
        self.inner
            .stats
            .registrations
            .fetch_add(1, Ordering::Relaxed);
        Ok(lease)
    }

    /// `RenewNodeLease`; a lease the plane no longer holds re-registers.
    pub async fn renew(&self) -> Result<NodeLease, String> {
        let Some(lease) = self.lease() else {
            return self.register().await;
        };
        let mut control = self.control().await?;
        match control
            .renew_node_lease(RenewNodeLeaseRequest {
                node_id: self.inner.config.node_id.clone(),
                lease_token: lease.lease_token,
                capacity: Some(self.capacity()),
                lease_ms: self.inner.config.lease_ms,
                collection: self.inner.config.collection.clone(),
            })
            .await
        {
            Ok(renewed) => {
                let renewed = renewed.into_inner();
                *self.inner.lease.lock().expect("lease lock") = Some(renewed.clone());
                self.inner.stats.renewals.fetch_add(1, Ordering::Relaxed);
                Ok(renewed)
            }
            Err(status) if matches!(status.code(), Code::NotFound | Code::FailedPrecondition) => {
                eprintln!(
                    "node {:?}: lease not renewable ({}); registering again",
                    self.inner.config.node_id,
                    status.message()
                );
                self.register().await
            }
            Err(status) => Err(format!(
                "renew lease of node {:?}: {status}",
                self.inner.config.node_id
            )),
        }
    }

    async fn lease_token(&self) -> Result<u64, String> {
        match self.lease() {
            Some(lease) => Ok(lease.lease_token),
            None => self.register().await.map(|lease| lease.lease_token),
        }
    }

    /// `GetClusterPlan` for this agent's collection.
    pub async fn plan(&self) -> Result<ClusterPlan, String> {
        let mut control = self.control().await?;
        Ok(control
            .get_cluster_plan(GetClusterPlanRequest {
                collection: self.inner.config.collection.clone(),
            })
            .await
            .map_err(|e| format!("get cluster plan: {e}"))?
            .into_inner())
    }

    /// The hash range a served shard reports: the configuration's, else
    /// the plane's record of that shard on any node, else the published
    /// route whose primary or replica address is this listener.
    fn hash_range_of(shard: &ServedShard, plan: &ClusterPlan) -> Option<(u64, u64)> {
        if let Some(range) = shard.hash_range {
            return Some(range);
        }
        if let Some(record) = plan
            .replicas
            .iter()
            .find(|record| record.shard_id == shard.shard_id)
        {
            return Some((record.hash_lo, record.hash_hi));
        }
        plan.topology
            .iter()
            .find(|route| route.addr == shard.addr || route.replica == shard.addr)
            .map(|route| (route.hash_lo, route.hash_hi))
    }

    /// The state one served shard reports, from the node's own health.
    async fn replica_state(
        &self,
        shard: &ServedShard,
        plan: &ClusterPlan,
    ) -> Result<Option<ShardReplicaState>, String> {
        let Some((hash_lo, hash_hi)) = Self::hash_range_of(shard, plan) else {
            if self
                .inner
                .unranged
                .lock()
                .expect("unranged lock")
                .insert(shard.shard_id.clone())
            {
                eprintln!(
                    "node {:?}: shard {:?} at {} has no hash range in its configuration, the \
                     plane's records, or the published topology; not reported until one names it",
                    self.inner.config.node_id, shard.shard_id, shard.addr
                );
            }
            return Ok(None);
        };
        let health = NodeService::health(&shard.node, Request::new(HealthRequest {}))
            .await
            .map_err(|e| format!("health of shard {:?}: {e}", shard.shard_id))?
            .into_inner();
        let record = plan.replicas.iter().find(|record| {
            record.shard_id == shard.shard_id && record.node_id == self.inner.config.node_id
        });
        // The plane owns roles: a shard it has a record of reports the
        // role that record holds (a promotion or demotion happened
        // there); one it has never seen is a primary if configured, a
        // replica if placed.
        let role = match record {
            Some(record) => record.role,
            None if shard.placed.is_some() => ShardReplicaRole::Replica as i32,
            None => ShardReplicaRole::Primary as i32,
        };
        let (generation, ready) = match &shard.placed {
            Some(placed) => (placed.source_generation, placed.ready),
            None => (health.wal_generation, true),
        };
        let index_path = shard.node.config().index_path.clone();
        Ok(Some(ShardReplicaState {
            shard_id: shard.shard_id.clone(),
            node_id: self.inner.config.node_id.clone(),
            addr: shard.addr.clone(),
            generation,
            hash_lo,
            hash_hi,
            slot_offset: health.slot_offset,
            rows: rows_of(&health),
            bytes: index_path.as_deref().map_or(0, shard_bytes),
            role,
            ready: ready && !health.bm25_building,
            scoring_fingerprint: health.scoring_fingerprint.clone(),
            analysis_fingerprint: fingerprint_string(&shard.node.analysis_fingerprints()),
            immutable_segments: shard.node.immutable_segments(),
            tombstones: health.deleted_docs,
            collection: self.inner.config.collection.clone(),
        }))
    }

    /// `ReportShard` for one shard; `Ok(false)` when it has no range yet.
    pub async fn report_shard(&self, shard_id: &str) -> Result<bool, String> {
        let plan = self.plan().await?;
        self.report_shard_with(shard_id, &plan).await
    }

    async fn report_shard_with(&self, shard_id: &str, plan: &ClusterPlan) -> Result<bool, String> {
        let (node, addr, hash_range, placed) = {
            let shards = self.inner.shards.lock().expect("shards lock");
            let shard = shards
                .get(shard_id)
                .ok_or_else(|| format!("shard {shard_id:?} is not served here"))?;
            (
                shard.node.clone(),
                shard.addr.clone(),
                shard.hash_range,
                shard.placed.clone(),
            )
        };
        let shard = ServedShard {
            shard_id: shard_id.to_string(),
            node,
            addr,
            hash_range,
            placed,
            server: None,
        };
        let Some(replica) = self.replica_state(&shard, plan).await? else {
            return Ok(false);
        };
        let token = self.lease_token().await?;
        let mut control = self.control().await?;
        let request = ReportShardRequest {
            node_id: self.inner.config.node_id.clone(),
            lease_token: token,
            replica: Some(replica.clone()),
            collection: self.inner.config.collection.clone(),
        };
        match control.report_shard(request.clone()).await {
            Ok(_) => {}
            Err(status)
                if matches!(status.code(), Code::NotFound | Code::FailedPrecondition)
                    && status.message().contains("lease") =>
            {
                let lease = self.register().await?;
                control
                    .report_shard(ReportShardRequest {
                        lease_token: lease.lease_token,
                        ..request
                    })
                    .await
                    .map_err(|e| format!("report shard {shard_id:?}: {e}"))?;
            }
            Err(status) => return Err(format!("report shard {shard_id:?}: {status}")),
        }
        self.inner.stats.reports.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    /// Report every served shard.
    pub async fn report_all(&self) -> Result<usize, String> {
        let plan = self.plan().await?;
        let mut reported = 0;
        for shard_id in self.shard_ids() {
            if self.report_shard_with(&shard_id, &plan).await? {
                reported += 1;
            }
        }
        Ok(reported)
    }

    /// One worker pass: read the plan, execute the actions assigned to
    /// this node in order, and keep placed replicas caught up.
    pub async fn run_once(&self) -> Result<(), String> {
        let plan = self.plan().await?;
        let mut first_error = None;
        for action in plan
            .actions
            .iter()
            .filter(|action| action.target_node_id == self.inner.config.node_id)
        {
            let result = match PlacementActionKind::try_from(action.kind) {
                Ok(PlacementActionKind::CopyReplica) => self.execute_copy(action, &plan).await,
                Ok(PlacementActionKind::DropReplica) => self.execute_drop(action, &plan).await,
                Ok(PlacementActionKind::SplitShard) => self.execute_split(action, &plan).await,
                other => {
                    if self
                        .inner
                        .unhandled
                        .lock()
                        .expect("unhandled lock")
                        .insert(action.action_id)
                    {
                        self.inner
                            .stats
                            .unhandled_actions
                            .fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "node {:?}: action {} ({}) on shard {:?} is not handled by this \
                             worker; it stays pending",
                            self.inner.config.node_id,
                            action.action_id,
                            other
                                .map(|kind| kind.as_str_name().to_string())
                                .unwrap_or_else(|_| format!("kind {}", action.kind)),
                            action.shard_id
                        );
                    }
                    Ok(())
                }
            };
            if let Err(error) = result {
                eprintln!(
                    "node {:?}: action {} on shard {:?}: {error}",
                    self.inner.config.node_id, action.action_id, action.shard_id
                );
                first_error.get_or_insert(error);
            }
        }
        // Placed replicas that finished their bootstrap keep following
        // their primary between plan changes.
        for shard_id in self.shard_ids() {
            let Some(placed) = self.placed(&shard_id) else {
                continue;
            };
            if !placed.ready || placed.completed_action == 0 {
                continue;
            }
            let Some(primary) = plan.replicas.iter().find(|record| {
                record.shard_id == shard_id
                    && record.role == ShardReplicaRole::Primary as i32
                    && record.node_id != self.inner.config.node_id
            }) else {
                continue;
            };
            if let Err(error) = self.catch_up(&shard_id, &primary.addr, 1).await {
                eprintln!(
                    "node {:?}: replica {shard_id:?} catch-up from {}: {error}",
                    self.inner.config.node_id, primary.addr
                );
                first_error.get_or_insert(error);
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    fn update_placed(
        &self,
        shard_id: &str,
        update: impl FnOnce(&mut PlacedShard),
    ) -> Result<PlacedShard, String> {
        let mut shards = self.inner.shards.lock().expect("shards lock");
        let shard = shards
            .get_mut(shard_id)
            .ok_or_else(|| format!("shard {shard_id:?} is not served here"))?;
        let placed = shard
            .placed
            .as_mut()
            .ok_or_else(|| format!("shard {shard_id:?} is configured, not placed"))?;
        update(placed);
        placed.write(&self.inner.config.data_dir.join(shard_id))?;
        Ok(placed.clone())
    }

    /// Run `sync_once` from the persisted cursor until the primary's
    /// watermark is within the lag bound, at most `rounds` times.
    /// Returns the lag after the last round.
    async fn catch_up(
        &self,
        shard_id: &str,
        primary_addr: &str,
        rounds: u32,
    ) -> Result<u64, String> {
        let (replica_addr, placed) = {
            let shards = self.inner.shards.lock().expect("shards lock");
            let shard = shards
                .get(shard_id)
                .ok_or_else(|| format!("shard {shard_id:?} is not served here"))?;
            (
                shard.addr.clone(),
                shard
                    .placed
                    .clone()
                    .ok_or_else(|| format!("shard {shard_id:?} is configured, not placed"))?,
            )
        };
        let mut cursor = ReplicaCursor {
            primary: primary_addr.to_string(),
            replica: replica_addr,
            wal_generation: placed.cursor_generation,
            clock: placed.cursor_clock,
        };
        let mut lag = u64::MAX;
        for _ in 0..rounds {
            cursor = sync_once(&cursor).await?;
            self.update_placed(shard_id, |placed| {
                placed.cursor_generation = cursor.wal_generation;
                placed.cursor_clock = cursor.clock;
            })?;
            let source = node_health(primary_addr).await?;
            lag = source.wal_high_watermark.saturating_sub(cursor.clock);
            if lag <= self.inner.config.lag_bound {
                break;
            }
        }
        Ok(lag)
    }

    /// `COPY_REPLICA`: place the shard, install the primary's image,
    /// catch up, report ready, and complete with counts that match the
    /// source at the completion clock.
    pub async fn execute_copy(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
    ) -> Result<(), String> {
        let source = plan
            .replicas
            .iter()
            .find(|record| {
                record.shard_id == action.shard_id && record.node_id == action.source_node_id
            })
            .or_else(|| {
                plan.replicas.iter().find(|record| {
                    record.shard_id == action.shard_id
                        && record.role == ShardReplicaRole::Primary as i32
                })
            })
            .ok_or_else(|| {
                format!(
                    "the plan has no record of shard {:?} on source node {:?}",
                    action.shard_id, action.source_node_id
                )
            })?;
        if source.generation != action.source_generation {
            return Err(format!(
                "action {} was planned from generation {} but the source is at {}; waiting for \
                 the plane to replan",
                action.action_id, action.source_generation, source.generation
            ));
        }
        let placed = match self.placed(&action.shard_id) {
            Some(placed) => placed,
            None => {
                let placed = PlacedShard {
                    shard_id: action.shard_id.clone(),
                    collection: self.inner.config.collection.clone(),
                    slot_offset: source.slot_offset,
                    hash_lo: source.hash_lo,
                    hash_hi: source.hash_hi,
                    source_generation: action.target_generation,
                    ..Default::default()
                };
                self.serve_placed(placed).await?;
                self.placed(&action.shard_id).expect("served above")
            }
        };
        let node = self
            .inner
            .shards
            .lock()
            .expect("shards lock")
            .get(&action.shard_id)
            .map(|shard| shard.node.clone())
            .ok_or_else(|| format!("shard {:?} vanished", action.shard_id))?;
        if !placed.installed || placed.source_generation != action.target_generation {
            let installed = NodeService::install_snapshot_from(
                &node,
                Request::new(InstallSnapshotFromRequest {
                    source: Some(crate::pb::install_snapshot_from_request::Source::PeerAddr(
                        source.addr.clone(),
                    )),
                    expected_manifest_sha256: String::new(),
                    bearer_token: String::new(),
                }),
            )
            .await
            .map_err(|e| format!("install from {}: {e}", source.addr))?
            .into_inner();
            let manifest = installed
                .manifest
                .ok_or_else(|| "install returned no manifest".to_string())?;
            if !manifest.wal_clocked {
                return Err(format!(
                    "source {} has no fully clocked WAL; a replica cannot catch up from it",
                    source.addr
                ));
            }
            self.inner.stats.installs.fetch_add(1, Ordering::Relaxed);
            self.update_placed(&action.shard_id, |placed| {
                placed.installed = true;
                placed.source_generation = action.target_generation;
                placed.cursor_generation = manifest.wal_generation;
                placed.cursor_clock = manifest.wal_high_watermark;
                placed.ready = false;
            })?;
        }
        let lag = self
            .catch_up(&action.shard_id, &source.addr, CATCH_UP_ROUNDS)
            .await?;
        if lag > self.inner.config.lag_bound {
            return Err(format!(
                "replica {:?} is still {lag} clocks behind {} after {CATCH_UP_ROUNDS} rounds; \
                 continuing next tick",
                action.shard_id, source.addr
            ));
        }
        self.update_placed(&action.shard_id, |placed| placed.ready = true)?;
        self.report_shard_with(&action.shard_id, plan).await?;
        let hook = self
            .inner
            .before_complete
            .lock()
            .expect("hook lock")
            .clone();
        if let Some(hook) = hook {
            hook().await;
        }
        // Complete only with counts that match the source now: a source
        // that moved since the last catch-up is synced again first, and
        // a completion the plane refuses (its record of the source is
        // older or newer than the live source) is retried next tick.
        for _ in 0..COMPLETION_ROUNDS {
            self.catch_up(&action.shard_id, &source.addr, 1).await?;
            let local = NodeService::health(&node, Request::new(HealthRequest {}))
                .await
                .map_err(|e| format!("local health: {e}"))?
                .into_inner();
            let live = node_health(&source.addr).await?;
            if rows_of(&local) != rows_of(&live) || local.deleted_docs != live.deleted_docs {
                self.inner
                    .stats
                    .stale_resyncs
                    .fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let Some(output) = ({
                let shards = self.inner.shards.lock().expect("shards lock");
                shards.get(&action.shard_id).map(|shard| ServedShard {
                    shard_id: shard.shard_id.clone(),
                    node: shard.node.clone(),
                    addr: shard.addr.clone(),
                    hash_range: shard.hash_range,
                    placed: shard.placed.clone(),
                    server: None,
                })
            }) else {
                return Err(format!("shard {:?} vanished", action.shard_id));
            };
            let Some(mut output) = self.replica_state(&output, plan).await? else {
                return Err("placed shard has no hash range".to_string());
            };
            output.role = ShardReplicaRole::Replica as i32;
            output.ready = true;
            let token = self.lease_token().await?;
            let mut control = self.control().await?;
            match control
                .complete_placement_action(CompletePlacementActionRequest {
                    node_id: self.inner.config.node_id.clone(),
                    lease_token: token,
                    action_id: action.action_id,
                    outputs: vec![output],
                    collection: self.inner.config.collection.clone(),
                })
                .await
            {
                Ok(_) => {
                    self.update_placed(&action.shard_id, |placed| {
                        placed.completed_action = action.action_id
                    })?;
                    self.inner
                        .stats
                        .copies_completed
                        .fetch_add(1, Ordering::Relaxed);
                    eprintln!(
                        "node {:?}: replica {:?} bootstrapped from {} (action {})",
                        self.inner.config.node_id, action.shard_id, source.addr, action.action_id
                    );
                    return Ok(());
                }
                Err(status)
                    if status.code() == Code::FailedPrecondition
                        && status.message().contains("differs from its source") =>
                {
                    self.inner
                        .stats
                        .completion_refusals
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(format!(
                        "completion of action {} refused: {}; the copy is caught up to the live \
                         source and completes once the source's report agrees",
                        action.action_id,
                        status.message()
                    ));
                }
                Err(status) => {
                    return Err(format!("complete action {}: {status}", action.action_id))
                }
            }
        }
        Err(format!(
            "source {} kept moving through {COMPLETION_ROUNDS} completion rounds; continuing \
             next tick",
            source.addr
        ))
    }

    /// `DROP_REPLICA`: remove this node's retired copy. A copy the plane
    /// still lists as primary, or a shard from the configuration, is
    /// refused and the action stays pending.
    /// `SPLIT_SHARD` (`docs/cluster-control.md`, "Shard split"): build
    /// two children covering the source range from the source's own
    /// WAL, place them here on fresh listeners, tail the source's log
    /// into them by stable key until they are within the lag bound,
    /// fence ingest on the source, drain the last records, verify the
    /// children conserve the source's live rows, complete the action
    /// with the children as primaries, and retire the source. Every
    /// step is durable in `split.toml`, so a crash resumes where it
    /// stopped; a completion the plane refuses is retried next tick
    /// with the source still fenced.
    pub async fn execute_split(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
    ) -> Result<(), String> {
        let node_id = self.inner.config.node_id.clone();
        let source_record = plan
            .replicas
            .iter()
            .find(|record| {
                record.shard_id == action.shard_id
                    && record.node_id == node_id
                    && record.role == ShardReplicaRole::Primary as i32
            })
            .ok_or_else(|| {
                format!(
                    "the plan has no primary record of shard {:?} on this node",
                    action.shard_id
                )
            })?;
        if source_record.generation != action.source_generation {
            return Err(format!(
                "action {} was planned from generation {} but the source is at {}; waiting for \
                 the plane to replan",
                action.action_id, action.source_generation, source_record.generation
            ));
        }
        if action.hash_lo >= action.hash_hi {
            return Err(format!(
                "shard {:?} covers the single hash value {}; nothing to split",
                action.shard_id, action.hash_lo
            ));
        }
        let (source_node, source_addr, source_index) = {
            let shards = self.inner.shards.lock().expect("shards lock");
            let shard = shards
                .get(&action.shard_id)
                .ok_or_else(|| format!("shard {:?} is not served on this node", action.shard_id))?;
            let index = shard
                .node
                .config()
                .index_path
                .clone()
                .ok_or_else(|| format!("shard {:?} has no index path", action.shard_id))?;
            (shard.node.clone(), shard.addr.clone(), index)
        };
        let split_dir = self.split_dir(&action.shard_id);
        let mut state = match SplitState::load(&split_dir)? {
            Some(state) if state.action_id == action.action_id => state,
            Some(state) => {
                return Err(format!(
                    "shard {:?} has split state for action {} on disk; action {} cannot start \
                     until it is resolved ({})",
                    action.shard_id,
                    state.action_id,
                    action.action_id,
                    split_dir.display()
                ));
            }
            None => {
                let source_health = node_health(&source_addr).await?;
                if source_health.deleted_docs != 0 {
                    return Err(format!(
                        "shard {:?} has {} tombstones; the live split moves appends only, so \
                         compact it first",
                        action.shard_id, source_health.deleted_docs
                    ));
                }
                let rows = rows_of(&source_health);
                // Fresh, non-overlapping slot ranges above every range the
                // plan and this node know, spaced by the source's row count
                // rounded up to a mebi: a child's ids never reuse another
                // shard's.
                let span = rows.saturating_add(1).div_ceil(1 << 20).max(1) << 20;
                let known_top = plan
                    .replicas
                    .iter()
                    .map(|record| record.slot_offset.saturating_add(record.rows))
                    .chain(
                        self.inner
                            .shards
                            .lock()
                            .expect("shards lock")
                            .values()
                            .map(|shard| shard.node.config().slot_offset)
                            .map(|offset| offset.saturating_add(rows)),
                    )
                    .max()
                    .unwrap_or(0);
                let base = known_top.div_ceil(span).saturating_mul(span);
                let mid = action.hash_lo + (action.hash_hi - action.hash_lo) / 2;
                let children = vec![
                    SplitChild {
                        shard_id: format!("{}-0", action.shard_id),
                        hash_lo: action.hash_lo,
                        hash_hi: mid,
                        slot_offset: base,
                    },
                    SplitChild {
                        shard_id: format!("{}-1", action.shard_id),
                        hash_lo: mid + 1,
                        hash_hi: action.hash_hi,
                        slot_offset: base.saturating_add(span),
                    },
                ];
                for child in &children {
                    if self
                        .inner
                        .shards
                        .lock()
                        .expect("shards lock")
                        .contains_key(&child.shard_id)
                        || plan.replicas.iter().any(|r| r.shard_id == child.shard_id)
                    {
                        return Err(format!(
                            "split child shard id {:?} already exists",
                            child.shard_id
                        ));
                    }
                }
                let state = SplitState {
                    action_id: action.action_id,
                    shard_id: action.shard_id.clone(),
                    source_generation: action.source_generation,
                    target_generation: action.target_generation,
                    children,
                    built: false,
                    live: None,
                    fenced: false,
                    completed: false,
                };
                state.write(&split_dir)?;
                state
            }
        };

        // 1. Build the baseline children from the source's WAL and place
        //    them on fresh listeners.
        if !state.built {
            let ranges: Vec<(u64, u64)> = state
                .children
                .iter()
                .map(|child| (child.hash_lo, child.hash_hi))
                .collect();
            let offsets: Vec<u64> = state
                .children
                .iter()
                .map(|child| child.slot_offset)
                .collect();
            let work = split_dir.join("build");
            let _ = std::fs::remove_dir_all(&work);
            let generation = crate::reshard::resolve_gen(&crate::wal::wal_dir(&source_index))?;
            let analysis_addr = self
                .inner
                .config
                .template
                .analysis_addr
                .clone()
                .ok_or_else(|| {
                    "the node has no analysis backend; a split rebuilds the children's postings"
                        .to_string()
                })?;
            let bm25_fields = self.inner.config.template.bm25_fields.clone();
            let handle = tokio::runtime::Handle::current();
            let build_dir = work.clone();
            let built = tokio::task::spawn_blocking(move || {
                let mut analyze = move |docs: &[(
                    &str,
                    Option<&crate::pb::AnalysisSpec>,
                    crate::analyzer::SessionLayers,
                )]| {
                    handle
                        .block_on(crate::analyzer::analyze_batch_streams(
                            &analysis_addr,
                            docs,
                            1,
                        ))
                        .map_err(|error| error.to_string())
                };
                crate::reshard::split_stable_logs_ranged(
                    &[generation],
                    &ranges,
                    &build_dir,
                    &offsets,
                    false,
                    (!bm25_fields.is_empty()).then_some(bm25_fields.as_slice()),
                    &mut analyze,
                )
            })
            .await
            .map_err(|e| format!("split build task: {e}"))??;
            let cutoff = *built
                .source_cutoffs
                .first()
                .ok_or_else(|| "the split reported no source cutoff".to_string())?;
            let mut live_children = Vec::with_capacity(state.children.len());
            for (child, image) in state.children.iter().zip(&built.images.children) {
                let dir = self.shard_dir(&child.shard_id);
                std::fs::create_dir_all(&dir)
                    .map_err(|e| format!("create {}: {e}", dir.display()))?;
                let target = dir.join("shard");
                move_image_file(&image.vector_path, &target)?;
                move_image_file(
                    &image.exact_vector_path,
                    &crate::node::exact_vector_sidecar_path(&target),
                )?;
                if let Some(bm25) = &image.bm25_path {
                    move_image_file(bm25, &crate::node::bm25_sidecar_path(&target))?;
                }
                let placed = PlacedShard {
                    shard_id: child.shard_id.clone(),
                    collection: self.inner.config.collection.clone(),
                    slot_offset: child.slot_offset,
                    hash_lo: child.hash_lo,
                    hash_hi: child.hash_hi,
                    source_generation: action.target_generation,
                    installed: true,
                    ..Default::default()
                };
                let addr = self.serve_placed(placed).await?;
                live_children.push(crate::replication::LiveChild {
                    addr,
                    replica: None,
                    hash_lo: child.hash_lo,
                    hash_hi: child.hash_hi,
                    slot_offset: child.slot_offset,
                    base_vectors: image.num_vectors,
                    base_document_slots: image.num_documents,
                    applied_vectors: 0,
                    applied_documents: 0,
                });
            }
            let _ = std::fs::remove_dir_all(&work);
            state.live = Some(crate::replication::LiveReshardState {
                source: source_addr.clone(),
                source_wal_generation: cutoff.generation,
                source_clock: cutoff.high_watermark,
                old_topology_generation: plan.topology_generation,
                new_topology_generation: plan.topology_generation.saturating_add(1),
                children: live_children,
            });
            state.built = true;
            state.write(&split_dir)?;
            self.inner.stats.installs.fetch_add(1, Ordering::Relaxed);
        } else {
            // A restart re-serves the children through open_placed; make
            // sure they are up before tailing into them.
            for child in &state.children {
                if self.shard_addr(&child.shard_id).is_none() {
                    return Err(format!(
                        "split child {:?} is not served; open the placed shards first",
                        child.shard_id
                    ));
                }
            }
        }
        let mut live = state
            .live
            .clone()
            .ok_or_else(|| "split state has no live cursor".to_string())?;

        // 2. Tail the source into the children until within the lag bound.
        let mut lag = u64::MAX;
        for _ in 0..CATCH_UP_ROUNDS {
            live = crate::replication::catch_up_children_once(&live).await?;
            state.live = Some(live.clone());
            state.write(&split_dir)?;
            let source = node_health(&source_addr).await?;
            lag = source.wal_high_watermark.saturating_sub(live.source_clock);
            if lag <= self.inner.config.lag_bound {
                break;
            }
        }
        if lag > self.inner.config.lag_bound {
            return Err(format!(
                "split children of {:?} are still {lag} clocks behind after \
                 {CATCH_UP_ROUNDS} rounds; continuing next tick",
                action.shard_id
            ));
        }

        // 3. Fence the source and drain the last records: with no append
        //    able to land, the children's rows are the source's rows.
        source_node.fence_ingest(format!(
            "shard {:?} is splitting into {} (action {}); retry against the new topology",
            action.shard_id,
            state
                .children
                .iter()
                .map(|child| child.shard_id.as_str())
                .collect::<Vec<_>>()
                .join(" and "),
            action.action_id
        ));
        if !state.fenced {
            state.fenced = true;
            state.write(&split_dir)?;
        }
        live = crate::replication::catch_up_children_once(&live).await?;
        state.live = Some(live.clone());
        state.write(&split_dir)?;
        let source = node_health(&source_addr).await?;
        if source.wal_high_watermark != live.source_clock {
            return Err(format!(
                "fenced source {:?} still moved: watermark {} past the children's clock {}",
                action.shard_id, source.wal_high_watermark, live.source_clock
            ));
        }
        let mut child_rows = 0u64;
        let mut outputs = Vec::with_capacity(state.children.len());
        for child in &state.children {
            let served = {
                let shards = self.inner.shards.lock().expect("shards lock");
                shards.get(&child.shard_id).map(|shard| ServedShard {
                    shard_id: shard.shard_id.clone(),
                    node: shard.node.clone(),
                    addr: shard.addr.clone(),
                    hash_range: shard.hash_range,
                    placed: shard.placed.clone(),
                    server: None,
                })
            }
            .ok_or_else(|| format!("split child {:?} vanished", child.shard_id))?;
            let health = NodeService::health(&served.node, Request::new(HealthRequest {}))
                .await
                .map_err(|e| format!("health of child {:?}: {e}", child.shard_id))?
                .into_inner();
            child_rows = child_rows.saturating_add(rows_of(&health));
            let Some(mut output) = self.replica_state(&served, plan).await? else {
                return Err(format!(
                    "split child {:?} has no hash range",
                    child.shard_id
                ));
            };
            output.role = ShardReplicaRole::Primary as i32;
            output.ready = true;
            output.generation = action.target_generation;
            outputs.push(output);
        }
        // The fenced source's counts are final: report them so the plane's
        // record of the source is the one the children are checked against.
        self.report_shard_with(&action.shard_id, plan).await?;
        let source_live = rows_of(&source).saturating_sub(source.deleted_docs);
        if child_rows != source_live {
            return Err(format!(
                "split children of {:?} hold {child_rows} rows, the fenced source {source_live}; \
                 the split does not conserve the source and is not completed",
                action.shard_id
            ));
        }

        // 4. Complete with the children as primaries; the plane replaces
        //    the source's record and publishes the topology.
        let hook = self
            .inner
            .before_complete
            .lock()
            .expect("hook lock")
            .clone();
        if let Some(hook) = hook {
            hook().await;
        }
        let token = self.lease_token().await?;
        let mut control = self.control().await?;
        match control
            .complete_placement_action(CompletePlacementActionRequest {
                node_id: node_id.clone(),
                lease_token: token,
                action_id: action.action_id,
                outputs,
                collection: self.inner.config.collection.clone(),
            })
            .await
        {
            Ok(_) => {}
            Err(status) if status.code() == Code::FailedPrecondition => {
                // The plane's record of the source is behind the fenced
                // source; report it (its counts no longer move) and let
                // the next tick complete.
                self.inner
                    .stats
                    .completion_refusals
                    .fetch_add(1, Ordering::Relaxed);
                self.report_shard_with(&action.shard_id, plan).await?;
                return Err(format!(
                    "split of {:?} refused by the plane: {}; the source is fenced and reported, \
                     retrying next tick",
                    action.shard_id,
                    status.message()
                ));
            }
            Err(status) => {
                return Err(format!(
                    "complete split action {}: {status}",
                    action.action_id
                ))
            }
        }
        for child in &state.children {
            self.update_placed(&child.shard_id, |placed| {
                placed.ready = true;
                placed.completed_action = action.action_id;
            })?;
        }
        // 5. Retire the source: it is no longer reported or served; a
        //    configured source keeps its files and a marker, a placed one
        //    is removed like a dropped copy.
        let retired = self
            .inner
            .shards
            .lock()
            .expect("shards lock")
            .remove(&action.shard_id);
        if let Some(shard) = retired {
            if shard.placed.is_some() {
                if let Some(server) = shard.server {
                    server.abort();
                }
                drop(shard.node);
                let dir = self.shard_dir(&action.shard_id);
                std::fs::remove_dir_all(&dir)
                    .map_err(|e| format!("remove {}: {e}", dir.display()))?;
            } else {
                let marker = retired_marker(&self.inner.config.data_dir, &action.shard_id);
                if let Some(parent) = marker.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("create {}: {e}", parent.display()))?;
                }
                std::fs::write(&marker, format!("split by action {}\n", action.action_id))
                    .map_err(|e| format!("write {}: {e}", marker.display()))?;
            }
        }
        state.completed = true;
        state.write(&split_dir)?;
        self.inner
            .stats
            .splits_completed
            .fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "node {:?}: shard {:?} split into {} (action {}), source retired",
            node_id,
            action.shard_id,
            state
                .children
                .iter()
                .map(|child| child.shard_id.as_str())
                .collect::<Vec<_>>()
                .join(" and "),
            action.action_id
        );
        Ok(())
    }

    fn split_dir(&self, shard_id: &str) -> PathBuf {
        self.inner.config.data_dir.join(format!("{shard_id}.split"))
    }

    pub async fn execute_drop(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
    ) -> Result<(), String> {
        if let Some(record) = plan.replicas.iter().find(|record| {
            record.shard_id == action.shard_id && record.node_id == self.inner.config.node_id
        }) {
            if record.role == ShardReplicaRole::Primary as i32 {
                return Err(format!(
                    "refusing to drop shard {:?}: the plane lists this node's copy as its serving \
                     primary",
                    action.shard_id
                ));
            }
        }
        let removed = {
            let mut shards = self.inner.shards.lock().expect("shards lock");
            match shards.get(&action.shard_id) {
                Some(shard) if shard.placed.is_none() => {
                    return Err(format!(
                        "refusing to drop shard {:?}: it is configured statically on this node; \
                         remove it from the configuration instead",
                        action.shard_id
                    ));
                }
                Some(_) => shards.remove(&action.shard_id),
                None => None,
            }
        };
        if let Some(shard) = removed {
            if let Some(server) = shard.server {
                server.abort();
            }
            drop(shard.node);
            let dir = self.shard_dir(&action.shard_id);
            std::fs::remove_dir_all(&dir).map_err(|e| format!("remove {}: {e}", dir.display()))?;
            eprintln!(
                "node {:?}: dropped shard {:?} ({})",
                self.inner.config.node_id,
                action.shard_id,
                dir.display()
            );
        }
        let token = self.lease_token().await?;
        let mut control = self.control().await?;
        control
            .complete_placement_action(CompletePlacementActionRequest {
                node_id: self.inner.config.node_id.clone(),
                lease_token: token,
                action_id: action.action_id,
                outputs: Vec::new(),
                collection: self.inner.config.collection.clone(),
            })
            .await
            .map_err(|e| format!("complete drop action {}: {e}", action.action_id))?;
        self.inner
            .stats
            .drops_completed
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Stop the listeners of placed shards (the configured shards'
    /// servers belong to the binary).
    pub fn stop(&self) {
        let mut shards = self.inner.shards.lock().expect("shards lock");
        for shard in shards.values_mut() {
            if let Some(server) = shard.server.take() {
                server.abort();
            }
        }
    }

    /// Start the membership loops: register (retrying until the plane
    /// answers), renew every lease/3, report on the timer and after
    /// every flush, and run the worker every reconcile interval. Each
    /// loop ends when `shutdown` turns true.
    pub fn start(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Vec<tokio::task::JoinHandle<()>> {
        let agent = self.clone();
        let mut handles = Vec::new();
        handles.push(tokio::spawn(async move {
            // Registration first; the other loops wait for it.
            loop {
                if *shutdown.borrow() {
                    return;
                }
                match agent.register().await {
                    Ok(lease) => {
                        eprintln!(
                            "node {:?}: registered at {} (lease {} ms)",
                            agent.inner.config.node_id,
                            agent.inner.config.node_addr,
                            lease.expires_unix_ms.saturating_sub(now_ms())
                        );
                        break;
                    }
                    Err(error) => {
                        eprintln!("node {:?}: {error}; retrying", agent.inner.config.node_id);
                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                            _ = shutdown.changed() => return,
                        }
                    }
                }
            }
            if let Err(error) = agent.open_placed().await {
                eprintln!("node {:?}: {error}", agent.inner.config.node_id);
            }
            if let Err(error) = agent.report_all().await {
                eprintln!("node {:?}: report: {error}", agent.inner.config.node_id);
            }
            let lease_ms = agent
                .lease()
                .map(|lease| lease.expires_unix_ms.saturating_sub(now_ms()))
                .filter(|ms| *ms > 0)
                .unwrap_or(15_000);
            let renew_every = Duration::from_millis((lease_ms / 3).max(200));
            let report_every = Duration::from_millis(agent.inner.config.report_ms.max(100));
            let reconcile_every = Duration::from_millis(agent.inner.config.reconcile_ms.max(100));
            let renew = agent.clone();
            let mut renew_shutdown = shutdown.clone();
            let lease_loop = tokio::spawn(async move {
                let mut interval = tokio::time::interval(renew_every);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = renew_shutdown.changed() => return,
                        _ = interval.tick() => {
                            if let Err(error) = renew.renew().await {
                                eprintln!("node {:?}: {error}", renew.inner.config.node_id);
                            }
                        }
                    }
                }
            });
            let report = agent.clone();
            let mut report_shutdown = shutdown.clone();
            let notify = report.flush_notify();
            let report_loop = tokio::spawn(async move {
                let mut interval = tokio::time::interval(report_every);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = report_shutdown.changed() => return,
                        _ = interval.tick() => {}
                        _ = notify.notified() => {}
                    }
                    if let Err(error) = report.report_all().await {
                        eprintln!("node {:?}: report: {error}", report.inner.config.node_id);
                    }
                }
            });
            let worker = agent.clone();
            let mut worker_shutdown = shutdown.clone();
            let worker_loop = tokio::spawn(async move {
                let mut interval = tokio::time::interval(reconcile_every);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    tokio::select! {
                        _ = worker_shutdown.changed() => return,
                        _ = interval.tick() => {
                            if let Err(error) = worker.run_once().await {
                                eprintln!("node {:?}: worker: {error}", worker.inner.config.node_id);
                            }
                        }
                    }
                }
            });
            let _ = tokio::join!(lease_loop, report_loop, worker_loop);
            agent.stop();
        }));
        handles
    }
}

/// The row count a shard reports: the larger of its vector and document
/// tips (aligned shards have one number).
pub fn rows_of(health: &HealthResponse) -> u64 {
    health.num_vectors.max(health.document_slots)
}

/// Per-field analysis fingerprints as one string, field-table order.
pub fn fingerprint_string(fingerprints: &[u64]) -> String {
    fingerprints
        .iter()
        .map(|fp| format!("{fp:016x}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u128::from(u64::MAX)) as u64
}

async fn node_health(addr: &str) -> Result<HealthResponse, String> {
    let endpoint =
        tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(addr))
            .map_err(|e| format!("node address {addr}: {e}"))?;
    let endpoint = crate::security::secure_endpoint(endpoint)?;
    Ok(
        crate::pb::node_service_client::NodeServiceClient::new(endpoint.connect_lazy())
            .health(HealthRequest {})
            .await
            .map_err(|e| format!("health of {addr}: {e}"))?
            .into_inner(),
    )
}

/// Bytes on disk of a shard's file family: the index path and every
/// sibling that starts with its name (`.bm25`, `.segments/`, `.snap/`,
/// `.wal/`, ...).
pub fn shard_bytes(index_path: &Path) -> u64 {
    fn size_of(path: &Path) -> u64 {
        match std::fs::metadata(path) {
            Ok(meta) if meta.is_dir() => std::fs::read_dir(path)
                .map(|entries| entries.flatten().map(|e| size_of(&e.path())).sum())
                .unwrap_or(0),
            Ok(meta) => meta.len(),
            Err(_) => 0,
        }
    }
    let (Some(parent), Some(name)) = (index_path.parent(), index_path.file_name()) else {
        return 0;
    };
    let prefix = name.to_string_lossy().into_owned();
    std::fs::read_dir(parent)
        .map(|entries| {
            entries
                .flatten()
                .filter(|e| e.file_name().to_string_lossy().starts_with(&prefix))
                .map(|e| size_of(&e.path()))
                .sum()
        })
        .unwrap_or(0)
}

/// `(total, used)` bytes of the filesystem holding `path` (created when
/// missing); zeros when the statistics are unavailable.
// statvfs field widths differ by platform; the conversions are the
// portable spelling even where they are identities.
#[allow(clippy::useless_conversion)]
pub fn filesystem_bytes(path: &Path) -> (u64, u64) {
    let _ = std::fs::create_dir_all(path);
    #[cfg(unix)]
    {
        use std::os::unix::ffi::OsStrExt;
        let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
            return (0, 0);
        };
        let mut stats: libc::statvfs = unsafe { std::mem::zeroed() };
        // SAFETY: `c_path` is a valid NUL-terminated string and `stats`
        // is a zeroed statvfs the call fills in.
        if unsafe { libc::statvfs(c_path.as_ptr(), &mut stats) } != 0 {
            return (0, 0);
        }
        let frag = u64::from(stats.f_frsize).max(1);
        let total = u64::from(stats.f_blocks).saturating_mul(frag);
        let available = u64::from(stats.f_bavail).saturating_mul(frag);
        (total, total.saturating_sub(available))
    }
    #[cfg(not(unix))]
    {
        (0, 0)
    }
}

/// Total memory in bytes from `/proc/meminfo`; 0 elsewhere.
pub fn memory_bytes() -> u64 {
    std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| {
            text.lines()
                .find(|line| line.starts_with("MemTotal:"))
                .and_then(|line| {
                    line.split_whitespace()
                        .nth(1)
                        .and_then(|kb| kb.parse::<u64>().ok())
                })
        })
        .map_or(0, |kb| kb.saturating_mul(1024))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placed_shard_round_trips_and_bytes_count_the_file_family() {
        let dir = std::env::temp_dir().join(format!("node-agent-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let placed = PlacedShard {
            shard_id: "s0".into(),
            slot_offset: 7,
            hash_hi: u64::MAX,
            port: 5000,
            installed: true,
            ..Default::default()
        };
        placed.write(&dir).unwrap();
        assert_eq!(PlacedShard::load(&dir).unwrap(), Some(placed));
        assert_eq!(PlacedShard::load(&dir.join("none")).unwrap(), None);
        std::fs::write(dir.join("shard"), b"abcd").unwrap();
        std::fs::create_dir_all(dir.join("shard.segments/segments")).unwrap();
        std::fs::write(dir.join("shard.segments/segments.json"), b"ab").unwrap();
        std::fs::write(dir.join("other"), b"zzzzzz").unwrap();
        assert_eq!(shard_bytes(&dir.join("shard")), 6);
        assert_eq!(
            fingerprint_string(&[1, 255]),
            "0000000000000001,00000000000000ff"
        );
        let (total, used) = filesystem_bytes(&dir);
        assert!(total > 0 && used <= total);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
