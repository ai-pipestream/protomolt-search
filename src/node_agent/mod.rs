//! Node membership in the control plane: register, renew the lease, report every served
//! shard, and run the worker that executes the plan's actions assigned
//! to this node — `COPY_REPLICA` bootstraps a replica under `--data-dir`
//! from the primary's `StreamSnapshot` and catches its WAL tail up with
//! `replication::sync_once`; `DROP_REPLICA` removes a retired copy;
//! `SPLIT_SHARD` splits a primary served here into two children.
//!
//! One agent serves one collection: the shards it reports and the plane
//! it talks to belong to that collection. The serving binary starts one
//! per collection its shards name.

mod control;
mod copy;
mod host;
mod lifecycle;
mod placed;
mod split;
mod state;
mod worker;

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, Mutex};

use tokio::sync::Notify;
use tonic::transport::Channel;

use crate::node::{NodeConfig, NodeServiceImpl};
use crate::pb::cluster_control_client::ClusterControlClient;
use crate::pb::NodeLease;

pub use self::host::{filesystem_bytes, fingerprint_string, memory_bytes, rows_of, shard_bytes};
use self::state::retired_marker;
pub use self::state::{PlacedShard, SplitChild, SplitState};

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
    /// Listener TLS for placed replicas (the node listeners' material).
    pub tls: Option<crate::security::ServerTls>,
    pub max_message_bytes: usize,
}

/// One shard this node serves and reports.
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

    /// A copy of the shard's handles without its server task, for work
    /// done outside the shards lock.
    fn snapshot(&self) -> ServedShard {
        ServedShard {
            shard_id: self.shard_id.clone(),
            node: self.node.clone(),
            addr: self.addr.clone(),
            hash_range: self.hash_range,
            placed: self.placed.clone(),
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
        let shards = shards
            .into_iter()
            .filter(|shard| !retired_by_split(&config, &shard.shard_id))
            .map(|shard| (shard.shard_id.clone(), shard))
            .collect();
        NodeAgent {
            inner: Arc::new(Inner {
                config,
                shards: Mutex::new(shards),
                lease: Mutex::new(None),
                control: Mutex::new(None),
                stats: AgentStats::default(),
                flush_notify: Arc::new(Notify::new()),
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
        self.shards().get(shard_id).map(|shard| shard.addr.clone())
    }

    /// The durable record of a placed shard.
    pub fn placed(&self, shard_id: &str) -> Option<PlacedShard> {
        self.shards()
            .get(shard_id)
            .and_then(|shard| shard.placed.clone())
    }

    /// Served shard ids.
    pub fn shard_ids(&self) -> Vec<String> {
        self.shards().keys().cloned().collect()
    }

    /// Stop the listeners of placed shards (the configured shards'
    /// servers belong to the binary).
    pub fn stop(&self) {
        for shard in self.shards().values_mut() {
            if let Some(server) = shard.server.take() {
                server.abort();
            }
        }
    }

    fn shards(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, ServedShard>> {
        self.inner.shards.lock().expect("shards lock")
    }

    /// A served shard's handles, taken out of the shards lock.
    fn served(&self, shard_id: &str) -> Option<ServedShard> {
        self.shards().get(shard_id).map(ServedShard::snapshot)
    }

    fn shard_dir(&self, shard_id: &str) -> PathBuf {
        self.inner.config.data_dir.join(shard_id)
    }

    fn split_dir(&self, shard_id: &str) -> PathBuf {
        self.inner.config.data_dir.join(format!("{shard_id}.split"))
    }

    /// Await the test hook installed before completions, if any.
    async fn before_complete(&self) {
        let hook = self
            .inner
            .before_complete
            .lock()
            .expect("hook lock")
            .clone();
        if let Some(hook) = hook {
            hook().await;
        }
    }

    /// Change a placed shard's durable record and write it.
    fn update_placed(
        &self,
        shard_id: &str,
        update: impl FnOnce(&mut PlacedShard),
    ) -> Result<PlacedShard, String> {
        let mut shards = self.shards();
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
}

/// A configured shard a split retired stays retired across a restart:
/// its rows live in its children now, and a report of it would
/// re-register a range the children already tile.
fn retired_by_split(config: &NodeAgentConfig, shard_id: &str) -> bool {
    let marker = retired_marker(&config.data_dir, shard_id);
    let retired = marker.exists();
    if retired {
        eprintln!(
            "node {:?}: shard {shard_id:?} was retired by a split; not served or reported \
             (remove it from the configuration and {} to forget it)",
            config.node_id,
            marker.display()
        );
    }
    retired
}
