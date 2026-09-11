//! Raft-host kit for the slice-3c/3d targets
//! (`docs/control-authority-test-harness.md`), rebuilt on the supported
//! surface after the R1 repair closed raw submission, the replay paths, the
//! log store and the state machine constructor to product callers.
//!
//! Three fixtures: a single-node host on `RaftHost::bootstrap_single`
//! (`bootstrap_host`, `start_host`); a two-member group over the tonic
//! transport (`Cluster`: node 1 bootstrapped with `bootstrap_cluster`, node
//! 2 prepared with `prepare_member` and seeded through `add_learner`), which
//! is the only supported path a snapshot install can take; and a raw
//! registered peer (`peer_client`, `install_chunks`) that speaks the
//! transport's `InstallSnapshot` RPC with node 3's certificate, the
//! adversary model for R3 and R5. Snapshot generations are read from the
//! documented layout (`raft-snapshots/current` naming
//! `generations/<n>/{image.redb,meta}`).
//!
//! Shared between the raft targets and the other adversarial targets, which
//! compile it under the `raft` feature without using every helper.
#![cfg(all(feature = "raft", feature = "tls"))]
#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use pipestream_search::pb::storage::raft_transport_client::RaftTransportClient;
use pipestream_search::pb::storage::{
    raft_entry::Payload, raft_install_snapshot_response::Outcome as InstallOutcome,
    RaftAppendEntriesRequest, RaftEntry, RaftInstallSnapshotRequest, RaftInstallSnapshotResponse,
    RaftLogId, RaftMembership, RaftNode, RaftRpcHeader, RaftSnapshotMeta, RaftStoredMembership,
    RaftVote, RaftVoterSet, SourceAuthorityIdentity, SourceAuthorityLimits,
};
use pipestream_search::pb::AccessPolicy;
use pipestream_search::raft::transport::{
    certificate_sha256, PeerDirectory, TransportLimits, TIMING_METADATA,
};
use pipestream_search::raft::{ClusterTransport, HostConfig, RaftHost};
use pipestream_search::security::{apply_client_tls, ClientTls, ServerTls};
use prost::Message;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use super::kit::{identity, limits, policy, TestDir, SEED};

pub const NODE_ID: u64 = 1;
pub const MEMBER_ID: u64 = 2;
/// A certificate the group registers but never adds to membership: the raw
/// peer of the R3/R5 reproductions.
pub const PEER_ID: u64 = 3;
pub const NODE_ADDR: &str = "127.0.0.1:9917";
/// One chunk on the wire; a store image crosses several.
pub const CHUNK_BYTES: usize = 256 << 10;

/// The in-crate recipe: fast heartbeats, snapshots on demand only, a lease
/// within the election floor, small snapshot chunks.
pub fn host_config() -> HostConfig {
    HostConfig {
        heartbeat_interval_ms: 50,
        election_timeout_min_ms: 150,
        election_timeout_max_ms: 300,
        snapshot_logs_since_last: 1_000_000,
        admission_lease_ms: 100,
        clock_skew_ms: 50,
        snapshot_chunk_bytes: CHUNK_BYTES as u64,
        install_snapshot_timeout_ms: 5_000,
        ..HostConfig::default()
    }
}

// ---- single node ----------------------------------------------------------

/// Bootstrap a one-voter group in `dir`, waiting for leadership.
pub async fn bootstrap_host(dir: &TestDir) -> RaftHost {
    bootstrap_host_with(dir, &policy()).await
}

/// [`bootstrap_host`] with an explicit access policy (e.g. one that also
/// grants Ingest for fence-admission checks).
pub async fn bootstrap_host_with(dir: &TestDir, policy: &AccessPolicy) -> RaftHost {
    bootstrap_host_with_limits(dir, policy, &limits()).await
}

pub async fn bootstrap_host_with_limits(
    dir: &TestDir,
    policy: &AccessPolicy,
    limits: &SourceAuthorityLimits,
) -> RaftHost {
    RaftHost::bootstrap_single(
        dir.path(),
        &identity(SEED),
        NODE_ID,
        NODE_ADDR,
        policy,
        limits,
        &host_config(),
    )
    .await
    .unwrap()
}

/// Reopen a bootstrapped group.
pub async fn start_host(dir: &TestDir) -> RaftHost {
    RaftHost::start(dir.path(), &identity(SEED), NODE_ID, &host_config())
        .await
        .unwrap()
}

/// Block until the node leads again after a restart.
pub async fn wait_leader(host: &RaftHost) {
    host.wait(Some(Duration::from_secs(30)))
        .state(openraft::ServerState::Leader, "regression host leader")
        .await
        .unwrap();
}

/// Poll raft metrics until the state machine has applied at least `index`.
/// A proposal resolves on commit; the apply (and the metrics watch) land a
/// tick later, so a bare read races the state machine.
pub async fn wait_applied(host: &RaftHost, index: u64) {
    host.wait(Some(Duration::from_secs(30)))
        .applied_index_at_least(Some(index), "applied")
        .await
        .unwrap_or_else(|e| panic!("applied position never reached index {index}: {e}"));
}

/// The committed index the state machine has applied, from raft metrics.
pub fn last_applied(host: &RaftHost) -> u64 {
    host.metrics()
        .borrow()
        .clone()
        .last_applied
        .unwrap_or_else(|| panic!("host has applied nothing yet"))
        .index
}

/// The durable applied position the store recorded with its last entry —
/// the R2 observable, read through the host.
pub fn durable_position(host: &RaftHost) -> u64 {
    host.applied_position()
        .unwrap()
        .unwrap_or_else(|| panic!("the store carries no applied position"))
        .index
}

/// Whether the raft core is still running (a refused install is a storage
/// error the library treats as fatal; the observation is recorded, the
/// safety assertions do not depend on it). Read one `metrics().borrow()`
/// at a time: two live borrows of the same host in one expression block
/// the core's next `send_replace`, and with it the test.
pub fn core_running(host: &RaftHost) -> bool {
    host.metrics().borrow().running_state.is_ok()
}

// ---- the generation layout -------------------------------------------------

pub fn snapshots_dir(dir: &Path) -> PathBuf {
    dir.join("raft-snapshots")
}

/// The published generation number from the pointer file, if any.
pub fn published_generation(dir: &Path) -> Option<u64> {
    std::fs::read_to_string(snapshots_dir(dir).join("current"))
        .ok()
        .map(|text| {
            text.trim()
                .parse::<u64>()
                .expect("pointer names a generation")
        })
}

pub fn generation_dir(dir: &Path, generation: u64) -> PathBuf {
    snapshots_dir(dir)
        .join("generations")
        .join(generation.to_string())
}

/// The published image and its prost meta, as the layout serves them.
pub fn published_pair(dir: &Path) -> Option<(Vec<u8>, RaftSnapshotMeta)> {
    let generation = published_generation(dir)?;
    let base = generation_dir(dir, generation);
    let image = std::fs::read(base.join("image.redb")).ok()?;
    let meta = RaftSnapshotMeta::decode(std::fs::read(base.join("meta")).ok()?.as_slice()).ok()?;
    Some((image, meta))
}

/// Trigger a snapshot on `host` and wait until the library reports it at
/// the applied position, then return the published image and meta.
pub async fn snapshot_image(dir: &TestDir, host: &RaftHost) -> (Vec<u8>, RaftSnapshotMeta) {
    // A proposal resolves once applied, so the store's recorded position
    // is current; the metrics watch lands a tick later.
    let applied = durable_position(host);
    wait_applied(host, applied).await;
    host.trigger_snapshot().await.unwrap();
    host.wait(Some(Duration::from_secs(30)))
        .metrics(
            |metrics| metrics.snapshot.is_some_and(|s| s.index >= applied),
            "snapshot at the applied position",
        )
        .await
        .unwrap();
    published_pair(dir.path()).expect("the generation is published")
}

/// Every `incoming-*` receive directory currently on disk.
pub fn incoming_dirs(dir: &Path) -> Vec<PathBuf> {
    let mut found: Vec<PathBuf> = std::fs::read_dir(snapshots_dir(dir))
        .unwrap()
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("incoming-"))
        })
        .collect();
    found.sort();
    found
}

/// `meta` renamed to `index` with its id recomputed over `image`: the
/// forged generation pointer of the R3 reproductions (self-consistent by
/// checksum, wrong about the content).
pub fn renamed(meta: &RaftSnapshotMeta, index: u64, image: &[u8]) -> RaftSnapshotMeta {
    let mut forged = meta.clone();
    forged
        .last_log_id
        .as_mut()
        .expect("meta names a position")
        .index = index;
    if let Some(stored) = forged.membership.as_mut() {
        if let Some(log_id) = stored.log_id.as_mut() {
            log_id.index = index;
        }
    }
    let digest = pipestream_search::sha256::digest(image);
    forged.snapshot_id = format!(
        "{}-{}-{}",
        index,
        image.len(),
        pipestream_search::sha256::to_hex(&digest)
    );
    forged.length = image.len() as u64;
    forged.sha256 = digest.to_vec();
    forged
}

/// A crafted prost meta for `image` naming `index` and `voters` in `group`:
/// the id format the receiver verifies, with the length and digest fields
/// the transport checks against it.
pub fn meta_pb_for(
    group: &SourceAuthorityIdentity,
    index: u64,
    image: &[u8],
    voters: &[u64],
) -> RaftSnapshotMeta {
    let digest = pipestream_search::sha256::digest(image);
    let log_id = RaftLogId {
        term: 1,
        node_id: NODE_ID,
        index,
    };
    RaftSnapshotMeta {
        format_version: 1,
        group: Some(group.clone()),
        last_log_id: Some(log_id),
        membership: Some(RaftStoredMembership {
            log_id: Some(log_id),
            membership: Some(RaftMembership {
                configs: vec![RaftVoterSet {
                    node_ids: voters.to_vec(),
                }],
                nodes: voters
                    .iter()
                    .map(|id| RaftNode {
                        node_id: *id,
                        addr: format!("127.0.0.1:{}", 9000 + id),
                    })
                    .collect(),
            }),
        }),
        snapshot_id: format!(
            "{}-{}-{}",
            index,
            image.len(),
            pipestream_search::sha256::to_hex(&digest)
        ),
        length: image.len() as u64,
        sha256: digest.to_vec(),
    }
}

/// The same meta with its `group` replaced (a forged origin) and nothing
/// else changed.
pub fn with_group(meta: &RaftSnapshotMeta, group: &SourceAuthorityIdentity) -> RaftSnapshotMeta {
    RaftSnapshotMeta {
        group: Some(group.clone()),
        ..meta.clone()
    }
}

// ---- two members over the transport --------------------------------------

fn fixtures() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/certs/raft")
}

fn pem(name: &str) -> Vec<u8> {
    std::fs::read(fixtures().join(name)).unwrap()
}

/// A directory binding nodes 1, 2 and 3 to their fixture certificates.
pub fn directory(group: &SourceAuthorityIdentity) -> Arc<PeerDirectory> {
    let directory = PeerDirectory::new(group);
    for node in [NODE_ID, MEMBER_ID, PEER_ID] {
        directory
            .register(
                node,
                certificate_sha256(&pem(&format!("node-{node}.pem"))).unwrap(),
            )
            .unwrap();
    }
    Arc::new(directory)
}

fn client_tls(node: u64) -> ClientTls {
    ClientTls {
        ca_pem: pem("ca.pem"),
        identity_pem: Some((
            pem(&format!("node-{node}.pem")),
            pem(&format!("node-{node}.key.pem")),
        )),
        domain: Some("localhost".into()),
    }
}

pub fn transport(
    node: u64,
    directory: &Arc<PeerDirectory>,
    listen: std::net::SocketAddr,
) -> ClusterTransport {
    ClusterTransport {
        directory: Arc::clone(directory),
        server_tls: ServerTls {
            cert_pem: pem(&format!("node-{node}.pem")),
            key_pem: pem(&format!("node-{node}.key.pem")),
            client_ca_pem: Some(pem("ca.pem")),
        },
        client_tls: client_tls(node),
        listen,
        advertise: None,
        limits: TransportLimits::default(),
    }
}

fn ephemeral() -> std::net::SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

/// A bootstrapped leader (node 1) and, once joined, a member (node 2) seeded
/// by the leader's snapshot; both in this process on loopback mTLS.
pub struct Cluster {
    pub group: SourceAuthorityIdentity,
    pub directory: Arc<PeerDirectory>,
    pub leader_dir: TestDir,
    pub member_dir: TestDir,
    pub leader: Option<RaftHost>,
    pub member: Option<RaftHost>,
    member_listen: Option<std::net::SocketAddr>,
}

impl Cluster {
    /// Node 1 bootstrapped with `policy` and `limits` in `name-leader`;
    /// node 2's directory prepared but not started.
    pub async fn bootstrap(
        name: &str,
        group: &SourceAuthorityIdentity,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
    ) -> Self {
        Self::bootstrap_with_config(name, group, policy, limits, &host_config()).await
    }

    /// [`bootstrap`](Self::bootstrap) with the leader under `config`.
    pub async fn bootstrap_with_config(
        name: &str,
        group: &SourceAuthorityIdentity,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
        config: &HostConfig,
    ) -> Self {
        let directory = directory(group);
        let leader_dir = TestDir::new(&format!("{name}-leader"));
        let member_dir = TestDir::new(&format!("{name}-member"));
        let leader = RaftHost::bootstrap_cluster(
            leader_dir.path(),
            group,
            NODE_ID,
            policy,
            limits,
            config,
            transport(NODE_ID, &directory, ephemeral()),
        )
        .await
        .unwrap();
        RaftHost::prepare_member(member_dir.path(), group, MEMBER_ID, policy, limits).unwrap();
        Self {
            group: group.clone(),
            directory,
            leader_dir,
            member_dir,
            leader: Some(leader),
            member: None,
            member_listen: None,
        }
    }

    pub fn leader(&self) -> &RaftHost {
        self.leader.as_ref().expect("leader running")
    }

    pub fn member(&self) -> &RaftHost {
        self.member.as_ref().expect("member running")
    }

    /// Start node 2 from its directory (on its recorded address after a
    /// restart) without adding it to the group.
    pub async fn start_member(&mut self) {
        self.start_member_with(&host_config()).await;
    }

    /// [`start_member`](Self::start_member) under `config` (e.g. a smaller
    /// `max_snapshot_bytes` than the leader's image).
    pub async fn start_member_with(&mut self, config: &HostConfig) {
        let listen = self.member_listen.unwrap_or_else(ephemeral);
        let member = RaftHost::start_member(
            self.member_dir.path(),
            &self.group,
            MEMBER_ID,
            config,
            transport(MEMBER_ID, &self.directory, listen),
        )
        .await
        .unwrap();
        self.member_listen = member.listen_addr();
        self.member = Some(member);
    }

    /// Add the started member as a learner: the leader builds a snapshot at
    /// its applied position, purges to it and seeds the member by image.
    pub async fn add_member(&self) {
        let addr = self.member().advertised_addr().unwrap().to_string();
        if let Err(error) = self.leader().add_learner(MEMBER_ID, &addr).await {
            // One borrow at a time (see `core_running`): a second live
            // borrow in the same expression would hold both cores.
            let member = self.member().metrics().borrow().clone();
            let leader = self.leader().metrics().borrow().clone();
            panic!("add_learner: {error}\nmember metrics: {member}\nleader metrics: {leader}");
        }
    }

    /// Start node 2 and seed it.
    pub async fn join_member(&mut self) {
        self.start_member().await;
        self.add_member().await;
    }

    /// Start node 2 and seed it from the leader's published generation by
    /// a raw push, WITHOUT adding it to the group: a detached replica at the
    /// leader's snapshot position that receives nothing by replication, so
    /// later crafted images can name a newer index than it holds.
    pub async fn seed_detached(&mut self) {
        let (image, meta) = published_pair(self.leader_dir.path()).expect("leader snapshot");
        self.start_member().await;
        self.push(&meta, &image).await.expect("seeding push");
        assert!(!self.member().awaiting_snapshot().unwrap());
    }

    /// Push `image` under `meta` at the member from the raw registered peer,
    /// carrying the leader's committed vote (the only vote a snapshot can
    /// arrive under).
    pub async fn push(&self, meta: &RaftSnapshotMeta, image: &[u8]) -> Result<(), Status> {
        let mut peer = peer_client(self.member().listen_addr().unwrap(), PEER_ID).await;
        let vote = current_vote(self.leader());
        install_chunks(
            &mut peer,
            &self.group,
            PEER_ID,
            MEMBER_ID,
            &vote,
            meta,
            image,
        )
        .await
        .map(|_| ())
    }

    pub async fn stop_member(&mut self) {
        if let Some(member) = self.member.take() {
            let _ = member.shutdown().await;
        }
    }

    pub async fn restart_member(&mut self) {
        self.stop_member().await;
        self.start_member().await;
    }

    pub async fn restart_leader(&mut self) {
        let leader = self.leader.take().expect("leader running");
        let listen = leader.listen_addr().unwrap();
        let _ = leader.shutdown().await;
        let leader = RaftHost::start_member(
            self.leader_dir.path(),
            &self.group,
            NODE_ID,
            &host_config(),
            transport(NODE_ID, &self.directory, listen),
        )
        .await
        .unwrap();
        leader
            .wait(Some(Duration::from_secs(30)))
            .state(openraft::ServerState::Leader, "leader re-elected")
            .await
            .unwrap();
        self.leader = Some(leader);
    }

    pub async fn shutdown(mut self) {
        self.stop_member().await;
        if let Some(leader) = self.leader.take() {
            let _ = leader.shutdown().await;
        }
    }
}

// ---- a raw registered peer ---------------------------------------------------

/// A raw transport client presenting node `node`'s certificate.
pub async fn peer_client(addr: std::net::SocketAddr, node: u64) -> RaftTransportClient<Channel> {
    let endpoint = apply_client_tls(
        Endpoint::from_shared(format!("https://{addr}"))
            .unwrap()
            .timeout(Duration::from_secs(30)),
        Some(&client_tls(node)),
    )
    .unwrap();
    RaftTransportClient::new(endpoint.connect().await.unwrap())
        .max_decoding_message_size(16 << 20)
        .max_encoding_message_size(16 << 20)
}

/// A request with the timing agreement every peer must present.
pub fn timed<T>(message: T) -> Request<T> {
    let mut request = Request::new(message);
    request.metadata_mut().insert(
        TIMING_METADATA,
        host_config().timing_agreement().parse().unwrap(),
    );
    request
}

pub fn header(group: &SourceAuthorityIdentity, from: u64, to: u64) -> RaftRpcHeader {
    RaftRpcHeader {
        protocol_version: 1,
        group: Some(group.clone()),
        from_node_id: from,
        to_node_id: to,
    }
}

/// The receiver's current vote, which an install must carry to be accepted
/// by the library at all.
pub fn current_vote(host: &RaftHost) -> RaftVote {
    let vote = host.metrics().borrow().vote;
    RaftVote {
        term: vote.leader_id.term,
        node_id: vote.leader_id.node_id,
        committed: vote.committed,
    }
}

/// One chunk of `image` at `offset` under `meta`.
pub fn chunk_request(
    group: &SourceAuthorityIdentity,
    from: u64,
    to: u64,
    vote: &RaftVote,
    meta: &RaftSnapshotMeta,
    offset: usize,
    data: &[u8],
    done: bool,
) -> RaftInstallSnapshotRequest {
    RaftInstallSnapshotRequest {
        header: Some(header(group, from, to)),
        vote: Some(vote.clone()),
        meta: Some(meta.clone()),
        offset: offset as u64,
        data: data.to_vec(),
        done,
    }
}

/// Append `count` blank entries at indexes `0..count` to `to` from
/// registered peer `from` under `vote`, with nothing committed: the
/// receiver's log advances and its state machine applies none of them.
/// `vote` must be a committed vote (the transport refuses appends under
/// any other), and openraft numbers a log from index 0, so the first
/// entry carries no previous log id.
pub async fn append_blanks(
    client: &mut RaftTransportClient<Channel>,
    group: &SourceAuthorityIdentity,
    from: u64,
    to: u64,
    vote: &RaftVote,
    count: u64,
) -> Result<(), Status> {
    for index in 0..count {
        client
            .append_entries(timed(RaftAppendEntriesRequest {
                header: Some(header(group, from, to)),
                vote: Some(*vote),
                prev_log_id: index.checked_sub(1).map(|previous| RaftLogId {
                    term: vote.term,
                    node_id: vote.node_id,
                    index: previous,
                }),
                entries: vec![RaftEntry {
                    log_id: Some(RaftLogId {
                        term: vote.term,
                        node_id: vote.node_id,
                        index,
                    }),
                    payload: Some(Payload::Blank(true)),
                }],
                leader_commit: None,
            }))
            .await?;
    }
    Ok(())
}

/// Stream `bytes` to `to` as chunks of [`CHUNK_BYTES`] under `meta` from
/// registered peer `from`, the last chunk marked done. Returns the replies,
/// or the first refusing status.
pub async fn install_chunks(
    client: &mut RaftTransportClient<Channel>,
    group: &SourceAuthorityIdentity,
    from: u64,
    to: u64,
    vote: &RaftVote,
    meta: &RaftSnapshotMeta,
    bytes: &[u8],
) -> Result<Vec<RaftInstallSnapshotResponse>, Status> {
    let mut replies = Vec::new();
    let mut offset = 0usize;
    let chunks: Vec<&[u8]> = if bytes.is_empty() {
        vec![&[][..]]
    } else {
        bytes.chunks(CHUNK_BYTES).collect()
    };
    for (i, chunk) in chunks.iter().enumerate() {
        let done = i + 1 == chunks.len();
        let reply = client
            .install_snapshot(timed(chunk_request(
                group, from, to, vote, meta, offset, chunk, done,
            )))
            .await?
            .into_inner();
        if let Some(InstallOutcome::Mismatch(mismatch)) = &reply.outcome {
            return Err(Status::aborted(format!(
                "snapshot segment mismatch: expected {}@{}, got {}@{}",
                mismatch.expect_id, mismatch.expect_offset, mismatch.got_id, mismatch.got_offset
            )));
        }
        replies.push(reply);
        offset += chunk.len();
    }
    Ok(replies)
}

/// Guard so a panicking test still shuts its host down: call
/// `shutdown().await` on the happy path; the drop path spawns the shutdown
/// on the running runtime, which the test runtime reaps.
pub struct HostGuard(Option<RaftHost>);

impl HostGuard {
    pub fn new(host: RaftHost) -> Self {
        Self(Some(host))
    }
    pub async fn shutdown(mut self) {
        self.0.take().unwrap().shutdown().await.unwrap();
    }
}

impl Deref for HostGuard {
    type Target = RaftHost;
    fn deref(&self) -> &RaftHost {
        self.0.as_ref().unwrap()
    }
}

impl Drop for HostGuard {
    fn drop(&mut self) {
        if let (Some(host), Ok(handle)) = (self.0.take(), tokio::runtime::Handle::try_current()) {
            let _ = handle.spawn(async move {
                let _ = host.shutdown().await;
            });
        }
    }
}

/// The voter ids of the membership a host reports.
pub fn voters(host: &RaftHost) -> BTreeSet<u64> {
    host.metrics()
        .borrow()
        .membership_config
        .membership()
        .voter_ids()
        .collect()
}

// ---- three voters over the transport (slice 4b) ------------------------------
//
// The admission-timing target needs a group whose surviving pair can elect a
// successor while the old leader is isolated, so it builds on a third voter
// instead of the two-member `Cluster` above. Mirrors the in-crate
// `src/raft/transport_tests.rs::three_voters` recipe.

use pipestream_search::pb::storage::{
    source_authority_command::Action, LogicalSourceOwner, SourceAuthorityCommand,
};

/// A throwaway owner for the bootstrap prepare; distinct from any bridge
/// owner the tests prepare afterwards (a second Prepare on a held key
/// refuses).
fn bootstrap_owner_key() -> LogicalSourceOwner {
    LogicalSourceOwner {
        owner_id: b"admission-bootstrap".to_vec(),
        ..super::kit::key()
    }
}

/// A three-voter group over loopback mTLS with control-revision tracking.
pub struct Voters {
    pub group: SourceAuthorityIdentity,
    pub directory: Arc<PeerDirectory>,
    dirs: BTreeMap<u64, TestDir>,
    hosts: BTreeMap<u64, RaftHost>,
    /// The control revision the next command must expect.
    pub revision: u64,
    /// The timing every member of this group runs under; a member started
    /// again on its directory takes the same one.
    pub config: HostConfig,
}

impl Voters {
    pub fn host(&self, node: u64) -> &RaftHost {
        &self.hosts[&node]
    }

    /// Take a host out to wrap in an `Arc` (the paused-grant recipe spawns
    /// a `with_admission` against it); put it back with [`Voters::insert`].
    pub fn take(&mut self, node: u64) -> RaftHost {
        self.hosts.remove(&node).unwrap()
    }

    pub fn insert(&mut self, node: u64, host: RaftHost) {
        self.hosts.insert(node, host);
    }

    pub fn nodes(&self) -> Vec<u64> {
        self.hosts.keys().copied().collect()
    }

    /// The on-disk directory of `node`'s member state, for a restart on
    /// the same directory the member served from.
    pub fn member_dir(&self, node: u64) -> PathBuf {
        self.dirs[&node].path().to_path_buf()
    }

    /// Poll until some host leads; bounded and loud.
    pub async fn leader(&self) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for host in self.hosts.values() {
                if host.metrics().borrow().state == openraft::ServerState::Leader {
                    return host.node_id();
                }
            }
            assert!(Instant::now() < deadline, "no leader within 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Wait until some running host other than `not` believes in a leader
    /// that is not `not`; returns that leader.
    pub async fn leader_other_than(&self, not: u64) -> u64 {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for (id, host) in &self.hosts {
                if *id == not {
                    continue;
                }
                if let Some(leader) = host.believed_leader() {
                    if leader != not
                        && self.hosts[&leader].metrics().borrow().state
                            == openraft::ServerState::Leader
                    {
                        return leader;
                    }
                }
            }
            assert!(Instant::now() < deadline, "no other leader within 10 s");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Propose `action` on `leader`, asserting the commit, and advance the
    /// revision tracker.
    pub async fn propose(
        &mut self,
        leader: u64,
        principal: &str,
        id: &str,
        key: &LogicalSourceOwner,
        action: Action,
    ) -> pipestream_search::pb::storage::SourceAuthorityDecision {
        let command = SourceAuthorityCommand {
            format_version: 1,
            authority: Some(self.group.clone()),
            key: Some(key.clone()),
            command_id: id.as_bytes().to_vec(),
            expected_control_revision: self.revision,
            expected_policy_revision: 1,
            expected_ownership_generation: 0,
            action: Some(action),
        };
        let decision = self
            .host(leader)
            .propose_command(principal, &command)
            .await
            .unwrap();
        assert_eq!(decision.code, 0, "{}", decision.message);
        self.revision += 1;
        decision
    }

    /// Poll raft metrics until `node` has applied at least `index`.
    pub async fn wait_applied(&self, node: u64, index: u64) {
        self.host(node)
            .wait(Some(Duration::from_secs(30)))
            .applied_index_at_least(Some(index), "applied")
            .await
            .unwrap_or_else(|e| panic!("applied position never reached index {index}: {e}"));
    }

    /// Prepare and start `node`, let `leader` seed it by snapshot, and
    /// wait until the seed is installed.
    async fn join(&mut self, node: u64, leader: u64) {
        let dir = TestDir::new(&format!("voters-{node}"));
        RaftHost::prepare_member(dir.path(), &self.group, node, &policy(), &limits()).unwrap();
        let host = RaftHost::start_member(
            dir.path(),
            &self.group,
            node,
            &self.config,
            transport(node, &self.directory, ephemeral()),
        )
        .await
        .unwrap();
        assert!(host.awaiting_snapshot().unwrap());
        let addr = host.advertised_addr().unwrap().to_string();
        self.dirs.insert(node, dir);
        self.hosts.insert(node, host);
        self.host(leader).add_learner(node, &addr).await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        while self.host(node).awaiting_snapshot().unwrap() {
            assert!(Instant::now() < deadline, "member {node} never seeded");
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    pub async fn shutdown(mut self) {
        for (_, host) in std::mem::take(&mut self.hosts) {
            let _ = host.shutdown().await;
        }
    }
}

/// Bootstrap node 1 with a throwaway prepared owner, seed 2 and 3 from its
/// snapshot and promote them: a group of three voters on `policy` under
/// the kit's fast timing.
pub async fn three_voters(name: &str, policy: &AccessPolicy) -> Voters {
    three_voters_with(name, policy, &host_config()).await
}

/// `three_voters` under an explicit timing, for scenarios that park a
/// write inside its transaction for longer than the kit's 100 ms lease
/// allows (the lease plus the skew must stay under the election floor,
/// `HostConfig::validate`).
pub async fn three_voters_with(name: &str, policy: &AccessPolicy, config: &HostConfig) -> Voters {
    let group = identity(SEED);
    let directory = directory(&group);
    let dir = TestDir::new(&format!("{name}-1"));
    let leader = RaftHost::bootstrap_cluster(
        dir.path(),
        &group,
        NODE_ID,
        policy,
        &limits(),
        config,
        transport(NODE_ID, &directory, ephemeral()),
    )
    .await
    .unwrap();
    let mut voters = Voters {
        group,
        directory,
        dirs: BTreeMap::from([(1, dir)]),
        hosts: BTreeMap::from([(1, leader)]),
        revision: 1,
        config: config.clone(),
    };
    let target = pipestream_search::pb::storage::SourceStorageTarget {
        node_id: "server-a".into(),
        storage_incarnation: vec![41; 16],
        history_id: vec![42; 16],
        residency: pipestream_search::pb::storage::SourceResidency::Server as i32,
        resident_device_id: String::new(),
    };
    voters
        .propose(
            1,
            "alice",
            "bootstrap-prepare",
            &bootstrap_owner_key(),
            Action::Prepare(pipestream_search::pb::storage::PrepareSourceOwner {
                workflow_id: b"admission-bootstrap".to_vec(),
                target: Some(target),
            }),
        )
        .await;
    voters.join(2, 1).await;
    voters.join(3, 1).await;
    voters
        .host(1)
        .promote(BTreeSet::from([2, 3]))
        .await
        .unwrap();
    voters
        .host(1)
        .wait(Some(Duration::from_secs(30)))
        .voter_ids([1, 2, 3], "three voters")
        .await
        .unwrap();
    voters
}
