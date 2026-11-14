//! Building, bootstrapping and starting a `RaftHost`, and its read-only
//! accessors and gauges.

use super::*;

impl RaftHost {
    fn raft_config(config: &HostConfig) -> Result<Arc<Config>, Status> {
        config.validate()?;
        let raft = Config {
            cluster_name: config.cluster_name.clone(),
            heartbeat_interval: config.heartbeat_interval_ms,
            election_timeout_min: config.election_timeout_min_ms,
            election_timeout_max: config.election_timeout_max_ms,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(config.snapshot_logs_since_last),
            snapshot_max_chunk_size: config.snapshot_chunk_bytes,
            install_snapshot_timeout: config.install_snapshot_timeout_ms,
            max_payload_entries: config.max_payload_entries,
            max_in_snapshot_log_to_keep: config.max_in_snapshot_log_to_keep,
            ..Default::default()
        }
        .validate()
        .map_err(|e| Status::invalid_argument(format!("raft config: {e}")))?;
        Ok(Arc::new(raft))
    }

    /// Explicit one-time bootstrap of a group of one voter: creates the
    /// store and the log in `dir` and commits the initial membership.
    pub async fn bootstrap_single(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        addr: &str,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
        config: &HostConfig,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::create(&dir.join(STORE_FILE), identity, policy, limits)?;
        let log_store = RaftLogStore::create(&dir.join(LOG_FILE), identity, node_id)?;
        let host =
            Self::start_with(dir, store, log_store, node_id, config, NoNetwork, None).await?;
        host.initialize(addr).await?;
        Ok(host)
    }

    /// Start from existing durable state. A missing store or log refuses;
    /// nothing is created on the normal startup path.
    pub async fn start(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        config: &HostConfig,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::open(&dir.join(STORE_FILE), identity)?;
        let log_store = RaftLogStore::open(&dir.join(LOG_FILE), identity, node_id)?;
        Self::start_with(dir, store, log_store, node_id, config, NoNetwork, None).await
    }

    /// Bootstrap a networked group of one voter: the first member, from
    /// which every later member is seeded by snapshot.
    #[cfg(feature = "tls")]
    pub async fn bootstrap_cluster(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
        config: &HostConfig,
        transport: ClusterTransport,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::create(&dir.join(STORE_FILE), identity, policy, limits)?;
        let log_store = RaftLogStore::create(&dir.join(LOG_FILE), identity, node_id)?;
        let host =
            Self::start_networked(dir, identity, store, log_store, node_id, config, transport)
                .await?;
        let advertised = host.advertised_addr().expect("networked host").to_string();
        host.initialize(&advertised).await?;
        Ok(host)
    }

    /// Create the durable state of a member that will join by snapshot:
    /// a store marked pending, on which no log entry applies until the
    /// group's image replaces it, and an empty log. No host is started.
    pub fn prepare_member(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        policy: &AccessPolicy,
        limits: &SourceAuthorityLimits,
    ) -> Result<(), Status> {
        std::fs::create_dir_all(dir)
            .map_err(|e| Status::internal(format!("member directory: {e}")))?;
        SourceAuthorityStore::create_member(&dir.join(STORE_FILE), identity, policy, limits)?;
        RaftLogStore::create(&dir.join(LOG_FILE), identity, node_id)?;
        Ok(())
    }

    /// Start a networked member from existing durable state (a prepared
    /// member, or any member restarting). Nothing is created.
    #[cfg(feature = "tls")]
    pub async fn start_member(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        node_id: NodeId,
        config: &HostConfig,
        transport: ClusterTransport,
    ) -> Result<Self, Status> {
        let store = SourceAuthorityStore::open(&dir.join(STORE_FILE), identity)?;
        let log_store = RaftLogStore::open(&dir.join(LOG_FILE), identity, node_id)?;
        Self::start_networked(dir, identity, store, log_store, node_id, config, transport).await
    }

    #[cfg(feature = "tls")]
    async fn start_networked(
        dir: &Path,
        identity: &SourceAuthorityIdentity,
        store: SourceAuthorityStore,
        log_store: RaftLogStore,
        node_id: NodeId,
        config: &HostConfig,
        transport: ClusterTransport,
    ) -> Result<Self, Status> {
        transport.limits.validate(config.snapshot_chunk_bytes)?;
        if transport.directory.group() != identity {
            return Err(Status::invalid_argument(
                "peer directory is for another group",
            ));
        }
        if !transport.directory.knows(node_id) {
            return Err(Status::failed_precondition(format!(
                "peer directory does not register this node ({node_id}); its own certificate must be bound before it serves"
            )));
        }
        let socket = tokio::net::TcpListener::bind(transport.listen)
            .await
            .map_err(|e| Status::unavailable(format!("raft listener {}: {e}", transport.listen)))?;
        let addr = socket
            .local_addr()
            .map_err(|e| Status::internal(format!("raft listener address: {e}")))?;
        let isolation = Isolation::default();
        let timing = config.timing_agreement();
        let factory = TonicNetworkFactory::new(
            node_id,
            Arc::clone(&transport.directory),
            transport.client_tls,
            transport.limits.clone(),
            isolation.clone(),
            timing.clone(),
        );
        let peer_rejections = factory.rejections();
        let forwarder = factory.forwarder();
        let mut host =
            Self::start_with(dir, store, log_store, node_id, config, factory, None).await?;
        host.forwarder = Some(forwarder);
        host.digest_watch = Some(tokio::spawn(rebuild_on_digest_rejection(
            host.node_id,
            peer_rejections.subscribe(),
            Arc::clone(&host.published),
            host.raft.clone(),
        )));
        host.peer_rejections = Some(peer_rejections);
        let service = RaftTransportService::new(
            host.raft.clone(),
            node_id,
            Arc::clone(&transport.directory),
            isolation.clone(),
            timing,
            host.hold.clone(),
            Arc::clone(&host.staging),
            Arc::clone(&host.snapshot_rejections),
            &transport.limits,
            host.read_timeout,
        );
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tonic::transport::Server::builder()
            .tls_config(transport.server_tls.server_config(true))
            .map_err(|e| Status::internal(format!("raft listener TLS: {e}")))?
            .add_service(service.into_server(&transport.limits))
            .serve_with_incoming_shutdown(crate::harness::nodelay_incoming(socket), async {
                let _ = stopped.await;
            });
        let task = tokio::spawn(server);
        host.listener = Some(Listener {
            addr,
            advertised: transport.advertise.unwrap_or_else(|| addr.to_string()),
            stop: Some(stop),
            task,
        });
        host.directory = Some(transport.directory);
        host.isolation = isolation;
        Ok(host)
    }

    async fn start_with<N: RaftNetworkFactory<ControlRaft>>(
        dir: &Path,
        store: SourceAuthorityStore,
        log_store: RaftLogStore,
        node_id: NodeId,
        config: &HostConfig,
        network: N,
        listener: Option<Listener>,
    ) -> Result<Self, Status> {
        let machine =
            ControlStateMachine::new(store, &dir.join(SNAPSHOT_DIR), config.max_snapshot_bytes)?;
        let shared = machine.shared_store();
        let staging = machine.staging();
        let last_build = machine.last_build();
        let published = machine.published_image();
        #[cfg(any(test, feature = "fault-injection"))]
        let snapshot_gates = machine.gates();
        #[cfg(any(test, feature = "fault-injection"))]
        let install_fault = machine.install_fault();
        let mut log_store = log_store;
        log_store
            .bind_applied_floor(Arc::clone(&shared))
            .map_err(|e| Status::data_loss(format!("raft log and store disagree: {e}")))?;
        let raft = Raft::new(
            node_id,
            Self::raft_config(config)?,
            network,
            log_store.clone(),
            machine,
        )
        .await
        .map_err(|e| Status::internal(format!("raft start: {e}")))?;
        Ok(Self {
            raft,
            store: shared,
            log_store,
            node_id,
            dir: dir.to_path_buf(),
            lease: Duration::from_millis(config.admission_lease_ms),
            read_timeout: config.read_timeout(),
            hold: LeaseHold::new(Duration::from_millis(config.election_timeout_max_ms)),
            staging,
            last_build,
            published,
            listener,
            #[cfg(feature = "tls")]
            digest_watch: None,
            #[cfg(feature = "tls")]
            directory: None,
            #[cfg(feature = "tls")]
            peer_rejections: None,
            #[cfg(feature = "tls")]
            snapshot_rejections: Arc::new(SnapshotRejections::default()),
            #[cfg(feature = "tls")]
            isolation: Isolation::default(),
            #[cfg(feature = "tls")]
            forwarder: None,
            #[cfg(any(test, feature = "fault-injection"))]
            grant_gate: std::sync::Mutex::new(None),
            #[cfg(any(test, feature = "fault-injection"))]
            snapshot_gates,
            #[cfg(any(test, feature = "fault-injection"))]
            install_fault,
        })
    }

    async fn initialize(&self, addr: &str) -> Result<(), Status> {
        let mut members = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: addr.to_string(),
            },
        );
        self.raft
            .initialize(members)
            .await
            .map_err(|e| Status::internal(format!("raft initialize: {e}")))?;
        self.raft
            .wait(Some(Duration::from_secs(30)))
            .state(openraft::ServerState::Leader, "bootstrap leader")
            .await
            .map_err(|e| Status::internal(format!("raft bootstrap: {e}")))?;
        Ok(())
    }

    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where the transport listens, for a networked member.
    pub fn listen_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.as_ref().map(|l| l.addr)
    }

    /// The address peers dial, for a networked member.
    pub fn advertised_addr(&self) -> Option<&str> {
        self.listener.as_ref().map(|l| l.advertised.as_str())
    }

    /// Every entry the log holds, in index order: a read for audits.
    pub fn log_entries(&self) -> Result<Vec<openraft::Entry<ControlRaft>>, Status> {
        self.log_store.entries()
    }

    pub fn metrics(&self) -> watch::Receiver<RaftMetrics<NodeId, BasicNode>> {
        self.raft.metrics()
    }

    /// Wait for a metrics condition (state, applied index, snapshot).
    pub fn wait(
        &self,
        timeout: Option<Duration>,
    ) -> openraft::metrics::Wait<NodeId, BasicNode, openraft::TokioRuntime> {
        self.raft.wait(timeout)
    }

    /// The applied position the store recorded, as a read.
    pub fn applied_position(&self) -> Result<Option<crate::pb::storage::RaftLogId>, Status> {
        Ok(self.store()?.raft_applied()?.and_then(|a| a.last_applied))
    }

    /// Whether this member's store is still the prepared genesis image
    /// waiting for the group's first snapshot.
    pub fn awaiting_snapshot(&self) -> Result<bool, Status> {
        self.store()?.awaiting_snapshot()
    }

    /// The latest build of this node's own store: its size against the
    /// bound this node applies to images it receives.
    pub fn last_snapshot_build(&self) -> Option<super::super::state_machine::SnapshotBuildReport> {
        self.last_build.lock().ok().and_then(|last| last.clone())
    }

    /// The latest rejection each peer answered to this node's RPCs (empty
    /// for a node with no transport).
    #[cfg(feature = "tls")]
    pub fn peer_rejections(&self) -> BTreeMap<NodeId, PeerRejection> {
        self.peer_rejections
            .as_ref()
            .map(PeerRejections::latest)
            .unwrap_or_default()
    }

    /// Installs this node rejected at the announce, before any byte.
    #[cfg(feature = "tls")]
    pub fn snapshot_rejections(&self) -> &SnapshotRejections {
        &self.snapshot_rejections
    }

    /// The leader this node currently believes in, if any: an applied
    /// view, never an admission.
    pub fn believed_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// This member's gauges, sampled live for the metrics page: the
    /// same reads `member_status` serves, as values. The store reads
    /// fall back to the not-ready zero (not the leader, nothing
    /// applied, awaiting snapshot unknown as false) rather than fail
    /// a scrape.
    pub fn member_gauges(&self) -> crate::metrics::MemberGauges {
        let metrics = self.raft.metrics().borrow().clone();
        let build = self.last_snapshot_build();
        crate::metrics::MemberGauges {
            is_leader: metrics.state == openraft::ServerState::Leader,
            applied_index: metrics.last_applied.map(|id| id.index).unwrap_or(0),
            last_log_index: metrics.last_log_index.unwrap_or(0),
            awaiting_snapshot: self.awaiting_snapshot().unwrap_or(false),
            #[cfg(feature = "tls")]
            peer_rejections: self
                .peer_rejections()
                .iter()
                .map(|(node, rejection)| crate::metrics::PeerRejectionGauges {
                    peer: *node,
                    action: rejection.action.to_string(),
                    code: format!("{:?}", rejection.code),
                    count: rejection.count,
                })
                .collect(),
            #[cfg(not(feature = "tls"))]
            peer_rejections: Vec::new(),
            #[cfg(feature = "tls")]
            snapshot_rejections: self.snapshot_rejections().count(),
            #[cfg(not(feature = "tls"))]
            snapshot_rejections: 0,
            last_snapshot_build_bytes: build.as_ref().map(|build| build.bytes).unwrap_or(0),
            last_snapshot_receiver_bound_bytes: build
                .as_ref()
                .map(|build| build.receiver_bound)
                .unwrap_or(0),
        }
    }

    /// A scrape-time sampler of [`RaftHost::member_gauges`] for the
    /// metrics page.
    pub fn member_gauge_provider(host: Arc<Self>) -> crate::metrics::MemberGaugeProvider {
        Box::new(move || host.member_gauges())
    }
}
