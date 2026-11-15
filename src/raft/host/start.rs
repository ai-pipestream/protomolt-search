//! Starting a host: a single node, a bootstrapped cluster, or a member
//! joining one.

use super::*;

impl RaftHost {
    pub(super) fn raft_config(config: &HostConfig) -> Result<Arc<Config>, Status> {
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
    pub(super) async fn start_networked(
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

    pub(super) async fn start_with<N: RaftNetworkFactory<ControlRaft>>(
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

    pub(super) async fn initialize(&self, addr: &str) -> Result<(), Status> {
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
}
