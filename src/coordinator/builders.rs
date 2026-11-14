//! Builders.

use super::*;

impl CoordinatorServiceImpl {
    /// A coordinator over the given shard nodes (fan-out order = shard
    /// index for merge tie-breaks).
    pub fn new(node_addrs: Vec<String>) -> Self {
        let stats_cache = Arc::new(crate::stats_cache::StatsCache::new(node_addrs.len()));
        Self {
            node_addrs,
            collection: String::new(),
            cursor_signer: Arc::new(crate::query_cursor::CursorSigner::default()),
            client_tls: None,
            udp_hmac_key: None,
            replica_addrs: Vec::new(),
            analysis_addr: None,
            bm25_params: Bm25Params::default(),
            limits: FanoutLimits::default(),
            stream_search: false,
            stream_finish_settle: None,
            stream_finish_deadline: None,
            bm25_stream: false,
            knobs: Arc::new(crate::diagnostics::Knobs::coordinator(
                "coordinator",
                crate::diagnostics::CoordinatorKnobValues {
                    max_k: DEFAULT_MAX_K,
                    hedge_delay_ms: 0,
                    shard_pruning: true,
                    signal_batch: DEFAULT_SIGNAL_BATCH,
                },
                Vec::new(),
            )),
            max_rerank_bytes: DEFAULT_MAX_RERANK_BYTES,
            links: Arc::new(Mutex::new(HashMap::new())),
            allow_network: true,
            floor_socket: Arc::new(std::sync::OnceLock::new()),
            floor_targets: Arc::new(Mutex::new(HashMap::new())),
            shard_vector_rows: Arc::new(Mutex::new(HashMap::new())),
            stats_cache,
            document_visibility: None,
            field_permissions: None,
            query_read_versions: None,
            vector_read_field: None,
            #[cfg(feature = "net")]
            clustered_vectors: None,
            dense_quality_profile: None,
            synonyms: None,
            dense_execution_policy: None,
            topology_generation: 0,
            hash_ranges: Vec::new(),
            placement_codes: Vec::new(),
            placement: None,
            derived: None,
            live_topology: None,
            topology_watch: Arc::new(watch::channel(0).0),
            write_gate: Arc::new(tokio::sync::RwLock::new(())),
            cutover_guard: Arc::new(Mutex::new(None)),
            cutover_pending: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            phrase_index: None,
            query_progress: None,
            surface_lexicon: None,
            spellchecker: None,
            one_pass_query: None,
            rerank_spec: None,
        }
    }

    /// A coordinator whose shard traffic is restricted to the supplied
    /// in-process channels. Cache misses never fall back to TCP and the UDP
    /// signal lane is disabled. The labels are diagnostic identities only;
    /// no address parsing or name resolution occurs.
    /// A coordinator over nodes living in this process, reached through
    /// [`crate::link::NodeLink::Local`]: no network fallback, no UDP lane.
    pub fn with_local_nodes(nodes: Vec<Arc<crate::node::NodeServiceImpl>>) -> Self {
        let node_addrs: Vec<String> = (0..nodes.len())
            .map(|shard| format!("in-process://shard-{shard}"))
            .collect();
        let links = node_addrs
            .iter()
            .cloned()
            .zip(nodes.into_iter().map(crate::link::NodeLink::local))
            .collect::<HashMap<_, _>>();
        let mut coordinator = Self::new(node_addrs);
        coordinator.links = Arc::new(Mutex::new(links));
        coordinator.allow_network = false;
        coordinator
    }

    /// Serve plain vector `Search` over the streaming protocol: shards
    /// emit above the relayed floor and this coordinator holds the only
    /// top-k. Results are identical to the default fan-out; the modes
    /// differ in where pruning happens, not in what they return.
    pub fn with_stream_search(mut self, on: bool) -> Self {
        self.stream_search = on;
        self
    }

    /// Configure the coordinator's default streaming settle window before
    /// issuing `FinishStreamSearch` to unfinished ANN shards.
    pub fn with_stream_finish_settle(mut self, settle: Option<Duration>) -> Self {
        self.stream_finish_settle = settle;
        self
    }

    /// Configure the coordinator's default streaming deadline before issuing
    /// `FinishStreamSearch { deadline: true }` to unfinished ANN shards.
    pub fn with_stream_finish_deadline(mut self, deadline: Option<Duration>) -> Self {
        self.stream_finish_deadline = deadline;
        self
    }

    /// Serve flat `Bm25Search` over the `Bm25QueryStream` floor relay:
    /// shards publish their running k-th best, this coordinator relays
    /// the fleet maximum back, and block-max turns each raise into
    /// blocks never read. Results are identical to the unary fan-out.
    pub fn with_bm25_stream(mut self, on: bool) -> Self {
        self.bm25_stream = on;
        self
    }

    /// Set the hard cap on client-facing `k` (also the depth a request
    /// omitting `k` runs at). Zero is rejected at config parse time, so
    /// this takes the already-validated value.
    pub fn with_max_k(mut self, max_k: u32) -> Self {
        self.rebuild_knobs(
            max_k,
            self.knobs.hedge_delay(),
            self.knobs.shard_pruning(),
            self.knobs.signal_batch(),
        );
        self.refresh_fixed_knobs();
        self
    }

    /// Candidate ids per rescore call for a boolean group's clauses
    /// ([`DEFAULT_SIGNAL_BATCH`]). Zero is rejected at config parse
    /// time. Live afterwards as the `signal_batch` knob; `max_k` is the
    /// earlier behavior for an A/B.
    pub fn with_signal_batch(mut self, batch: u32) -> Self {
        self.rebuild_knobs(
            self.knobs.max_k(),
            self.knobs.hedge_delay(),
            self.knobs.shard_pruning(),
            batch,
        );
        self.refresh_fixed_knobs();
        self
    }

    /// Ids per rescore call, live.
    pub fn signal_batch(&self) -> usize {
        self.knobs.signal_batch().max(1) as usize
    }

    /// Whether a filtered request skips the shards its placement leaf
    /// rules out (`docs/placement.md`); `false` is the A/B switch and
    /// changes no answer. Live afterwards as the `shard_pruning` knob.
    pub fn with_shard_pruning(mut self, enabled: bool) -> Self {
        self.rebuild_knobs(
            self.knobs.max_k(),
            self.knobs.hedge_delay(),
            enabled,
            self.knobs.signal_batch(),
        );
        self.refresh_fixed_knobs();
        self
    }

    /// Builders run before the coordinator is shared, so a knob change
    /// at build time replaces the set; the live values carry over.
    pub(super) fn rebuild_knobs(
        &mut self,
        max_k: u32,
        hedge_delay: Option<Duration>,
        shard_pruning: bool,
        signal_batch: u32,
    ) {
        self.knobs = Arc::new(crate::diagnostics::Knobs::coordinator(
            self.knobs.process().to_string(),
            crate::diagnostics::CoordinatorKnobValues {
                max_k,
                hedge_delay_ms: hedge_delay.map_or(0, |d| d.as_millis() as u64),
                shard_pruning,
                signal_batch,
            },
            Vec::new(),
        ));
    }

    /// The read-at-startup settings listed beside the live knobs.
    pub(super) fn refresh_fixed_knobs(&self) {
        use crate::diagnostics::FixedKnob;
        use crate::pb::KnobKind;
        self.knobs.set_fixed(vec![
            FixedKnob {
                name: "collection",
                kind: KnobKind::String,
                value: self.collection.clone(),
                description: "The collection this coordinator serves.",
            },
            FixedKnob {
                name: "nodes",
                kind: KnobKind::Int,
                value: self.node_addrs.len().to_string(),
                description: "Shard nodes in the construction-time shard map.",
            },
            FixedKnob {
                name: "replicas",
                kind: KnobKind::Int,
                value: self
                    .replica_addrs
                    .iter()
                    .filter(|r| r.is_some())
                    .count()
                    .to_string(),
                description: "Shards with a replica configured.",
            },
            FixedKnob {
                name: "stream_search",
                kind: KnobKind::Bool,
                value: self.stream_search.to_string(),
                description: "Vector legs over the streaming shard search (--stream-search).",
            },
            FixedKnob {
                name: "bm25_stream",
                kind: KnobKind::Bool,
                value: self.bm25_stream.to_string(),
                description: "Lexical legs over the streaming BM25 query (--bm25-stream).",
            },
            FixedKnob {
                name: "max_rerank_bytes",
                kind: KnobKind::Int,
                value: self.max_rerank_bytes.to_string(),
                description: "Largest exact-rerank pool in FP32 bytes (--max-rerank-bytes).",
            },
            FixedKnob {
                name: "shard_deadline_ms",
                kind: KnobKind::Int,
                value: self
                    .limits
                    .shard_deadline
                    .map_or(0, |d| d.as_millis() as u64)
                    .to_string(),
                description: "Bound on one shard's attempt; 0 is none (--shard-deadline-ms).",
            },
            FixedKnob {
                name: "dense_execution_policy",
                kind: KnobKind::Bool,
                value: self.dense_execution_policy.is_some().to_string(),
                description: "A dense execution policy is installed (--dense-execution-policy).",
            },
        ]);
    }

    /// The knobs this coordinator reads at request time.
    pub fn knobs(&self) -> &Arc<crate::diagnostics::Knobs> {
        &self.knobs
    }

    /// Fan-out limits with the live hedge delay.
    pub(super) fn limits(&self) -> FanoutLimits {
        FanoutLimits {
            shard_deadline: self.limits.shard_deadline,
            hedge_delay: self.knobs.hedge_delay(),
        }
    }

    /// Layouts of this coordinator's shard nodes (docs/diagnostics.md):
    /// in-process nodes answer directly, remote ones through their own
    /// diagnostics service; a node without it is listed with the status
    /// it returned in `layout`.
    pub async fn shard_diagnostics(
        &self,
        only: Option<u32>,
    ) -> Vec<crate::pb::ShardLayoutDiagnostics> {
        let mut out = Vec::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let shard = shard as u32;
            if only.is_some_and(|s| s != shard) {
                continue;
            }
            let layout = match self.node_client(addr) {
                Ok(crate::link::NodeLink::Local(node)) => {
                    node.shard_diagnostics(shard, addr.clone())
                }
                #[cfg(feature = "net")]
                Ok(crate::link::NodeLink::Remote(_)) => match self.connect(addr) {
                    Ok(channel) => {
                        let mut client =
                            crate::pb::diagnostics_service_client::DiagnosticsServiceClient::new(
                                channel,
                            );
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            client.get_shard_diagnostics(crate::pb::ShardDiagnosticsRequest {
                                shard: None,
                            }),
                        )
                        .await
                        {
                            Ok(Ok(reply)) => match reply.into_inner().shards.into_iter().next() {
                                Some(mut layout) => {
                                    layout.shard = shard;
                                    layout.address = addr.clone();
                                    layout
                                }
                                None => crate::diagnostics::unserved_layout(
                                    shard,
                                    addr.clone(),
                                    "EMPTY: the node reported no shard",
                                ),
                            },
                            Ok(Err(status)) => crate::diagnostics::unserved_layout(
                                shard,
                                addr.clone(),
                                &format!("{:?}: {}", status.code(), status.message()),
                            ),
                            Err(_) => crate::diagnostics::unserved_layout(
                                shard,
                                addr.clone(),
                                "DEADLINE_EXCEEDED: diagnostics probe timed out",
                            ),
                        }
                    }
                    Err(status) => crate::diagnostics::unserved_layout(
                        shard,
                        addr.clone(),
                        &format!("{:?}: {}", status.code(), status.message()),
                    ),
                },
                Err(status) => crate::diagnostics::unserved_layout(
                    shard,
                    addr.clone(),
                    &format!("{:?}: {}", status.code(), status.message()),
                ),
            };
            out.push(layout);
        }
        out
    }

    pub fn with_max_rerank_bytes(mut self, bytes: u64) -> Self {
        self.max_rerank_bytes = bytes;
        self
    }

    /// Resolve a request's `k` against the configured cap. Proto3 makes
    /// an omitted field 0, so 0 selects `max_k` (the documented sentinel
    /// idiom here, like `rbo_p`); anything above the cap is refused with
    /// both numbers named rather than silently clamped.
    pub(super) fn resolve_k(&self, requested: u32) -> Result<u32, Status> {
        if requested == 0 {
            return Ok(self.knobs.max_k());
        }
        if requested > self.knobs.max_k() {
            return Err(Status::invalid_argument(format!(
                "k={requested} exceeds this coordinator's max_k={}; \
                 lower k or raise --max-k",
                self.knobs.max_k()
            )));
        }
        Ok(requested)
    }

    /// Configure the BM25 path: lexical analyzer for query analysis and
    /// the scoring parameters every shard is told to use.
    pub fn with_bm25(mut self, analysis_addr: Option<String>, params: Bm25Params) -> Self {
        self.analysis_addr = analysis_addr;
        self.bm25_params = params;
        self
    }

    /// Attach the same immutable phrase vocabulary used by every shard.
    pub fn with_phrase_index(
        mut self,
        phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    ) -> Self {
        self.phrase_index = phrase_index;
        self
    }

    /// Configure per-shard deadlines and hedging.
    pub fn with_limits(mut self, limits: FanoutLimits) -> Self {
        self.limits = limits;
        self.rebuild_knobs(
            self.knobs.max_k(),
            limits.hedge_delay,
            self.knobs.shard_pruning(),
            self.knobs.signal_batch(),
        );
        self.refresh_fixed_knobs();
        self
    }

    /// Configure replica addresses (one optional entry per shard, same
    /// order as the node list). A replica must serve identical data —
    /// searches are exact, so either copy returns identical results.
    pub fn with_replicas(mut self, replica_addrs: Vec<Option<String>>) -> Self {
        self.replica_addrs = replica_addrs;
        self
    }

    /// Route vector work through one distributed TurboVec collection. The
    /// backend itself owns its only global heap and shard completion.
    #[cfg(feature = "net")]
    pub fn with_clustered_turbovec(mut self, backend: ClusteredTurboVecBackend) -> Self {
        self.clustered_vectors = Some(backend);
        self
    }

    /// Install the synonym table (`docs/synonyms.md`).
    pub fn with_synonyms(mut self, table: crate::synonyms::SynonymTable) -> Self {
        self.synonyms = Some(Arc::new(table));
        self
    }

    /// Install a memory-mapped stem-to-surface lexicon so
    /// `SearchService.Suggest` on stemmed fields returns true surface completions.
    pub fn with_surface_lexicon(
        mut self,
        lexicon: Arc<crate::turbo_bridge::query_plan::SharedSurfaceLexicon>,
    ) -> Self {
        self.surface_lexicon = Some(lexicon);
        self
    }

    /// Install a contextual bigram spellchecker consulted by `SearchService.TermSuggest`.
    pub fn with_spellchecker(mut self, spellchecker: Arc<turbo_client::QuerySpellchecker>) -> Self {
        self.spellchecker = Some(spellchecker);
        self
    }

    /// Install a one-pass `AnalyzeQuery` planner and target NLP column mapping
    /// for structured filter binding and query synonyms.
    pub fn with_one_pass_query(
        mut self,
        analyzer: turbo_client::OnePassQueryAnalyzer,
        columns: crate::turbo_bridge::NlpColumnSpec,
    ) -> Self {
        self.one_pass_query = Some(Arc::new((analyzer, columns)));
        self
    }

    /// Install a Stage-2 calibrated `RerankAndHighlight` policy applied to dense
    /// and hybrid shortlists.
    pub fn with_rerank_spec(mut self, spec: crate::turbo_bridge::rerank::RerankSpec) -> Self {
        self.rerank_spec = Some(Arc::new(spec));
        self
    }

    pub(crate) fn rerank_spec(&self) -> Option<&crate::turbo_bridge::rerank::RerankSpec> {
        self.rerank_spec.as_deref()
    }

    pub(crate) fn one_pass_query(
        &self,
    ) -> Option<&(
        turbo_client::OnePassQueryAnalyzer,
        crate::turbo_bridge::NlpColumnSpec,
    )> {
        self.one_pass_query.as_deref()
    }

    pub fn with_dense_quality_profile(
        mut self,
        profile: crate::quality::DenseQualityProfile,
    ) -> Self {
        self.dense_quality_profile = Some(Arc::new(profile));
        self
    }

    pub fn with_dense_execution_policy(
        mut self,
        policy: crate::dense_policy::DenseExecutionPolicy,
    ) -> Self {
        self.dense_execution_policy = Some(Arc::new(policy));
        self
    }

    /// The index's derived-column declaration (`docs/derived-columns.md`),
    /// the shard map's `[[derived]]` table compiled.
    pub fn with_derived(mut self, derived: Option<Arc<crate::derived::Declaration>>) -> Self {
        self.derived = derived;
        self
    }

    /// The declaration this coordinator routes and scopes with.
    pub fn derived(&self) -> Option<&Arc<crate::derived::Declaration>> {
        self.derived.as_ref()
    }

    pub fn with_topology_generation(mut self, generation: u64) -> Self {
        self.topology_generation = generation;
        self
    }

    /// Supply a host-managed cursor signing key. Hosts sharing a key still need
    /// identical request, authorization and routing contexts to resume a token.
    /// Without this option the key is random and lost when the coordinator drops.
    pub fn with_cursor_signing_key(mut self, key: [u8; 32]) -> Self {
        self.cursor_signer = Arc::new(crate::query_cursor::CursorSigner::from_key(key));
        self
    }

    /// Name the collection this coordinator serves (`docs/collections.md`).
    pub fn with_collection(mut self, name: &str) -> Self {
        self.collection = name.to_string();
        self
    }

    /// The collection this coordinator serves; empty for the unnamed
    /// single dataset.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// TLS material for every channel this coordinator opens to a shard
    /// (`docs/security.md`).
    pub fn with_client_tls(mut self, tls: crate::security::ClientTls) -> Self {
        self.client_tls = Some(tls);
        self
    }

    /// The key that signs this coordinator's UDP floor and cancel
    /// datagrams (`docs/security.md`).
    pub fn with_udp_hmac_key(mut self, key: crate::security::UdpKey) -> Self {
        self.udp_hmac_key = Some(key);
        self
    }

    /// The key that signs this coordinator's UDP datagrams, when one is
    /// configured (`docs/security.md`); a relay signs its own parent-facing
    /// lane with the same key.
    #[cfg(feature = "net")]
    pub(crate) fn udp_key(&self) -> Option<&crate::security::UdpKey> {
        self.udp_hmac_key.as_ref()
    }

    /// Build the tonic server for this service with explicit message size
    /// limits (see [`crate::MAX_MESSAGE_BYTES`]).
    pub fn into_server(self, max_message_bytes: usize) -> SearchServiceServer<Self> {
        SearchServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }
}
