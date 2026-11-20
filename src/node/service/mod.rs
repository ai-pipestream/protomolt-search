//! Shard service state container, startup initialization, and runtime diagnostics.

use crate::node::*;

mod attach;
mod builder;
mod layout_diagnostics;
mod open;

/// The shard-owner gRPC service. Cheap to clone (state is shared).
#[derive(Clone)]
pub struct NodeServiceImpl {
    /// Locked shard state; see [`ShardState`].
    pub(crate) state: Arc<RwLock<ShardState>>,
    /// Commits share a permit; compaction reserves the final tail while
    /// analysis runs. Waiting writers yield without blocking shard reads.
    pub(crate) mutation_gate: Arc<tokio::sync::RwLock<()>>,
    /// Single-writer gate for ingest streams. Two concurrent AddDocuments
    /// (or AddVectors) streams would interleave positional ids into one
    /// shard — every doc logged, none attributable — so the second stream
    /// is refused outright rather than merged.
    pub(in crate::node) ingest_busy: Arc<std::sync::atomic::AtomicBool>,
    /// Ingest fence set by the node agent during shard split cutover so no
    /// append lands between final child catch-up and topology publication;
    /// fenced shards refuse ingest streams while continuing to serve queries.
    pub(in crate::node) ingest_fence: Arc<std::sync::Mutex<Option<String>>>,
    pub(crate) config: NodeConfig,
    /// Bearer principals for public callers; cluster members use mTLS.
    pub(in crate::node) principals: Option<Arc<crate::security::Principals>>,
    /// Runtime knobs read at request time.
    pub(in crate::node) knobs: Arc<crate::diagnostics::Knobs>,
    /// Shared scan queue for coalesced searches; the scheduler task is
    /// spawned on first use (shared across service clones).
    pub(in crate::node) scan_jobs: Arc<std::sync::OnceLock<mpsc::Sender<ScanJob>>>,
    /// Lane budget shared by every exact-rerank request on this node.
    pub(in crate::node) rerank_slots: Arc<tokio::sync::Semaphore>,
    /// UDP fast-lane registry: stream token -> that stream's monotone floor
    /// and advisory cancellation flag. Fed by [`Self::spawn_floor_listener`]
    /// and polled by the streaming scan before every chunk.
    pub(in crate::node) stream_signals: Arc<std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>>,
    /// The shard's vocabulary listener (`<index path>.vocab/`), attached
    /// to every ingest AnalyzeStream. `None` when vocabulary accumulation
    /// is off (the default) or its directory failed to initialize.
    pub(in crate::node) vocab: Option<Arc<crate::vocab::VocabularyListener>>,
    /// Compiled materialization spec cached per distinct spec across ingest.
    pub(in crate::node) materialize_cache: Arc<std::sync::Mutex<Option<CompiledMaterialize>>>,
    /// Optional product-owned phrase vocabulary shared by ingest and query.
    pub(crate) phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    /// Mutex serializing tail seals on this shard while a frozen part is in flight.
    pub(crate) seal_lock: Arc<std::sync::Mutex<()>>,
    /// Single-compaction gate per shard; concurrent `CompactShard` calls refuse.
    pub(crate) compacting: Arc<std::sync::atomic::AtomicBool>,
    /// Notification fired after each flush so the node agent reports to the control plane.
    pub(in crate::node) flush_notify: Option<Arc<tokio::sync::Notify>>,
    /// Optional per-poll sleep injected on streaming scans for deterministic
    /// straggler simulation in multi-shard tests and benchmarks.
    pub(in crate::node) stream_poll_delay: Option<std::time::Duration>,
    /// Optional one-pass `DocumentNlpProjection` -> native typed column spec.
    pub(in crate::node) nlp_columns: Option<Arc<crate::turbo_bridge::NlpColumnSpec>>,
    /// Optional index-generation cryptographic handshake verified against analyzer / bundle fingerprints.
    pub(in crate::node) turbo_handshake: Option<Arc<turbo_client::IndexGenerationHandshake>>,
    /// Automatically pinned first-document lexical handshake on this shard.
    pub(in crate::node) auto_turbo_handshake:
        Arc<std::sync::Mutex<Option<turbo_client::IndexGenerationHandshake>>>,
}

/// Open the shard's vocabulary listener at `<index path>.vocab/`,
/// resuming its snapshot history. Unlike the WAL this is analytics, not
/// a ledger: a directory that cannot be created, probed, or scanned
/// degrades the shard to uncounted with a loud warning — ingest itself
/// is unaffected.
fn open_vocab(config: &NodeConfig) -> Option<Arc<crate::vocab::VocabularyListener>> {
    if !config.vocab && config.vocab_kserve_endpoint.is_none() {
        return None;
    }
    let index_path = config.index_path.as_ref()?;
    let dir = crate::vocab::vocab_dir(index_path);
    match crate::vocab::VocabularyListener::create(
        &dir,
        config.vocab_window_docs,
        config.vocab_top_k,
    ) {
        Ok(listener) => {
            eprintln!(
                "vocab: accumulating to {} (window {} docs, top-K {})",
                dir.display(),
                config.vocab_window_docs,
                config.vocab_top_k
            );
            Some(Arc::new(listener))
        }
        Err(e) => {
            eprintln!(
                "vocab: {} is not writable ({e}); vocabulary accumulation is DISABLED, \
                 ingest is unaffected",
                dir.display()
            );
            None
        }
    }
}

impl NodeServiceImpl {
    /// Wrap an optional preloaded index in a node service.
    pub fn new(index: Option<VectorIndex>, mut config: NodeConfig) -> Self {
        declare_placement_column(&mut config);
        if let Err(error) =
            declare_derived_columns(&mut config).and_then(|()| check_placement_columns(&config))
        {
            panic!("{error}");
        }
        let wal = open_wal(index.as_ref(), &config);
        let mapped_binding = wal.as_ref().and_then(|wal| {
            crate::reshard::read_generation_binding(wal.dir())
                .unwrap_or_else(|error| panic!("cannot recover mapped binding: {error}"))
        });
        let vocab = open_vocab(&config);
        let rerank_parallel = resolved_rerank_parallel(config.rerank_parallel);
        let knobs = Arc::new(node_knobs(&config));
        let configured_handshake = config.turbo_handshake.clone().map(Arc::new);
        Self {
            knobs,
            state: Arc::new(RwLock::new(ShardState {
                index,
                exact_vectors: None,
                multi_res_vectors: Default::default(),
                bm25: None,
                live_docs: LiveDocs::default(),
                wal,
                parents: None,
                mapped_binding,
                stats_epoch: 1,
                files_current: false,
                stats_incarnation: Default::default(),
                pending_compaction: None,
            })),
            ingest_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            mutation_gate: Arc::new(tokio::sync::RwLock::new(())),
            ingest_fence: Arc::new(std::sync::Mutex::new(None)),
            seal_lock: Arc::new(std::sync::Mutex::new(())),
            compacting: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            config,
            principals: None,
            scan_jobs: Arc::new(std::sync::OnceLock::new()),
            rerank_slots: Arc::new(tokio::sync::Semaphore::new(rerank_parallel)),
            stream_signals: Arc::new(std::sync::Mutex::new(HashMap::new())),
            vocab,
            materialize_cache: Arc::new(std::sync::Mutex::new(None)),
            phrase_index: None,
            flush_notify: None,
            stream_poll_delay: None,
            nlp_columns: None,
            turbo_handshake: configured_handshake,
            auto_turbo_handshake: Arc::new(std::sync::Mutex::new(None)),
        }
    }

    /// Wake `notify` after every flush (the node agent's report trigger).
    pub fn with_flush_notify(mut self, notify: Arc<tokio::sync::Notify>) -> Self {
        self.flush_notify = Some(notify);
        self
    }

    /// Inject a per-hop poll delay on streaming searches for deterministic
    /// straggler simulation in tests and benchmarks.
    pub fn with_stream_poll_delay(mut self, delay: std::time::Duration) -> Self {
        self.stream_poll_delay = Some(delay);
        self
    }

    /// The shard's configuration.
    pub fn config(&self) -> &NodeConfig {
        &self.config
    }

    /// Authenticate public bearer callers on this node.
    pub fn with_principals(mut self, principals: Arc<crate::security::Principals>) -> Self {
        self.principals = Some(principals);
        self
    }

    /// Attach the process-wide phrase vocabulary. It is immutable and cheap
    /// to share across every shard in a `both` process.
    pub fn with_phrase_index(
        mut self,
        phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    ) -> Self {
        self.phrase_index = phrase_index;
        self
    }

    /// Configure pre-WAL materialization of first-class one-pass NLP typed
    /// columns (`money`, `quantity`, `date`, `geo_spine`, `sentiment`,
    /// `affirmed`/`negated` entities, `simhash`).
    pub fn with_nlp_columns(mut self, spec: crate::turbo_bridge::NlpColumnSpec) -> Self {
        self.nlp_columns = Some(Arc::new(spec));
        self
    }

    /// Pin an [`turbo_client::IndexGenerationHandshake`] verified against
    /// field analysis and neural bundle scoring fingerprints.
    pub fn with_turbo_handshake(
        mut self,
        handshake: turbo_client::IndexGenerationHandshake,
    ) -> Self {
        self.turbo_handshake = Some(Arc::new(handshake));
        self
    }

    /// Verify a loaded neural bundle's scoring fingerprint against this
    /// shard's pinned [`turbo_client::IndexGenerationHandshake`], refusing
    /// with `FAILED_PRECONDITION` on drift.
    pub fn verify_turbo_bundle_handshake(
        &self,
        actual: &turbo_client::BundleScoringFingerprint,
    ) -> Result<(), Status> {
        if let Some(hs) = self.turbo_handshake.as_deref() {
            hs.verify_bundle(actual)
                .map_err(|e| Status::failed_precondition(e.to_string()))?;
        }
        Ok(())
    }

    /// Seal the live vocabulary window on graceful shutdown. No-op when
    /// accumulation is off or the window is empty.
    pub fn snapshot_vocab_on_shutdown(&self) {
        if let Some(vocab) = &self.vocab {
            vocab.persist_on_shutdown();
        }
    }

    /// Fence ingest on this shard: every later ingest stream is refused
    /// naming `reason`; queries are unaffected. Idempotent.
    pub fn fence_ingest(&self, reason: String) {
        *self.ingest_fence.lock().expect("ingest fence lock") = Some(reason);
    }

    /// The fence reason, when the shard is fenced.
    pub fn ingest_fence(&self) -> Option<String> {
        self.ingest_fence.lock().expect("ingest fence lock").clone()
    }

    /// Claim the single-writer ingest gate, or refuse the stream.
    pub(in crate::node) fn claim_ingest(&self) -> Result<IngestGuard, Status> {
        use std::sync::atomic::Ordering;
        if let Some(reason) = self.ingest_fence() {
            return Err(Status::failed_precondition(format!(
                "ingest is fenced on this shard: {reason}"
            )));
        }
        validate_column_tables(&self.config).map_err(Status::failed_precondition)?;
        if self
            .ingest_busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(Status::failed_precondition(
                "another ingest stream is active on this shard",
            ));
        }
        Ok(IngestGuard(self.ingest_busy.clone()))
    }

    /// The knobs this node reads at request time.
    pub fn knobs(&self) -> &Arc<crate::diagnostics::Knobs> {
        &self.knobs
    }

    /// The diagnostics service over this node, for the same listener
    /// as its `NodeService`.
    pub fn diagnostics_server(
        &self,
        max_message_bytes: usize,
    ) -> crate::pb::diagnostics_service_server::DiagnosticsServiceServer<
        crate::diagnostics::NodeDiagnostics,
    > {
        crate::diagnostics::NodeDiagnostics::new(self.clone()).into_server(max_message_bytes)
    }

    /// A metrics gauge sampler over this shard's live state: called at scrape
    /// time under the state lock so reported counts never go stale.
    pub fn metrics_provider(&self) -> crate::metrics::GaugeProvider {
        let state = Arc::clone(&self.state);
        let slot_offset = self.config.slot_offset;
        Box::new(move || {
            let guard = read_shard(&state);
            crate::metrics::ShardGauges {
                slot_offset,
                vectors: guard.index.as_ref().map_or(0, |i| i.len() as u64),
                documents: guard.bm25.as_ref().map_or(0, |b| b.doc_count()),
                stats_epoch: guard.stats_epoch,
            }
        })
    }

    /// Build the tonic server for this service with explicit decoding and
    /// encoding message size limits (see [`crate::MAX_MESSAGE_BYTES`]).
    pub fn into_server(self, max_message_bytes: usize) -> NodeServiceServer<Self> {
        NodeServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// Capture lineage with its physical version. The cache publication and
    /// each scan's final read guard must both still match that version.
    pub(in crate::node) fn parent_map(
        state: &Arc<std::sync::RwLock<ShardState>>,
        slot_offset: u64,
        n: usize,
    ) -> Result<ParentMap, Status> {
        const SELF_PARENT_TAG: u64 = 1 << 63;
        let built = {
            let guard = read_shard(state);
            let claim = crate::stats_identity::StatsClaim::required(
                guard.stats_epoch,
                &guard.stats_incarnation.bytes()?,
            )?;
            if let Some(cached) = &guard.parents {
                if cached.claim == claim && cached.rows.len() == n {
                    return Ok(cached.clone());
                }
            }
            let store = guard.bm25.as_ref().map(Bm25Shard::as_index);
            let rows = (0..n)
                .map(|slot| {
                    store
                        .and_then(|s| s.lineage(slot as u32))
                        .map(|l| l.parent_id)
                        .unwrap_or(SELF_PARENT_TAG | (slot_offset + slot as u64))
                })
                .collect();
            ParentMap {
                claim,
                rows: Arc::new(rows),
            }
        };
        write_shard(state).publish_parent_map(&built)?;
        Ok(built)
    }

    /// A new provider image for `dim`: wrapped as the tail of a
    /// segmented provider when the shard is segmented, so every row the
    /// node adds joins the catalog's positional space.
    pub(in crate::node) fn fresh_index(
        &self,
        bm25: Option<&Bm25Shard>,
        dim: usize,
    ) -> Result<VectorIndex, Status> {
        let created = VectorIndex::create(&self.config.vector_backend, dim, self.config.bit_width)
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        Self::adopt_layout(bm25, created)
    }

    /// Create an empty exact-vector store: disk-spilling at the tail's exact
    /// sidecar path on persisted shards, or heap-backed for in-memory shards.
    pub(in crate::node) fn fresh_exact_store(
        &self,
        dim: usize,
    ) -> Result<ExactVectorStore, Status> {
        match self.config.index_path.as_ref() {
            Some(index_path) => {
                let exact_path = exact_vector_sidecar_path(index_path);
                ExactVectorStore::spilling(&exact_path, Some(dim)).map_err(|e| {
                    Status::internal(format!(
                        "exact-vector builder {}: {e}",
                        exact_path.display()
                    ))
                })
            }
            None => Ok(ExactVectorStore::empty(Some(dim))),
        }
    }
}
