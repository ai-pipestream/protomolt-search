//! Shard service state container, startup initialization, and runtime diagnostics.

use crate::node::*;

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
    /// A fence on ingest (`docs/cluster-control.md`, "Shard split"): set
    /// by the node agent when the shard's rows are moving to its split
    /// children, so no append can land between the children's final
    /// catch-up and the topology cutover. The reason names the
    /// children; a fenced shard refuses every ingest stream by name and
    /// keeps serving queries.
    pub(in crate::node) ingest_fence: Arc<std::sync::Mutex<Option<String>>>,
    pub(crate) config: NodeConfig,
    /// Runtime knobs read at request time (docs/diagnostics.md).
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
    /// Compiled materialization spec for the current ingest stream
    /// (docs/cel-values.md): expressions compile once per spec CHANGE,
    /// never per document — the ingest analog of the query rule.
    pub(in crate::node) materialize_cache: Arc<std::sync::Mutex<Option<CompiledMaterialize>>>,
    /// Optional product-owned phrase vocabulary shared by ingest and query.
    pub(crate) phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    /// Seals on this shard run one at a time; the frozen part of a seal
    /// in flight is the reason (`docs/immutable-segments.md`).
    pub(crate) seal_lock: Arc<std::sync::Mutex<()>>,
    /// One compaction at a time per shard (`docs/mutations.md`): a
    /// second `CompactShard` refuses by name while this is set.
    pub(crate) compacting: Arc<std::sync::atomic::AtomicBool>,
    /// Woken after every flush: the node agent reports the shard to the
    /// control plane on it (`docs/cluster-control.md`).
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
                generation: None,
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

    /// Inject a per-hop poll delay on streaming searches (used to simulate a
    /// straggler shard deterministically in tests and benchmarks).
    pub fn with_stream_poll_delay(mut self, delay: std::time::Duration) -> Self {
        self.stream_poll_delay = Some(delay);
        self
    }

    /// The shard's configuration.
    pub fn config(&self) -> &NodeConfig {
        &self.config
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

    /// The builder shape for a fresh ingest: persisted shards bulk-build
    /// through the disk spiller (bounded heap, not searchable until
    /// Flush); path-less demo shards build in heap.
    pub(in crate::node) fn new_builder(
        &self,
        generation: Option<&PathBuf>,
    ) -> Result<Bm25Shard, Status> {
        validate_column_tables(&self.config).map_err(Status::failed_precondition)?;
        let names: Vec<&str> = self.config.bm25_fields.iter().map(String::as_str).collect();
        let facets: Vec<&str> = self
            .config
            .facet_fields
            .iter()
            .map(String::as_str)
            .collect();
        let numerics: Vec<&str> = self
            .config
            .numeric_fields
            .iter()
            .map(String::as_str)
            .collect();
        let map_facets: Vec<&str> = self
            .config
            .map_facet_fields
            .iter()
            .map(String::as_str)
            .collect();
        let map_numerics: Vec<&str> = self
            .config
            .map_numeric_fields
            .iter()
            .map(String::as_str)
            .collect();
        let map_integers: Vec<&str> = self
            .config
            .map_integer_fields
            .iter()
            .map(String::as_str)
            .collect();
        let map_unsigned_integers: Vec<&str> = self
            .config
            .map_unsigned_integer_fields
            .iter()
            .map(String::as_str)
            .collect();
        let integers: Vec<&str> = self
            .config
            .integer_fields
            .iter()
            .map(String::as_str)
            .collect();
        let unsigned_integers: Vec<&str> = self
            .config
            .unsigned_integer_fields
            .iter()
            .map(String::as_str)
            .collect();
        let geos: Vec<&str> = self.config.geo_fields.iter().map(String::as_str).collect();
        let positions: Vec<&str> = self
            .config
            .position_fields
            .iter()
            .map(String::as_str)
            .collect();
        for name in &positions {
            if !names.contains(name) {
                return Err(Status::failed_precondition(format!(
                    "positional field {name:?} is not in this shard's BM25 field table {names:?}"
                )));
            }
        }
        let sentences: Vec<&str> = self
            .config
            .sentence_fields
            .iter()
            .map(String::as_str)
            .collect();
        for name in &sentences {
            if !names.contains(name) {
                return Err(Status::failed_precondition(format!(
                    "sentence field {name:?} is not in this shard's BM25 field table {names:?}"
                )));
            }
        }
        match self.config.index_path.as_ref() {
            // A new persisted shard under the segment layout: the tail is a
            // heap store, searchable at once, sealed into the catalog on
            // flush (docs/immutable-segments.md).
            Some(p) if self.config.layout == Layout::Segments && generation.is_none() => {
                let root = segments_root(p);
                let tail = heap_store(&self.config).map_err(Status::failed_precondition)?;
                SegmentedShard::open_with(&root, tail, self.config.vector_load())
                    .map(Bm25Shard::Segmented)
                    .map_err(|e| {
                        Status::internal(format!("segment catalog {}: {e}", root.display()))
                    })
            }
            Some(p) => {
                let dir = bm25_build_dir(&storage_paths(p, generation).2);
                SpillBuilder::create_with_fields(&dir, &names)
                    .map(|b| {
                        let mut b = b
                            .with_facet_fields(&facets)
                            .with_numeric_fields(&numerics)
                            .with_map_facet_fields(&map_facets)
                            .with_map_numeric_fields(&map_numerics)
                            .with_map_integer_fields(&map_integers)
                            .with_map_unsigned_integer_fields(&map_unsigned_integers)
                            .with_integer_fields(&integers)
                            .with_unsigned_integer_fields(&unsigned_integers)
                            .with_geo_fields(&geos)
                            .with_position_fields(&positions)
                            .with_sentence_fields(&sentences);
                        b.set_derived(stored_derived(&self.config));
                        Bm25Shard::Spilling(b)
                    })
                    .map_err(|e| Status::internal(format!("spill dir {}: {e}", dir.display())))
            }
            None => {
                let mut store = Bm25Store::with_fields(&names)
                    .with_facets(&facets)
                    .with_numerics(&numerics)
                    .with_map_facets(&map_facets)
                    .with_map_numerics(&map_numerics)
                    .with_map_integers(&map_integers)
                    .with_map_unsigned_integers(&map_unsigned_integers)
                    .with_integers(&integers)
                    .with_unsigned_integers(&unsigned_integers)
                    .with_geos(&geos)
                    .with_positions(&positions)
                    .with_sentences(&sentences);
                store.set_derived(stored_derived(&self.config));
                Ok(Bm25Shard::Building(store))
            }
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

    /// Attach a preloaded BM25 shard (from `<index path>.bm25`). The
    /// store's persisted mapped-plan binding becomes the shard's: the
    /// file IS the durable record of what its columns were written
    /// under.
    pub fn with_bm25(self, store: Option<Bm25Shard>) -> Self {
        {
            let mut guard = write_shard(&self.state);
            if let Err(error) = guard.check_legacy_mutation() {
                drop(guard);
                panic!("{error}");
            }
            if let Some(Bm25Shard::Segmented(shard)) = store.as_ref() {
                if let Some(owner) = &shard.snapshot().manifest().source_owner {
                    let owner = owner.decode().expect("validated source owner");
                    assert!(
                        owner.collection == self.config.collection
                            && !self.config.wal
                            && self.config.layout == Layout::Segments,
                        "source-managed index requires its original collection, segmented layout and WAL disabled"
                    );
                    let set = shard.snapshot();
                    let has_vectors = (0..set.len()).any(|i| set.vector(i).is_some());
                    let matches = guard.index.as_ref().map_or(!has_vectors, |index| {
                        index
                            .as_segmented()
                            .map_or(!has_vectors && index.is_empty(), |provider| {
                                Arc::ptr_eq(provider.snapshot(), set)
                                    && provider.tail().is_empty()
                                    && provider.frozen().is_none()
                            })
                    });
                    if !matches {
                        drop(guard);
                        panic!("source-managed index vectors must come from its serving catalog");
                    }
                }
            }
            let persisted = store.as_ref().and_then(|s| s.binding().cloned());
            if let (Some(recovered), Some(persisted)) = (&guard.mapped_binding, &persisted) {
                assert_eq!(
                    recovered, persisted,
                    "WAL and BM25 mapped bindings disagree"
                );
            }
            if let Some(store) = store.as_ref() {
                if let Err(error) = check_attached_derived(
                    &self.config,
                    store.derived(),
                    store.doc_count(),
                    "the BM25 store",
                ) {
                    panic!("{error}");
                }
            }
            if let Some(Bm25Shard::Segmented(shard)) = store.as_ref() {
                shard
                    .snapshot()
                    .merge_tombstones(&mut guard.live_docs)
                    .unwrap_or_else(|error| panic!("attach segment tombstones: {error}"));
                if shard.snapshot().manifest().source_owner.is_some() {
                    if let Err(error) =
                        publication::check_source_live_view(shard.snapshot(), &guard.live_docs)
                    {
                        drop(guard);
                        panic!("{error}");
                    }
                }
            }
            guard.mapped_binding = persisted.or_else(|| guard.mapped_binding.clone());
            guard.bm25 = store;
            guard.advance_stats_epoch();
        }
        self
    }

    /// Attach the product-owned FP32 sidecar loaded at startup. The sidecar
    /// must describe exactly the provider slots in this shard generation.
    pub fn with_exact_vectors(self, store: Option<ExactVectorStore>) -> Result<Self, String> {
        {
            let mut guard = write_shard(&self.state);
            if let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() {
                let set = shard.snapshot();
                if set.manifest().source_owner.is_some() {
                    let matches = store.as_ref().map_or_else(
                        || (0..set.len()).all(|i| set.exact_vectors(i).is_none()),
                        |exact| exact.matches_segments(set),
                    );
                    if !matches {
                        return Err(
                            "source-managed index exact vectors must come from its serving catalog"
                                .into(),
                        );
                    }
                }
            }
            if let Some(exact) = store.as_ref() {
                if let Some(index) = guard.index.as_ref() {
                    if exact.len() != index.len() || exact.dim() != index.dim_opt() {
                        return Err(format!(
                            "exact-vector sidecar shape {:?}x{} does not match provider shape {:?}x{}",
                            exact.dim(),
                            exact.len(),
                            index.dim_opt(),
                            index.len()
                        ));
                    }
                } else if guard
                    .bm25
                    .as_ref()
                    .is_some_and(|bm25| exact.len() != bm25.next_doc_id() as usize)
                {
                    return Err(
                        "exact-vector sidecar row count does not match product document rows"
                            .to_string(),
                    );
                }
            }
            guard.exact_vectors = store;
        }
        Ok(self)
    }

    /// Attach the persisted generation overlay loaded at startup.
    pub fn with_live_docs(self, mut live_docs: LiveDocs) -> Result<Self, String> {
        {
            let mut guard = write_shard(&self.state);
            let rows = physical_rows(&guard);
            if live_docs.persisted_rows() > rows {
                return Err(format!(
                    "live-doc overlay describes {} rows but the shard has only {rows}",
                    live_docs.persisted_rows()
                ));
            }
            if let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() {
                shard.snapshot().merge_tombstones(&mut live_docs)?;
                if shard.snapshot().manifest().source_owner.is_some() {
                    publication::check_source_live_view(shard.snapshot(), &live_docs)
                        .map_err(|error| error.message().to_string())?;
                }
            }
            guard.live_docs = live_docs;
        }
        Ok(self)
    }

    /// Mark the shard as serving from a snapshot generation directory
    /// (startup found one via [`recover_generation`]): Flush and the
    /// AddDocuments reload path then read/write inside it.
    pub fn with_generation(self, dir: Option<PathBuf>) -> Self {
        {
            let mut guard = write_shard(&self.state);
            if dir.is_some() {
                if let Err(error) = guard.check_legacy_mutation() {
                    drop(guard);
                    panic!("{error}");
                }
            }
            guard.generation = dir;
        }
        self
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

    /// This shard's layout for `GetShardDiagnostics`
    /// (docs/diagnostics.md): the catalog's sealed segments with their
    /// summaries, the tail, rows, tombstones, and the live knob values.
    pub fn shard_diagnostics(
        &self,
        shard: u32,
        address: String,
    ) -> crate::pb::ShardLayoutDiagnostics {
        use crate::pb::{SegmentColumnRange, SegmentDiagnostics, ShardLayoutDiagnostics};
        let guard = read_shard(&self.state);
        let vector_rows = guard.index.as_ref().map_or(0, |i| i.len() as u64);
        let doc_rows = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let rows = vector_rows.max(doc_rows);
        let tombstones = guard.live_docs.deleted_count();
        let scoring_fingerprint = guard
            .index
            .as_ref()
            .map(|i| i.descriptor().scoring_fingerprint)
            .unwrap_or_default();
        let mut out = ShardLayoutDiagnostics {
            shard,
            address,
            layout: match guard.bm25.as_ref() {
                Some(Bm25Shard::Segmented(_)) => "segments".to_string(),
                Some(_) => "single-image".to_string(),
                None => match self.config.layout {
                    Layout::Segments => "segments".to_string(),
                    Layout::SingleImage => "single-image".to_string(),
                },
            },
            rows,
            live_rows: rows.saturating_sub(tombstones),
            tombstones,
            scoring_fingerprint,
            segment_pruning: self.knobs.segment_pruning(),
            floor_sharing: self.knobs.share_floors(),
            has_placement: self.config.placement_leaf.is_some(),
            placement: self.config.placement_leaf.unwrap_or_default() as u64,
            ..Default::default()
        };
        if let Some(Bm25Shard::Segmented(segmented)) = guard.bm25.as_ref() {
            // Mixed placement: a sealed segment whose placement column
            // spans more than one code, two segments under different
            // codes, or a segment under a code other than the pinned
            // one. A pinned shard's tail cannot be mixed: ingest refuses
            // another code before it is applied.
            if let Some(column) = self.config.placement_column.as_deref() {
                let set = segmented.snapshot();
                let mut codes: Vec<i64> = Vec::new();
                let mut mixed = false;
                for i in 0..set.len() {
                    let Some(summary) = set.metadata(i).summary.as_ref() else {
                        continue;
                    };
                    let Some(range) = summary.int_columns.iter().find(|c| c.name == column) else {
                        continue;
                    };
                    if range.present == 0 {
                        continue;
                    }
                    if range.min != range.max {
                        mixed = true;
                    }
                    if !codes.contains(&range.min) {
                        codes.push(range.min);
                    }
                }
                if codes.len() > 1 {
                    mixed = true;
                }
                if let Some(pinned) = self.config.placement_leaf {
                    if codes.iter().any(|code| *code != pinned) {
                        mixed = true;
                    }
                }
                out.placement_mixed = mixed;
            }
            let set = segmented.snapshot();
            out.catalog_epoch = set.epoch();
            out.partition_key = set.manifest().partition_key.clone().unwrap_or_default();
            out.tail_rows = u64::from(
                segmented
                    .next_doc_id()
                    .saturating_sub(segmented.tail_base()),
            );
            for i in 0..set.len() {
                let meta = set.metadata(i);
                let mut columns = Vec::new();
                let mut partition = None;
                if let Some(summary) = meta.summary.as_ref() {
                    for c in &summary.int_columns {
                        columns.push(SegmentColumnRange {
                            column: c.name.clone(),
                            lo: c.min,
                            hi: c.max,
                            present: c.present,
                            ..Default::default()
                        });
                    }
                    for c in &summary.numeric_columns {
                        columns.push(SegmentColumnRange {
                            column: c.name.clone(),
                            lo_f: c.min,
                            hi_f: c.max,
                            present: c.present,
                            floating: true,
                            ..Default::default()
                        });
                    }
                    partition = summary.partition.as_ref().map(|p| SegmentColumnRange {
                        column: p.column.clone(),
                        lo: p.lo,
                        hi: p.hi,
                        present: meta.live_rows,
                        ..Default::default()
                    });
                }
                out.segments.push(SegmentDiagnostics {
                    segment_id: meta.segment_id.clone(),
                    generation: meta.generation,
                    base: meta.base_label,
                    rows: meta.rows,
                    live_rows: meta.live_rows,
                    has_summary: meta.summary.is_some(),
                    columns,
                    partition,
                    mapped: set.vector(i).is_some_and(|v| v.is_mapped()),
                });
            }
        }
        out
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
            let store = guard.bm25.as_ref().and_then(|b| b.as_index());
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

    /// A fresh, empty exact-vector store for this shard: on disk beside
    /// the generation's sidecar path when the shard persists, so an
    /// ingest never holds its FP32 rows in heap (`docs/mmap-vectors.md`);
    /// in memory for an in-memory shard.
    pub(in crate::node) fn fresh_exact_store(
        &self,
        generation: Option<&PathBuf>,
        dim: usize,
    ) -> Result<ExactVectorStore, Status> {
        match self.config.index_path.as_ref() {
            Some(index_path) => {
                let (_, exact_path, _) = storage_paths(index_path, generation);
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
