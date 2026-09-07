//! A trusted local segment transaction and its coherent serving-state switch.
use super::*;
use crate::document_catalog::{DocumentCatalog, ProjectionRecovery};
use crate::pb::{storage::ProjectionIntent, DocumentProjectionActivation};
use crate::segments::{OpenedSegmentSet, SegmentRowRetirement, SegmentSource};
use crate::stats_identity::StatsClaim;

#[cfg(test)]
thread_local! { pub(crate) static FAIL_PROJECTION_DECISION: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[derive(Clone, Copy)]
struct SourcePublication<'a> {
    catalog: &'a DocumentCatalog,
    index_key: &'a [u8],
    candidate: &'a StagedDocumentCandidate,
}

struct PreparedServingState {
    index: Option<VectorIndex>,
    exact: Option<ExactVectorStore>,
    bm25: Bm25Shard,
    live: LiveDocs,
}

pub(super) fn check_source_live_view(
    set: &OpenedSegmentSet,
    live: &LiveDocs,
) -> Result<(), Status> {
    let mut durable = LiveDocs::default();
    set.merge_tombstones(&mut durable)
        .map_err(Status::failed_precondition)?;
    let actual = live.words();
    if live.deleted_count() != durable.deleted_count()
        || durable.words().is_some_and(|words| {
            words.iter().enumerate().any(|(i, word)| {
                *word
                    != actual
                        .as_ref()
                        .and_then(|words| words.get(i))
                        .copied()
                        .unwrap_or(0)
            })
        })
    {
        return Err(Status::failed_precondition(
            "source publication cannot certify uncommitted runtime tombstones",
        ));
    }
    Ok(())
}

fn activation(
    intent: &ProjectionIntent,
    claim: StatsClaim,
    catalog_epoch: u64,
) -> DocumentProjectionActivation {
    let source = intent
        .source
        .as_ref()
        .expect("validated publication source");
    DocumentProjectionActivation {
        history_id: intent.history_id.clone(),
        index_key: intent.index_key.clone(),
        document_key: source.document_key.clone(),
        version: source.version,
        accepted_sequence: source.accepted_sequence,
        deleted: source.deleted,
        rows: intent.rows,
        intent_id: intent.intent_id.clone(),
        catalog_epoch,
        stats_epoch: claim.epoch,
        stats_incarnation: claim.incarnation().to_vec(),
    }
}

impl NodeServiceImpl {
    /// Join the reopened serving view with source-journal recovery. The result
    /// identifies this node's current read version, including a new incarnation
    /// after restart. A pending/dirty tail or uncommitted deletion cannot qualify.
    pub fn recover_document_projection_blocking(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
    ) -> Result<Option<DocumentProjectionActivation>, Status> {
        if source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "publication source and target collections differ",
            ));
        }
        let _ingest = self.claim_ingest()?;
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self
            .seal_lock
            .lock()
            .map_err(|_| Status::internal("seal lock poisoned"))?;
        let guard = write_shard(&self.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(
                "source recovery requires a segmented target",
            ));
        };
        let claim = StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes()?)?;
        if let Some(owner) = &shard.snapshot().manifest().source_owner {
            if owner.decode().map_err(Status::data_loss)? != source.index_owner(index_key)? {
                return Err(Status::failed_precondition(
                    "source recovery belongs to another index owner",
                ));
            }
        }
        self.check_segment_publication(&guard, claim, shard.snapshot().epoch())?;
        check_source_live_view(shard.snapshot(), &guard.live_docs)?;
        match source.recover_index_publication(index_key, shard.catalog()) {
            Ok(_) => {}
            Err(status) if status.code() == tonic::Code::NotFound => {}
            Err(status) => return Err(status),
        }
        source
            .current_index_publication_decision(index_key, shard.catalog())
            .map(|intent| intent.map(|intent| activation(&intent, claim, shard.snapshot().epoch())))
    }

    /// Activate one complete private candidate and record its artifact decision.
    /// Call on a blocking worker. The target read version captured at staging
    /// must still hold. This is a trusted local owner operation, not a public
    /// ingest route or a collection-wide searchable write receipt.
    pub fn publish_document_projection_blocking(
        &self,
        source: &DocumentCatalog,
        index_key: &[u8],
        candidate: &StagedDocumentCandidate,
    ) -> Result<DocumentProjectionActivation, Status> {
        if index_key.is_empty() || index_key.len() > 1024 {
            return Err(Status::invalid_argument(
                "projection index_key must contain 1 to 1024 bytes",
            ));
        }
        if source.collection()? != self.config.collection {
            return Err(Status::failed_precondition(
                "publication source and target collections differ",
            ));
        }
        let info = candidate.info();
        let expected =
            StatsClaim::required(info.target_stats_epoch, &info.target_stats_incarnation)?;
        let before = {
            let guard = read_shard(&self.state);
            guard.check_stats_epoch(expected.epoch, &expected.incarnation())?;
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(Status::failed_precondition(
                    "source publication requires a segmented target",
                ));
            };
            shard.snapshot().clone()
        };
        let retired = before
            .document_retirements(&info.document_key, 65536)
            .map_err(Status::failed_precondition)?;
        let staged = candidate.segments();
        let ids: Vec<_> = (0..staged.map_or(0, OpenedSegmentSet::len))
            .map(|i| {
                format!(
                    "source-{}-{}-{i}",
                    crate::sha256::hex_digest(index_key),
                    info.accepted_sequence
                )
            })
            .collect();
        let paths: Vec<_> = staged
            .into_iter()
            .flat_map(|set| {
                (0..set.len()).map(move |i| {
                    let meta = set.metadata(i);
                    let dir =
                        crate::segments::SegmentCatalog::segment_dir(set.root(), &meta.segment_id);
                    [
                        dir.join(&meta.vector.file),
                        dir.join(&meta.exact_vectors.file),
                        dir.join(&meta.bm25.file),
                        dir.join(&meta.live_docs.file),
                    ]
                })
            })
            .collect();
        let mut base = before
            .manifest()
            .segments
            .last()
            .map(|m| m.end_label_exclusive())
            .transpose()
            .map_err(Status::failed_precondition)?
            .unwrap_or(0);
        let mut sources = Vec::with_capacity(paths.len());
        for (i, paths) in paths.iter().enumerate() {
            let meta = staged
                .expect("candidate paths require segments")
                .metadata(i);
            sources.push(SegmentSource {
                segment_id: &ids[i],
                generation: info.accepted_sequence,
                base_label: base,
                backend_kind: &meta.backend_kind,
                vector_path: (!meta.vector.file.is_empty()).then_some(paths[0].as_path()),
                exact_vector_path: (!meta.exact_vectors.file.is_empty())
                    .then_some(paths[1].as_path()),
                bm25_path: &paths[2],
                live_docs_path: &paths[3],
                partition_column: None,
            });
            base = base
                .checked_add(meta.rows)
                .ok_or_else(|| Status::resource_exhausted("publication row range overflows"))?;
        }
        let (claim, intent) = self.publish_rows_inner(
            expected,
            before.epoch(),
            &retired,
            sources,
            Some(SourcePublication {
                catalog: source,
                index_key,
                candidate,
            }),
        )?;
        let intent = intent.expect("source publication produced an intent");
        Ok(activation(&intent, claim, intent.after_epoch))
    }

    /// Publish immutable segment rows and activate both search legs under one
    /// shard write fence. Call on a blocking worker, not a Tokio worker thread.
    /// This privileged local operation is not a document API: the source
    /// authority must still certify accepted versions and coordinate shards.
    /// No authentication boundary or document durability receipt is introduced.
    ///
    /// The node must have a persistent segment catalog, no mutable/frozen tail,
    /// and no WAL or pending compaction. A WAL-backed generation needs a joined
    /// source/WAL transaction; bypassing its replay history is refused.
    pub fn publish_segment_rows_blocking(
        &self,
        expected: StatsClaim,
        catalog_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
    ) -> Result<StatsClaim, Status> {
        self.publish_rows_inner(expected, catalog_epoch, retirements, sources, None)
            .map(|(claim, _)| claim)
    }

    fn publish_rows_inner(
        &self,
        expected: StatsClaim,
        catalog_epoch: u64,
        retirements: &[SegmentRowRetirement],
        sources: Vec<SegmentSource<'_>>,
        projection: Option<SourcePublication<'_>>,
    ) -> Result<(StatsClaim, Option<ProjectionIntent>), Status> {
        StatsClaim::required(expected.epoch, &expected.incarnation())?;
        let _ingest = self.claim_ingest()?;
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self
            .seal_lock
            .lock()
            .map_err(|_| Status::internal("seal lock poisoned"))?;
        let (catalog, old_set, old_live, backend, old_binding, next_claim) = {
            let guard = read_shard(&self.state);
            if projection.is_none() {
                guard.check_legacy_mutation()?;
            }
            self.check_segment_publication(&guard, expected, catalog_epoch)?;
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                unreachable!()
            };
            let backend = guard
                .index
                .as_ref()
                .map(|index| {
                    Ok::<_, Status>((
                        index.dim_opt().ok_or_else(|| {
                            Status::failed_precondition("provider has no dimension")
                        })?,
                        index
                            .backend_config()
                            .map_err(|error| Status::failed_precondition(error.to_string()))?,
                    ))
                })
                .transpose()?;
            let next = guard.stats_epoch.checked_add(1).ok_or_else(|| {
                Status::resource_exhausted("publication statistics epoch exhausted")
            })?;
            let next_claim = StatsClaim::required(next, &guard.stats_incarnation.bytes()?)?;
            (
                shard.catalog().clone(),
                shard.snapshot().clone(),
                guard.live_docs.clone(),
                backend,
                guard.mapped_binding.clone(),
                next_claim,
            )
        };
        if projection.is_some() {
            check_source_live_view(&old_set, &old_live)?;
        }
        let source_owner = projection
            .map(|source| source.catalog.index_owner(source.index_key))
            .transpose()?;
        let declaration = projection
            .map(|source| {
                use crate::segments::generation;
                // The reviewed configuration supplies even columns with no rows.
                let mut declared =
                    generation::from_store(&heap_store(&self.config)?, backend.clone())?;
                if let Some(previous) = old_set.generation_declaration() {
                    generation::check_upgrade(&declared, previous)?;
                    declared = previous.clone();
                } else if !old_set.is_empty() {
                    let previous = generation::from_reader(old_set.bm25(0), backend.clone())?;
                    generation::check_upgrade(&declared, &previous)?;
                    declared = previous;
                }
                if let Some(staged) = source.candidate.segments().filter(|set| !set.is_empty()) {
                    let mut selected_backend = generation::backend(&declared);
                    if selected_backend.is_none() {
                        if let Some(vector) = (0..staged.len()).find_map(|i| staged.vector(i)) {
                            selected_backend = Some((
                                vector
                                    .dim_opt()
                                    .ok_or("candidate vector dimension is absent")?,
                                vector.backend_config().map_err(|error| error.to_string())?,
                            ));
                        }
                    }
                    let candidate = generation::from_reader(staged.bm25(0), selected_backend)?;
                    generation::check_upgrade(&declared, &candidate)?;
                    declared = candidate;
                }
                Ok::<_, String>(declared)
            })
            .transpose()
            .map_err(Status::failed_precondition)?;
        let mut prepared_commit = false;
        let prepare = |set: &Arc<OpenedSegmentSet>| {
            let prepared = self.prepare_serving_state(&catalog, set, old_live, backend)?;
            if (!old_set.is_empty() || old_binding.is_some())
                && prepared.bm25.binding() != old_binding.as_ref()
            {
                return Err("publication changes the current mapped binding".into());
            }
            // All expensive preparation happened while queries could still read
            // the old state. Keep this guard through manifest sync and activation.
            let guard = write_shard(&self.state);
            self.check_segment_publication(&guard, expected, catalog_epoch)
                .map_err(|status| status.to_string())?;
            prepared_commit = true;
            let intent = projection
                .map(|source| {
                    source
                        .catalog
                        .prepare_index_publication(
                            source.index_key,
                            source.candidate,
                            &old_set,
                            set,
                        )
                        .map_err(|status| status.to_string())
                })
                .transpose()?;
            Ok((guard, prepared, intent))
        };
        let result = match projection {
            Some(source) => catalog.commit_projection_prepared(
                catalog_epoch,
                retirements,
                sources,
                source
                    .candidate
                    .segments()
                    .and_then(OpenedSegmentSet::binding),
                source_owner
                    .as_ref()
                    .expect("source publication has an owner"),
                declaration
                    .as_ref()
                    .expect("source publication has a declaration"),
                prepare,
            ),
            None => catalog.commit_rows_prepared(catalog_epoch, retirements, sources, prepare),
        };
        let (_, (mut guard, prepared, intent)) = match result {
            Ok(result) => result,
            Err(error) => {
                if prepared_commit {
                    self.fence_ingest(
                        "segment publication did not acknowledge its manifest; reopen for recovery"
                            .into(),
                    );
                }
                return Err(Status::failed_precondition(error));
            }
        };
        // Install the already-prepared fields without fallible work. For source
        // publication, the journal decision is checked next under this guard.
        guard.index = prepared.index;
        guard.exact_vectors = prepared.exact;
        guard.mapped_binding = prepared.bm25.binding().cloned();
        guard.bm25 = Some(prepared.bm25);
        guard.live_docs = prepared.live;
        guard.parents = None;
        guard.stats_epoch = next_claim.epoch;
        guard.files_current = false;
        if let Some(source) = projection {
            #[cfg(test)]
            if FAIL_PROJECTION_DECISION.with(|fail| fail.replace(false)) {
                self.fence_ingest(
                    "injected interruption after activation; reopen for recovery".into(),
                );
                return Err(Status::unavailable(
                    "injected interruption before source decision",
                ));
            }
            let resolved = source
                .catalog
                .recover_index_publication(source.index_key, &catalog);
            match resolved {
                Ok(ProjectionRecovery::Committed(ref decided))
                    if Some(decided) == intent.as_ref() => {}
                result => {
                    self.fence_ingest("source publication activation lacks a matching durable decision; reopen for recovery".into());
                    return Err(match result {
                        Err(status) => status,
                        Ok(_) => Status::data_loss(
                            "source publication resolved a different artifact decision",
                        ),
                    });
                }
            }
        }
        Ok((next_claim, intent))
    }

    fn check_segment_publication(
        &self,
        guard: &ShardState,
        expected: StatsClaim,
        epoch: u64,
    ) -> Result<(), Status> {
        guard.check_stats_epoch(expected.epoch, &expected.incarnation())?;
        if self.config.index_path.is_none()
            || self.config.wal
            || guard.generation.is_some()
            || guard.wal.is_some()
            || guard.pending_compaction.is_some()
            || self.compacting.load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(Status::failed_precondition("segment publication requires a persistent catalog without WAL or active compaction"));
        }
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(
                "segment publication requires the segmented layout",
            ));
        };
        if shard.snapshot().epoch() != epoch || shard.catalog().snapshot().epoch() != epoch {
            return Err(Status::failed_precondition(
                "segment publication catalog epoch changed",
            ));
        }
        if shard.tail().next_doc_id() != 0 || shard.frozen().is_some() {
            return Err(Status::failed_precondition(
                "segment publication requires a sealed document tail; Flush first",
            ));
        }
        if let Some(index) = guard.index.as_ref() {
            match index.as_segmented() {
                Some(provider) if !provider.tail().is_empty() || provider.frozen().is_some() => {
                    return Err(Status::failed_precondition(
                        "segment publication requires a sealed vector tail; Flush first",
                    ))
                }
                None if !index.is_empty() => {
                    return Err(Status::failed_precondition(
                        "segment publication cannot replace an unsealed vector image",
                    ))
                }
                _ => {}
            }
        } else if guard
            .exact_vectors
            .as_ref()
            .is_some_and(|exact| !exact.is_empty())
        {
            return Err(Status::failed_precondition(
                "segment publication cannot bypass a remote provider's exact-vector sidecar",
            ));
        }
        Ok(())
    }

    fn prepare_serving_state(
        &self,
        catalog: &crate::segments::SegmentCatalog,
        set: &Arc<OpenedSegmentSet>,
        mut live: LiveDocs,
        mut backend: Option<(usize, crate::vector::VectorBackendConfig)>,
    ) -> Result<PreparedServingState, String> {
        let shard =
            SegmentedShard::from_snapshot(catalog.clone(), set.clone(), heap_store(&self.config)?)?;
        check_attached_derived(
            &self.config,
            shard.derived(),
            shard.doc_count(),
            "the publication catalog",
        )?;
        if let Some(declaration) = set.generation_declaration() {
            let declared = crate::segments::generation::backend(declaration);
            if backend
                .as_ref()
                .is_some_and(|held| declared.as_ref() != Some(held))
            {
                return Err("publication changes the established vector backend".into());
            }
            backend = declared;
        }
        if backend.is_none() {
            if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
                if first.descriptor().backend_kind != self.config.vector_backend {
                    return Err("publication vector backend differs from node configuration".into());
                }
                backend = Some((
                    first
                        .dim_opt()
                        .ok_or("publication provider has no dimension")?,
                    first.backend_config().map_err(|error| error.to_string())?,
                ));
            }
        }
        let (index, exact) = if let Some((dim, config)) = backend {
            if config.backend_kind != self.config.vector_backend {
                return Err("publication vector backend differs from node configuration".into());
            }
            let tail = VectorIndex::from_backend_config(dim, &config)
                .map_err(|error| error.to_string())?;
            let provider =
                SegmentedProvider::open(set.clone(), tail).map_err(|error| error.to_string())?;
            let exact =
                ExactVectorStore::from_segments(set, dim).map_err(|error| error.to_string())?;
            (Some(VectorIndex::from_provider(provider)), Some(exact))
        } else {
            (None, None)
        };
        set.merge_tombstones(&mut live)?;
        Ok(PreparedServingState {
            index,
            exact,
            bm25: Bm25Shard::Segmented(shard),
            live,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::node_service_server::NodeService;
    use crate::segments::{SegmentCatalog, SegmentSource};

    fn fixture(label: &str) -> (PathBuf, NodeConfig, Arc<OpenedSegmentSet>) {
        let root =
            std::env::temp_dir().join(format!("node-publication-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("index");
        let bm25_path = root.join("documents.bm25");
        let live_path = root.join("live.bin");
        let mut store = crate::postings::Bm25Store::with_fields(&["body"]);
        for row in 0..2 {
            store.add_document(
                row,
                "document".into(),
                crate::analyzer::analyze_document_native(
                    "document",
                    Some(&crate::analyzer::body_spec()),
                )
                .unwrap(),
            );
        }
        store.save(&bm25_path).unwrap();
        LiveDocs::default().write(&live_path, 2).unwrap();
        let catalog = SegmentCatalog::open(segments_root(&path)).unwrap();
        let old = catalog
            .append(SegmentSource {
                segment_id: "old",
                generation: 1,
                base_label: 0,
                backend_kind: "",
                vector_path: None,
                exact_vector_path: None,
                bm25_path: &bm25_path,
                live_docs_path: &live_path,
                partition_column: None,
            })
            .unwrap();
        let config = NodeConfig {
            index_path: Some(path),
            wal: false,
            ..Default::default()
        };
        (root, config, old)
    }

    fn claim(node: &NodeServiceImpl) -> StatsClaim {
        let guard = read_shard(&node.state);
        StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes().unwrap()).unwrap()
    }

    #[test]
    fn uncertain_manifest_keeps_old_runtime_and_blocks_flush_until_reopen() {
        let (root, config, old) = fixture("uncertain");
        let node = NodeServiceImpl::open(config.clone(), None, false).unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let claim = {
            let guard = read_shard(&node.state);
            StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes().unwrap())
                .unwrap()
        };
        node.flush_index().unwrap();
        assert!(read_shard(&node.state).files_current);
        crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
        let error = node
            .publish_segment_rows_blocking(
                claim,
                old.epoch(),
                &[SegmentRowRetirement {
                    segment_id: "old".into(),
                    rows: vec![0],
                }],
                vec![],
            )
            .unwrap_err();
        assert!(error.message().contains("after manifest rename"), "{error}");
        let held = runtime
            .block_on(node.health(Request::new(crate::pb::HealthRequest {})))
            .unwrap()
            .into_inner();
        assert_eq!(read_shard(&node.state).stats_epoch, claim.epoch);
        assert_eq!(held.deleted_docs, 0);
        assert!(node.ingest_fence().is_some());
        // Model an export whose initial flush preceded the uncertain commit.
        let export = root.join("export");
        std::fs::create_dir(&export).unwrap();
        let copy_error = node
            .export_if_flushed(config.index_path.as_ref().unwrap(), &export)
            .err()
            .expect("an already-flushed snapshot must reject uncertain publication");
        assert!(copy_error.message().contains("uncertain"));
        assert_eq!(std::fs::read_dir(&export).unwrap().count(), 0);
        assert!(node
            .flush_index()
            .unwrap_err()
            .message()
            .contains("uncertain"));
        assert_eq!(old.live_docs(0).deleted_count(), 0);
        drop(node);
        let recovered = NodeServiceImpl::open(config, None, false).unwrap();
        let recovered_health = runtime
            .block_on(recovered.health(Request::new(crate::pb::HealthRequest {})))
            .unwrap()
            .into_inner();
        assert_eq!(recovered_health.deleted_docs, 1);
        drop(recovered);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_refuses_stale_owners_and_unsealed_rows_without_changing_disk() {
        let (root, config, old) = fixture("preconditions");
        let node = NodeServiceImpl::open(config.clone(), None, false).unwrap();
        let expected = claim(&node);
        let manifest_path =
            segments_root(config.index_path.as_ref().unwrap()).join("segments.json");
        let before = std::fs::read(&manifest_path).unwrap();
        let retirement = [SegmentRowRetirement {
            segment_id: "old".into(),
            rows: vec![0],
        }];
        for (version, epoch) in [
            (StatsClaim::default(), old.epoch()),
            (
                StatsClaim::required(expected.epoch, &[1; 32]).unwrap(),
                old.epoch(),
            ),
            (expected, old.epoch() + 1),
        ] {
            assert!(node
                .publish_segment_rows_blocking(version, epoch, &retirement, vec![])
                .is_err());
            assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
        }
        {
            let mut guard = write_shard(&node.state);
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_mut() else {
                panic!("segmented fixture")
            };
            shard
                .add_document(
                    2,
                    "pending".into(),
                    crate::analyzer::analyze_document_native(
                        "pending",
                        Some(&crate::analyzer::body_spec()),
                    )
                    .unwrap(),
                    None,
                )
                .unwrap();
        }
        let error = node
            .publish_segment_rows_blocking(expected, old.epoch(), &retirement, vec![])
            .unwrap_err();
        assert!(error.message().contains("sealed document tail"), "{error}");
        assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
        assert_eq!(
            read_shard(&node.state).bm25.as_ref().unwrap().doc_count(),
            3
        );
        assert!(node.ingest_fence().is_none());
        drop(node);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn publication_cannot_bypass_wal_recovery() {
        let (root, mut config, old) = fixture("wal");
        config.wal = true;
        let node = NodeServiceImpl::open(config.clone(), None, false).unwrap();
        let manifest_path =
            segments_root(config.index_path.as_ref().unwrap()).join("segments.json");
        let before = std::fs::read(&manifest_path).unwrap();
        let error = node
            .publish_segment_rows_blocking(
                claim(&node),
                old.epoch(),
                &[SegmentRowRetirement {
                    segment_id: "old".into(),
                    rows: vec![0],
                }],
                vec![],
            )
            .unwrap_err();
        assert!(error.message().contains("without WAL"), "{error}");
        assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
        assert!(node.ingest_fence().is_none());
        // Retiring a failed writer does not change the configured recovery
        // contract. It must not turn this into an unlogged publication target.
        write_shard(&node.state).wal = None;
        let error = node
            .publish_segment_rows_blocking(
                claim(&node),
                old.epoch(),
                &[SegmentRowRetirement {
                    segment_id: "old".into(),
                    rows: vec![0],
                }],
                vec![],
            )
            .unwrap_err();
        assert!(error.message().contains("without WAL"), "{error}");
        assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
        drop(node);
        std::fs::remove_dir_all(root).unwrap();
    }
}
