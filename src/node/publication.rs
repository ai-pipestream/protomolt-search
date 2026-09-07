//! A trusted local segment transaction and its coherent serving-state switch.
use super::*;
use crate::segments::{OpenedSegmentSet, SegmentRowRetirement, SegmentSource};
use crate::stats_identity::StatsClaim;

struct PreparedServingState {
    index: Option<VectorIndex>,
    exact: Option<ExactVectorStore>,
    bm25: Bm25Shard,
    live: LiveDocs,
}

impl NodeServiceImpl {
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
        StatsClaim::required(expected.epoch, &expected.incarnation())?;
        let _ingest = self.claim_ingest()?;
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self
            .seal_lock
            .lock()
            .map_err(|_| Status::internal("seal lock poisoned"))?;
        let (catalog, old_set, old_live, backend, old_binding, next_claim) = {
            let guard = read_shard(&self.state);
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
        let mut prepared_commit = false;
        let result = catalog.commit_rows_prepared(catalog_epoch, retirements, sources, |set| {
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
            Ok((guard, prepared))
        });
        let (_, (mut guard, prepared)) = match result {
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
        // No fallible work remains after the durable manifest commit.
        guard.index = prepared.index;
        guard.exact_vectors = prepared.exact;
        guard.mapped_binding = prepared.bm25.binding().cloned();
        guard.bm25 = Some(prepared.bm25);
        guard.live_docs = prepared.live;
        guard.parents = None;
        guard.stats_epoch = next_claim.epoch;
        guard.files_current = false;
        Ok(next_claim)
    }

    fn check_segment_publication(
        &self,
        guard: &ShardState,
        expected: StatsClaim,
        epoch: u64,
    ) -> Result<(), Status> {
        guard.check_stats_epoch(expected.epoch, &expected.incarnation())?;
        if self.config.index_path.is_none()
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
        drop(node);
        std::fs::remove_dir_all(root).unwrap();
    }
}
