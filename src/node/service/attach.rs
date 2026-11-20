//! Startup attachment of preloaded stores and sidecars.

use crate::node::*;

impl NodeServiceImpl {
    /// Attach a preloaded BM25 shard. The store's persisted mapped-plan
    /// binding becomes the shard's: the catalog IS the durable record of
    /// what its columns were written under.
    pub fn with_bm25(self, store: Option<Bm25Shard>) -> Self {
        {
            let mut guard = write_shard(&self.state);
            if let Err(error) = guard.check_index_owned_write() {
                drop(guard);
                panic!("{error}");
            }
            if let Some(Bm25Shard::Segmented(shard)) = store.as_ref() {
                if let Some(owner) = &shard.snapshot().manifest().source_owner {
                    let owner = owner.decode().expect("validated source owner");
                    assert!(
                        owner.collection == self.config.collection && !self.config.wal,
                        "source-managed index requires its original collection and WAL disabled"
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
}
