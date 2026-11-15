//! Prepare single-image artifacts and schema before durable generation adoption.

use crate::node::*;

pub(super) struct PreparedImage {
    index: Option<VectorIndex>,
    live: LiveDocs,
    retained: crate::multi_res::MultiResVectors,
}

impl PreparedImage {
    pub(super) fn open(
        service: &NodeServiceImpl,
        directory: &Path,
        with_exact: bool,
        with_bm25: bool,
        with_live: bool,
    ) -> Result<Self, Status> {
        let vector = generation_vector(directory);
        let index = if vector.exists() {
            Some(
                VectorIndex::load(&service.config.vector_backend, &vector).map_err(|e| {
                    Status::invalid_argument(format!(
                        "snapshot is not a valid vector backend image: {e}"
                    ))
                })?,
            )
        } else {
            None
        };
        check_received_provider(service, index.as_ref())?;
        check_exact(
            index.as_ref(),
            &generation_exact_vectors(directory),
            with_exact,
        )?;
        let document_rows =
            check_documents(&service.config, &generation_bm25(directory), with_bm25)?;
        let live = if with_live {
            LiveDocs::open(&generation_live_docs(directory)).map_err(|e| {
                Status::invalid_argument(format!("snapshot live-row overlay is invalid: {e}"))
            })?
        } else {
            LiveDocs::default()
        };
        let rows = index
            .as_ref()
            .map_or(0, |index| index.len() as u64)
            .max(document_rows);
        if live.persisted_rows() > rows {
            return Err(Status::invalid_argument(
                "snapshot live-row overlay exceeds every aligned artifact's row count",
            ));
        }
        let retained = crate::multi_res_storage::open_generation(
            &directory.join(crate::multi_res::FILE_NAME),
            rows,
            &live,
        )
        .map_err(|e| Status::invalid_argument(format!("snapshot retained vectors: {e}")))?;
        crate::multi_res_wal::require_coverage(
            &retained,
            0..index.as_ref().map_or(0, VectorIndex::len),
            &live,
            &service.config.matryoshka_prefix_dims,
            service.config.matryoshka_active_dim,
        )
        .map_err(Status::failed_precondition)?;
        Ok(Self {
            index,
            live,
            retained,
        })
    }

    pub(super) fn check_provider(&self, guard: &ShardState) -> Result<(), Status> {
        if let (Some(index), Some(loaded)) = (guard.index.as_ref(), self.index.as_ref()) {
            let current = index.descriptor();
            let incoming = loaded.descriptor();
            if current.backend_kind != incoming.backend_kind
                || current.scoring_fingerprint != incoming.scoring_fingerprint
            {
                return Err(Status::failed_precondition(
                    "snapshot vector backend or scoring fingerprint differs from the \
                     generation locked on this shard; mixed native scores are not mergeable",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn activate(
        self,
        guard: &mut ShardState,
        snap: &Path,
        with_exact_vectors: bool,
        with_bm25: bool,
    ) -> Result<(u64, u64), Status> {
        guard.bm25 = if with_bm25 {
            Some(Bm25Shard::open(&generation_bm25(snap)).map_err(|e| {
                Status::internal(format!(
                    "open installed {}: {e}",
                    generation_bm25(snap).display()
                ))
            })?)
        } else {
            // Wholesale replacement: an image without a sidecar replaces
            // any existing postings store (its ids would describe a
            // different corpus). The old store's files left with the old
            // generation.
            None
        };
        guard.exact_vectors = if with_exact_vectors {
            Some(
                ExactVectorStore::open(&generation_exact_vectors(snap)).map_err(|e| {
                    Status::internal(format!(
                        "open installed {}: {e}",
                        generation_exact_vectors(snap).display()
                    ))
                })?,
            )
        } else {
            None
        };
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let num_vectors = self.index.as_ref().map_or(0, |index| index.len() as u64);
        // Wholesale replace: the image's binding (usually none) is now
        // the shard's. A stale binding describing replaced columns
        // would lie.
        guard.mapped_binding = guard.bm25.as_ref().and_then(|b| b.binding().cloned());
        guard.index = self.index;
        guard.multi_res_vectors = self.retained;
        guard.live_docs = self.live;
        guard.parents = None;
        guard.generation = Some(snap.to_path_buf());
        guard.advance_stats_epoch();
        Ok((num_vectors, num_documents))
    }
}

fn check_received_provider(
    service: &NodeServiceImpl,
    loaded: Option<&VectorIndex>,
) -> Result<(), Status> {
    if let Some(loaded) = loaded {
        let incoming = loaded.descriptor();
        let guard = read_shard(&service.state);
        let serving_kind = guard
            .index
            .as_ref()
            .map(|index| index.descriptor().backend_kind)
            .or_else(|| {
                guard
                    .wal
                    .as_ref()
                    .map(|wal| wal.manifest().vector_backend.clone())
                    .filter(|kind| !kind.is_empty())
            });
        if let Some(kind) = serving_kind {
            if kind != incoming.backend_kind {
                return Err(Status::failed_precondition(format!(
                    "snapshot image is a {:?} image but this shard serves {kind:?}; a \
                         snapshot installs only from the same provider",
                    incoming.backend_kind
                )));
            }
        }
        if let Some(serving) = guard.index.as_ref().map(|index| index.descriptor()) {
            if serving.scoring_fingerprint != incoming.scoring_fingerprint {
                return Err(Status::failed_precondition(format!(
                    "snapshot image scores under {}/{} but this shard serves {}/{}; a \
                         snapshot installs only from the same provider state (calibration \
                         included), or the fleet would score in two spaces",
                    incoming.backend_kind,
                    incoming.scoring_fingerprint,
                    serving.backend_kind,
                    serving.scoring_fingerprint
                )));
            }
        }
    }
    Ok(())
}

fn check_exact(loaded: Option<&VectorIndex>, path: &Path, enabled: bool) -> Result<(), Status> {
    if enabled {
        let loaded = loaded
            .ok_or_else(|| Status::invalid_argument("exact vectors require a provider image"))?;
        let exact = ExactVectorStore::open(path).map_err(|e| {
            Status::invalid_argument(format!(
                "snapshot sidecar is not a valid exact-vector store: {e}"
            ))
        })?;
        exact.verify_payload().map_err(|e| {
            Status::invalid_argument(format!("snapshot exact-vector integrity check failed: {e}"))
        })?;
        if exact.len() != loaded.len() || exact.dim() != loaded.dim_opt() {
            return Err(Status::invalid_argument(format!(
                "snapshot exact-vector shape {:?}x{} does not match provider shape {:?}x{}",
                exact.dim(),
                exact.len(),
                loaded.dim_opt(),
                loaded.len()
            )));
        }
    }
    Ok(())
}

fn check_documents(config: &NodeConfig, path: &Path, enabled: bool) -> Result<u64, Status> {
    if !enabled {
        return Ok(0);
    }
    let store = Bm25Shard::open(path).map_err(|e| {
        Status::invalid_argument(format!("snapshot sidecar is not a valid BM25 store: {e}"))
    })?;
    check_attached_derived(
        config,
        store.derived(),
        store.doc_count(),
        "the installed snapshot's store",
    )
    .map_err(Status::failed_precondition)?;
    let mut tail = heap_store(config).map_err(Status::failed_precondition)?;
    let declaration = match &store {
        Bm25Shard::Building(heap) => crate::segments::generation::from_store(heap, None),
        Bm25Shard::Resident(reader) => crate::segments::generation::from_reader(reader, None),
        _ => return Err(Status::internal("opened snapshot has no standalone schema")),
    }
    .map_err(Status::failed_precondition)?;
    crate::segments::generation::restore_tail(&declaration, &mut tail).map_err(|error| {
        Status::failed_precondition(format!("snapshot field or column table differs: {error}"))
    })?;
    if let Some(binding) = store.binding() {
        if tail.binding().is_some_and(|held| held != binding) {
            return Err(Status::failed_precondition(
                "snapshot and configured mapped bindings disagree",
            ));
        }
        tail.set_binding(Some(binding.clone()));
    } else if store.next_doc_id() != 0 && tail.binding().is_some() {
        return Err(Status::failed_precondition(
            "a bound tail cannot label a populated unbound snapshot",
        ));
    }
    Ok(u64::from(store.next_doc_id()))
}

pub(super) fn check_pending_compaction(guard: &ShardState) -> Result<(), Status> {
    if guard.pending_compaction.is_some() {
        return Err(Status::failed_precondition(
            "a compaction cutover is pending its closing flush on this shard; call Flush \
             before installing a snapshot",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn legacy_images_keep_loader_dispatch_and_schema_admission() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("snapshot-legacy-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        for version in [1, 2] {
            let path = root.join(format!("v{version}.bm25"));
            std::fs::write(&path, legacy_document(version)).unwrap();
            assert!(Bm25Reader::open(&path).is_err());
            assert!(matches!(
                Bm25Shard::open(&path).unwrap(),
                Bm25Shard::Building(_)
            ));
            assert_eq!(
                check_documents(&NodeConfig::default(), &path, true).unwrap(),
                1
            );
            let config = NodeConfig {
                facet_fields: vec!["required".into()],
                ..Default::default()
            };
            let error = check_documents(&config, &path, true).unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(error.message().contains("column table"), "{error}");
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    fn legacy_document(version: u8) -> Vec<u8> {
        let mut bytes = format!("TVBM250{version}").into_bytes();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // One document slot.
        bytes.extend_from_slice(&0u32.to_le_bytes()); // No analyzed terms.
        bytes.extend_from_slice(&0u64.to_le_bytes()); // Total field length.
        bytes.extend_from_slice(&1u32.to_le_bytes()); // One stored text.
        bytes.extend_from_slice(&6u32.to_le_bytes());
        bytes.extend_from_slice(b"legacy");
        if version == 2 {
            bytes.push(0);
        } // No lineage.
        bytes.extend_from_slice(&0u32.to_le_bytes()); // No posting lists.
        bytes
    }
}
