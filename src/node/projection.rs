//! Accepted sources enter a private instance of the ordinary ingest pipeline.
use super::*;
use crate::document_catalog::DocumentCatalog;
use crate::pb::{DocumentProjectionStage, StageDocumentProjectionRequest};
use crate::segments::OpenedSegmentSet;
use crate::stats_identity::StatsClaim;

/// A private candidate, owned until publication or abandonment. Its description
/// is data, not a commit certificate. No constructor accepts caller-supplied rows.
pub struct StagedDocumentCandidate {
    info: DocumentProjectionStage,
    source: Option<crate::pb::ProtobufSource>,
    set: Option<Arc<OpenedSegmentSet>>,
    // Declared last so held mappings close before the owned files are removed.
    _directory: Option<Arc<StageDirectory>>,
}

impl StagedDocumentCandidate {
    pub fn info(&self) -> &DocumentProjectionStage {
        &self.info
    }

    /// Exact accepted bytes, including a source that produced no rows.
    pub fn source(&self) -> Option<&crate::pb::ProtobufSource> {
        self.source.as_ref()
    }

    /// Read-only artifact access for the trusted owner. A deletion has no set;
    /// an empty source has an empty set carrying its validated index binding.
    pub fn segments(&self) -> Option<&OpenedSegmentSet> {
        self.set.as_deref()
    }
}

struct StageDirectory(PathBuf);
impl StageDirectory {
    fn create(parent: &Path) -> Result<Arc<Self>, Status> {
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random).map_err(|e| Status::internal(e.to_string()))?;
        let suffix: String = random.iter().map(|b| format!("{b:02x}")).collect();
        let path = parent.join(format!(".document-projection-{suffix}"));
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&path)
            .map_err(|e| Status::internal(format!("create projection stage: {e}")))?;
        Ok(Arc::new(Self(path)))
    }
}
impl Drop for StageDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

pub(super) struct PreparedSource {
    rows: std::vec::IntoIter<crate::pb::DocumentProjectionRow>,
    original: crate::pb::ProtobufSource,
    key: Vec<u8>,
    analysis: crate::mapped_analysis::MappedAnalysis,
    materialize: Option<crate::pb::MaterializeSpec>,
}
impl PreparedSource {
    pub(super) fn next(&mut self) -> Result<Option<IngestDoc>, Status> {
        let Some(row) = self.rows.next() else {
            return Ok(None);
        };
        let mut req = row
            .document
            .ok_or_else(|| Status::data_loss("prepared document missing"))?;
        req.original_source = Some(self.original.clone());
        req.analysis = self.analysis.body.clone();
        for field in &mut req.fields {
            field.analysis = self.analysis.fields.get(&field.field).cloned();
        }
        req.materialize = self.materialize.clone();
        Ok(Some(IngestDoc {
            req,
            vector: Some(row.vector),
            stable_routing_key: Some(self.key.clone()),
        }))
    }
}

impl NodeServiceImpl {
    /// Stage an exact accepted version using this node's schema, provider and
    /// configured analyzer. All analysis, derived values and artifact writes
    /// affect a private node. The serving state and source receipts are untouched.
    /// Publication must revalidate the target version and join source recovery.
    pub async fn stage_document_projection(
        &self,
        catalog: Arc<DocumentCatalog>,
        request: StageDocumentProjectionRequest,
    ) -> Result<StagedDocumentCandidate, Status> {
        if !(1..=1024 * 1024 * 1024).contains(&request.max_staged_bytes) {
            return Err(Status::invalid_argument(
                "staging requires max_staged_bytes 1 byte to 1 GiB",
            ));
        }
        let projection = request.projection.ok_or_else(|| {
            Status::invalid_argument("staging requires an exact accepted projection request")
        })?;
        let collection = self.config.collection.clone();
        let (mut batch, projection) = tokio::task::spawn_blocking(move || {
            if catalog.collection()? != collection {
                return Err(Status::failed_precondition(
                    "projection catalog and target collection differ",
                ));
            }
            catalog
                .prepare_projection(&projection)
                .map(|batch| (batch, projection))
        })
        .await
        .map_err(|e| Status::internal(format!("prepare accepted projection task: {e}")))??;
        let (expected, bound, backend, rows) = {
            let guard = read_shard(&self.state);
            guard.check_catalog_publication()?;
            let expected =
                StatsClaim::required(guard.stats_epoch, &guard.stats_incarnation.bytes()?)?;
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
                            .map_err(|e| Status::failed_precondition(e.to_string()))?,
                    ))
                })
                .transpose()?;
            (
                expected,
                guard.mapped_binding.clone(),
                backend,
                physical_rows(&guard),
            )
        };
        let mut info = DocumentProjectionStage {
            history_id: batch.history_id.clone(),
            document_key: batch.document_key.clone(),
            version: batch.version,
            accepted_sequence: batch.accepted_sequence,
            deleted: batch.deleted,
            rows: batch.rows.len() as u64,
            plan_fingerprint: batch.plan_fingerprint.clone(),
            target_stats_epoch: expected.epoch,
            target_stats_incarnation: expected.incarnation(),
            ..Default::default()
        };
        if batch.deleted {
            if !request.field_analysis.is_empty() || request.materialize.is_some() {
                return Err(Status::invalid_argument(
                    "deletion staging cannot declare analysis or materialization",
                ));
            }
            return Ok(StagedDocumentCandidate {
                info,
                source: None,
                set: None,
                _directory: None,
            });
        }
        if request.field_analysis.is_empty() {
            return Err(Status::invalid_argument(
                "staging requires explicit analysis for every projected TEXT path",
            ));
        }
        if bound.is_none() && rows != 0 {
            return Err(Status::failed_precondition(
                "cannot stage a mapping for a populated unbound shard",
            ));
        }
        let source = batch
            .source
            .take()
            .ok_or_else(|| Status::data_loss("accepted projection source missing"))?;
        let bind = crate::pb::MappedBind {
            descriptor_set: source.descriptor_set.clone(),
            message_type: source.message_type.clone(),
            expected_fingerprint: batch.plan_fingerprint,
            body_path: batch.body_path,
            index_definition: projection.index_definition,
            collection: self.config.collection.clone(),
            field_analysis: request.field_analysis,
            materialize: request.materialize.clone(),
            ..Default::default()
        };
        let mut config = self.config.clone();
        if config.analysis_addr.is_none() {
            return Err(Status::failed_precondition(
                "projection target has no configured analyzer",
            ));
        }
        let index_path = config.index_path.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "private artifact staging requires a persistent target path",
            )
        })?;
        let parent = index_path
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf();
        let phrases = self.phrase_index.clone();
        let (directory, shadow, analysis) = tokio::task::spawn_blocking(move || {
            let directory = StageDirectory::create(&parent)?;
            config.index_path = Some(directory.0.join("index"));
            config.slot_offset = 0;
            config.wal = false;
            config.vocab = false;
            config.layout = Layout::Segments;
            // One bounded source is private until the closing flush. Avoid
            // detached seal tasks outliving cancellation and stage ownership.
            config.seal_tail_docs = 0;
            let index = backend.map(|(dim, config)| VectorIndex::from_backend_config(dim, &config).map_err(|e| Status::failed_precondition(e.to_string()))).transpose()?;
            let shadow = Self::new(index, config).with_phrase_index(phrases);
            let (_, analysis) = shadow.bind_mapped(&bind)?;
            if bound.is_some() && bound != read_shard(&shadow.state).mapped_binding {
                return Err(Status::failed_precondition("projection differs from the target's durable mapping, analysis or materialization binding"));
            }
            Ok::<_, Status>((directory, shadow, analysis))
        }).await.map_err(|e| Status::internal(format!("create projection stage task: {e}")))??;
        let mut input = IngestSource::Prepared(Box::new(PreparedSource {
            rows: batch.rows.into_iter(),
            original: source.clone(),
            key: batch.document_key,
            analysis,
            materialize: request.materialize,
        }));
        let mut added = 0;
        let mut first_id = 0;
        if let Some(first) = input.next().await? {
            let addr = shadow.config.analysis_addr.as_deref().ok_or_else(|| {
                Status::failed_precondition("projection target has no configured analyzer")
            })?;
            let session = crate::analyzer::AnalyzeStream::open_with_vocab(
                addr,
                first.req.analysis.as_ref(),
                None,
                session_layers(
                    &first.req,
                    shadow.phrase_index.as_deref(),
                    &shadow.config.sentence_fields,
                ),
            )
            .await?;
            shadow
                .ingest_streamed(session, first, &mut input, addr, &mut added, &mut first_id)
                .await?;
        }
        if added != info.rows {
            return Err(Status::data_loss(
                "staged row count differs from the complete accepted projection",
            ));
        }
        // The blocking writer owns a lease even if its awaiting task is dropped.
        let lease = Arc::clone(&directory);
        let max_bytes = request.max_staged_bytes;
        let (set, bytes) = tokio::task::spawn_blocking(move || {
            let _lease = lease;
            shadow.flush_index()?;
            let set = {
                let guard = read_shard(&shadow.state);
                let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                    return Err(Status::internal(
                        "projection stage lost its segmented layout",
                    ));
                };
                shard.snapshot().clone()
            };
            let bytes = staged_bytes(&_lease.0, max_bytes)?;
            Ok::<_, Status>((set, bytes))
        })
        .await
        .map_err(|e| Status::internal(format!("seal projection stage task: {e}")))??;
        info.staged_bytes = bytes;
        Ok(StagedDocumentCandidate {
            info,
            source: Some(source),
            set: Some(set),
            _directory: Some(directory),
        })
    }
}

fn staged_bytes(root: &Path, limit: u64) -> Result<u64, Status> {
    let mut pending = vec![root.to_path_buf()];
    let mut bytes = 0u64;
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(directory).map_err(|e| Status::internal(e.to_string()))? {
            let entry = entry.map_err(|e| Status::internal(e.to_string()))?;
            let kind = entry
                .file_type()
                .map_err(|e| Status::internal(e.to_string()))?;
            if kind.is_dir() {
                pending.push(entry.path());
            } else if kind.is_file() {
                let len = entry
                    .metadata()
                    .map_err(|e| Status::internal(e.to_string()))?
                    .len();
                bytes = bytes.checked_add(len).ok_or_else(|| {
                    Status::resource_exhausted("projection stage byte count overflow")
                })?;
                if bytes > limit {
                    return Err(Status::resource_exhausted(
                        "projection stage exceeds max_staged_bytes",
                    ));
                }
            } else {
                return Err(Status::failed_precondition(
                    "projection stage contains a non-regular artifact",
                ));
            }
        }
    }
    Ok(bytes)
}
