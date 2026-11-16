//! The shared publication fixture: a durable source catalog, a target
//! segment catalog the tests publish to directly, and a node that owns a
//! private index. Every helper publishes under `INDEX`; the document key,
//! version, row count, segment and fault a test differs in are arguments
//! at its call site.
use super::*;

mod support;
pub(super) use support::*;

pub(super) const INDEX: &[u8] = b"logical-index\0v1";
pub(super) const KEY: &[u8] = b"document\0one";
pub(super) const DESCRIPTOR: &[u8] =
    include_bytes!("../../../../tests/fixtures/unsigned-mapping/descriptor.bin");
pub(super) const TYPE: &str = "unsigned_mapping.Parent";
#[derive(Clone, PartialEq, Message)]
struct Parent {
    #[prost(fixed64, tag = "1")]
    id: u64,
    #[prost(message, repeated, tag = "2")]
    chunks: Vec<Chunk>,
}
#[derive(Clone, PartialEq, Message)]
struct Chunk {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(string, tag = "2")]
    body: String,
    #[prost(float, repeated, tag = "3")]
    embedding: Vec<f32>,
}
pub(super) struct Fixture {
    pub(super) root: PathBuf,
    pub(super) source: Arc<DocumentCatalog>,
    pub(super) target: SegmentCatalog,
    pub(super) node: NodeServiceImpl,
}
impl Fixture {
    pub(super) fn new() -> Self {
        static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "projection-journal-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        let source = Arc::new(DocumentCatalog::create(&root.join("source.redb"), "books").unwrap());
        let target = SegmentCatalog::open(root.join("target")).unwrap();
        let node = NodeServiceImpl::open(
            NodeConfig {
                collection: "books".into(),
                index_path: Some(root.join("private-target")),
                wal: false,
                analysis_addr: Some("native".into()),
                unsigned_integer_fields: mapping::derive_plan(DESCRIPTOR, TYPE)
                    .unwrap()
                    .fields
                    .iter()
                    .filter(|f| f.family == ColumnFamily::U64 as i32)
                    .map(|f| f.name.clone())
                    .collect(),
                ..Default::default()
            },
            None,
        )
        .unwrap();
        Self {
            root,
            source,
            target,
            node,
        }
    }
    pub(super) async fn stage(
        &self,
        key: &[u8],
        version: u64,
        rows: Option<usize>,
    ) -> (DocumentWriteReceipt, StagedDocumentCandidate) {
        let source = rows.map(|count| ProtobufSource {
            descriptor_set: DESCRIPTOR.to_vec(),
            message_type: TYPE.into(),
            payload: Parent {
                id: u64::MAX,
                chunks: (0..count)
                    .map(|i| Chunk {
                        id: i as u64,
                        body: format!("version {version} chunk {i}"),
                        embedding: vec![0.25; 8],
                    })
                    .collect(),
            }
            .encode_to_vec(),
        });
        let receipt = self
            .source
            .accept(&AcceptDocumentRequest {
                contract_version: 1,
                document_key: key.to_vec(),
                operation_id: [key, &version.to_be_bytes()].concat(),
                expected_version: Some(version - 1),
                mutation: Some(source.map_or(Mutation::Delete(true), Mutation::Source)),
                ..Default::default()
            })
            .unwrap();
        let candidate = self
            .node
            .stage_document_projection(
                self.source.clone(),
                StageDocumentProjectionRequest {
                    projection: Some(PrepareDocumentProjectionRequest {
                        history_id: receipt.history_id.clone(),
                        document_key: key.to_vec(),
                        version,
                        expected_plan_fingerprint: if rows.is_some() {
                            mapping::derive_plan(DESCRIPTOR, TYPE).unwrap().fingerprint
                        } else {
                            String::new()
                        },
                        max_rows: 100,
                        max_bytes: 1024 * 1024,
                        ..Default::default()
                    }),
                    field_analysis: if rows.is_some() {
                        vec![MappedFieldAnalysis {
                            path: "chunks.body".into(),
                            analysis: Some(body_spec()),
                        }]
                    } else {
                        vec![]
                    },
                    max_staged_bytes: 16 * 1024 * 1024,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        (receipt, candidate)
    }
    pub(super) fn bind(&self, candidate: &StagedDocumentCandidate) {
        self.target
            .publish_binding(candidate.segments().unwrap().binding().unwrap())
            .unwrap();
    }
    pub(super) fn publish(
        &self,
        candidate: &StagedDocumentCandidate,
        retire: Option<Vec<SegmentRowRetirement>>,
        stop: bool,
    ) -> Result<ProjectionIntent, String> {
        self.publish_for(INDEX, candidate, retire, stop)
    }
    pub(super) fn publish_for(
        &self,
        index: &[u8],
        candidate: &StagedDocumentCandidate,
        retire: Option<Vec<SegmentRowRetirement>>,
        stop: bool,
    ) -> Result<ProjectionIntent, String> {
        let before = self.target.snapshot();
        let retired = retire.unwrap_or_else(|| {
            before
                .document_retirements(&candidate.info().document_key, 65536)
                .unwrap()
        });
        let ids: Vec<_> = (0..candidate.segments().map_or(0, OpenedSegmentSet::len))
            .map(|i| format!("accepted-{}-{i}", candidate.info().accepted_sequence))
            .collect();
        let paths: Vec<_> = candidate
            .segments()
            .into_iter()
            .flat_map(|set| {
                (0..set.len()).map(move |i| {
                    let meta = set.metadata(i);
                    let dir = SegmentCatalog::segment_dir(set.root(), &meta.segment_id);
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
            .map(|m| m.end_label_exclusive().unwrap())
            .unwrap_or(0);
        let sources: Vec<_> = paths
            .iter()
            .enumerate()
            .map(|(i, paths)| {
                let meta = candidate.segments().unwrap().metadata(i);
                let source = SegmentSource {
                    multi_res_path: None,
                    segment_id: &ids[i],
                    generation: candidate.info().accepted_sequence,
                    base_label: base,
                    backend_kind: &meta.backend_kind,
                    vector_path: (!meta.vector.file.is_empty()).then_some(paths[0].as_path()),
                    exact_vector_path: (!meta.exact_vectors.file.is_empty())
                        .then_some(paths[1].as_path()),
                    bm25_path: &paths[2],
                    live_docs_path: &paths[3],
                    partition_column: None,
                };
                base += meta.rows;
                source
            })
            .collect();
        self.target
            .commit_rows_prepared(before.epoch(), &retired, sources, |after| {
                let intent = self
                    .source
                    .prepare_index_publication(index, candidate, &before, after)
                    .map_err(|s| s.to_string())?;
                assert_eq!(
                    self.source
                        .prepare_index_publication(index, candidate, &before, after)
                        .unwrap(),
                    intent
                );
                if stop {
                    Err("interrupted after intent".into())
                } else {
                    Ok(intent)
                }
            })
            .map(|(_, intent)| intent)
    }

    /// Stage `rows` chunks of `key` at `version` (`None` deletes it) and
    /// publish the projection through the node that owns the private index.
    pub(super) async fn publish_through_node(
        &self,
        key: &[u8],
        version: u64,
        rows: Option<usize>,
    ) -> DocumentProjectionActivation {
        let (_, candidate) = self.stage(key, version, rows).await;
        let node = self.node.clone();
        let source = self.source.clone();
        tokio::task::spawn_blocking(move || {
            node.publish_document_projection_blocking(&source, INDEX, &candidate)
        })
        .await
        .unwrap()
        .unwrap()
    }

    /// Close the source catalog's file. `self.source` is an unrelated
    /// in-memory catalog until the caller replaces it.
    pub(super) fn close_source(&mut self) {
        let old = std::mem::replace(
            &mut self.source,
            Arc::new(DocumentCatalog::in_memory("books").unwrap()),
        );
        drop(old);
    }

    /// Close the source catalog and open it again from its file.
    pub(super) fn reopen_source(&mut self) {
        self.close_source();
        self.source =
            Arc::new(DocumentCatalog::open(&self.root.join("source.redb"), "books").unwrap());
    }

    /// The history identity of the source catalog.
    pub(super) fn history_id(&self) -> Vec<u8> {
        history_id(&self.source)
    }

    /// The segment-set root of the node's private index.
    pub(super) fn node_segments_root(&self) -> PathBuf {
        crate::node::segments_root(self.node.config.index_path.as_ref().unwrap())
    }

    /// Submit segment `rewrite.segment` of `before`, unchanged, as a
    /// maintenance rewrite of the node's index under the `active` claim.
    /// `arm` runs first on the publishing thread, where the fault hooks
    /// are thread-local. Returns the node's answer.
    pub(super) async fn rewrite_segment(
        &self,
        active: &DocumentProjectionActivation,
        before: &OpenedSegmentSet,
        rewrite: Rewrite<'_>,
        arm: impl FnOnce() + Send + 'static,
    ) -> Result<DocumentMaintenanceActivation, Status> {
        let root = self.node_segments_root();
        let selected = before.metadata(rewrite.segment).clone();
        let directory = SegmentCatalog::segment_dir(&root, &selected.segment_id);
        let node = self.node.clone();
        let source = self.source.clone();
        let scratch = self.root.join(rewrite.scratch);
        let epoch = before.epoch();
        let stats_epoch = active.stats_epoch;
        let stats_incarnation = active.stats_incarnation.clone();
        let output = rewrite.output;
        tokio::task::spawn_blocking(move || {
            arm();
            node.publish_document_maintenance_blocking(crate::node::DocumentMaintenanceRequest {
                source: &source,
                index_key: INDEX,
                expected: StatsClaim::required(stats_epoch, &stats_incarnation).unwrap(),
                catalog_epoch: epoch,
                sources: vec![SegmentSource {
                    multi_res_path: None,
                    segment_id: output,
                    generation: epoch + 1,
                    base_label: 0,
                    backend_kind: &selected.backend_kind,
                    vector_path: Some(&directory.join(&selected.vector.file)),
                    exact_vector_path: Some(&directory.join(&selected.exact_vectors.file)),
                    bm25_path: &directory.join(&selected.bm25.file),
                    live_docs_path: &directory.join(&selected.live_docs.file),
                    partition_column: None,
                }],
                proof_scratch: &scratch,
                proof_batch_rows: 1,
            })
        })
        .await
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

/// One segment rewrite: which segment of the opened set is republished,
/// the id of the segment it becomes, and the proof scratch directory's
/// name under the fixture root.
pub(super) struct Rewrite<'a> {
    pub(super) segment: usize,
    pub(super) output: &'static str,
    pub(super) scratch: &'a str,
}
