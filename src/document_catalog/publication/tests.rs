use super::*;
use crate::{
    analyzer::body_spec,
    mapping,
    node::{NodeConfig, NodeServiceImpl},
    pb::*,
    segments::{SegmentRowRetirement, SegmentSource},
};
use std::{path::PathBuf, sync::Arc};

const INDEX: &[u8] = b"logical-index\0v1";
const KEY: &[u8] = b"document\0one";
const DESCRIPTOR: &[u8] = include_bytes!("../../../tests/fixtures/unsigned-mapping/descriptor.bin");
const TYPE: &str = "unsigned_mapping.Parent";
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
struct Fixture {
    root: PathBuf,
    source: Arc<DocumentCatalog>,
    target: SegmentCatalog,
    node: NodeServiceImpl,
}
impl Fixture {
    fn new() -> Self {
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
            false,
        )
        .unwrap();
        Self {
            root,
            source,
            target,
            node,
        }
    }
    async fn stage(
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
    fn bind(&self, candidate: &StagedDocumentCandidate) {
        self.target
            .publish_binding(candidate.segments().unwrap().binding().unwrap())
            .unwrap();
    }
    fn publish(
        &self,
        candidate: &StagedDocumentCandidate,
        retire: Option<Vec<SegmentRowRetirement>>,
        stop: bool,
    ) -> Result<ProjectionIntent, String> {
        self.publish_for(INDEX, candidate, retire, stop)
    }
    fn publish_for(
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
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn decisions_survive_reopen_and_keep_acceptance_receipts_unchanged() {
    let mut fixture = Fixture::new();
    let (receipt, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    let intent = fixture.publish(&candidate, None, false).unwrap();
    assert_eq!(
        fixture.source.index_publication_decision(INDEX, 1).unwrap(),
        None
    );
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Committed(intent.clone())
    );
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Idle
    );
    assert!(!receipt.searchable);
    let (second, replacement) = fixture.stage(KEY, 2, Some(1)).await;
    let next = fixture.publish(&replacement, None, false).unwrap();
    assert_eq!(
        fixture
            .target
            .snapshot()
            .document_retirements(KEY, 100)
            .unwrap()
            .iter()
            .map(|r| r.rows.len())
            .sum::<usize>(),
        1
    );
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Committed(next.clone())
    );
    assert!(!second.searchable);
    let (retried, _) = fixture.stage(KEY, 1, Some(2)).await;
    assert_eq!(
        retried,
        DocumentWriteReceipt {
            replayed: true,
            ..receipt
        }
    );
    assert_eq!(
        fixture.source.index_publication_decision(INDEX, 1).unwrap(),
        Some(intent)
    );
    let old = std::mem::replace(
        &mut fixture.source,
        Arc::new(DocumentCatalog::in_memory("books").unwrap()),
    );
    drop(old);
    let reopened = DocumentCatalog::open(&fixture.root.join("source.redb"), "books").unwrap();
    assert_eq!(
        reopened.index_publication_decision(INDEX, 2).unwrap(),
        Some(next)
    );
}

#[tokio::test]
async fn interrupted_prepare_aborts_and_exact_retry_can_publish() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&candidate);
    assert!(fixture
        .publish(&candidate, None, true)
        .unwrap_err()
        .contains("interrupted after intent"));
    assert!(matches!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Aborted(_)
    ));
    let intent = fixture.publish(&candidate, None, false).unwrap();
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap(),
        ProjectionRecovery::Committed(intent)
    );
}

#[tokio::test]
async fn failed_manifest_sync_requires_reopen_before_deciding() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&candidate);
    crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
    assert!(fixture.publish(&candidate, None, false).is_err());
    assert!(fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap_err()
        .message()
        .contains("uncertain"));
    let reopened = SegmentCatalog::open(fixture.root.join("target")).unwrap();
    assert!(matches!(
        fixture
            .source
            .recover_index_publication(INDEX, &reopened)
            .unwrap(),
        ProjectionRecovery::Committed(_)
    ));
}

#[tokio::test]
async fn out_of_order_sources_and_incomplete_retirements_refuse() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(2)).await;
    let (_, second) = fixture.stage(KEY, 2, Some(1)).await;
    fixture.bind(&first);
    assert!(fixture
        .publish(&second, None, false)
        .unwrap_err()
        .contains("next accepted sequence"));
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let before = fixture.target.snapshot();
    assert!(fixture
        .publish(&second, Some(vec![]), false)
        .unwrap_err()
        .contains("exact staged document replacement"));
    assert_eq!(fixture.target.snapshot().manifest(), before.manifest());
    fixture.publish(&second, None, false).unwrap();
}

#[tokio::test]
async fn unrelated_retirements_are_not_certified() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let (_, other) = fixture.stage(b"other", 1, Some(1)).await;
    let retire = fixture
        .target
        .snapshot()
        .document_retirements(KEY, 100)
        .unwrap();
    assert!(fixture
        .publish(&other, Some(retire), false)
        .unwrap_err()
        .contains("exact staged document replacement"));
    fixture.publish(&other, None, false).unwrap();
}

#[tokio::test]
async fn a_third_manifest_preserves_pending_intent_and_refuses() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    assert!(fixture.publish(&first, None, true).is_err());
    fixture
        .target
        .publish_partition_key(Some("unrelated".into()))
        .unwrap();
    for _ in 0..2 {
        assert!(fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap_err()
            .message()
            .contains("neither projection manifest"));
    }
    assert!(fixture
        .publish(&first, None, false)
        .unwrap_err()
        .contains("existing projection intent"));
}

#[tokio::test]
async fn empty_source_and_deletion_each_advance_the_publication_sequence() {
    let fixture = Fixture::new();
    let (_, initial) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&initial);
    fixture.publish(&initial, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let (_, empty) = fixture.stage(KEY, 2, Some(0)).await;
    let intent = fixture.publish(&empty, None, false).unwrap();
    assert_eq!(intent.rows, 0);
    assert!(!intent.source.as_ref().unwrap().deleted);
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let (_, deletion) = fixture.stage(KEY, 3, None).await;
    // No physical rows remain. The source transition still needs a distinct
    // manifest epoch; prepare the validated empty change without adding rows.
    let before = fixture.target.snapshot();
    let mut manifest = before.published_manifest();
    manifest.epoch += 1;
    let shadow =
        SegmentCatalog::open_staged(before.root(), manifest.clone(), Default::default()).unwrap();
    let intent = fixture
        .source
        .prepare_index_publication(INDEX, &deletion, &before, &shadow.snapshot())
        .unwrap();
    assert!(intent.source.as_ref().unwrap().deleted);
    crate::segments::write_manifest_file(&SegmentCatalog::manifest_path(before.root()), &manifest)
        .unwrap();
    let reopened = SegmentCatalog::open(before.root()).unwrap();
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &reopened)
            .unwrap(),
        ProjectionRecovery::Committed(intent)
    );
}

#[test]
fn journal_header_cannot_disappear_or_change_history() {
    let fixture = Fixture::new();
    let tx = fixture.source.database.begin_write().unwrap();
    {
        tx.open_table(STATES).unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .source
            .validate_projection_journal()
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
    assert!(journal_header(
        Some(
            &ProjectionJournalHeader {
                format_version: 1,
                history_id: vec![2; 16]
            }
            .encode_to_vec()
        ),
        &[STATES.name().into(), DECISIONS.name().into()],
        &[1; 16]
    )
    .is_err());
}

#[tokio::test]
async fn an_idle_journal_still_checks_its_committed_manifest() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    fixture
        .target
        .publish_partition_key(Some("unrelated".into()))
        .unwrap();
    assert!(fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap_err()
        .message()
        .contains("committed projection manifest"));
}

#[tokio::test]
async fn committed_cursor_must_match_its_immutable_decision() {
    let fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let tx = fixture.source.database.begin_write().unwrap();
    {
        let mut table = tx.open_table(STATES).unwrap();
        let mut state: ProjectionJournalState =
            decode(table.get(INDEX).unwrap().unwrap().value()).unwrap();
        state.committed_manifest_sha256 = vec![7; 32];
        table
            .insert(INDEX, state.encode_to_vec().as_slice())
            .unwrap();
    }
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .source
            .recover_index_publication(INDEX, &fixture.target)
            .unwrap_err()
            .code(),
        tonic::Code::DataLoss
    );
}

#[tokio::test]
async fn identical_source_bytes_from_another_history_cannot_be_adopted() {
    let fixture = Fixture::new();
    let foreign = Fixture::new();
    let (_, local) = fixture.stage(KEY, 1, Some(1)).await;
    let (_, candidate) = foreign.stage(KEY, 1, Some(1)).await;
    assert_eq!(local.source(), candidate.source());
    fixture.bind(&local);
    assert!(fixture
        .publish(&candidate, None, false)
        .unwrap_err()
        .contains("another catalog history"));
    assert!(!fixture.source.validate_projection_journal().unwrap());
    fixture.publish(&local, None, false).unwrap();
}

#[tokio::test]
async fn reopening_refuses_missing_journal_tables() {
    let mut fixture = Fixture::new();
    let (_, first) = fixture.stage(KEY, 1, Some(1)).await;
    fixture.bind(&first);
    fixture.publish(&first, None, false).unwrap();
    let tx = fixture.source.database.begin_write().unwrap();
    assert!(tx.delete_table(DECISIONS).unwrap());
    tx.commit().unwrap();
    let old = std::mem::replace(
        &mut fixture.source,
        Arc::new(DocumentCatalog::in_memory("books").unwrap()),
    );
    drop(old);
    let error = DocumentCatalog::open(&fixture.root.join("source.redb"), "books")
        .err()
        .expect("missing journal table must refuse reopening");
    assert_eq!(error.code(), tonic::Code::DataLoss);
}

#[tokio::test]
async fn node_recovery_joins_both_crash_windows_after_reopening_source_and_index() {
    use crate::pb::node_service_server::NodeService;
    for after_activation in [false, true] {
        let mut fixture = Fixture::new();
        let (receipt, candidate) = fixture.stage(KEY, 1, Some(1)).await;
        let before_incarnation = candidate.info().target_stats_incarnation.clone();
        let node = fixture.node.clone();
        let source = fixture.source.clone();
        let error = tokio::task::spawn_blocking(move || {
            if after_activation {
                crate::node::FAIL_PROJECTION_DECISION.with(|fail| fail.set(true));
            } else {
                crate::segments::FAIL_SET_SYNC.with(|fail| fail.set(true));
            }
            node.publish_document_projection_blocking(&source, INDEX, &candidate)
        })
        .await
        .unwrap()
        .unwrap_err();
        assert!(
            error.message().contains(if after_activation {
                "before source decision"
            } else {
                "after manifest rename"
            }),
            "{error}"
        );
        assert!(fixture.node.ingest_fence().is_some());
        assert!(fixture
            .source
            .index_publication_decision(INDEX, 1)
            .unwrap()
            .is_none());
        let health = fixture
            .node
            .health(tonic::Request::new(HealthRequest {}))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(health.num_vectors, u64::from(after_activation));
        let old = std::mem::replace(
            &mut fixture.source,
            Arc::new(DocumentCatalog::in_memory("books").unwrap()),
        );
        drop(old);
        fixture.source =
            Arc::new(DocumentCatalog::open(&fixture.root.join("source.redb"), "books").unwrap());
        fixture.node = NodeServiceImpl::open(fixture.node.config.clone(), None, false).unwrap();
        let source = fixture.source.clone();
        let node = fixture.node.clone();
        let recovered = tokio::task::spawn_blocking(move || {
            node.recover_document_projection_blocking(&source, INDEX)
        })
        .await
        .unwrap()
        .unwrap()
        .unwrap();
        assert_eq!(recovered.history_id, receipt.history_id);
        assert_eq!(recovered.accepted_sequence, receipt.accepted_sequence);
        assert_ne!(recovered.stats_incarnation, before_incarnation);
        assert_eq!(
            fixture
                .node
                .health(tonic::Request::new(HealthRequest {}))
                .await
                .unwrap()
                .into_inner()
                .num_vectors,
            1
        );
        assert_eq!(
            fixture
                .source
                .index_publication_decision(INDEX, 1)
                .unwrap()
                .unwrap()
                .intent_id,
            recovered.intent_id
        );
    }
}

mod maintenance;

mod cutover;

#[tokio::test]
async fn checkpoint_refuses_pending_source_decisions_without_resolving_them() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    assert!(fixture.publish(&candidate, None, true).is_err());
    let error = fixture.source.capture_checkpoint(1 << 20).err().unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("pending"));
    let tx = fixture.source.database.begin_read().unwrap();
    let state: ProjectionJournalState = decode(
        tx.open_table(STATES)
            .unwrap()
            .get(INDEX)
            .unwrap()
            .unwrap()
            .value(),
    )
    .unwrap();
    assert!(state.pending.is_some());
    assert_eq!(state.committed_sequence, 0);
}

#[tokio::test]
async fn checkpoint_includes_every_index_in_one_source_history() {
    let mut fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    fixture.publish(&candidate, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    fixture.target = SegmentCatalog::open(fixture.root.join("second-target")).unwrap();
    fixture.bind(&candidate);
    fixture
        .publish_for(b"second-index", &candidate, None, false)
        .unwrap();
    fixture
        .source
        .recover_index_publication(b"second-index", &fixture.target)
        .unwrap();
    let checkpoint = fixture.source.capture_checkpoint(1 << 20).unwrap();
    assert_eq!(
        checkpoint
            .metadata()
            .indexes
            .iter()
            .map(|s| s.index_key.as_slice())
            .collect::<Vec<_>>(),
        [INDEX, b"second-index"]
    );
    assert!(checkpoint
        .metadata()
        .indexes
        .iter()
        .all(|s| s.committed_sequence == 1));
    let output = fixture.root.join("both.redb");
    let info = checkpoint
        .write_to(
            &output,
            &crate::pb::storage::DocumentCatalogCheckpointLimits {
                batch_bytes: 64 << 10,
                max_file_bytes: 32 << 20,
            },
        )
        .unwrap();
    let copy = DocumentCatalog::open(&output, "books").unwrap();
    assert_eq!(
        copy.capture_checkpoint(1 << 20).unwrap().metadata().indexes,
        info.indexes
    );
}

#[tokio::test]
async fn checkpoint_refuses_a_journal_tip_detached_from_accepted_history() {
    let fixture = Fixture::new();
    let (_, candidate) = fixture.stage(KEY, 1, Some(2)).await;
    fixture.bind(&candidate);
    fixture.publish(&candidate, None, false).unwrap();
    fixture
        .source
        .recover_index_publication(INDEX, &fixture.target)
        .unwrap();
    let tx = fixture.source.database.begin_write().unwrap();
    let key = DocumentVersionKey {
        document_key: KEY.to_vec(),
        version: 1,
    }
    .encode_to_vec();
    tx.open_table(VERSIONS)
        .unwrap()
        .remove(key.as_slice())
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        fixture
            .source
            .capture_checkpoint(1 << 20)
            .err()
            .expect("a detached journal anchor must refuse capture")
            .code(),
        tonic::Code::DataLoss
    );
}

mod backup;
mod restore;
