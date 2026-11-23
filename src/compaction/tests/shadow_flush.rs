use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cutover_analysis_allows_reads_and_includes_writes_during_preparation() {
    cutover_with_writes_during_analysis(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cutover_refuses_persistent_writes_without_replacing_the_live_generation() {
    cutover_with_writes_during_analysis(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_shadow_flush_allows_reads_under_the_cutover_fences() {
    cutover_with_shadow_flush(false, ShadowFlushCase::ReadsAvailable).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_shadow_flush_error_waits_for_a_matching_live_fence() {
    cutover_with_shadow_flush(false, ShadowFlushCase::IoError).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rotated_live_generation_takes_priority_over_private_shadow_flush_error() {
    cutover_with_shadow_flush(false, ShadowFlushCase::RotatedGeneration).await;
}

#[derive(Clone, Copy)]
enum ShadowFlushCase {
    Real,
    ReadsAvailable,
    IoError,
    RotatedGeneration,
}

async fn cutover_with_writes_during_analysis(always_advance: bool) {
    cutover_with_shadow_flush(always_advance, ShadowFlushCase::Real).await;
}

const TEXTS: [&str; 4] = ["seed", "first tail", "second tail", "late tail"];

struct CutoverFixture {
    dir: PathBuf,
    node: NodeServiceImpl,
    sample: Vec<f32>,
    spec: crate::pb::AnalysisSpec,
    analyzed: Vec<crate::postings::AnalyzedDoc>,
}

impl CutoverFixture {
    fn new(handle: &tokio::runtime::Handle) -> Self {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("psearch-cutover-{}-{nonce}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sample = crate::harness::unit_vectors(32, 64, 19);
        let config = VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, 64, 4, &sample).unwrap();
        let index = VectorIndex::from_backend_config(64, &config).unwrap();
        let node = NodeServiceImpl::new(
            Some(index),
            NodeConfig {
                index_path: Some(dir.join("shard.vector")),
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                wal: true,
                ..Default::default()
            },
        );
        let spec = body_spec();
        let analyzed = TEXTS
            .iter()
            .map(|text| {
                handle
                    .block_on(crate::analyzer::analyze_document(
                        NATIVE_ANALYSIS_BACKEND,
                        text,
                        Some(&spec),
                    ))
                    .unwrap()
            })
            .collect::<Vec<_>>();
        Self {
            dir,
            node,
            sample,
            spec,
            analyzed,
        }
    }

    fn append(&self, i: usize) {
        let mut guard = self.node.state.write().unwrap();
        self.node
            .apply_document_locked(
                &mut guard,
                crate::node::MaterializedDocument {
                    doc: AddDocumentsRequest {
                        text: TEXTS[i].into(),
                        analysis: Some(self.spec.clone()),
                        ..Default::default()
                    },
                    analyzed: self.analyzed[i].clone(),
                    vector: Some(self.sample[..64].to_vec()),
                    stable_routing_key: None,
                },
                &mut 0,
                &mut 0,
            )
            .unwrap();
    }

    /// The private shadow flush under test: it runs with the live shard
    /// readable and both cutover fences held.
    fn flush_shadow(
        &self,
        pre: &Preflight,
        flush_case: ShadowFlushCase,
        shadow: &mut Shadow,
    ) -> Result<(), Status> {
        let node = &self.node;
        {
            let live = node
                .state
                .try_read()
                .expect("private shadow flush ran while the live shard was write-locked");
            assert_eq!(live.wal.as_ref().unwrap().generation(), 0);
            assert!(node.mutation_gate.try_read().is_err());
            assert!(node.seal_lock.try_lock().is_err());
        }
        match flush_case {
            ShadowFlushCase::Real | ShadowFlushCase::ReadsAvailable => {
                NodeServiceImpl::flush_compaction_shadow(shadow)
            }
            ShadowFlushCase::IoError => {
                Err(Status::internal("fsync the rewritten generation: injected"))
            }
            ShadowFlushCase::RotatedGeneration => {
                let mut live = node.state.write().unwrap();
                let mut manifest = live.wal.as_ref().unwrap().manifest().clone();
                manifest.generation += 1;
                live.wal =
                    Some(WalWriter::create(&wal::wal_dir(&pre.index_path), manifest).unwrap());
                Err(Status::internal("fsync the rewritten generation: injected"))
            }
        }
    }
}

/// What one cutover run did, for the per-case assertions.
struct CutoverRun {
    always_advance: bool,
    flush_case: ShadowFlushCase,
    result: Result<CompactShardResponse, Status>,
    calls: usize,
    flush_calls: usize,
}

/// Assert the run's outcome and return the texts the shard must now serve.
fn assert_cutover_outcome(
    node: &NodeServiceImpl,
    pre: &Preflight,
    run: CutoverRun,
) -> Vec<&'static str> {
    let CutoverRun {
        always_advance,
        flush_case,
        result,
        calls,
        flush_calls,
    } = run;
    if matches!(
        flush_case,
        ShadowFlushCase::IoError | ShadowFlushCase::RotatedGeneration
    ) {
        let error = result.unwrap_err();
        let live = node.state.read().unwrap();
        if matches!(flush_case, ShadowFlushCase::IoError) {
            assert_eq!(error.code(), tonic::Code::Internal);
            assert_eq!(error.message(), "fsync the rewritten generation: injected");
            // The first attempt advanced during analysis: its sync
            // error must not replace the original watermark retry.
            assert_eq!(flush_calls, 2);
            assert_eq!(calls, 4);
            assert_eq!(live.wal.as_ref().unwrap().generation(), 0);
            assert!(!wal::gen_dir(&wal::wal_dir(&pre.index_path), 1).exists());
        } else {
            assert_eq!(error.code(), tonic::Code::Aborted);
            assert!(error.message().contains("snapshot was installed"));
            assert_eq!(flush_calls, 1);
            assert_eq!(calls, 3);
            assert_eq!(live.wal.as_ref().unwrap().generation(), 1);
        }
        assert!(!marker_path(&pre.index_path).exists());
        TEXTS.to_vec()
    } else if always_advance {
        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("writes outpace compaction"));
        assert_eq!(calls, 2 + CUTOVER_RETRIES);
        let live = node.state.read().unwrap();
        assert_eq!(live.wal.as_ref().unwrap().generation(), 0);
        assert!(!marker_path(&pre.index_path).exists());
        (0..=calls).map(|i| TEXTS[i.min(3)]).collect::<Vec<_>>()
    } else {
        let response = result.unwrap();
        assert_eq!(calls, 4);
        assert_eq!(response.rows_after, 4);
        if matches!(flush_case, ShadowFlushCase::ReadsAvailable) {
            assert_eq!(flush_calls, 2);
        }
        TEXTS.to_vec()
    }
}

fn assert_served(handle: &tokio::runtime::Handle, node: &NodeServiceImpl, expected: &[&str]) {
    let fetched = handle
        .block_on(crate::pb::node_service_server::NodeService::get_documents(
            node,
            tonic::Request::new(crate::pb::GetDocumentsRequest {
                doc_ids: (0..expected.len() as u64).collect(),
            }),
        ))
        .unwrap()
        .into_inner();
    let actual = fetched
        .documents
        .into_iter()
        .map(|doc| doc.text)
        .collect::<Vec<_>>();
    assert_eq!(actual, expected);
}

async fn cutover_with_shadow_flush(always_advance: bool, flush_case: ShadowFlushCase) {
    tokio::task::spawn_blocking(move || {
        let handle = tokio::runtime::Handle::current();
        let fixture = CutoverFixture::new(&handle);
        let node = &fixture.node;
        fixture.append(0);
        node.flush_index().unwrap();
        let preflight = node
            .preflight_at_row_boundary(&CompactShardRequest::default())
            .unwrap();
        let PreflightKind::Wal(pre) = preflight else {
            panic!("a WAL-backed shard preflights to the log path")
        };
        std::fs::create_dir_all(&pre.work_dir).unwrap();
        let mut calls = 0;
        let mut analyze = |docs: &[(
            &str,
            Option<&crate::pb::AnalysisSpec>,
            crate::analyzer::SessionLayers,
        )]| {
            assert!(
                node.state.try_read().is_ok(),
                "analysis ran while the live shard was write-locked"
            );
            calls += 1;
            // Build, catch-up, and the first cutover preparation each
            // receive another committed row. The final fence must retry.
            if always_advance || calls <= 3 {
                fixture.append(calls.min(3));
            }
            handle
                .block_on(crate::analyzer::analyze_batch_streams(
                    NATIVE_ANALYSIS_BACKEND,
                    docs,
                    2,
                ))
                .map_err(|error| error.to_string())
        };
        let mut flush_calls = 0;
        let mut flush_shadow = |shadow: &mut Shadow| {
            flush_calls += 1;
            fixture.flush_shadow(&pre, flush_case, shadow)
        };
        let result = match flush_case {
            ShadowFlushCase::Real => node.build_and_cut_over(&pre, 256, &mut analyze),
            _ => node.build_and_cut_over_with_shadow_flush(
                &pre,
                256,
                &mut analyze,
                &mut flush_shadow,
            ),
        };
        let run = CutoverRun {
            always_advance,
            flush_case,
            result,
            calls,
            flush_calls,
        };
        let expected = assert_cutover_outcome(node, &pre, run);
        assert_served(&handle, node, &expected);
        let CutoverFixture { dir, node, .. } = fixture;
        drop(node);
        std::fs::remove_dir_all(dir).unwrap();
    })
    .await
    .unwrap();
}
