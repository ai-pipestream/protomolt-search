//! Unit tests for node scan windows, floor signals, snapshots, and filter domains.

use super::*;

#[cfg(test)]
mod scan_window_tests {
    use super::*;

    /// The window reports unknown below the minimum sample count, then
    /// the ratio of the bytes and active time it holds; a scan that
    /// streamed nothing is not a sample.
    #[test]
    fn the_window_reports_unknown_until_it_holds_enough_scans() {
        let window = ScanWindow::new();
        assert_eq!(
            window.rate(),
            ScanRate {
                window_ms: SCAN_WINDOW_MS,
                ..Default::default()
            }
        );
        window.observe(0, 5_000);
        assert_eq!(window.rate().samples, 0, "an empty scan is not a sample");
        for _ in 0..(SCAN_WINDOW_MIN_SAMPLES - 1) {
            window.observe(1_000, 1_000_000);
        }
        let rate = window.rate();
        assert_eq!(rate.samples as usize, SCAN_WINDOW_MIN_SAMPLES - 1);
        assert_eq!(rate.bytes_per_second, 0, "below the minimum is unknown");
        assert!(rate.observed_unix_ms > 0);
        window.observe(1_000, 1_000_000);
        let rate = window.rate();
        assert_eq!(rate.samples as usize, SCAN_WINDOW_MIN_SAMPLES);
        // 4,000 bytes in 4 ms is one million bytes per second.
        assert_eq!(rate.bytes_per_second, 1_000_000);
        // The window keeps at most SCAN_WINDOW_SAMPLES scans.
        for _ in 0..(2 * SCAN_WINDOW_SAMPLES) {
            window.observe(2_000, 1_000_000);
        }
        let rate = window.rate();
        assert_eq!(rate.samples as usize, SCAN_WINDOW_SAMPLES);
        assert_eq!(
            rate.bytes_per_second, 2_000_000,
            "only the newest scans remain"
        );
        window.clear();
        assert_eq!(window.rate().samples, 0);
    }
}

#[cfg(test)]
mod floor_lane_tests {
    use super::*;
    use std::sync::atomic::Ordering;

    fn state_of(
        signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>,
        token: u64,
    ) -> Arc<StreamSignals> {
        Arc::clone(&signals.lock().unwrap()[&token])
    }

    fn floor_of(signals: &std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>>, token: u64) -> f32 {
        f32::from_bits(state_of(signals, token).floor.load(Ordering::Acquire))
    }

    /// Typed UDP raises remain monotone, cancellation is sticky, and garbage
    /// cannot mutate either signal or panic the listener.
    #[test]
    fn stream_datagrams_fold_typed_signals_and_ignore_garbage() {
        let signals: std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>> =
            std::sync::Mutex::new(HashMap::new());
        signals
            .lock()
            .unwrap()
            .insert(7, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));

        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.25));
        assert_eq!(floor_of(&signals, 7), 0.25);
        // Lower and equal floors are ignored.
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.10));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.25));
        assert_eq!(floor_of(&signals, 7), 0.25);
        // Duplicated and reordered raises: max wins regardless.
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.75));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.50));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(7, 0.75));
        assert_eq!(floor_of(&signals, 7), 0.75);

        let state = state_of(&signals, 7);
        assert_eq!(state.finish_state(), None);
        apply_stream_datagram(
            &signals,
            None,
            &crate::stream_signal::encode_finish(7, false),
        );
        assert_eq!(state.finish_state(), Some(false));
        apply_stream_datagram(
            &signals,
            None,
            &crate::stream_signal::encode_finish(7, true),
        );
        assert_eq!(state.finish_state(), Some(true));
        // A later non-deadline finish does not downgrade a deadline finish.
        apply_stream_datagram(
            &signals,
            None,
            &crate::stream_signal::encode_finish(7, false),
        );
        assert_eq!(state.finish_state(), Some(true));

        assert!(!state.cancelled.load(Ordering::Acquire));
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_cancel(7));
        assert!(state.cancelled.load(Ordering::Acquire));
        assert_eq!(
            floor_of(&signals, 7),
            0.75,
            "cancel is not a score sentinel"
        );

        // Unknown tokens, malformed frames, and empty datagrams are dropped.
        apply_stream_datagram(&signals, None, &crate::stream_signal::encode_floor(8, 9.0));
        apply_stream_datagram(
            &signals,
            None,
            &crate::stream_signal::encode_floor(7, 9.0)[..crate::stream_signal::FRAME_LEN - 1],
        );
        apply_stream_datagram(&signals, None, &[0u8; crate::stream_signal::FRAME_LEN + 1]);
        apply_stream_datagram(&signals, None, &[]);
        assert_eq!(floor_of(&signals, 7), 0.75);
        assert!(state.cancelled.load(Ordering::Acquire));
        assert_eq!(signals.lock().unwrap().len(), 1);
    }

    /// With a key, only a signed datagram with a fresh sequence moves the
    /// floor (docs/security.md): a plain frame, a foreign key, a damaged
    /// tag, a replay, and a stale sequence all leave it where it was, so
    /// a forger cannot cut candidates. The gRPC twin still governs.
    #[test]
    fn signed_floor_lane_ignores_forgeries_and_replays() {
        use crate::security::UdpKey;
        use crate::stream_signal::{encode_cancel, encode_floor, sign};
        let key = UdpKey::from_bytes(&[1u8; 32]).unwrap();
        let other = UdpKey::from_bytes(&[2u8; 32]).unwrap();
        let signals: std::sync::Mutex<HashMap<u64, Arc<StreamSignals>>> =
            std::sync::Mutex::new(HashMap::new());
        signals
            .lock()
            .unwrap()
            .insert(7, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));
        let state = state_of(&signals, 7);

        // An authentic raise applies.
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 0.25)));
        assert_eq!(floor_of(&signals, 7), 0.25);
        // A plain frame is not read when a key is configured.
        apply_stream_datagram(&signals, Some(&key), &encode_floor(7, 9.0));
        // A foreign key does not verify.
        apply_stream_datagram(
            &signals,
            Some(&key),
            &sign(&other, 2, &encode_floor(7, 9.0)),
        );
        // A damaged tag does not verify.
        let mut damaged = sign(&key, 2, &encode_floor(7, 9.0));
        let last = damaged.len() - 1;
        damaged[last] ^= 1;
        apply_stream_datagram(&signals, Some(&key), &damaged);
        // A replay of the authentic datagram, and a stale sequence with a
        // higher floor, are both behind the newest applied sequence.
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 0.25)));
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(7, 9.0)));
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 0, &encode_floor(7, 9.0)));
        assert_eq!(floor_of(&signals, 7), 0.25, "no forgery moved the floor");
        assert!(!state.cancelled.load(Ordering::Acquire));
        // A forged cancel is ignored the same way; an authentic one lands.
        apply_stream_datagram(&signals, Some(&key), &encode_cancel(7));
        apply_stream_datagram(&signals, Some(&key), &sign(&other, 3, &encode_cancel(7)));
        assert!(!state.cancelled.load(Ordering::Acquire));
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 2, &encode_floor(7, 0.5)));
        assert_eq!(floor_of(&signals, 7), 0.5);
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 3, &encode_cancel(7)));
        assert!(state.cancelled.load(Ordering::Acquire));
        // Sequences are per stream: a fresh token starts at zero, and signed
        // FINISH datagrams obey the same HMAC + replay check.
        signals
            .lock()
            .unwrap()
            .insert(8, Arc::new(StreamSignals::new(f32::NEG_INFINITY)));
        let state8 = state_of(&signals, 8);
        apply_stream_datagram(&signals, Some(&key), &sign(&key, 1, &encode_floor(8, 0.1)));
        assert_eq!(floor_of(&signals, 8), 0.1);
        apply_stream_datagram(
            &signals,
            Some(&key),
            &crate::stream_signal::encode_finish(8, false),
        );
        apply_stream_datagram(
            &signals,
            Some(&key),
            &sign(&other, 2, &crate::stream_signal::encode_finish(8, false)),
        );
        assert_eq!(state8.finish_state(), None);
        apply_stream_datagram(
            &signals,
            Some(&key),
            &sign(&key, 2, &crate::stream_signal::encode_finish(8, true)),
        );
        assert_eq!(state8.finish_state(), Some(true));
    }
}

#[cfg(test)]
mod unsigned_materialize_tests {
    use super::*;

    #[test]
    fn shared_materialization_preserves_uint_and_checks_target_family() {
        let doc = AddDocumentsRequest {
            unsigned_integers: vec![crate::pb::UnsignedIntegerValue {
                field: "counter".into(),
                value: u64::MAX,
            }],
            ..Default::default()
        };
        for (expression, value) in [
            ("counter", Some(u64::MAX)),
            ("counter + 1u", None),
            ("counter - 1u", Some(u64::MAX - 1)),
            ("counter > 0u ? counter : 0u", Some(u64::MAX)),
        ] {
            let mut spec = crate::pb::MaterializeSpec {
                columns: vec![crate::pb::MaterializedColumn {
                    name: "derived".into(),
                    expression: expression.into(),
                    kind: crate::pb::MaterializeKind::U64 as i32,
                }],
            };
            let compiled = compile_materialize_spec(&spec).unwrap();
            let result = apply_materialize(doc.clone(), &compiled).unwrap();
            assert_eq!(
                result
                    .unsigned_integers
                    .iter()
                    .find(|v| v.field == "derived")
                    .map(|v| v.value),
                value
            );
            if value.is_some() {
                spec.columns[0].kind = crate::pb::MaterializeKind::I64 as i32;
                let error =
                    apply_materialize(doc.clone(), &compile_materialize_spec(&spec).unwrap())
                        .unwrap_err();
                assert!(error.message().contains("evaluated uint"), "{error}");
            }
        }
    }
}

#[cfg(test)]
mod stats_lifetime_tests {
    use super::*;

    #[test]
    fn mutation_counter_exhaustion_rotates_the_lifetime_instead_of_reusing_a_version() {
        let mut state = ShardState {
            stats_epoch: u64::MAX,
            ..Default::default()
        };
        let previous = state.stats_incarnation.bytes().unwrap();
        state.advance_stats_epoch();
        assert_eq!(state.stats_epoch, 1);
        assert_ne!(state.stats_incarnation.bytes().unwrap(), previous);
        assert!(state.check_stats_epoch(1, &previous).is_err());
    }
}

#[cfg(test)]
mod vector_column_namespace_tests {
    use super::*;
    #[test]
    fn active_store_columns_are_checked_even_if_the_config_does_not_declare_them() {
        let node = NodeServiceImpl::new(None, NodeConfig::default());
        let stores = [
            Bm25Store::with_fields(&["body", "embedding"]),
            Bm25Store::new().with_facets(&["embedding"]),
            Bm25Store::new().with_numerics(&["embedding"]),
            Bm25Store::new().with_integers(&["embedding"]),
            Bm25Store::new().with_unsigned_integers(&["embedding"]),
            Bm25Store::new().with_map_facets(&["embedding"]),
            Bm25Store::new().with_map_numerics(&["embedding"]),
            Bm25Store::new().with_geos(&["embedding"]),
        ];
        for store in stores {
            let store = Bm25Shard::Building(store);
            assert_eq!(
                node.validate_vector_column_name("embedding", Some(&store))
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
            node.validate_vector_column_name("distinct", Some(&store))
                .unwrap();
        }
    }
}

#[cfg(test)]
mod snapshot_publication_tests {
    use super::*;
    use crate::postings::StoredBinding;

    #[test]
    fn configuring_and_appending_vectors_require_a_new_snapshot_flush() {
        let root = std::env::temp_dir().join(format!(
            "snapshot-vector-publication-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        for wal in [false, true] {
            for layout in [Layout::SingleImage, Layout::Segments] {
                let path = root.join(format!("{wal}-{layout:?}.tv"));
                let service = NodeServiceImpl::open(
                    NodeConfig {
                        index_path: Some(path.clone()),
                        layout,
                        wal,
                        ..Default::default()
                    },
                    None,
                    false,
                )
                .unwrap();
                service.flush_index().unwrap();
                let epoch = read_shard(&service.state).stats_epoch;
                service
                    .apply_backend_config(crate::harness::embedded_backend_request(
                        8, 4, &[0.0; 8], &[1.0; 8],
                    ))
                    .unwrap();
                assert_ne!(read_shard(&service.state).stats_epoch, epoch);
                let directory = root.join(format!("copy-{wal}-{layout:?}"));
                std::fs::create_dir_all(&directory).unwrap();
                assert!(service
                    .export_if_flushed(&path, &directory)
                    .unwrap()
                    .is_none());
                service.flush_index().unwrap();
                let epoch = read_shard(&service.state).stats_epoch;
                service
                    .apply_batch(
                        AddVectorsRequest {
                            materialized_rows: Vec::new(),
                            dim: 8,
                            vectors: vec![0.25; 8],
                        },
                        None,
                    )
                    .unwrap();
                assert_ne!(read_shard(&service.state).stats_epoch, epoch);
                assert!(service
                    .export_if_flushed(&path, &directory)
                    .unwrap()
                    .is_none());
                assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
                service.flush_index().unwrap();
                let (manifest, _) = service
                    .export_if_flushed(&path, &directory)
                    .unwrap()
                    .unwrap();
                assert_eq!(manifest.vector_rows, 1);
                if manifest.layout == LAYOUT_SEGMENTS {
                    let set = crate::segments::OpenedSegmentSet::open(directory.join(CATALOG_DIR))
                        .unwrap();
                    let image = set.vector(0).unwrap();
                    assert_eq!(image.len(), 1);
                    assert_eq!(image.dim_opt(), Some(8));
                } else {
                    let image = VectorIndex::load(
                        &service.config.vector_backend,
                        &directory.join("vector.index"),
                    )
                    .unwrap();
                    assert_eq!(image.len(), 1);
                    assert_eq!(image.dim_opt(), Some(8));
                }
            }
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn metadata_only_snapshots_reject_false_shape_claims_before_install() {
        let root =
            std::env::temp_dir().join(format!("snapshot-empty-shape-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let source_path = root.join("source.tv");
        let source = NodeServiceImpl::new(
            None,
            NodeConfig {
                index_path: Some(source_path.clone()),
                layout: Layout::SingleImage,
                ..Default::default()
            },
        );
        let binding = StoredBinding {
            plan_fingerprint: "a".repeat(64),
            body_path: "body".into(),
            ..Default::default()
        };
        NodeServiceImpl::apply_binding_locked(&mut write_shard(&source.state), binding.clone())
            .unwrap();
        source.flush_index().unwrap();
        let directory = root.join("repository");
        std::fs::create_dir_all(&directory).unwrap();
        let (manifest, _) = source
            .export_if_flushed(&source_path, &directory)
            .unwrap()
            .unwrap();
        let target_path = root.join("target.tv");
        let target = NodeServiceImpl::new(
            None,
            NodeConfig {
                index_path: Some(target_path.clone()),
                layout: Layout::SingleImage,
                ..Default::default()
            },
        );
        for case in 0..5 {
            let mut invalid = manifest.clone();
            match case {
                0 => invalid.vector_rows = 1,
                1 => invalid.document_rows = 1,
                2 => invalid.live_rows = 1,
                3 => invalid.dim = 8,
                _ => invalid.scoring_fingerprint = "invented".into(),
            }
            assert_eq!(
                target
                    .install_staged_repository(&directory, &invalid)
                    .unwrap_err()
                    .code(),
                tonic::Code::FailedPrecondition
            );
            assert!(read_shard(&target.state).mapped_binding.is_none());
            assert!(!generation_dir(&target_path).exists());
        }
        target
            .install_staged_repository(&directory, &manifest)
            .unwrap();
        assert_eq!(
            read_shard(&target.state).mapped_binding,
            Some(binding.clone())
        );
        drop(target);
        // A crash after retiring the old generation but before adopting its
        // replacement restores the BM25-only generation, including its binding.
        std::fs::rename(
            generation_dir(&target_path),
            generation_old_dir(&target_path),
        )
        .unwrap();
        let reopened = NodeServiceImpl::open(
            NodeConfig {
                index_path: Some(target_path),
                layout: Layout::SingleImage,
                ..Default::default()
            },
            None,
            false,
        )
        .unwrap();
        assert_eq!(read_shard(&reopened.state).mapped_binding, Some(binding));
        assert!(read_shard(&reopened.state).index.is_none());
        drop(reopened);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn copy_rechecks_mutations_after_acquiring_the_seal_fence_with_or_without_wal() {
        let root =
            std::env::temp_dir().join(format!("snapshot-publication-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        for wal in [false, true] {
            let path = root.join(format!("{wal}.tv"));
            let service = NodeServiceImpl::new(
                None,
                NodeConfig {
                    index_path: Some(path.clone()),
                    layout: Layout::SingleImage,
                    wal,
                    ..Default::default()
                },
            );
            let binding = crate::postings::StoredBinding {
                plan_fingerprint: "a".repeat(64),
                body_path: "body".into(),
                ..Default::default()
            };
            NodeServiceImpl::apply_binding_locked(
                &mut write_shard(&service.state),
                binding.clone(),
            )
            .unwrap();
            service.flush_index().unwrap();
            let directory = root.join(format!("copy-{wal}"));
            std::fs::create_dir_all(&directory).unwrap();
            let sealing = service.seal_lock.lock().unwrap();
            let copying = service.clone();
            let copy_path = path.clone();
            let copy_directory = directory.clone();
            let copy =
                std::thread::spawn(move || copying.export_if_flushed(&copy_path, &copy_directory));
            // A real append after Flush, while publication is held out.
            service
                .apply_document_locked(
                    &mut write_shard(&service.state),
                    AddDocumentsRequest {
                        text: "word".into(),
                        ..Default::default()
                    },
                    crate::postings::AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
                    None,
                    None,
                    &mut 0,
                    &mut 0,
                )
                .unwrap();
            drop(sealing);
            assert!(copy.join().unwrap().unwrap().is_none());
            assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);
            service.flush_index().unwrap();
            let (manifest, _) = service
                .export_if_flushed(&path, &directory)
                .unwrap()
                .unwrap();
            assert_eq!(manifest.document_rows, 1);
            let image = Bm25Reader::open(&directory.join("documents.bm25")).unwrap();
            assert_eq!(image.next_doc_id(), 1);
            assert_eq!(image.binding(), Some(&binding));
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod vector_scan_view_tests {
    use super::*;

    #[test]
    fn one_coalesced_pass_keeps_each_jobs_authority_and_rejects_only_the_bad_job() {
        let mut store = crate::postings::Bm25Store::new().with_facets(&["audience"]);
        for (row, audience) in ["public", "private"].into_iter().enumerate() {
            store.add_document(
                row as u32,
                "word".into(),
                crate::postings::AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
            );
            store.set_facet(0, row as u32, audience);
        }
        let mut index =
            crate::vector::VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, 16, 4).unwrap();
        index.add(&[0.25; 48], 16).unwrap(); // final row has no document metadata
        let node = NodeServiceImpl::new(Some(index), Default::default())
            .with_bm25(Some(Bm25Shard::Building(store)));
        let mut jobs = Vec::new();
        let mut outputs = Vec::new();
        for (field, predicate, expected) in [
            ("unbound", "audience == 'public'", None),
            ("", "audience == 'public'", Some(vec![0])),
            ("", "audience == 'private'", Some(vec![1])),
            ("", "!has(audience)", Some(vec![])),
        ] {
            let context = crate::pb::VectorReadContext {
                field: field.into(),
                visibility: Some(crate::pb::DocumentVisibility {
                    filter: crate::cel::compile_filter(predicate).unwrap(),
                }),
                ..Default::default()
            };
            let scope =
                crate::visibility::VisibilityScope::new(context.visibility.as_ref()).unwrap();
            let (ready_tx, ready_rx) = mpsc::channel(4);
            let (done, outcome) = tokio::sync::oneshot::channel();
            jobs.push(ScanJob {
                read_context: Some(context),
                ready_tx,
                vector: vec![0.25; 16],
                k: 10,
                tie_complete: false,
                geo_filters: vec![],
                geo_regions: vec![],
                filter: None,
                enumerate_max_rows: None,
                patience_saturation_ppm: None,
                patience_hops: None,
                floor_prune: None,
                floor_k: None,
                floor_share_ppm: None,
                floor_greediness_ppm: None,
                floor_clamp_min: None,
                include_rerank_candidate: false,
                external: Box::new(|| None),
                publish: Box::new(|_| false),
                done,
            });
            outputs.push((scope, expected, ready_rx, outcome));
        }
        run_scan_batch(&node.state, 1, true, jobs);
        for (scope, expected, mut ready, outcome) in outputs {
            let result = outcome.blocking_recv().unwrap();
            if let Some(expected) = expected {
                let search_shard_response::Payload::ReadReady(receipt) =
                    ready.blocking_recv().unwrap().unwrap().payload.unwrap()
                else {
                    panic!("missing read receipt")
                };
                scope
                    .validate_echo(
                        &receipt.visibility_fingerprint,
                        &receipt.visibility_columns_known,
                    )
                    .unwrap();
                assert_eq!(receipt.visibility_columns_known, vec![true]);
                assert_eq!(
                    result
                        .unwrap()
                        .hits
                        .into_iter()
                        .map(|hit| hit.slot)
                        .collect::<Vec<_>>(),
                    expected
                );
            } else {
                assert_eq!(
                    result.err().unwrap().code(),
                    tonic::Code::FailedPrecondition
                );
                assert!(ready.blocking_recv().is_none());
            }
        }
    }
    #[test]
    fn parent_cache_cannot_cross_same_sized_replacements_or_publish_an_old_build() {
        fn store(parent: u64) -> Bm25Shard {
            let mut store = crate::postings::Bm25Store::new();
            store.add_document_with_lineage(
                0,
                "word".into(),
                crate::postings::AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1),
                Some(crate::postings::DocLineage {
                    parent_id: parent,
                    group_id: 0,
                    span_start: 0,
                    span_end: 4,
                }),
            );
            Bm25Shard::Building(store)
        }
        let node = NodeServiceImpl::new(None, Default::default()).with_bm25(Some(store(10)));
        let old = NodeServiceImpl::parent_map(&node.state, 100, 1).unwrap();
        assert_eq!(old.rows.as_slice(), &[10]);
        let node = node.with_bm25(Some(store(20)));
        let fresh = NodeServiceImpl::parent_map(&node.state, 100, 1).unwrap();
        assert_eq!(fresh.rows.as_slice(), &[20]);
        assert_ne!(fresh.claim, old.claim);
        let mut guard = write_shard(&node.state);
        // This is the publication gap: an old read finishes building after
        // a replacement and must not overwrite its cache with old lineage.
        assert_eq!(
            guard.publish_parent_map(&old).unwrap_err().code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(guard.parents.as_ref().unwrap().rows.as_slice(), &[20]);
        // The scan takes its final read guard after obtaining the cache.
        assert!(guard
            .check_stats_epoch(old.claim.epoch, &old.claim.incarnation())
            .is_err());
        guard
            .check_stats_epoch(fresh.claim.epoch, &fresh.claim.incarnation())
            .unwrap();
    }
}

#[cfg(test)]
mod filter_domain_tests {
    use super::*;
    use crate::boolean_bits::Bits;
    use crate::pb::boolean_plan_leaf::Leaf;
    use crate::pb::boolean_plan_node::Node;
    use crate::pb::{
        BooleanPlanDense, BooleanPlanFilter, BooleanPlanGroup, BooleanPlanLeaf, BooleanPlanLexical,
        BooleanPlanNode, BooleanShardRequest,
    };

    fn leaf_node(index: u32) -> BooleanPlanNode {
        BooleanPlanNode {
            node: Some(Node::Leaf(index)),
        }
    }

    fn group_node(group: BooleanPlanGroup) -> BooleanPlanNode {
        BooleanPlanNode {
            node: Some(Node::Group(group)),
        }
    }

    fn group(
        must: Vec<BooleanPlanNode>,
        should: Vec<BooleanPlanNode>,
        must_not: Vec<BooleanPlanNode>,
        minimum_should_match: u32,
    ) -> BooleanPlanGroup {
        BooleanPlanGroup {
            must,
            should,
            must_not,
            minimum_should_match,
        }
    }

    fn lexical() -> BooleanPlanLeaf {
        BooleanPlanLeaf {
            leaf: Some(Leaf::Lexical(BooleanPlanLexical::default())),
        }
    }

    fn dense() -> BooleanPlanLeaf {
        BooleanPlanLeaf {
            leaf: Some(Leaf::Dense(BooleanPlanDense::default())),
        }
    }

    fn filter() -> BooleanPlanLeaf {
        BooleanPlanLeaf {
            leaf: Some(Leaf::Filter(BooleanPlanFilter::default())),
        }
    }

    fn of(len: usize, slots: &[usize]) -> LeafSlot {
        let mut membership = Bits::empty(len);
        for &slot in slots {
            membership.set(slot);
        }
        LeafSlot::Done(EvaluatedLeaf {
            membership,
            positive: true,
            filter_known: None,
            filter_recheck: None,
        })
    }

    /// The domains one plan walk assigns, in leaf order; `None` is the
    /// whole shard.
    fn domains(
        req: &BooleanShardRequest,
        leaves: &[LeafSlot],
    ) -> Result<Vec<Option<Bits>>, Status> {
        let mut bounded: Vec<Option<Bits>> = vec![None; req.leaves.len()];
        let mut unbounded = vec![false; req.leaves.len()];
        let root = req.root.as_ref().expect("the test plans have a root");
        filter_leaf_domains(req, root, None, leaves, &mut bounded, &mut unbounded, 1)?;
        Ok(bounded
            .into_iter()
            .zip(unbounded)
            .map(|(domain, unbounded)| if unbounded { None } else { domain })
            .collect())
    }

    fn request(leaves: Vec<BooleanPlanLeaf>, root: BooleanPlanGroup) -> BooleanShardRequest {
        BooleanShardRequest {
            root: Some(root),
            leaves,
            ..Default::default()
        }
    }

    /// A MUST filter leaf beside an exact MUST sibling resolves over the
    /// sibling's members; alone, or beside another filter only, it keeps
    /// the whole shard.
    #[test]
    fn a_must_filter_is_bounded_by_exact_must_siblings_only() {
        let n = 100;
        let mut lex_bits = Bits::empty(n);
        for slot in [3, 4, 5] {
            lex_bits.set(slot);
        }
        let lex = of(n, &[3, 4, 5]);
        // MUST lexical + MUST filter: the filter takes the lexical set.
        let req = request(
            vec![lexical(), filter()],
            group(vec![leaf_node(0), leaf_node(1)], vec![], vec![], 0),
        );
        let leaves = vec![lex, LeafSlot::Todo];
        let assigned = domains(&req, &leaves).unwrap();
        assert_eq!(assigned, vec![None, Some(lex_bits)]);
        // A dense sibling narrows no filter: dense leaves resolve
        // after the filters, so a filter that empties its group never
        // pays a dense membership.
        let req = request(
            vec![dense(), filter()],
            group(vec![leaf_node(0), leaf_node(1)], vec![], vec![], 0),
        );
        let assigned = domains(&req, &leaves).unwrap();
        assert_eq!(assigned, vec![None, None]);
        // A lone MUST filter, or two filters without an exact sibling,
        // keep the whole shard.
        let req = request(vec![filter()], group(vec![leaf_node(0)], vec![], vec![], 0));
        assert_eq!(domains(&req, &[LeafSlot::Todo]).unwrap(), vec![None]);
        let req = request(
            vec![filter(), filter()],
            group(vec![leaf_node(0), leaf_node(1)], vec![], vec![], 0),
        );
        assert_eq!(
            domains(&req, &[LeafSlot::Todo, LeafSlot::Todo]).unwrap(),
            vec![None, None]
        );
    }

    /// The bound narrows down a MUST chain of groups; a SHOULD or
    /// MUST_NOT position narrows no filter, and a leaf shared between a
    /// bounded and an unbounded position is evaluated whole.
    #[test]
    fn the_bound_follows_must_edges_and_unions_over_occurrences() {
        let n = 100;
        let a = of(n, &[1, 2, 3]).done().unwrap().membership.clone();
        let b = of(n, &[2, 3, 4]).done().unwrap().membership.clone();
        // Nested: root MUST lexical + MUST group(MUST lexical + MUST
        // filter): the filter takes the intersection of both lexicals.
        let inner = group(vec![leaf_node(1), leaf_node(2)], vec![], vec![], 0);
        let req = request(
            vec![lexical(), lexical(), filter()],
            group(vec![leaf_node(0), group_node(inner)], vec![], vec![], 0),
        );
        let leaves = vec![of(n, &[1, 2, 3]), of(n, &[2, 3, 4]), LeafSlot::Todo];
        let assigned = domains(&req, &leaves).unwrap();
        let mut both = a.clone();
        both.and_with(&b);
        assert_eq!(assigned, vec![None, None, Some(both)]);
        // MUST_NOT and SHOULD filters keep the whole shard; their
        // subtrees keep only the incoming (here: no) bound.
        let inner = group(vec![leaf_node(2)], vec![], vec![], 0);
        let req = request(
            vec![lexical(), filter(), filter(), filter()],
            group(
                vec![leaf_node(0), leaf_node(1)],
                vec![group_node(inner)],
                vec![leaf_node(3)],
                1,
            ),
        );
        let leaves = vec![
            of(n, &[1, 2, 3]),
            LeafSlot::Todo,
            LeafSlot::Todo,
            LeafSlot::Todo,
        ];
        let assigned = domains(&req, &leaves).unwrap();
        assert_eq!(assigned, vec![None, Some(a.clone()), None, None]);
        // A leaf shared between two bounded positions takes the union.
        let left = group(vec![leaf_node(0), leaf_node(2)], vec![], vec![], 0);
        let right = group(vec![leaf_node(1), leaf_node(2)], vec![], vec![], 0);
        let req = request(
            vec![lexical(), lexical(), filter()],
            group(vec![group_node(left), group_node(right)], vec![], vec![], 0),
        );
        let leaves = vec![of(n, &[1, 2, 3]), of(n, &[2, 3, 4]), LeafSlot::Todo];
        let assigned = domains(&req, &leaves).unwrap();
        let mut union = a.clone();
        union.or_with(&b);
        assert_eq!(assigned, vec![None, None, Some(union)]);
        // A leaf shared between a bounded and an unbounded position is
        // whole-shard.
        let inner = group(vec![leaf_node(1)], vec![], vec![], 0);
        let req = request(
            vec![lexical(), filter()],
            group(
                vec![leaf_node(0), leaf_node(1)],
                vec![group_node(inner)],
                vec![],
                0,
            ),
        );
        let leaves = vec![of(n, &[1, 2, 3]), LeafSlot::Todo];
        let assigned = domains(&req, &leaves).unwrap();
        assert_eq!(assigned, vec![None, None]);
    }

    /// The recursion limit is the planner's own.
    #[test]
    fn the_domain_walk_keeps_the_recursion_limit() {
        let mut root = group(vec![leaf_node(0)], vec![], vec![], 0);
        for _ in 0..70 {
            root = group(vec![group_node(root)], vec![], vec![], 0);
        }
        let req = request(vec![lexical()], root);
        let err = domains(&req, &[of(8, &[1])]).unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }
}
