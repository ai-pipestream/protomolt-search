//! Unlinked images and parts: tiny segments, and the segmented provider's linked-first search.

use std::path::Path;
use std::sync::Arc;

use super::super::MIN_GRAPH_ROWS;
use super::*;
use crate::exact_vectors::ExactVectorStore;
use crate::live_docs::LiveDocs;
use crate::postings::{AnalyzedDoc, Bm25Store};
use crate::segmented_vectors::SegmentedProvider;
use crate::segments::{OpenedSegmentSet, SegmentCatalog, SegmentSource, VectorLoad};

fn every_backing_enumerates_the_tiny_segment(
    vectors: &[f32],
    queries: &[f32],
    dim: usize,
    candidates: [&HnswIndex; 3],
) {
    let rows = vectors.len() / dim;
    let all_true = vec![true; rows];
    let allow: Vec<bool> = (0..rows).map(|i| i % 2 == 0).collect();
    let allowed = allow.iter().filter(|&&b| b).count();
    for query in queries.chunks_exact(dim) {
        let truth_all = exact_top_masked(vectors, dim, query, &all_true, 10);
        let truth_masked = exact_top_masked(vectors, dim, query, &allow, 10);
        for candidate in candidates {
            let res = candidate
                .search(query, 10, VectorSearchOptions::new())
                .unwrap();
            assert_eq!(res.tail_enumerated_rows, rows as u64);
            let got: Vec<(u32, f32)> = res
                .slots_for_query(0)
                .iter()
                .zip(res.scores_for_query(0))
                .map(|(&id, &s)| (id as u32, s))
                .collect();
            assert_eq!(got, truth_all);

            let masked = candidate
                .search(query, 10, VectorSearchOptions::new().with_mask(&allow))
                .unwrap();
            assert_eq!(masked.tail_enumerated_rows, allowed as u64);
            assert_eq!(masked.regimes.enumerate, 1);
            let got_masked: Vec<(u32, f32)> = masked
                .slots_for_query(0)
                .iter()
                .zip(masked.scores_for_query(0))
                .map(|(&id, &s)| (id as u32, s))
                .collect();
            assert_eq!(got_masked, truth_masked);

            // Streaming raises floor and counts `tail_enumerated_rows`.
            let floor = truth_all[4].1;
            let mut streamed = Vec::new();
            let summary = candidate
                .search_streaming_controlled(
                    query,
                    VectorSearchOptions::new()
                        .with_candidate_depth(32)
                        .with_minimum_score(floor),
                    &mut |batch| {
                        streamed.extend(
                            batch
                                .slots
                                .iter()
                                .copied()
                                .zip(batch.scores.iter().copied()),
                        );
                        VectorStreamControl::Continue
                    },
                    &mut || VectorStreamControl::Continue,
                )
                .unwrap();
            assert!(summary.completed);
            assert_eq!(summary.tail_enumerated_rows, rows as u64);
            assert!(streamed.iter().all(|&(_, s)| s >= floor));
            for &(id, _) in &truth_all[..5] {
                assert!(streamed.iter().any(|&(slot, _)| slot == i64::from(id)));
            }
        }
    }
}

fn hostile_unlinked_headers_are_refused(dir: &Path, path: &Path) {
    // Hostile `entry == NO_NODE` headers with non-zero `top`, `upper_nodes`,
    // or `upper_cells` must be refused before mapping or allocating links.
    let bytes = std::fs::read(path).unwrap();
    let header_len = super::super::image::header_len();
    let reseal = |b: &mut [u8]| {
        let crc = crate::wal::crc32(&b[..136]);
        b[136..140].copy_from_slice(&crc.to_le_bytes());
        let end = b.len() - 32;
        let digest = crate::sha256::digest(&b[..end]);
        b[end..].copy_from_slice(&digest);
    };
    assert_eq!(header_len, 192);
    for (tag, patch_off, patch_len, val, expect) in [
        ("bad-top", 68usize, 4usize, 1u64, "top=1"),
        ("bad-upper-nodes", 48, 8, 1, "upper_nodes=1"),
        ("bad-upper-cells", 56, 8, 17, "upper_cells=17"),
    ] {
        let mut bad = bytes.clone();
        if patch_len == 4 {
            bad[patch_off..patch_off + 4].copy_from_slice(&(val as u32).to_le_bytes());
        } else {
            bad[patch_off..patch_off + 8].copy_from_slice(&val.to_le_bytes());
        }
        reseal(&mut bad);
        let bad_path = dir.join(tag);
        std::fs::write(&bad_path, &bad).unwrap();
        for (how, res) in [
            ("load", HnswIndex::load(&bad_path)),
            ("map", HnswIndex::load_mapped(&bad_path)),
        ] {
            let err = res.err().unwrap_or_else(|| panic!("{how} accepted {tag}"));
            assert!(
                err.to_string().contains(expect),
                "{how} {tag}: {err} (expected {expect:?})"
            );
        }
    }
}

#[test]
fn tiny_segments_persist_unlinked_and_refuse_hostile_unlinked_headers() {
    let dim = 16;
    let rows = 100;
    assert!(rows < MIN_GRAPH_ROWS);
    let (vectors, queries) = corpus_with_queries(rows, 8, dim, 0x9701);
    let mut index = HnswIndex::new(dim, config(BuildMode::Parallel)).unwrap();
    index.add(&vectors, dim).unwrap();
    index.prepare().unwrap();
    assert!(!index.is_graph_linked());

    let dir = temp_dir("tiny-unlinked");
    let path = dir.join("tiny.hnsw");
    index.write(&path).unwrap();
    let file_len = std::fs::metadata(&path).unwrap().len();
    let bytes_per_row = file_len as f64 / rows as f64;
    // Rows (64 B) + inv_norm (4 B) + 192 B header + 32 B trailer + 64 B section
    // alignment: well under 115 B/row, vs > 200 B/row when links are written.
    assert!(
        bytes_per_row < 115.0,
        "unlinked tiny image wrote {bytes_per_row} B/row"
    );

    let heap = HnswIndex::load(&path).unwrap();
    let mapped = HnswIndex::load_mapped(&path).unwrap();
    assert!(!heap.is_graph_linked() && !mapped.is_graph_linked());
    assert!(!heap.is_mapped() && mapped.is_mapped());
    assert_eq!(heap.row_identity(), index.row_identity());
    assert_eq!(mapped.row_identity(), index.row_identity());
    every_backing_enumerates_the_tiny_segment(&vectors, &queries, dim, [&index, &heap, &mapped]);
    hostile_unlinked_headers_are_refused(&dir, &path);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Where staged segments are built and the catalog they are appended to.
struct SegmentStage<'a> {
    dir: &'a Path,
    vectors: &'a [f32],
    dim: usize,
    cfg: HnswConfig,
    catalog: &'a SegmentCatalog,
}

impl SegmentStage<'_> {
    fn stage(&self, seg_id: &str, gen: u64, base: usize, count: usize) -> Arc<OpenedSegmentSet> {
        let (dir, vectors, dim, cfg, catalog) =
            (self.dir, self.vectors, self.dim, self.cfg, self.catalog);
        let stage = dir.join(seg_id);
        std::fs::create_dir_all(&stage).unwrap();
        let slice = &vectors[base * dim..(base + count) * dim];
        let mut idx = HnswIndex::new(dim, cfg).unwrap();
        idx.add(slice, dim).unwrap();
        let vec_path = stage.join("vector.index");
        idx.write(&vec_path).unwrap();
        let exact_path = stage.join("vectors.f32");
        ExactVectorStore::from_values(dim, slice.to_vec())
            .unwrap()
            .write(&exact_path)
            .unwrap();
        let mut bm25 = Bm25Store::new();
        for id in 0..count as u32 {
            bm25.add_document(id, String::new(), AnalyzedDoc::body(Vec::new(), 0));
        }
        let bm25_path = stage.join("bm25.bin");
        bm25.save(&bm25_path).unwrap();
        let live_path = stage.join("live.bin");
        LiveDocs::default().write(&live_path, count as u64).unwrap();
        catalog
            .append(SegmentSource {
                multi_res_path: None,
                segment_id: seg_id,
                generation: gen,
                base_label: base as u64,
                backend_kind: HNSW,
                vector_path: Some(&vec_path),
                exact_vector_path: Some(&exact_path),
                bm25_path: &bm25_path,
                live_docs_path: &live_path,
                partition_column: None,
            })
            .unwrap()
    }
}

/// One query over the linked segment, the tiny segment, and the tail;
/// returns how many of the exact top `k` the unary search found.
fn unary_and_streaming_agree_over_all_parts(
    provider: &SegmentedProvider,
    vectors: &[f32],
    dim: usize,
    query: &[f32],
    k: usize,
) -> usize {
    let mut recall_hits = 0usize;
    let truth = exact_top(vectors, dim, query, k);
    let unary = provider
        .search(
            query,
            k,
            VectorSearchOptions::new().with_candidate_depth(64),
        )
        .unwrap();
    // Segment 1 (100 rows) + live tail (150 rows) = 250 unlinked rows.
    assert_eq!(unary.tail_enumerated_rows, 250);
    let slots = unary.slots_for_query(0);
    let scores = unary.scores_for_query(0);
    for (&slot, &score) in slots.iter().zip(scores) {
        let exact = crate::exact_vectors::dot(&vectors[slot as usize * dim..][..dim], query);
        assert_eq!(score.to_bits(), exact.to_bits());
        if truth.iter().any(|&id| i64::from(id) == slot) {
            recall_hits += 1;
        }
    }

    // Controlled streaming raises the floor from Segment 0's HNSW walk so
    // Segment 1 and the tail emit only rows that beat the running top-k.
    let mut streamed_top: Vec<(i64, f32)> = Vec::new();
    let mut emitted_unlinked = 0usize;
    let summary = provider
        .search_streaming_controlled(
            query,
            VectorSearchOptions::new().with_candidate_depth(64),
            &mut |batch| {
                for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                    if slot >= 600 {
                        emitted_unlinked += 1;
                    }
                    streamed_top.push((slot, score));
                }
                streamed_top.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
                streamed_top.truncate(k);
                if streamed_top.len() == k {
                    VectorStreamControl::RaiseFloor(streamed_top[k - 1].1)
                } else {
                    VectorStreamControl::Continue
                }
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert!(summary.completed);
    assert_eq!(summary.tail_enumerated_rows, 250);
    assert!(
        emitted_unlinked < 100,
        "linked-first floor should prune most of the 250 unlinked rows (emitted {emitted_unlinked})"
    );
    assert_eq!(
        streamed_top.iter().map(|r| r.0).collect::<Vec<_>>(),
        slots,
        "unary and floor-pruned streaming over linked + unlinked parts must agree"
    );
    recall_hits
}

#[test]
fn segmented_provider_searches_linked_first_and_enumerates_unlinked_parts() {
    let dim = 24;
    // Segment 0: 600 rows (>= MIN_GRAPH_ROWS, linked).
    // Segment 1: 100 rows (< MIN_GRAPH_ROWS, unlinked tiny segment).
    // Live tail: 150 rows (unlinked heap tail).
    let total = 850;
    let (vectors, queries) = corpus_with_queries(total, 10, dim, 0x9702);
    let cfg = config(BuildMode::FixedOrder);
    let dir = temp_dir("seg-linked-first");
    let catalog = SegmentCatalog::open_with(dir.join("catalog"), VectorLoad::Mapped).unwrap();
    let stage = SegmentStage {
        dir: &dir,
        vectors: &vectors,
        dim,
        cfg,
        catalog: &catalog,
    };
    let _ = stage.stage("seg-0001", 1, 0, 600);
    let set = stage.stage("seg-0002", 2, 600, 100);
    assert!(set.vector(0).unwrap().is_graph_linked());
    assert!(!set.vector(1).unwrap().is_graph_linked());

    let tail = HnswIndex::new(dim, cfg).unwrap().into_index();
    let mut provider = SegmentedProvider::open(set, tail).unwrap();
    provider.add(&vectors[700 * dim..total * dim], dim).unwrap();
    assert!(!provider.tail().is_graph_linked());

    let k = 10;
    let mut recall_hits = 0usize;
    for query in queries.chunks_exact(dim) {
        recall_hits += unary_and_streaming_agree_over_all_parts(&provider, &vectors, dim, query, k);
    }
    assert!(
        recall_hits >= 95,
        "segmented recall@10 {recall_hits}/100 across linked + tiny + tail"
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
