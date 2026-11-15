//! Locality reordering against the unordered index: external identity,
//! appends, healing, merges, and hostile node maps.

use super::super::config::LocalityOrder;
use super::super::{MergeStrategy, SegmentMergeInput};
use super::locality::{LocalityCase, Reordered};
use super::*;
use crate::vector::Mask;

fn searches_match_the_unordered_index_bit_for_bit(
    c: &LocalityCase,
    order: LocalityOrder,
    candidate: &HnswIndex,
) {
    let (qn, queries, none_mapped) = (c.qn, &c.queries[..], &c.none_mapped);
    let (sparse_mask, mid_mask, dense_mask) = (&c.sparse_mask, &c.mid_mask, &c.dense_mask);
    for mask_opt in [
        None,
        Some(sparse_mask.as_slice()),
        Some(mid_mask.as_slice()),
        Some(dense_mask.as_slice()),
    ] {
        let mut opts = VectorSearchOptions::new().with_candidate_depth(48);
        if let Some(m) = mask_opt {
            opts = opts.with_mask(m);
        }
        let got = candidate.search(queries, 10, opts).unwrap();
        let want = none_mapped.search(queries, 10, opts).unwrap();
        assert_eq!(got.regimes, want.regimes, "{order:?} regimes");
        for qi in 0..qn {
            assert_eq!(
                got.slots_for_query(qi),
                want.slots_for_query(qi),
                "{order:?} q={qi} slots"
            );
            let got_bits: Vec<u32> = got
                .scores_for_query(qi)
                .iter()
                .map(|s| s.to_bits())
                .collect();
            let want_bits: Vec<u32> = want
                .scores_for_query(qi)
                .iter()
                .map(|s| s.to_bits())
                .collect();
            assert_eq!(got_bits, want_bits, "{order:?} q={qi} scores");
        }
    }
}

fn streams_match_the_unordered_index_as_sets(
    c: &LocalityCase,
    order: LocalityOrder,
    candidate: &HnswIndex,
) {
    let (dim, queries, none_mapped, mid_mask) =
        (c.dim, &c.queries[..], &c.none_mapped, &c.mid_mask);
    // Floor-pruned streaming over `local_mapped`.
    let q0 = &queries[..dim];
    let mut got_stream = Vec::new();
    let mut want_stream = Vec::new();
    let s_got = candidate
        .search_streaming_controlled(
            q0,
            VectorSearchOptions::new()
                .with_mask(mid_mask)
                .with_candidate_depth(48),
            &mut |b| {
                for (&id, &sc) in b.slots.iter().zip(b.scores) {
                    got_stream.push((id, sc.to_bits()));
                }
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    let s_want = none_mapped
        .search_streaming_controlled(
            q0,
            VectorSearchOptions::new()
                .with_mask(mid_mask)
                .with_candidate_depth(48),
            &mut |b| {
                for (&id, &sc) in b.slots.iter().zip(b.scores) {
                    want_stream.push((id, sc.to_bits()));
                }
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    assert_eq!(s_got.emitted, s_want.emitted);
    got_stream.sort_unstable();
    want_stream.sort_unstable();
    assert_eq!(got_stream, want_stream, "{order:?} streamed set");
}

pub(super) fn external_identity_matches_the_unordered_index(c: &LocalityCase, r: &Reordered) {
    let (n, none_mapped, base_tx) = (c.n, &c.none_mapped, &c.base_tx[..]);
    let (order, local_heap, local_loaded, local_mapped) =
        (r.order, &r.local_heap, &r.local_loaded, &r.local_mapped);
    // 4. External row identity, transcripts, and search hits across all filter regimes
    //    and streaming match `LocalityOrder::None` bit-for-bit!
    for candidate in [local_heap, local_loaded, local_mapped] {
        assert_eq!(candidate.row_identity(), none_mapped.row_identity());
        let mut tx = Vec::new();
        candidate
            .row_transcripts(0..n, &mut |r, b| tx.push((r, b.to_vec())))
            .unwrap();
        assert_eq!(tx, base_tx);
        searches_match_the_unordered_index_bit_for_bit(c, order, candidate);
        streams_match_the_unordered_index_as_sets(c, order, candidate);
    }
}

pub(super) fn appending_unpermutes_a_reordered_index(c: &LocalityCase, order: LocalityOrder) {
    let (dim, vectors, base, base_cfg) = (c.dim, &c.vectors[..], &c.base, c.base_cfg);
    // 5. Appending unlinked tail rows onto a locally-reordered heap index
    //    unpermutes back to external order before appending.
    let (mut extendable, _) = base.permute_from_linked(order, true).unwrap();
    assert!(extendable.has_node_map());
    let (extra_vecs, _) = corpus_with_queries(80, 1, dim, 0x98EE);
    extendable.add(&extra_vecs, dim).unwrap();
    assert!(!extendable.has_node_map());
    assert!(!extendable.is_graph_linked());
    extendable.prepare().unwrap();
    assert!(extendable.has_node_map());
    let mut combined = vectors.to_vec();
    combined.extend_from_slice(&extra_vecs);
    let mut ref_idx = HnswIndex::new(dim, base_cfg).unwrap();
    ref_idx.add(&combined, dim).unwrap();
    assert_eq!(extendable.row_identity(), ref_idx.row_identity());
}

pub(super) fn healing_translates_external_masks(c: &LocalityCase, r: &Reordered) {
    let (dim, n, vectors, base_cfg) = (c.dim, c.n, &c.vectors[..], c.base_cfg);
    let local_mapped = &r.local_mapped;
    // 6. heal_tombstones and merge_segments over locally-reordered
    //    donor/source segments translate external masks & IDs accurately.
    let live_flags: Vec<bool> = (0..n).map(|i| i % 4 != 2).collect();
    let (healed, surv_ids, _) = local_mapped
        .heal_tombstones(Mask::Bools(&live_flags))
        .unwrap();
    assert!(
        !healed.has_node_map(),
        "heal_tombstones produces global order"
    );
    assert_eq!(healed.unreachable_from_entry(), 0);
    let mut expected_surv_vecs = Vec::new();
    for &old_id in &surv_ids {
        assert!(live_flags[old_id as usize]);
        expected_surv_vecs
            .extend_from_slice(&vectors[old_id as usize * dim..(old_id as usize + 1) * dim]);
    }
    let mut surv_ref = HnswIndex::new(dim, base_cfg).unwrap();
    surv_ref.add(&expected_surv_vecs, dim).unwrap();
    assert_eq!(healed.row_identity(), surv_ref.row_identity());
}

pub(super) fn merging_reordered_segments_keeps_identity(c: &LocalityCase, r: &Reordered) {
    let (dim, n, vectors, dir, none_mapped) = (c.dim, c.n, &c.vectors[..], &c.dir, &c.none_mapped);
    let (order, seal_cfg) = (r.order, r.seal_cfg);
    let half = n / 2;
    let mut seg_a = HnswIndex::new(dim, seal_cfg).unwrap();
    seg_a.add(&vectors[..half * dim], dim).unwrap();
    let path_a = dir.join(format!("{order:?}-seg-a.hnsw"));
    seg_a.write(&path_a).unwrap();
    let map_a = HnswIndex::load_mapped(&path_a).unwrap();

    let mut seg_b = HnswIndex::new(dim, seal_cfg).unwrap();
    seg_b.add(&vectors[half * dim..], dim).unwrap();
    let path_b = dir.join(format!("{order:?}-seg-b.hnsw"));
    seg_b.write(&path_b).unwrap();
    let map_b = HnswIndex::load_mapped(&path_b).unwrap();

    let (merged, _) = HnswIndex::merge_segments(
        dim,
        seal_cfg,
        &[
            SegmentMergeInput {
                index: &map_a,
                live: None,
            },
            SegmentMergeInput {
                index: &map_b,
                live: None,
            },
        ],
        &[],
        MergeStrategy::JoinSet,
    )
    .unwrap();
    assert!(merged.has_node_map());
    assert_eq!(merged.unreachable_from_entry(), 0);
    assert_eq!(merged.row_identity(), none_mapped.row_identity());
}

pub(super) fn hostile_node_maps_are_refused(c: &LocalityCase) {
    let (n, base, dir) = (c.n, &c.base, &c.dir);
    // 7. Hostile / corrupted node maps are rejected on both `load` and `load_mapped`.
    let reseal = |b: &mut [u8]| {
        let crc = crate::wal::crc32(&b[..136]);
        b[136..140].copy_from_slice(&crc.to_le_bytes());
        let end = b.len() - 32;
        let digest = crate::sha256::digest(&b[..end]);
        b[end..].copy_from_slice(&digest);
    };
    let good_local = std::fs::read(dir.join("Rcm-local.hnsw")).unwrap();
    let parsed_hdr =
        super::super::image::load_mapped(&dir.join("Rcm-local.hnsw"), base.kernel()).unwrap();
    let n2r_off = parsed_hdr.header().layout.node_to_row_off as usize;

    // Case A: duplicate entry in `node_to_row` breaks bijection with `row_to_node`.
    let mut dup_map = good_local.clone();
    let row1 = u32::from_le_bytes(dup_map[n2r_off + 4..n2r_off + 8].try_into().unwrap());
    dup_map[n2r_off..n2r_off + 4].copy_from_slice(&row1.to_le_bytes());
    reseal(&mut dup_map);
    let dup_path = dir.join("bad-dup-map.hnsw");
    std::fs::write(&dup_path, &dup_map).unwrap();
    assert!(HnswIndex::load(&dup_path).is_err());
    assert!(HnswIndex::load_mapped(&dup_path).is_err());

    // Case B: out-of-range row in `node_to_row`.
    let mut oor_map = good_local.clone();
    oor_map[n2r_off..n2r_off + 4].copy_from_slice(&(n as u32).to_le_bytes());
    reseal(&mut oor_map);
    let oor_path = dir.join("bad-oor-map.hnsw");
    std::fs::write(&oor_path, &oor_map).unwrap();
    assert!(HnswIndex::load(&oor_path).is_err());
    assert!(HnswIndex::load_mapped(&oor_path).is_err());

    // Case C: `has_node_map = 1` combined with `locality_order = None (0)` in header.
    let mut bad_hdr = good_local;
    let cur_top = u32::from_le_bytes(bad_hdr[68..72].try_into().unwrap()) & 0xFF;
    let invalid_packed = cur_top | (1u32 << 16);
    bad_hdr[68..72].copy_from_slice(&invalid_packed.to_le_bytes());
    reseal(&mut bad_hdr);
    let hdr_path = dir.join("bad-hdr-map.hnsw");
    std::fs::write(&hdr_path, &bad_hdr).unwrap();
    assert!(HnswIndex::load(&hdr_path).is_err());
    assert!(HnswIndex::load_mapped(&hdr_path).is_err());
}
