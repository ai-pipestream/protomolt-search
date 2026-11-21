//! Two equivalences. A chunked scan under a word-packed bitmap allowlist
//! is the scan under the same allowlist as one flag per row. A replica
//! caught up through coalesced append streams holds, byte for byte, the log
//! a record-at-a-time catch-up writes: keyed rows keep their key each, and
//! logged derived values replay without being derived again.

mod common;
#[path = "vector_bitmap_and_replication_batch/derived.rs"]
mod derived;
#[path = "vector_bitmap_and_replication_batch/pair.rs"]
mod pair;
#[path = "vector_bitmap_and_replication_batch/streams.rs"]
mod streams;

use pipestream_search::boolean_bits::Bits;
use pipestream_search::chunked::{
    chunked_topk, chunked_topk_batch_observed, chunked_topk_collapsed, BatchQuery, ChunkedScan,
    ScanStats,
};
use pipestream_search::vector::{Mask, VectorIndex, EMBEDDED_TURBOVEC};

const DIM: usize = 64;
const ROWS: usize = 5_003;

fn index(rows: usize) -> VectorIndex {
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, 4).unwrap();
    index
        .add(&common::unit_vectors(rows, DIM, 0xB17_0001), DIM)
        .unwrap();
    index
}

/// Allowlists that leave whole chunks, whole blocks and whole words empty,
/// plus the dense, full and empty ones.
fn allowlists(rows: usize) -> Vec<Vec<bool>> {
    let mut state = 0x5EED_B175_u64;
    let mut random = |keep: u64| -> Vec<bool> {
        (0..rows)
            .map(|_| {
                state = state
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (state >> 33).is_multiple_of(keep)
            })
            .collect()
    };
    let mut clustered = vec![false; rows];
    for slot in [0, 31, 32, 63, 64, 65, 2_047, 2_048, 4_999, rows - 1] {
        clustered[slot] = true;
    }
    vec![
        random(2),
        random(7),
        random(97),
        clustered,
        vec![true; rows],
        vec![false; rows],
    ]
}

fn timeless(mut stats: ScanStats) -> ScanStats {
    stats.scan_nanos = 0;
    stats
}

#[test]
fn a_bitmap_span_reads_like_the_flags_it_packs() {
    for rows in [1, 63, 64, 65, 1_000, 4_097] {
        for flags in allowlists(rows.max(5_000)) {
            let flags = &flags[..rows];
            let bits = Bits::from_bools(flags);
            let (packed, plain) = (Mask::Bits(&bits), Mask::Bools(flags));
            for (start, end) in [
                (0, rows),
                (0, rows / 2),
                (rows / 3, rows),
                (rows / 2, rows / 2),
                (rows / 2, rows + 65),
                (rows, rows / 2),
            ] {
                assert_eq!(packed.any_in(start, end), plain.any_in(start, end));
                let (mut from_bits, mut from_flags) = (Vec::new(), Vec::new());
                packed.for_each_in(start, end, |row| from_bits.push(row));
                plain.for_each_in(start, end, |row| from_flags.push(row));
                assert_eq!(from_bits, from_flags, "rows {rows} span {start}..{end}");
                let clamped_end = end.min(rows);
                let expected: Vec<usize> = (start.min(clamped_end)..clamped_end)
                    .filter(|&row| flags[row])
                    .collect();
                assert_eq!(from_bits, expected);
            }
        }
        let mut every = Vec::new();
        Mask::All(rows).for_each_in(0, rows, |row| every.push(row));
        assert_eq!(every, (0..rows).collect::<Vec<_>>());
        assert!(Mask::All(rows).any_in(0, rows) && !Mask::All(rows).any_in(rows, rows));
    }
}

#[test]
fn a_bitmap_allowlist_scans_exactly_like_its_flags() {
    let index = index(ROWS);
    let query = common::unit_vectors(1, DIM, 0xB17_0002);
    for flags in allowlists(ROWS) {
        let bits = Bits::from_bools(&flags);
        for (chunk_blocks, k, keep_ties) in [(1, 10, false), (3, 200, true), (64, 1, false)] {
            let run = |allow: Mask<'_>| {
                let mut floors = Vec::new();
                let scan = ChunkedScan {
                    index: &index,
                    query: &query,
                    k,
                    chunk_blocks,
                    allow: Some(allow),
                };
                let (hits, stats) = chunked_topk(scan, keep_ties, &mut || None, &mut |floor| {
                    floors.push(floor);
                    true
                });
                (hits, timeless(stats), floors)
            };
            let packed = run(Mask::Bits(&bits));
            assert_eq!(
                packed,
                run(Mask::Bools(&flags)),
                "chunk_blocks {chunk_blocks}"
            );
        }
    }
}

#[test]
fn a_bitmap_batch_shares_passes_exactly_like_its_flags() {
    let index = index(ROWS);
    let lists = allowlists(ROWS);
    let bits: Vec<Bits> = lists.iter().map(|flags| Bits::from_bools(flags)).collect();
    let vectors = common::unit_vectors(4, DIM, 0xB17_0003);
    let run = |packed: bool| {
        let allow = |list: usize| {
            Some(if packed {
                Mask::Bits(&bits[list])
            } else {
                Mask::Bools(&lists[list])
            })
        };
        let allows = [allow(1), None, allow(1), allow(2)];
        let queries: Vec<BatchQuery> = (0..4)
            .map(|qi| BatchQuery {
                vector: &vectors[qi * DIM..(qi + 1) * DIM],
                k: 5 + 40 * qi,
                keep_ties: qi == 3,
                allow: allows[qi],
                enumerate_max_rows: None,
                controls: Default::default(),
                patience: None,
                floor_prune: None,
            })
            .collect();
        let mut passes = Vec::new();
        let results = chunked_topk_batch_observed(
            &index,
            &queries,
            2,
            &mut |_| None,
            &mut |_, _| true,
            &mut |pass| passes.push(pass.bytes),
        );
        let results: Vec<_> = results
            .into_iter()
            .map(|(hits, stats)| (hits, timeless(stats)))
            .collect();
        (results, passes)
    };
    assert_eq!(run(true), run(false));
}

#[test]
fn a_collapsed_scan_under_a_bitmap_matches_its_flags() {
    let index = index(ROWS);
    let query = common::unit_vectors(1, DIM, 0xB17_0004);
    let parents: Vec<u64> = (0..ROWS as u64).map(|slot| slot / 9).collect();
    for flags in allowlists(ROWS) {
        let bits = Bits::from_bools(&flags);
        let scan = |allow| ChunkedScan {
            index: &index,
            query: &query,
            k: 25,
            chunk_blocks: 4,
            allow: Some(allow),
        };
        let run =
            |allow| chunked_topk_collapsed(scan(allow), &parents, &mut || None, &mut |_| true);
        let (packed, packed_stats) = run(Mask::Bits(&bits));
        let (plain, plain_stats) = run(Mask::Bools(&flags));
        assert_eq!(packed, plain);
        assert_eq!(timeless(packed_stats), timeless(plain_stats));
    }
}

#[test]
fn competitive_chunks_merge_through_two_buffers_without_changing_the_heap() {
    let index = index(ROWS);
    let query = common::unit_vectors(1, DIM, 0xB17_0005);
    for (k, keep_ties) in [(1_000, false), (1_000, true), (ROWS, false)] {
        let scan = |chunk_blocks| ChunkedScan {
            index: &index,
            query: &query,
            k,
            chunk_blocks,
            allow: None,
        };
        let whole = chunked_topk(scan(10_000), keep_ties, &mut || None, &mut |_| false).0;
        let chunked = chunked_topk(scan(1), keep_ties, &mut || None, &mut |_| false).0;
        assert_eq!(chunked, whole, "k {k} keep_ties {keep_ties}");
    }
}
