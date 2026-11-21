//! The provider's answers over a fixed-order graph of constantly tying rows,
//! pinned bit for bit: unary searches by depth, beam and mask, streams, and
//! the graphs that merging, healing and repair leave. The rows are small
//! integers, which every kernel sums exactly, so the digests do not depend
//! on the kernel the host picks.

use super::super::distance::Kernel;
use super::super::graph::{LinkBuilder, Store};
use super::super::merge::repair_connectivity;
use super::super::{Answer, MergeStrategy, SegmentMergeInput};
use super::repair::sever_cross_cluster_links;
use super::walk_identity::{graph_digest, hashed_mask, tie_corpus, Digest};
use super::*;
use crate::vector::{FloorPruneSpec, Mask, PatienceSpec};

const DIM: usize = 12;
const ROWS: usize = 3000;
/// `(k, beam)` pairs every unary ask runs; a beam below `k` walks at `k`.
const DEPTHS: [(usize, usize); 9] = [
    (10, 16),
    (10, 64),
    (10, 128),
    (100, 16),
    (100, 100),
    (100, 256),
    (1000, 64),
    (1000, 1000),
    (1000, 1500),
];

/// The tying corpus, its queries, and the masks searches run under.
struct Tied {
    vectors: Vec<f32>,
    queries: Vec<f32>,
    half: Bits,
    tenth: Bits,
    live: Vec<bool>,
    live_tenth: Bits,
}

impl Tied {
    fn new() -> Self {
        let tenth = hashed_mask(ROWS, 0x7E47, |hash| hash % 10 == 0);
        let live = hashed_mask(ROWS, 0x11FE, |hash| hash % 5 != 0);
        let live_tenth: Vec<bool> = tenth.iter().zip(&live).map(|(a, b)| *a && *b).collect();
        Self {
            vectors: tie_corpus(ROWS, DIM, 0x5EED_0004),
            queries: tie_corpus(5, DIM, 0x5EED_0005),
            half: Bits::from_bools(&hashed_mask(ROWS, 0x4A1F, |hash| hash % 2 == 0)),
            tenth: Bits::from_bools(&tenth),
            live_tenth: Bits::from_bools(&live_tenth),
            live,
        }
    }
}

fn digest_answer(digest: &mut Digest, hits: &[(u32, f32)], answer: &Answer) {
    let regime = match answer.regime {
        None => 0,
        Some(FilterRegime::Enumerate) => 1,
        Some(FilterRegime::FilterAware) => 2,
        Some(FilterRegime::Plain) => 3,
    };
    let flags = u64::from(answer.budget_handoff)
        | u64::from(answer.patience_stopped) << 1
        | u64::from(answer.floor_pruned) << 2;
    let stats = answer.stats;
    for word in [regime, flags, stats.hops, stats.scored, stats.bridges] {
        digest.word(word);
    }
    digest.word(stats.probed << 32 | answer.tail_enumerated_rows);
    digest.word(hits.len() as u64);
    for &(row, score) in hits {
        digest.scored(row, score);
    }
}

/// Digest every query at every depth under `options`.
fn search_digest(index: &HnswIndex, tied: &Tied, options: VectorSearchOptions<'_>) -> u64 {
    let mut digest = Digest::new();
    for query in tied.queries.as_chunks::<DIM>().0 {
        for (k, beam) in DEPTHS {
            let (hits, answer) = index.search_with_stats(query, k, beam, options).unwrap();
            digest_answer(&mut digest, &hits, &answer);
        }
    }
    digest.finish()
}

/// Digest the stream of every query at `depth` under `options`: each batch
/// in emission order, then the summary's counters.
fn stream_digest(index: &HnswIndex, tied: &Tied, options: VectorSearchOptions<'_>) -> u64 {
    let mut digest = Digest::new();
    let summary = index
        .search_streaming_controlled(
            &tied.queries,
            options,
            &mut |batch| {
                digest.word(batch.query_index as u64);
                for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                    digest.scored(slot as u32, score);
                }
                VectorStreamControl::Continue
            },
            &mut || VectorStreamControl::Continue,
        )
        .unwrap();
    let regimes = summary.regimes;
    for count in [
        summary.emitted as u64,
        summary.units_scanned as u64,
        u64::from(summary.completed),
        u64::from(regimes.enumerate) << 32 | u64::from(regimes.filter_aware),
        u64::from(summary.patience_stopped) << 32 | u64::from(summary.floor_pruned),
    ] {
        digest.word(count);
    }
    digest.finish()
}

fn unary_digests(index: &HnswIndex, tied: &Tied) -> Vec<(&'static str, u64)> {
    let new = VectorSearchOptions::new;
    let patience = PatienceSpec::from_ppm(900_000, 2);
    let prune = FloorPruneSpec {
        enabled: true,
        floor_k: 10,
        clamp_min: 8,
        ..FloorPruneSpec::OFF
    };
    let asks = [
        ("none", new()),
        ("half", new().with_bits(&tied.half)),
        ("tenth", new().with_bits(&tied.tenth)),
        ("tombstones", new().with_mask(&tied.live)),
        ("tombstones-tenth", new().with_bits(&tied.live_tenth)),
        (
            "half-plain",
            new()
                .with_bits(&tied.half)
                .with_filter_regime(FilterRegime::Plain),
        ),
        (
            "tenth-aware",
            new()
                .with_bits(&tied.tenth)
                .with_filter_regime(FilterRegime::FilterAware),
        ),
        ("floor", new().with_minimum_score(9.0)),
        ("patience", new().with_patience(patience)),
        (
            "floor-prune",
            new().with_minimum_score(12.0).with_floor_prune(prune),
        ),
    ];
    asks.into_iter()
        .map(|(name, options)| (name, search_digest(index, tied, options)))
        .collect()
}

fn stream_digests(index: &HnswIndex, tied: &Tied) -> Vec<(&'static str, u64)> {
    let at = |depth| VectorSearchOptions::new().with_candidate_depth(depth);
    let asks = [
        ("stream-64", at(64)),
        ("stream-1000", at(1000)),
        ("stream-half", at(64).with_bits(&tied.half)),
        ("stream-tenth", at(256).with_bits(&tied.tenth)),
        (
            "stream-tombstones-tenth",
            at(100).with_bits(&tied.live_tenth),
        ),
    ];
    asks.into_iter()
        .map(|(name, options)| (name, stream_digest(index, tied, options)))
        .collect()
}

/// Three fixed-order segments merged under each strategy with tombstones in
/// the second and third and an unlinked tail, then one index healed of its
/// tombstones: the graph each leaves, and a search over it.
fn merge_digests(index: &HnswIndex, tied: &Tied) -> Vec<(&'static str, u64)> {
    let bounds = [0, 1200, 2100, 2800];
    let segments: Vec<HnswIndex> = bounds
        .windows(2)
        .map(|w| {
            built(
                &tied.vectors[w[0] * DIM..w[1] * DIM],
                DIM,
                BuildMode::FixedOrder,
            )
        })
        .collect();
    let inputs: Vec<SegmentMergeInput<'_>> = segments
        .iter()
        .zip(bounds.windows(2))
        .enumerate()
        .map(|(i, (index, w))| SegmentMergeInput {
            index,
            live: (i > 0).then(|| Mask::Bools(&tied.live[w[0]..w[1]])),
        })
        .collect();
    let tail = &tied.vectors[2800 * DIM..];
    let cfg = config(BuildMode::FixedOrder);
    let mut out = Vec::new();
    for (graph, search, strategy) in [
        (
            "merge-donor",
            "merge-donor-search",
            MergeStrategy::DonorReinsert,
        ),
        (
            "merge-join-set",
            "merge-join-set-search",
            MergeStrategy::JoinSet,
        ),
    ] {
        let (merged, _) = HnswIndex::merge_segments(DIM, cfg, &inputs, tail, strategy).unwrap();
        out.push((graph, with_view!(merged, |view| graph_digest(&view))));
        let options = VectorSearchOptions::new();
        out.push((search, search_digest(&merged, tied, options)));
    }
    let (healed, _, _) = index.heal_tombstones(Mask::Bools(&tied.live)).unwrap();
    out.push(("heal", with_view!(healed, |view| graph_digest(&view))));
    out
}

/// The graph a connectivity repair leaves after every link between the two
/// halves of the corpus is cut.
fn repair_digest(tied: &Tied, kernel: Kernel) -> u64 {
    let mut store = Store::new(DIM, 16, 100, 7, kernel);
    store.append_unlinked(&tied.vectors).unwrap();
    let links = store.build_links(false).unwrap();
    sever_cross_cluster_links(&links, ROWS / 2, 16);
    store.install_links(links);
    let repaired = repair_connectivity(&LinkBuilder::new(&store, store.links().unwrap()));
    assert!(repaired >= 1, "the cut left nothing to repair");
    graph_digest(&store.view())
}

const GOLDEN: [(&str, u64); 22] = [
    ("graph", 0x6fe2_d326_0fc1_a2c5),
    ("none", 0xeb56_26cb_0c2c_c9c4),
    ("half", 0x8e4a_2342_60f0_9e76),
    ("tenth", 0x3f7e_416f_5dca_4131),
    ("tombstones", 0xb168_9ed7_0605_2546),
    ("tombstones-tenth", 0x13e3_d0f7_6147_88ae),
    ("half-plain", 0x1c06_a31e_636c_7dfd),
    ("tenth-aware", 0xe994_dcc9_aece_9d68),
    ("floor", 0xf1db_6b7c_4979_8833),
    ("patience", 0x0431_77dd_2a8d_ac05),
    ("floor-prune", 0x0a86_329c_2fc4_dfa8),
    ("stream-64", 0x7e79_ee66_ecf2_5e5b),
    ("stream-1000", 0x24e2_e8cd_93fc_fc93),
    ("stream-half", 0x1c2d_4cb6_2b8f_b486),
    ("stream-tenth", 0xaf29_1ae3_dcf4_87b6),
    ("stream-tombstones-tenth", 0x0b09_f691_3877_fd0c),
    ("merge-donor", 0x714e_b5c0_20e2_6163),
    ("merge-donor-search", 0xc5d9_48c1_569d_9525),
    ("merge-join-set", 0xe59a_eb15_66fe_450d),
    ("merge-join-set-search", 0xe2bc_f842_131d_3e11),
    ("heal", 0xcef6_5173_89d2_4e23),
    ("repair", 0xc822_4c4d_5b96_fceb),
];

#[test]
fn searches_streams_merges_and_repairs_over_tied_rows_are_pinned() {
    let tied = Tied::new();
    let index = built(&tied.vectors, DIM, BuildMode::FixedOrder);
    let mut digests = vec![("graph", with_view!(index, |view| graph_digest(&view)))];
    digests.extend(unary_digests(&index, &tied));
    digests.extend(stream_digests(&index, &tied));
    digests.extend(merge_digests(&index, &tied));
    for kernel in Kernel::available() {
        digests.push(("repair", repair_digest(&tied, kernel)));
    }
    digests.dedup();
    assert_eq!(digests, GOLDEN, "{digests:#x?}");
}
