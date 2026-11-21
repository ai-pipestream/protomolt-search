//! The fixed-order build and the walks over it, pinned bit for bit. Each
//! golden value digests one regime over every query and beam: the rows the
//! walk scored and its hops in order, how it ended, its work counters, its
//! results, and the visited set it left behind.

use super::super::distance::{kernel_dot_slack_per_norm, Kernel};
use super::super::graph::{
    with_scratch, Filter, FloorPrune, Hook, LinkCells, Patience, Route, Scratch, Store, View, Walk,
};
use super::*;
use crate::vector::Mask;

/// FNV-1a over 64-bit words.
pub(super) struct Digest(u64);

impl Digest {
    pub(super) fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    pub(super) fn word(&mut self, word: u64) {
        for byte in word.to_le_bytes() {
            self.0 = (self.0 ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    pub(super) fn scored(&mut self, row: u32, score: f32) {
        self.word(u64::from(row) << 32 | u64::from(score.to_bits()));
    }

    pub(super) fn finish(&self) -> u64 {
        self.0
    }
}

/// Every level, every link list in order, and the entry of `view`.
pub(super) fn graph_digest<L: LinkCells + ?Sized>(view: &View<'_, L>) -> u64 {
    let mut digest = Digest::new();
    digest.word(u64::from(view.entry) << 8 | u64::from(view.top));
    let mut links = Vec::new();
    for (node, &level) in view.level.iter().enumerate() {
        digest.word(u64::from(level));
        for layer in 0..=usize::from(level) {
            view.copy_links(node as u32, layer, None, &mut links);
            digest.word(links.len() as u64);
            for &neighbor in &links {
                digest.word(u64::from(neighbor));
            }
        }
    }
    digest.finish()
}

/// Small-integer rows, so every kernel sums them exactly and scores tie
/// constantly; every seventh row repeats an earlier one and a few are zero.
pub(super) fn tie_corpus(rows: usize, dim: usize, seed: u64) -> Vec<f32> {
    let mut rng = Rng(seed);
    let mut out: Vec<f32> = Vec::with_capacity(rows * dim);
    for row in 0..rows {
        if row % 7 == 3 {
            let from = (rng.next_u64() as usize % row) * dim;
            out.extend_from_within(from..from + dim);
        } else if row % 97 == 50 {
            out.resize(out.len() + dim, 0.0);
        } else {
            out.extend((0..dim).map(|_| (rng.next_u64() % 7) as f32 - 3.0));
        }
    }
    out
}

/// A seeded mask keeping the rows whose hash satisfies `keep`.
pub(super) fn hashed_mask(rows: usize, salt: u64, keep: impl Fn(u64) -> bool) -> Vec<bool> {
    (0..rows as u64)
        .map(|row| keep(Rng(salt ^ row).next_u64()))
        .collect()
}

const HOP: u64 = u64::MAX;
const HALF: usize = 0;
const TENTH: usize = 1;
const LIVE: usize = 2;
const LIVE_TENTH: usize = 3;
const REGION: usize = 4;

/// Records every scored row and hop, serves a floor, and can stop the walk.
struct Tape {
    digest: Digest,
    floor: f32,
    hops_left: Option<u32>,
}

impl Hook for Tape {
    fn scored(&mut self, node: u32, score: f32, _row: &[f32]) {
        self.digest.scored(node, score);
    }

    fn hop(&mut self) -> bool {
        self.digest.word(HOP);
        match &mut self.hops_left {
            Some(0) => false,
            Some(left) => {
                *left -= 1;
                true
            }
            None => true,
        }
    }

    fn current_floor(&self) -> f32 {
        self.floor
    }
}

/// How a regime gates rows: a mask index, and for the filter-aware walk
/// its route and budget.
#[derive(Clone, Copy)]
enum Gate {
    None,
    Plain(usize),
    Aware(usize, Option<Route>, Option<u64>),
}

/// One walk regime, run over every query at each of its beams.
#[derive(Clone, Copy)]
struct Regime {
    name: &'static str,
    efs: &'static [usize],
    gate: Gate,
    patience: Patience,
    /// Prune against the kernel score of the query's neighbor at this rank.
    floor_rank: Option<usize>,
    stop_after: Option<u32>,
}

const ROUTE: Route = Route {
    plain_share: 0.2,
    enumerate_rows: 64,
};
const SATURATED: Patience = Patience {
    saturation: 0.9,
    hops: 2,
    ..Patience::OFF
};
const BASE: Regime = Regime {
    name: "none",
    efs: &[1, 2, 10, 16, 64, 100, 128, 256, 1000, 5000],
    gate: Gate::None,
    patience: Patience::OFF,
    floor_rank: None,
    stop_after: None,
};

const fn regime(name: &'static str, efs: &'static [usize], gate: Gate) -> Regime {
    Regime {
        name,
        efs,
        gate,
        ..BASE
    }
}

const REGIMES: [Regime; 18] = [
    BASE,
    regime("plain-half", &[10, 64, 100, 256, 1000], Gate::Plain(HALF)),
    regime("plain-tenth", &[10, 100, 1000], Gate::Plain(TENTH)),
    regime("plain-live", &[10, 100, 1000], Gate::Plain(LIVE)),
    regime(
        "aware-half",
        &[10, 64, 100, 256, 1000],
        Gate::Aware(HALF, None, None),
    ),
    regime(
        "aware-tenth",
        &[10, 100, 1000],
        Gate::Aware(TENTH, None, None),
    ),
    regime(
        "aware-live-tenth",
        &[10, 100],
        Gate::Aware(LIVE_TENTH, None, None),
    ),
    regime(
        "routed-half",
        &[16, 100],
        Gate::Aware(HALF, Some(ROUTE), None),
    ),
    regime(
        "routed-tenth",
        &[16, 100],
        Gate::Aware(TENTH, Some(ROUTE), None),
    ),
    regime(
        "routed-region",
        &[16, 100, 1000],
        Gate::Aware(REGION, Some(ROUTE), None),
    ),
    regime(
        "budget",
        &[100, 1000],
        Gate::Aware(TENTH, Some(ROUTE), Some(200)),
    ),
    Regime {
        patience: SATURATED,
        ..regime("patience", &[64, 256, 1000], Gate::None)
    },
    Regime {
        patience: Patience {
            saturation: 1.0,
            hops: 3,
            top_k: 10,
            ..Patience::OFF
        },
        ..regime("patience-top-k", &[100, 1000], Gate::None)
    },
    Regime {
        patience: SATURATED,
        ..regime("patience-aware", &[64, 256], Gate::Aware(HALF, None, None))
    },
    Regime {
        floor_rank: Some(20),
        ..regime("floor", &[64, 256], Gate::None)
    },
    Regime {
        floor_rank: Some(20),
        ..regime("floor-aware", &[64, 256], Gate::Aware(HALF, None, None))
    },
    Regime {
        floor_rank: Some(40),
        patience: Patience {
            ascent_gate: 32,
            floor_active: true,
            ..SATURATED
        },
        ..regime("floor-patience", &[64, 256], Gate::None)
    },
    Regime {
        stop_after: Some(5),
        ..regime("stopped", &[64, 1000], Gate::Plain(HALF))
    },
];

/// A fixed-order graph with the queries and masks its regimes walk under.
struct Pinned {
    dim: usize,
    queries: Vec<f32>,
    masks: [Vec<bool>; 5],
    store: Store,
}

impl Pinned {
    fn new(kernel: Kernel, dim: usize, vectors: &[f32], queries: Vec<f32>) -> Self {
        let rows = vectors.len() / dim;
        let mut store = Store::new(dim, 16, 100, 7, kernel);
        store.append_unlinked(vectors).unwrap();
        let links = store.build_links(false).unwrap();
        store.install_links(links);
        let opposite: Vec<f32> = queries[..dim].iter().map(|x| -x).collect();
        let far = exact_top(vectors, dim, &opposite, 1)[0] as usize;
        let tenth = hashed_mask(rows, 0x7E47, |hash| hash % 10 == 0);
        let live = hashed_mask(rows, 0x11FE, |hash| hash % 5 != 0);
        let live_tenth = tenth.iter().zip(&live).map(|(a, b)| *a && *b).collect();
        Self {
            dim,
            queries,
            masks: [
                hashed_mask(rows, 0x4A1F, |hash| hash % 2 == 0),
                tenth,
                live,
                live_tenth,
                region_mask(vectors, dim, &vectors[far * dim..][..dim], 5),
            ],
            store,
        }
    }

    fn topical() -> Self {
        let (vectors, queries) = corpus_with_queries(3000, 6, 24, 0x5EED_0001);
        Self::new(Kernel::Scalar, 24, &vectors, queries)
    }

    fn tied(kernel: Kernel) -> Self {
        let vectors = tie_corpus(2400, 12, 0x5EED_0002);
        Self::new(kernel, 12, &vectors, tie_corpus(6, 12, 0x5EED_0003))
    }

    fn filter(&self, gate: Gate) -> Filter<'_> {
        match gate {
            Gate::None => Filter::None,
            Gate::Plain(mask) => Filter::Plain(Mask::Bools(&self.masks[mask])),
            Gate::Aware(mask, route, budget) => {
                let allow = Mask::Bools(&self.masks[mask]);
                Filter::Aware {
                    allow,
                    allowed: allow.count(),
                    route,
                    budget,
                }
            }
        }
    }
}

/// The kernel score of the row ranked `rank` for `q` over the whole graph.
fn ranked_score(view: &View<'_, [std::sync::atomic::AtomicU32]>, q: &[f32], rank: usize) -> f32 {
    let mut scores: Vec<f32> = (0..view.n() as u32)
        .map(|node| view.kernel.dot(q, view.row(node)))
        .collect();
    scores.sort_by(|a, b| b.total_cmp(a));
    scores[rank]
}

/// What the walk left in `scratch`: its routing flag, the allowed rows a
/// filter-aware walk recorded, and the visited set over `rows` rows.
fn digest_scratch(scratch: &Scratch, rows: usize, digest: &mut Digest) {
    digest.word(u64::from(scratch.walked_plain()));
    for item in scratch.scored_allowed() {
        digest.scored(item.node, item.score);
    }
    for base in (0..rows).step_by(64) {
        let word = (base..rows.min(base + 64))
            .filter(|&row| scratch.seen(row as u32))
            .fold(0u64, |word, row| word | 1 << (row - base));
        digest.word(word);
    }
}

/// Digest `regime` over every query and beam of `fx`, noting how each walk
/// ended in `ends`.
fn regime_digest(fx: &Pinned, regime: &Regime, ends: &mut Vec<Walk>) -> u64 {
    let view = fx.store.view();
    let mut tape = Tape {
        digest: Digest::new(),
        floor: f32::NEG_INFINITY,
        hops_left: None,
    };
    for q in fx.queries.chunks_exact(fx.dim) {
        let floor = regime.floor_rank.map(|rank| ranked_score(&view, q, rank));
        let prune = FloorPrune {
            enabled: floor.is_some(),
            gate: 8,
            slack_per_norm: kernel_dot_slack_per_norm(fx.dim, q),
        };
        for &ef in regime.efs {
            tape.floor = floor.unwrap_or(f32::NEG_INFINITY);
            tape.hops_left = regime.stop_after;
            let filter = fx.filter(regime.gate);
            with_scratch(|scratch| {
                let (hits, stats, walk) =
                    view.walk(q, ef, filter, regime.patience, prune, scratch, &mut tape);
                ends.push(walk);
                for count in [walk as u64, stats.hops, stats.scored] {
                    tape.digest.word(count);
                }
                tape.digest.word(stats.bridges << 32 | stats.probed);
                tape.digest.word(hits.len() as u64);
                for hit in hits {
                    tape.digest.scored(hit.node, hit.score);
                }
                digest_scratch(scratch, view.n(), &mut tape.digest);
            });
        }
    }
    tape.digest.finish()
}

/// The graph digest, then one digest per regime, with how the walks ended.
fn pinned_digests(fx: &Pinned) -> (Vec<(&'static str, u64)>, Vec<Walk>) {
    let mut ends = Vec::new();
    let mut digests = vec![("graph", graph_digest(&fx.store.view()))];
    for regime in &REGIMES {
        digests.push((regime.name, regime_digest(fx, regime, &mut ends)));
    }
    (digests, ends)
}

const TOPICAL: [(&str, u64); 19] = [
    ("graph", 0x1a0f_fbe1_d246_e6a9),
    ("none", 0xa783_5c49_b5c4_802c),
    ("plain-half", 0xef16_b479_1592_f1f7),
    ("plain-tenth", 0xefaa_b775_153b_160e),
    ("plain-live", 0x68ae_e880_4efe_e7e5),
    ("aware-half", 0xb55c_6a42_db20_701d),
    ("aware-tenth", 0x1ea6_72cf_41ac_8a67),
    ("aware-live-tenth", 0xfc6f_2768_4f1c_391b),
    ("routed-half", 0xc7a8_7285_5114_d897),
    ("routed-tenth", 0xc945_5030_9587_3351),
    ("routed-region", 0x862c_52cb_e264_70e7),
    ("budget", 0x01a9_a0f3_0a1d_465d),
    ("patience", 0x24cc_1653_a1f8_8784),
    ("patience-top-k", 0xd00c_1c85_88fc_9920),
    ("patience-aware", 0x4a88_b216_e07f_374b),
    ("floor", 0xb943_98e0_579e_490d),
    ("floor-aware", 0xd798_5ffe_22c0_3b45),
    ("floor-patience", 0xb7b8_49f6_13b6_c4c1),
    ("stopped", 0x9dcd_1ecd_812f_b959),
];

const TIED: [(&str, u64); 19] = [
    ("graph", 0x5c1d_5c3b_867a_bcd7),
    ("none", 0x8a89_5ba5_79f5_53c5),
    ("plain-half", 0x1c73_ae8e_9ed2_a029),
    ("plain-tenth", 0x6034_dc2e_da64_f3dc),
    ("plain-live", 0x62c2_da5b_ca3b_2277),
    ("aware-half", 0x5d6b_804b_5005_3cfe),
    ("aware-tenth", 0xe5c2_04d4_a6ef_dcf2),
    ("aware-live-tenth", 0x427f_7f55_7a58_2ba1),
    ("routed-half", 0xe2be_97fb_6f1d_ac87),
    ("routed-tenth", 0xe3c6_bd89_8b81_4f49),
    ("routed-region", 0x0fe4_e729_2b10_b603),
    ("budget", 0xca04_d632_4d0e_12a4),
    ("patience", 0x008f_2697_e003_ef0a),
    ("patience-top-k", 0x011d_a84a_9026_f349),
    ("patience-aware", 0x0c67_01b7_4e3c_cd3a),
    ("floor", 0x937b_4fe5_f9b8_af45),
    ("floor-aware", 0xedae_1259_7eb6_2475),
    ("floor-patience", 0x5927_5de3_eea3_ad3e),
    ("stopped", 0x838e_78e3_581e_7b41),
];

#[test]
fn fixed_order_walks_over_topical_rows_are_pinned() {
    let (digests, ends) = pinned_digests(&Pinned::topical());
    for end in [
        Walk::Completed,
        Walk::Stopped,
        Walk::Patience,
        Walk::FloorPruned,
        Walk::Far,
        Walk::Short,
        Walk::Budget,
    ] {
        assert!(ends.contains(&end), "no walk ended {end:?}");
    }
    assert_eq!(digests, TOPICAL, "{digests:#x?}");
}

#[test]
fn fixed_order_walks_over_tied_rows_are_pinned_on_every_kernel() {
    for kernel in Kernel::available() {
        let (digests, _) = pinned_digests(&Pinned::tied(kernel));
        assert_eq!(digests, TIED, "{}: {digests:#x?}", kernel.name());
    }
}
