//! Transplant of the donor's surviving edges into the output and local
//! healing of every adjacency list a tombstone broke.

use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use super::super::graph::{
    with_scratch, LinkBuilder, LinkCells, Locks, Scored, Scratch, View, NO_NODE,
};
use super::donor::DonorMap;

/// Maximum deleted-node hops traversed locally when healing an adjacency list
/// whose 1-hop neighborhood lost members to tombstones.
const MAX_DELETED_BRIDGE_EXPANDS: usize = 64;

/// A healed edge `source -> target` whose reciprocal is still to be wired.
#[derive(Clone, Copy)]
struct BridgeLink {
    target: u32,
    source: u32,
    layer: u8,
}

struct HealNodeOut {
    healed_lists: usize,
    bridges: Vec<BridgeLink>,
}

/// One donor adjacency list being healed.
#[derive(Clone, Copy)]
struct ListAt {
    old_u: u32,
    new_u: u32,
    layer: usize,
}

/// The working buffers of one node's healing; the first five are borrowed
/// from the thread's scratch.
struct HealBuffers {
    raw_nbrs: Vec<u32>,
    bridge_nbrs: Vec<u32>,
    cands: Vec<u32>,
    pool: Vec<Scored>,
    sel: Vec<u32>,
    deleted_queue: Vec<u32>,
    surv_1hop: Vec<u32>,
    healed: Vec<u32>,
}

impl HealBuffers {
    fn take(scratch: &mut Scratch) -> Self {
        Self {
            raw_nbrs: std::mem::take(&mut scratch.nbrs),
            bridge_nbrs: std::mem::take(&mut scratch.routes),
            cands: std::mem::take(&mut scratch.fresh),
            pool: std::mem::take(&mut scratch.pool),
            sel: std::mem::take(&mut scratch.sel),
            deleted_queue: Vec::new(),
            surv_1hop: Vec::new(),
            healed: Vec::new(),
        }
    }

    fn restore(self, scratch: &mut Scratch) {
        scratch.nbrs = self.raw_nbrs;
        scratch.routes = self.bridge_nbrs;
        scratch.fresh = self.cands;
        scratch.pool = self.pool;
        scratch.sel = self.sel;
    }
}

struct DonorHealer<'a, L: LinkCells + ?Sized> {
    donor_view: &'a View<'a, L>,
    builder: &'a LinkBuilder<'a>,
    /// `to_new[old_u]`: the output node of donor node `old_u`, or `NO_NODE`.
    to_new: &'a [u32],
}

impl<L: LinkCells + ?Sized> DonorHealer<'_, L> {
    /// Write the lists of one surviving donor node at every layer
    /// `0..=level[new_u]`: remapped as they are where no neighbor was
    /// deleted, healed where one was.
    fn heal_node(&self, (old_u, new_u): (u32, u32), scratch: &mut Scratch) -> HealNodeOut {
        let lvl = usize::from(self.builder.links.level[new_u as usize]);
        let mut bufs = HealBuffers::take(scratch);
        let mut out = HealNodeOut {
            healed_lists: 0,
            bridges: Vec::new(),
        };
        for layer in 0..=lvl {
            self.donor_view
                .copy_links(old_u, layer, None, &mut bufs.raw_nbrs);
            let any_deleted = bufs
                .raw_nbrs
                .iter()
                .any(|&old_v| self.to_new[old_v as usize] == NO_NODE);
            if any_deleted {
                out.healed_lists += 1;
                let at = ListAt {
                    old_u,
                    new_u,
                    layer,
                };
                self.heal_list(at, &mut bufs, scratch, &mut out.bridges);
                continue;
            }
            bufs.cands.clear();
            for &old_v in &bufs.raw_nbrs {
                let mapped = self.to_new[old_v as usize];
                if mapped != new_u {
                    bufs.cands.push(mapped);
                }
            }
            self.builder.write_links(new_u, layer, &bufs.cands, None);
        }
        bufs.restore(scratch);
        out
    }

    /// Heal the list `bufs.raw_nbrs` read at `at`: keep every surviving
    /// 1-hop donor edge, then refill the vacated slots with
    /// direction-diversified bridges across the deleted neighbors, recording
    /// each new edge in `bridges` for its reciprocal back-link.
    fn heal_list(
        &self,
        at: ListAt,
        bufs: &mut HealBuffers,
        scratch: &mut Scratch,
        bridges: &mut Vec<BridgeLink>,
    ) {
        let out_view = self.builder.view();
        let cap = out_view.cap(at.layer);
        let orig_deg = bufs.raw_nbrs.len().min(cap);
        self.gather_candidates(at, cap, bufs, scratch);
        if bufs.cands.len() <= bufs.surv_1hop.len() {
            self.builder
                .write_links(at.new_u, at.layer, &bufs.surv_1hop, None);
            return;
        }
        let HealBuffers {
            cands,
            pool,
            sel,
            surv_1hop,
            healed,
            ..
        } = bufs;
        let row_u = out_view.row(at.new_u);
        pool.clear();
        pool.reserve(cands.len());
        out_view.for_each_scored(row_u, cands, |v, score| {
            pool.push(Scored { score, node: v });
        });
        pool.sort_unstable_by(|a, b| b.cmp(a));
        out_view.select(out_view.inv_norm[at.new_u as usize], pool, cap, sel);

        // Keep every surviving 1-hop donor edge (with the cluster and
        // duplicate links `connect_back` gave it), then fill vacated slots
        // first from the direction-diversified `sel` bridges and finally from
        // the top-scoring remaining 2-hop bridges up to `orig_deg`.
        healed.clear();
        healed.extend_from_slice(surv_1hop);
        let mut bridge_to = |b: u32, limit: usize| {
            if healed.len() < limit && !healed.contains(&b) {
                healed.push(b);
                bridges.push(BridgeLink {
                    target: b,
                    source: at.new_u,
                    layer: at.layer as u8,
                });
            }
        };
        sel.iter().for_each(|&b| bridge_to(b, cap));
        pool.iter().for_each(|item| bridge_to(item.node, orig_deg));
        self.builder.write_links(at.new_u, at.layer, healed, None);
    }

    /// Fill `bufs.surv_1hop` with the surviving neighbors of the list at `at`
    /// and `bufs.cands` with them plus the surviving neighbors of its deleted
    /// ones, expanding a bounded second deleted hop when heavy deletions
    /// leave fewer than `2 · cap` candidates.
    fn gather_candidates(
        &self,
        at: ListAt,
        cap: usize,
        bufs: &mut HealBuffers,
        scratch: &mut Scratch,
    ) {
        let HealBuffers {
            raw_nbrs,
            bridge_nbrs,
            cands,
            deleted_queue,
            surv_1hop,
            ..
        } = bufs;
        surv_1hop.clear();
        cands.clear();
        deleted_queue.clear();
        scratch.begin(self.builder.inv_norm.len());
        scratch.visit(at.new_u);
        for &old_v in raw_nbrs.iter() {
            let mapped = self.to_new[old_v as usize];
            if mapped != NO_NODE {
                if scratch.visit(mapped) {
                    surv_1hop.push(mapped);
                    cands.push(mapped);
                }
            } else if deleted_queue.len() < MAX_DELETED_BRIDGE_EXPANDS {
                deleted_queue.push(old_v);
            }
        }
        let first_wave = deleted_queue.len();
        let mut d_idx = 0usize;
        while d_idx < deleted_queue.len() {
            if d_idx >= first_wave && cands.len() >= cap * 2 {
                break;
            }
            let del_node = deleted_queue[d_idx];
            d_idx += 1;
            self.donor_view
                .copy_links(del_node, at.layer, None, bridge_nbrs);
            for &old_w in bridge_nbrs.iter() {
                if old_w == at.old_u {
                    continue;
                }
                let mapped = self.to_new[old_w as usize];
                if mapped != NO_NODE {
                    if usize::from(self.builder.links.level[mapped as usize]) >= at.layer
                        && scratch.visit(mapped)
                    {
                        cands.push(mapped);
                    }
                } else if d_idx <= first_wave
                    && deleted_queue.len() < MAX_DELETED_BRIDGE_EXPANDS
                    && !deleted_queue.contains(&old_w)
                {
                    deleted_queue.push(old_w);
                }
            }
        }
    }
}

/// Remap surviving donor edges across all layers `0..=level[new_u]`, preserving
/// all surviving 1-hop donor edges while refilling vacated tombstone slots with
/// heuristic-diversified 2-hop bridges and reciprocal back-links. Returns the
/// number of lists healed.
pub(super) fn transplant_and_heal_donor<L: LinkCells + ?Sized>(
    donor_view: &View<'_, L>,
    builder: &LinkBuilder<'_>,
    donor: &DonorMap<'_>,
    parallel: bool,
) -> usize {
    let healer = DonorHealer {
        donor_view,
        builder,
        to_new: donor.to_new,
    };
    let parts: Vec<HealNodeOut> = if parallel {
        donor
            .pairs
            .par_iter()
            .map(|&pair| with_scratch(|scratch| healer.heal_node(pair, scratch)))
            .collect()
    } else {
        with_scratch(|scratch| {
            donor
                .pairs
                .iter()
                .map(|&pair| healer.heal_node(pair, scratch))
                .collect()
        })
    };
    let mut total_healed = 0usize;
    let mut bridge_batches: Vec<Vec<BridgeLink>> = Vec::new();
    for part in parts {
        total_healed += part.healed_lists;
        if !part.bridges.is_empty() {
            bridge_batches.push(part.bridges);
        }
    }
    connect_bridges(builder, &bridge_batches, parallel);
    total_healed
}

/// Wire the reciprocal back-link of every healed bridge.
fn connect_bridges(builder: &LinkBuilder<'_>, batches: &[Vec<BridgeLink>], parallel: bool) {
    if batches.is_empty() {
        return;
    }
    let connect = |batch: &Vec<BridgeLink>, locks: Option<&Locks>, scratch: &mut Scratch| {
        let mut pool = std::mem::take(&mut scratch.pool);
        for &edge in batch {
            let layer = usize::from(edge.layer);
            builder.connect_back(edge.target, edge.source, layer, locks, &mut pool, scratch);
        }
        scratch.pool = pool;
    };
    if parallel {
        let locks = Locks::default();
        batches
            .par_iter()
            .for_each(|batch| with_scratch(|scratch| connect(batch, Some(&locks), scratch)));
    } else {
        with_scratch(|scratch| {
            for batch in batches {
                connect(batch, None, scratch);
            }
        });
    }
}
