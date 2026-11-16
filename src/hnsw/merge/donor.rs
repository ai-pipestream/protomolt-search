//! The donor's remap into the output, its entry point, and the highways a
//! deleted donor entry leaves to its successor.

use std::cmp::Reverse;

use super::super::graph::{with_scratch, LinkBuilder, LinkCells, View, NO_NODE};
use super::super::{Backing, HnswIndex};

/// The donor's surviving rows in the output's node numbering.
#[derive(Clone, Copy)]
pub(super) struct DonorMap<'a> {
    /// `to_new[old_u]` is the output node of donor node `old_u`, `NO_NODE`
    /// when the row was deleted.
    pub(super) to_new: &'a [u32],
    /// `(old_u, new_u)` for every surviving donor row, in output order.
    pub(super) pairs: &'a [(u32, u32)],
}

/// The output node the merged graph is entered from once the donor is
/// transplanted.
pub(super) fn select_donor_entry<L: LinkCells + ?Sized>(
    out_view: &View<'_, L>,
    donor_index: &HnswIndex,
    donor: &DonorMap<'_>,
) -> u32 {
    let (donor_to_new, donor_pairs) = (donor.to_new, donor.pairs);
    let max_donor_lvl = donor_pairs
        .iter()
        .map(|&(_, new_u)| out_view.level[new_u as usize])
        .max()
        .unwrap_or(0);

    // Keep the donor's original entry when it survived and still sits at the
    // highest surviving donor level.
    let orig_donor_entry = match &donor_index.backing {
        Backing::Heap(st) => st.entry_state().0,
        Backing::Mapped(mp) => mp.header().entry,
    };
    if (orig_donor_entry as usize) < donor_to_new.len() {
        let mapped = donor_to_new[orig_donor_entry as usize];
        if mapped != NO_NODE && out_view.level[mapped as usize] == max_donor_lvl {
            return mapped;
        }
    }

    // Otherwise promote the surviving donor node at `max_donor_lvl` with the
    // highest surviving layer-0 degree (ties broken by lowest `new_u`).
    donor_pairs
        .iter()
        .map(|&(_, new_u)| new_u)
        .filter(|&new_u| out_view.level[new_u as usize] == max_donor_lvl)
        .max_by_key(|&new_u| {
            let start = out_view.list_start(new_u, 0);
            let deg = out_view.links0.get(start);
            (deg, Reverse(new_u))
        })
        .expect("donor_pairs is non-empty")
}

/// When the donor's original `entry` was deleted, wire reciprocal highways
/// between the newly promoted `initial_entry` and the deleted entry's surviving
/// neighbors across layers `0..=min(initial_top, orig_top)`.
pub(super) fn inherit_deleted_entry_highways<L: LinkCells + ?Sized>(
    donor_view: &View<'_, L>,
    builder: &LinkBuilder<'_>,
    donor_to_new: &[u32],
    initial_entry: u32,
) {
    let orig_entry = donor_view.entry;
    if orig_entry == NO_NODE || (orig_entry as usize) >= donor_to_new.len() {
        return;
    }
    if donor_to_new[orig_entry as usize] != NO_NODE {
        return;
    }
    let initial_top = usize::from(builder.links.level[initial_entry as usize]);
    let max_layer = initial_top.min(usize::from(donor_view.top));
    with_scratch(|scratch| {
        let mut raw = Vec::new();
        let mut hop2 = Vec::new();
        let mut pool = Vec::new();
        for layer in 0..=max_layer {
            donor_view.copy_links(orig_entry, layer, None, &mut raw);
            let mut surv = Vec::new();
            for &old_v in &raw {
                let mapped = donor_to_new[old_v as usize];
                if mapped != NO_NODE && mapped != initial_entry {
                    if usize::from(builder.links.level[mapped as usize]) >= layer
                        && !surv.contains(&mapped)
                    {
                        surv.push(mapped);
                    }
                } else if mapped == NO_NODE {
                    donor_view.copy_links(old_v, layer, None, &mut hop2);
                    for &old_w in &hop2 {
                        let m2 = donor_to_new[old_w as usize];
                        if m2 != NO_NODE
                            && m2 != initial_entry
                            && usize::from(builder.links.level[m2 as usize]) >= layer
                            && !surv.contains(&m2)
                        {
                            surv.push(m2);
                        }
                    }
                }
            }
            for &v in &surv {
                builder.connect_back(initial_entry, v, layer, None, &mut pool, scratch);
                builder.connect_back(v, initial_entry, layer, None, &mut pool, scratch);
            }
        }
    });
}
