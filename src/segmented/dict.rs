//! Global dictionaries over the parts' local ordinals.

use super::*;

/// One column's global dictionary over the sealed parts and the tail.
#[derive(Debug, Default)]
pub(super) struct UnionDict {
    /// Values in global ordinal order: the byte-sorted union of the
    /// sealed parts first, then the tail's values that none of them had,
    /// in the order the tail met them.
    pub(super) values: Vec<String>,
    /// How many leading `values` are the sorted sealed union.
    pub(super) sorted_len: usize,
    pub(super) index: HashMap<String, u32>,
    /// Per sealed part: local ordinal → global ordinal.
    pub(super) remaps: Vec<Vec<u32>>,
    /// The frozen part's local ordinal → global ordinal: the tail remap
    /// as it stood when a seal froze that tail.
    pub(super) frozen_remap: Vec<u32>,
    /// Tail local ordinal → global ordinal, grown by `sync_tail`.
    pub(super) tail_remap: Vec<u32>,
}

/// Which part of the shard a local ordinal or document id belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Source {
    Sealed(usize),
    Frozen,
    Tail,
}

impl UnionDict {
    pub(super) fn build(parts: &[&[String]]) -> Self {
        let mut values: Vec<String> = parts.iter().flat_map(|d| d.iter().cloned()).collect();
        values.sort_unstable();
        values.dedup();
        let index: HashMap<String, u32> = values
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i as u32))
            .collect();
        let remaps = parts
            .iter()
            .map(|d| d.iter().map(|v| index[v]).collect())
            .collect();
        UnionDict {
            sorted_len: values.len(),
            values,
            index,
            remaps,
            frozen_remap: Vec::new(),
            tail_remap: Vec::new(),
        }
    }

    /// The tail is now the frozen part: its remap moves over whole and a
    /// fresh tail starts with none.
    pub(super) fn freeze_tail(&mut self) {
        self.frozen_remap = std::mem::take(&mut self.tail_remap);
    }

    /// Extend the tail remap for tail ordinals seen since the last sync.
    pub(super) fn sync_tail(&mut self, tail_values: &[String]) {
        for value in &tail_values[self.tail_remap.len()..] {
            let ord = match self.index.get(value) {
                Some(&ord) => ord,
                None => {
                    let ord = self.values.len() as u32;
                    self.values.push(value.clone());
                    self.index.insert(value.clone(), ord);
                    ord
                }
            };
            self.tail_remap.push(ord);
        }
    }

    pub(super) fn sorted(&self) -> bool {
        self.values.len() == self.sorted_len
    }

    pub(super) fn ord_of(&self, value: &str) -> Option<u32> {
        self.index.get(value).copied()
    }

    pub(super) fn to_global(&self, source: Source, local: u32) -> Option<u32> {
        let remap = match source {
            Source::Sealed(i) => &self.remaps[i],
            Source::Frozen => &self.frozen_remap,
            Source::Tail => &self.tail_remap,
        };
        remap.get(local as usize).copied()
    }
}

/// Per part: the reverse key maps a routed read needs, cached so a
/// per-document lookup is an index, not a search.
#[derive(Debug, Default)]
pub(super) struct KeyReverse {
    /// Per part (sealed parts, then the frozen part at `parts`, then the
    /// tail at `parts + 1`): global key ordinal → local key ordinal,
    /// `u32::MAX` when absent.
    pub(super) per_part: Vec<Vec<u32>>,
}

impl KeyReverse {
    pub(super) fn build(dict: &UnionDict, frozen_keys: usize, tail_keys: usize) -> Self {
        let n = dict.values.len();
        let reverse_of = |remap: &[u32], keys: usize| {
            let mut reverse = vec![u32::MAX; n];
            for (local, &global) in remap.iter().enumerate().take(keys) {
                reverse[global as usize] = local as u32;
            }
            reverse
        };
        let mut per_part: Vec<Vec<u32>> = dict
            .remaps
            .iter()
            .map(|remap| reverse_of(remap, remap.len()))
            .collect();
        per_part.push(reverse_of(&dict.frozen_remap, frozen_keys));
        per_part.push(reverse_of(&dict.tail_remap, tail_keys));
        KeyReverse { per_part }
    }
}
