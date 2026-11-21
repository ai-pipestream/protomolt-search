//! Compact per-shard membership bitmaps for allocation-free popcount and
//! intersection counting without materializing document ID sets.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};

use tonic::Status;

use super::MembershipSet;
use crate::stats_identity::StatsClaim;

static MEMBERSHIP_ID_DECODES: AtomicU64 = AtomicU64::new(0);

/// Total calls that materialized a `BTreeSet<u64>` from wire membership bitmaps.
#[doc(hidden)]
pub fn membership_id_decodes() -> u64 {
    MEMBERSHIP_ID_DECODES.load(Ordering::Relaxed)
}

/// One validated non-empty shard bitmap in wire LSB-first packing.
#[derive(Debug, Clone)]
pub(crate) struct ShardBitmap {
    pub(crate) base_label: u64,
    pub(crate) label_count: u64,
    pub(crate) bits: Vec<u8>,
}

/// Validated disjoint shard bitmaps gathered across one distributed read.
#[derive(Debug, Clone, Default)]
pub(crate) struct CompactMembershipBitmaps {
    pub(crate) shards: Vec<ShardBitmap>,
    pub(crate) epochs: Vec<StatsClaim>,
    pub(crate) wire_bytes: u64,
    pub(crate) prune: crate::segment_prune::PruneStats,
}

impl CompactMembershipBitmaps {
    pub(crate) fn push_validated(
        &mut self,
        response: crate::pb::MembershipBitmapResponse,
    ) -> Result<(), Status> {
        let _label_count = usize::try_from(response.label_count).map_err(|_| {
            Status::resource_exhausted("membership label count does not fit this platform")
        })?;
        let expected = usize::try_from(response.label_count.div_ceil(8))
            .map_err(|_| Status::resource_exhausted("membership bitmap does not fit usize"))?;
        if response.bits.len() != expected {
            return Err(Status::internal(format!(
                "membership bitmap has {} bytes for {} labels; expected {expected}",
                response.bits.len(),
                response.label_count
            )));
        }
        let end = response
            .base_label
            .checked_add(response.label_count)
            .ok_or_else(|| Status::internal("membership bitmap label range overflows u64"))?;
        if !response.label_count.is_multiple_of(8)
            && response.bits.last().is_some_and(|last| {
                let used = (response.label_count % 8) as u8;
                *last & !((1u8 << used) - 1) != 0
            })
        {
            return Err(Status::internal(
                "membership bitmap sets padding bits beyond its label count",
            ));
        }
        self.wire_bytes = self
            .wire_bytes
            .checked_add(response.bits.len() as u64)
            .ok_or_else(|| Status::resource_exhausted("membership byte count overflow"))?;
        if response.label_count > 0 {
            if self.shards.iter().any(|held| {
                let held_end = held.base_label + held.label_count;
                response.base_label < held_end && held.base_label < end
            }) {
                return Err(Status::failed_precondition(format!(
                    "membership label range [{}, {end}) overlaps another shard",
                    response.base_label
                )));
            }
            self.shards.push(ShardBitmap {
                base_label: response.base_label,
                label_count: response.label_count,
                bits: response.bits,
            });
        }
        Ok(())
    }

    /// Count set bits across all disjoint shard bitmaps without decoding IDs.
    pub(crate) fn count_ones(&self) -> u64 {
        let mut total = 0u64;
        for shard in &self.shards {
            let (words, rem) = shard.bits.as_chunks::<8>();
            for chunk in words {
                total += u64::from(u64::from_le_bytes(*chunk).count_ones());
            }
            for &byte in rem {
                total += u64::from(byte.count_ones());
            }
        }
        total
    }

    /// Count `|self ∩ other|` across disjoint shard intervals word-by-word.
    pub(crate) fn count_intersection(&self, other: &Self) -> u64 {
        if self.shards.is_empty() || other.shards.is_empty() {
            return 0;
        }
        let mut left: Vec<&ShardBitmap> = self.shards.iter().collect();
        let mut right: Vec<&ShardBitmap> = other.shards.iter().collect();
        left.sort_unstable_by_key(|s| s.base_label);
        right.sort_unstable_by_key(|s| s.base_label);

        let (mut i, mut j, mut total) = (0usize, 0usize, 0u64);
        while i < left.len() && j < right.len() {
            let (a, b) = (left[i], right[j]);
            let end_a = a.base_label + a.label_count;
            let end_b = b.base_label + b.label_count;
            let start = a.base_label.max(b.base_label);
            let end = end_a.min(end_b);
            if start < end {
                let off_a = (start - a.base_label) as usize;
                let off_b = (start - b.base_label) as usize;
                total += count_overlap_and(&a.bits, off_a, &b.bits, off_b, (end - start) as usize);
            }
            if end_a <= end_b {
                i += 1;
            } else {
                j += 1;
            }
        }
        total
    }

    /// Materialize the `BTreeSet<u64>` when a caller requires explicit IDs.
    pub(crate) fn into_membership_set(self) -> MembershipSet {
        MEMBERSHIP_ID_DECODES.fetch_add(1, Ordering::Relaxed);
        let mut ids = BTreeSet::new();
        for shard in self.shards {
            let limit = shard.label_count as usize;
            for (byte_index, byte) in shard.bits.into_iter().enumerate() {
                let mut held = byte;
                while held != 0 {
                    let bit = held.trailing_zeros() as usize;
                    let pos = byte_index * 8 + bit;
                    if pos < limit {
                        ids.insert(shard.base_label + pos as u64);
                    }
                    held &= held - 1;
                }
            }
        }
        MembershipSet {
            ids,
            epochs: self.epochs,
            wire_bytes: self.wire_bytes,
            terms: Vec::new(),
            prune: self.prune,
        }
    }
}

fn read_bits_u64(bytes: &[u8], bit_offset: usize, take: usize) -> u64 {
    let byte_idx = bit_offset / 8;
    let bit_shift = bit_offset % 8;
    let word = if bit_shift == 0 {
        let avail = bytes.len().saturating_sub(byte_idx).min(8);
        let mut buf = [0u8; 8];
        buf[..avail].copy_from_slice(&bytes[byte_idx..byte_idx + avail]);
        u64::from_le_bytes(buf)
    } else {
        let avail = bytes.len().saturating_sub(byte_idx).min(9);
        let mut buf = [0u8; 9];
        buf[..avail].copy_from_slice(&bytes[byte_idx..byte_idx + avail]);
        let lo = u64::from_le_bytes(buf[..8].try_into().expect("8-byte prefix"));
        let hi = u64::from(buf[8]);
        (lo >> bit_shift) | (hi << (64 - bit_shift))
    };
    let mask = if take == 64 {
        u64::MAX
    } else {
        (1u64 << take) - 1
    };
    word & mask
}

fn count_overlap_and(a: &[u8], off_a: usize, b: &[u8], off_b: usize, len: usize) -> u64 {
    if off_a == 0 && off_b == 0 {
        let full_words = len / 64;
        let mut total = 0u64;
        for (ca, cb) in a
            .as_chunks::<8>()
            .0
            .iter()
            .zip(b.as_chunks::<8>().0.iter())
            .take(full_words)
        {
            let wa = u64::from_le_bytes(*ca);
            let wb = u64::from_le_bytes(*cb);
            total += u64::from((wa & wb).count_ones());
        }
        let rem = len - full_words * 64;
        if rem > 0 {
            let pos = full_words * 64;
            let wa = read_bits_u64(a, pos, rem);
            let wb = read_bits_u64(b, pos, rem);
            total += u64::from((wa & wb).count_ones());
        }
        return total;
    }
    let mut total = 0u64;
    let mut pos = 0usize;
    while pos < len {
        let take = (len - pos).min(64);
        let wa = read_bits_u64(a, off_a + pos, take);
        let wb = read_bits_u64(b, off_b + pos, take);
        total += u64::from((wa & wb).count_ones());
        pos += take;
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::MembershipBitmapResponse;

    fn pack_range(base: u64, count: u64, pred: impl Fn(u64) -> bool) -> MembershipBitmapResponse {
        let mut bits = vec![0u8; count.div_ceil(8) as usize];
        for pos in 0..count {
            if pred(base + pos) {
                bits[(pos / 8) as usize] |= 1u8 << (pos % 8);
            }
        }
        MembershipBitmapResponse {
            base_label: base,
            label_count: count,
            bits,
            ..Default::default()
        }
    }

    #[test]
    fn compact_bitmaps_match_id_set_across_unaligned_and_multi_shard_splits() {
        let mut vectors = CompactMembershipBitmaps::default();
        vectors
            .push_validated(pack_range(0, 137, |id| id.is_multiple_of(3) || id % 7 == 1))
            .unwrap();
        vectors
            .push_validated(pack_range(150, 293, |id| !id.is_multiple_of(5)))
            .unwrap();
        vectors
            .push_validated(pack_range(500, 71, |id| id.is_multiple_of(2)))
            .unwrap();

        let mut filters = CompactMembershipBitmaps::default();
        filters
            .push_validated(pack_range(11, 219, |id| {
                id % 2 == 1 || id.is_multiple_of(11)
            }))
            .unwrap();
        filters
            .push_validated(pack_range(230, 340, |id| id.is_multiple_of(4) || id == 570))
            .unwrap();

        let intersection = vectors.count_intersection(&filters);
        let vec_count = vectors.count_ones();
        let vec_ids = vectors.into_membership_set().ids;
        let flt_ids = filters.into_membership_set().ids;
        assert_eq!(vec_count, vec_ids.len() as u64);
        assert_eq!(intersection, vec_ids.intersection(&flt_ids).count() as u64);
    }

    #[test]
    fn large_bitmap_intersection_never_materializes_ids() {
        const LABELS: u64 = 8_000_000;
        let before = membership_id_decodes();
        let mut a = CompactMembershipBitmaps::default();
        a.push_validated(MembershipBitmapResponse {
            base_label: 0,
            label_count: LABELS,
            bits: vec![0b1010_0101; (LABELS / 8) as usize],
            ..Default::default()
        })
        .unwrap();
        let mut b = CompactMembershipBitmaps::default();
        b.push_validated(MembershipBitmapResponse {
            base_label: 0,
            label_count: LABELS,
            bits: vec![0b0011_1101; (LABELS / 8) as usize],
            ..Default::default()
        })
        .unwrap();
        assert_eq!(a.count_ones(), 4_000_000);
        // 0b1010_0101 & 0b0011_1101 == 0b0010_0101 (3 bits per byte * 1_000_000 bytes).
        assert_eq!(a.count_intersection(&b), 3_000_000);
        assert_eq!(membership_id_decodes(), before);
    }
}
