//! Slot bitmaps for the shard-side Boolean planner (`docs/query-api.md`,
//! "Recursive boolean execution").
//!
//! One bit per local slot, packed 64 to a word. The set algebra a
//! `BooleanQuery` needs is the word-wise AND, OR, AND NOT, and the
//! "at least t of these" count for `minimum_should_match`. A dense
//! clause is the universe and is not a bitmap: [`Membership`] tells
//! the two apart so a universal clause costs no words and the group
//! rules that mention it (`docs/query-api.md`) apply by name.

/// One bit per slot in `[0, len)`, packed 64 to a `u64` word. Bits at
/// or past `len` are clear and stay clear through every operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bits {
    words: Vec<u64>,
    len: usize,
}

impl Bits {
    /// All clear.
    pub fn empty(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
            len,
        }
    }

    /// All set below `len`.
    pub fn full(len: usize) -> Self {
        let mut bits = Self {
            words: vec![u64::MAX; len.div_ceil(64)],
            len,
        };
        bits.trim();
        bits
    }

    /// The slots below `rows`, within `len`.
    pub fn prefix(len: usize, rows: usize) -> Self {
        let mut bits = Self::full(len);
        for slot in rows.min(len)..len {
            bits.clear(slot);
        }
        bits
    }

    /// From a per-slot admission list.
    pub fn from_bools(allow: &[bool]) -> Self {
        let mut bits = Self::empty(allow.len());
        for (slot, &admitted) in allow.iter().enumerate() {
            if admitted {
                bits.set(slot);
            }
        }
        bits
    }

    /// From tombstone words (a set bit is a deleted slot): the live
    /// slots below `len`.
    pub fn live(deleted: Option<&[u64]>, len: usize) -> Self {
        let mut bits = Self::full(len);
        if let Some(words) = deleted {
            for (word, tomb) in bits.words.iter_mut().zip(words) {
                *word &= !tomb;
            }
        }
        bits
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    #[inline]
    pub fn test(&self, slot: usize) -> bool {
        slot < self.len && self.words[slot / 64] & (1u64 << (slot % 64)) != 0
    }

    #[inline]
    pub fn set(&mut self, slot: usize) {
        debug_assert!(slot < self.len);
        self.words[slot / 64] |= 1u64 << (slot % 64);
    }

    #[inline]
    pub fn clear(&mut self, slot: usize) {
        debug_assert!(slot < self.len);
        self.words[slot / 64] &= !(1u64 << (slot % 64));
    }

    /// Set bits.
    pub fn count(&self) -> u64 {
        self.words.iter().map(|w| u64::from(w.count_ones())).sum()
    }

    pub fn and_with(&mut self, other: &Bits) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= *b;
        }
    }

    pub fn or_with(&mut self, other: &Bits) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a |= *b;
        }
    }

    pub fn and_not(&mut self, other: &Bits) {
        debug_assert_eq!(self.len, other.len);
        for (a, b) in self.words.iter_mut().zip(&other.words) {
            *a &= !*b;
        }
    }

    pub fn clear_all(&mut self) {
        self.words.iter_mut().for_each(|w| *w = 0);
    }

    /// The set slots, ascending.
    pub fn iter(&self) -> impl Iterator<Item = usize> + '_ {
        self.words.iter().enumerate().flat_map(|(wi, &word)| {
            let mut held = word;
            std::iter::from_fn(move || {
                if held == 0 {
                    return None;
                }
                let bit = held.trailing_zeros() as usize;
                held &= held - 1;
                Some(wi * 64 + bit)
            })
        })
    }

    /// A per-slot admission list, the shape the vector kernel masks with.
    pub fn to_bools(&self) -> Vec<bool> {
        (0..self.len).map(|slot| self.test(slot)).collect()
    }

    /// The slots where at least `minimum` of `sets` hold the bit.
    /// `minimum == 0` is every slot below `len`; a minimum past the
    /// number of sets is no slot.
    pub fn at_least(sets: &[&Bits], minimum: usize, len: usize) -> Bits {
        if minimum == 0 {
            return Bits::full(len);
        }
        if minimum > sets.len() {
            return Bits::empty(len);
        }
        if minimum == 1 {
            let mut out = Bits::empty(len);
            for set in sets {
                out.or_with(set);
            }
            return out;
        }
        if minimum == sets.len() {
            let mut out = Bits::full(len);
            for set in sets {
                out.and_with(set);
            }
            return out;
        }
        // The general count: bit-sliced counters, one plane per bit of
        // the count, saturating at the number of sets.
        let planes = usize::BITS - sets.len().leading_zeros();
        let words = len.div_ceil(64);
        let mut counter: Vec<Vec<u64>> = (0..planes).map(|_| vec![0u64; words]).collect();
        for set in sets {
            for wi in 0..words {
                let mut carry = set.words[wi];
                for plane in counter.iter_mut() {
                    let sum = plane[wi] ^ carry;
                    carry &= plane[wi];
                    plane[wi] = sum;
                }
            }
        }
        let mut out = Bits::empty(len);
        for wi in 0..words {
            let mut admitted = 0u64;
            for bit in 0..64 {
                let mut count = 0usize;
                for (p, plane) in counter.iter().enumerate() {
                    if plane[wi] & (1u64 << bit) != 0 {
                        count |= 1 << p;
                    }
                }
                if count >= minimum {
                    admitted |= 1u64 << bit;
                }
            }
            out.words[wi] = admitted;
        }
        out.trim();
        out
    }

    fn trim(&mut self) {
        let used = self.len % 64;
        if used != 0 {
            if let Some(last) = self.words.last_mut() {
                *last &= (1u64 << used) - 1;
            }
        }
    }
}

/// A clause's membership on one shard: a bitmap, or the universe (a
/// dense clause, which every row with a vector matches).
#[derive(Debug, Clone)]
pub enum Membership {
    Universal,
    Bits(Bits),
}

impl Membership {
    pub fn is_universal(&self) -> bool {
        matches!(self, Membership::Universal)
    }

    /// Whether `slot` is a member; the universe holds every slot.
    pub fn test(&self, slot: usize) -> bool {
        match self {
            Membership::Universal => true,
            Membership::Bits(bits) => bits.test(slot),
        }
    }
}

/// The group rule of `docs/query-api.md`: MUST intersects the
/// non-universal required clauses; with none, the live set is the
/// seed when a MUST exists, when the minimum is zero, or when the
/// universal SHOULD clauses already meet it, and otherwise the SHOULD
/// count decides; then the SHOULD minimum is enforced on a seeded set,
/// and MUST_NOT subtracts (a universal MUST_NOT empties the group).
/// `live` is the live-slot bitmap. Returns the members and whether the
/// live set was consulted as the seed (the caller counts its segments
/// the way the live-document bitmap route did).
pub fn group_members(
    must: &[Membership],
    should: &[Membership],
    must_not: &[Membership],
    minimum_should_match: usize,
    live: &Bits,
) -> (Bits, bool) {
    let len = live.len();
    let universal_should = should.iter().filter(|m| m.is_universal()).count();
    let concrete_should: Vec<&Bits> = should
        .iter()
        .filter_map(|m| match m {
            Membership::Bits(b) => Some(b),
            Membership::Universal => None,
        })
        .collect();
    let mut seeded_live = false;
    let mut members = if must.iter().any(|m| !m.is_universal()) {
        let mut out = Bits::full(len);
        for m in must {
            if let Membership::Bits(b) = m {
                out.and_with(b);
            }
        }
        out
    } else if !must.is_empty()
        || minimum_should_match == 0
        || universal_should >= minimum_should_match
    {
        seeded_live = true;
        live.clone()
    } else {
        Bits::at_least(
            &concrete_should,
            minimum_should_match - universal_should,
            len,
        )
    };
    if !must.is_empty() && minimum_should_match > universal_should {
        let admitted = Bits::at_least(
            &concrete_should,
            minimum_should_match - universal_should,
            len,
        );
        members.and_with(&admitted);
    }
    for m in must_not {
        match m {
            Membership::Universal => members.clear_all(),
            Membership::Bits(b) => members.and_not(b),
        }
    }
    (members, seeded_live)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn of(len: usize, slots: &[usize]) -> Bits {
        let mut b = Bits::empty(len);
        for &s in slots {
            b.set(s);
        }
        b
    }

    #[test]
    fn full_and_live_trim_past_the_length() {
        let full = Bits::full(70);
        assert_eq!(full.count(), 70);
        assert!(!full.test(70));
        let live = Bits::live(Some(&[1 << 3, 1 << 5]), 70);
        assert_eq!(live.count(), 68);
        assert!(!live.test(3) && !live.test(69));
        assert!(live.test(4) && live.test(64));
        assert_eq!(live.iter().take(4).collect::<Vec<_>>(), vec![0, 1, 2, 4]);
    }

    #[test]
    fn at_least_counts_across_planes() {
        let a = of(130, &[1, 2, 3, 64, 129]);
        let b = of(130, &[2, 3, 64, 100]);
        let c = of(130, &[3, 64, 129]);
        let d = of(130, &[3, 5]);
        let sets = [&a, &b, &c, &d];
        let two = Bits::at_least(&sets, 2, 130);
        assert_eq!(two.iter().collect::<Vec<_>>(), vec![2, 3, 64, 129]);
        let three = Bits::at_least(&sets, 3, 130);
        assert_eq!(three.iter().collect::<Vec<_>>(), vec![3, 64]);
        let four = Bits::at_least(&sets, 4, 130);
        assert_eq!(four.iter().collect::<Vec<_>>(), vec![3]);
        assert_eq!(Bits::at_least(&sets, 5, 130).count(), 0);
        assert_eq!(Bits::at_least(&sets, 0, 130).count(), 130);
        assert_eq!(
            Bits::at_least(&sets, 1, 130).iter().collect::<Vec<_>>(),
            vec![1, 2, 3, 5, 64, 100, 129]
        );
    }

    #[test]
    fn the_group_rule_follows_the_planner() {
        let live = Bits::live(Some(&[1 << 9]), 12);
        let a = Membership::Bits(of(12, &[1, 2, 3, 9]));
        let b = Membership::Bits(of(12, &[2, 3, 4]));
        // MUST intersects; the universe is not a member set.
        let (m, seeded) = group_members(
            &[a.clone(), Membership::Universal, b.clone()],
            &[],
            &[],
            0,
            &live,
        );
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![2, 3]);
        assert!(!seeded);
        // Only universal MUSTs: the live seed.
        let (m, seeded) = group_members(&[Membership::Universal], &[], &[], 0, &live);
        assert_eq!(m.count(), 11);
        assert!(seeded);
        // SHOULD count with a universal SHOULD counting for every slot.
        let (m, seeded) =
            group_members(&[], &[a.clone(), b.clone(), Membership::Universal], &[], 2, &live);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4, 9]);
        assert!(!seeded);
        let (m, _) = group_members(&[], &[a.clone(), b.clone()], &[], 2, &live);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![2, 3]);
        // MUST plus a SHOULD minimum, then MUST_NOT.
        let (m, _) = group_members(
            &[a.clone()],
            &[b.clone()],
            &[Membership::Bits(of(12, &[3]))],
            1,
            &live,
        );
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![2]);
        let (m, _) = group_members(&[a], &[], &[Membership::Universal], 0, &live);
        assert_eq!(m.count(), 0);
    }
}
