//! Slot bitmaps for the shard-side Boolean planner's recursive boolean
//! execution.
//!
//! One bit per local slot, packed 64 to a word. The set algebra a
//! `BooleanQuery` needs is the word-wise AND, OR, AND NOT, and the
//! "at least t of these" count for `minimum_should_match`. A dense
//! clause is a bitmap like the others: the rows that hold a vector
//! ([`Bits::ranges`] over the provider's vector rows), so a document
//! without a vector and a row without a document take part in MUST,
//! SHOULD, and MUST_NOT under one rule.

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

    /// The slots inside the `(base, rows)` ranges, within `len`.
    pub fn ranges(len: usize, ranges: &[(usize, usize)]) -> Self {
        let mut bits = Self::empty(len);
        for &(base, rows) in ranges {
            for slot in base.min(len)..base.saturating_add(rows).min(len) {
                bits.set(slot);
            }
        }
        bits
    }

    /// From packed words: slot `i` is bit `i % 64` of word `i / 64`. Words
    /// are added or dropped to cover `len`, and bits at or past it cleared.
    pub fn from_words(mut words: Vec<u64>, len: usize) -> Self {
        words.resize(len.div_ceil(64), 0);
        let mut bits = Self { words, len };
        bits.trim();
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

    /// Set bits in `[start, end)`.
    pub fn count_range(&self, start: usize, end: usize) -> usize {
        let end = end.min(self.len);
        if start >= end {
            return 0;
        }
        let (first, last) = (start / 64, (end - 1) / 64);
        let mut count = 0usize;
        for wi in first..=last {
            let mut word = self.words[wi];
            if wi == first {
                word &= u64::MAX << (start % 64);
            }
            if wi == last && !end.is_multiple_of(64) {
                word &= (1u64 << (end % 64)) - 1;
            }
            count += word.count_ones() as usize;
        }
        count
    }

    /// The set slots in `[start, end)`, ascending, word-wise: a span
    /// covering no set bit costs its words, not its slots.
    pub fn iter_range(&self, start: usize, end: usize) -> impl Iterator<Item = usize> + '_ {
        let end = end.min(self.len);
        let start = start.min(end);
        let last_word = end.saturating_sub(1) / 64;
        let mask_for = move |wi: usize| {
            let mut mask = u64::MAX;
            if wi == start / 64 {
                mask &= u64::MAX << (start % 64);
            }
            if wi == last_word && !end.is_multiple_of(64) {
                mask &= (1u64 << (end % 64)) - 1;
            }
            mask
        };
        let mut word_index = start / 64;
        let mut held = if start < end {
            self.words[word_index] & mask_for(word_index)
        } else {
            0
        };
        std::iter::from_fn(move || loop {
            if held != 0 {
                let bit = held.trailing_zeros() as usize;
                held &= held - 1;
                return Some(word_index * 64 + bit);
            }
            if word_index >= last_word {
                return None;
            }
            word_index += 1;
            held = self.words[word_index] & mask_for(word_index);
        })
    }

    /// OR in a bitmap of `rows` slots packed in `words`, its slot 0 landing
    /// at `base`: a part's bitmap into the shard's. Slots landing at or
    /// past the length are dropped.
    pub fn or_words_at(&mut self, words: &[u64], rows: usize, base: usize) {
        let rows = rows
            .min(self.len.saturating_sub(base))
            .min(words.len() * 64);
        if rows == 0 {
            return;
        }
        let (word, shift) = (base / 64, base % 64);
        let whole = rows / 64;
        let tail = rows % 64;
        let piece = |i: usize| {
            if i < whole {
                words[i]
            } else {
                words[i] & ((1u64 << tail) - 1)
            }
        };
        for i in 0..whole + usize::from(tail != 0) {
            let bits = piece(i);
            self.words[word + i] |= bits << shift;
            if shift != 0 && bits >> (64 - shift) != 0 {
                self.words[word + i + 1] |= bits >> (64 - shift);
            }
        }
    }

    /// Clear the slots of `[start, end)`.
    pub fn clear_range(&mut self, start: usize, end: usize) {
        let end = end.min(self.len);
        let mut slot = start.min(end);
        while slot < end {
            if slot.is_multiple_of(64) && slot + 64 <= end {
                self.words[slot / 64] = 0;
                slot += 64;
            } else {
                self.clear(slot);
                slot += 1;
            }
        }
    }

    /// Clear every set slot `keep` turns down.
    pub fn retain(&mut self, mut keep: impl FnMut(usize) -> bool) {
        for wi in 0..self.words.len() {
            let mut held = self.words[wi];
            while held != 0 {
                let bit = held.trailing_zeros() as usize;
                held &= held - 1;
                if !keep(wi * 64 + bit) {
                    self.words[wi] &= !(1u64 << bit);
                }
            }
        }
    }

    /// The slots of `[start, end)` as their own bitmap, slot `start` at 0.
    pub fn slice(&self, start: usize, end: usize) -> Bits {
        let end = end.min(self.len);
        let start = start.min(end);
        let mut out = Bits::empty(end - start);
        for slot in self.iter_range(start, end) {
            out.set(slot - start);
        }
        out
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

/// The Boolean group rule: MUST intersects the required
/// clauses; with none, the live set is the seed when the SHOULD minimum
/// is zero and otherwise the SHOULD count decides; on a seeded set the
/// SHOULD minimum is then enforced; MUST_NOT subtracts. `live` is the
/// live-slot bitmap. Returns the members and whether the live set was
/// consulted as the seed (the caller counts its segments the way the
/// live-document bitmap route did).
pub fn group_members(
    must: &[Bits],
    should: &[Bits],
    must_not: &[Bits],
    minimum_should_match: usize,
    live: &Bits,
) -> (Bits, bool) {
    let len = live.len();
    let should_refs: Vec<&Bits> = should.iter().collect();
    let mut seeded_live = false;
    let mut members = if let Some((first, rest)) = must.split_first() {
        let mut out = first.clone();
        for m in rest {
            out.and_with(m);
        }
        out
    } else if minimum_should_match == 0 {
        seeded_live = true;
        live.clone()
    } else {
        Bits::at_least(&should_refs, minimum_should_match, len)
    };
    if !must.is_empty() && minimum_should_match > 0 {
        members.and_with(&Bits::at_least(&should_refs, minimum_should_match, len));
    }
    for m in must_not {
        members.and_not(m);
    }
    (members, seeded_live)
}

#[cfg(test)]
mod tests;
