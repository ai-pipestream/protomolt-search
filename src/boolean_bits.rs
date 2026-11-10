//! Slot bitmaps for the shard-side Boolean planner (`docs/query-api.md`,
//! "Recursive boolean execution").
//!
//! One bit per local slot, packed 64 to a word. The set algebra a
//! `BooleanQuery` needs is the word-wise AND, OR, AND NOT, and the
//! "at least t of these" count for `minimum_should_match`. A dense
//! clause is a bitmap like the others: the rows that hold a vector
//! ([`Bits::ranges`] over the provider's vector rows), so a document
//! without a vector and a row without a document take part in MUST,
//! SHOULD, and MUST_NOT under one rule (`docs/query-api.md`).

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

/// The group rule of `docs/query-api.md`: MUST intersects the required
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
mod tests {
    use super::*;

    #[test]
    fn a_part_bitmap_lands_at_its_base_and_ranges_clear() {
        let part: Vec<bool> = (0..150).map(|i| i % 3 == 0 || i == 149).collect();
        let words = {
            let mut words = Bits::from_bools(&part).words;
            // Stray bits past the part's rows must not leak.
            *words.last_mut().unwrap() |= u64::MAX << (150 % 64);
            words
        };
        for (len, base) in [
            (400, 0),
            (400, 1),
            (400, 64),
            (400, 77),
            (200, 77),
            (100, 90),
            (64, 64),
        ] {
            let mut bits = Bits::empty(len);
            if len > 3 {
                bits.set(3);
            }
            bits.or_words_at(&words, 150, base);
            let want: Vec<usize> = (0..len)
                .filter(|&s| s == 3 || (s >= base && s - base < 150 && part[s - base]))
                .collect();
            assert_eq!(
                bits.iter().collect::<Vec<_>>(),
                want,
                "len {len} base {base}"
            );
        }
        let mut bits = Bits::full(300);
        bits.clear_range(5, 5);
        bits.clear_range(60, 200);
        bits.clear_range(290, 999);
        let want: Vec<usize> = (0..300)
            .filter(|s| !(60..200).contains(s) && *s < 290)
            .collect();
        assert_eq!(bits.iter().collect::<Vec<_>>(), want);
        bits.retain(|slot| slot % 2 == 0);
        assert!(bits.iter().all(|slot| slot % 2 == 0));
        assert_eq!(
            bits.count() as usize,
            want.iter().filter(|s| *s % 2 == 0).count()
        );
    }

    #[test]
    fn slices_and_words_keep_the_slots() {
        let bools: Vec<bool> = (0..300).map(|i| i % 7 == 0 || i == 63 || i == 64).collect();
        let bits = Bits::from_bools(&bools);
        for (start, end) in [
            (0, 300),
            (0, 0),
            (1, 64),
            (63, 65),
            (64, 128),
            (130, 299),
            (290, 400),
        ] {
            let part = bits.slice(start, end);
            let end = end.min(300);
            assert_eq!(
                part.to_bools(),
                bools[start.min(end)..end],
                "{start}..{end}"
            );
        }
        // Stray bits past the length and a short or long word list.
        let words = vec![u64::MAX; 2];
        let made = Bits::from_words(words, 70);
        assert_eq!(made, Bits::full(70));
        assert_eq!(
            Bits::from_words(vec![1], 130).iter().collect::<Vec<_>>(),
            [0]
        );
        assert_eq!(
            Bits::from_words(vec![1, 2, 4], 64),
            Bits::from_words(vec![1], 64)
        );
    }

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
    fn count_range_is_the_count_of_iter_range() {
        let bits = Bits::from_bools(
            &(0..300)
                .map(|i| i % 7 == 0 || i % 64 == 63)
                .collect::<Vec<_>>(),
        );
        for (start, end) in [
            (0, 0),
            (0, 300),
            (5, 5),
            (63, 64),
            (63, 129),
            (64, 128),
            (1, 299),
            (290, 400),
            (400, 500),
        ] {
            assert_eq!(
                bits.count_range(start, end),
                bits.iter_range(start, end).count(),
                "{start}..{end}"
            );
        }
    }

    #[test]
    fn iter_range_walks_a_span_wordwise() {
        let bits = of(200, &[0, 3, 63, 64, 65, 129, 199]);
        assert_eq!(
            bits.iter_range(0, 200).collect::<Vec<_>>(),
            vec![0, 3, 63, 64, 65, 129, 199]
        );
        assert_eq!(
            bits.iter_range(60, 130).collect::<Vec<_>>(),
            vec![63, 64, 65, 129]
        );
        assert_eq!(
            bits.iter_range(4, 63).collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(
            bits.iter_range(64, 64).collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(bits.iter_range(199, 400).collect::<Vec<_>>(), vec![199]);
        assert_eq!(
            bits.iter_range(200, 400).collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
        assert_eq!(
            Bits::empty(10).iter_range(0, 10).collect::<Vec<_>>(),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn ranges_mark_the_rows_inside_each_range_within_the_length() {
        let bits = Bits::ranges(200, &[(0, 3), (64, 2), (190, 20)]);
        assert_eq!(
            bits.iter().collect::<Vec<_>>(),
            (0..3).chain(64..66).chain(190..200).collect::<Vec<_>>()
        );
        assert_eq!(Bits::ranges(10, &[]).count(), 0);
        assert_eq!(Bits::ranges(10, &[(20, 5)]).count(), 0);
    }

    #[test]
    fn the_group_rule_follows_the_planner() {
        let live = Bits::live(Some(&[1 << 9]), 12);
        let a = of(12, &[1, 2, 3, 9]);
        let b = of(12, &[2, 3, 4]);
        // The rows with a vector, as a dense clause resolves.
        let vectors = of(12, &[0, 1, 2, 3, 4, 5]);
        // MUST intersects; a dense clause is one more required set.
        let (m, seeded) =
            group_members(&[a.clone(), vectors.clone(), b.clone()], &[], &[], 0, &live);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![2, 3]);
        assert!(!seeded);
        // A lone dense MUST: the rows with a vector, deleted rows included
        // only through the live seed, which this shape does not consult.
        let (m, seeded) = group_members(std::slice::from_ref(&vectors), &[], &[], 0, &live);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![0, 1, 2, 3, 4, 5]);
        assert!(!seeded);
        // Only MUST_NOT: the live seed minus the clause.
        let (m, seeded) = group_members(&[], &[], std::slice::from_ref(&vectors), 0, &live);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![6, 7, 8, 10, 11]);
        assert!(seeded);
        // SHOULD count with a dense SHOULD counting for the vector rows.
        let (m, seeded) =
            group_members(&[], &[a.clone(), b.clone(), vectors.clone()], &[], 2, &live);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        assert!(!seeded);
        let (m, _) = group_members(&[], &[a.clone(), b.clone()], &[], 2, &live);
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![2, 3]);
        // A MUST with an optional dense SHOULD keeps the rows without a
        // vector: the minimum is zero, so the SHOULD admits nothing away.
        let (m, _) = group_members(
            std::slice::from_ref(&a),
            std::slice::from_ref(&vectors),
            &[],
            0,
            &live,
        );
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![1, 2, 3, 9]);
        // MUST plus a SHOULD minimum, then MUST_NOT.
        let (m, _) = group_members(
            std::slice::from_ref(&a),
            std::slice::from_ref(&b),
            &[of(12, &[3])],
            1,
            &live,
        );
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![2]);
        // MUST_NOT dense: the members without a vector remain.
        let (m, _) = group_members(
            std::slice::from_ref(&a),
            &[],
            std::slice::from_ref(&vectors),
            0,
            &live,
        );
        assert_eq!(m.iter().collect::<Vec<_>>(), vec![9]);
    }
}
