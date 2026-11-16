//! Facet, numeric, integer and geo columns read across every part.

use super::*;

impl crate::filter_bits::ColumnSets for SegmentedShard {
    /// A sealed part answers from its sorted column or, when the rows
    /// still in question there are the fewer, by reading them; the frozen
    /// part and the tail are read forward.
    fn number_rows(
        &self,
        range: &crate::filter_bits::NumberRange<'_>,
        want: bool,
        domain: &Bits,
        out: &mut Bits,
    ) {
        for (index, part) in self.parts.iter().enumerate() {
            let base = part.base as usize;
            range.part(
                self.reader(index),
                base..base + part.rows as usize,
                want,
                domain,
                out,
            );
        }
        if let Some(frozen) = &self.frozen {
            let base = frozen.base as usize;
            range.part(
                frozen.store.as_ref(),
                base..base + frozen.rows as usize,
                want,
                domain,
                out,
            );
        }
        let base = self.tail_base as usize;
        range.part(
            &self.tail,
            base..base + self.tail.next_doc_id() as usize,
            want,
            domain,
            out,
        );
    }

    /// Set in `out` the shard-local slots whose value in facet column `fi`
    /// has a global ordinal `accept` admits. A sealed part answers from
    /// its inverted column, a union per accepted value; the frozen part
    /// and the tail are read forward, a row at a time.
    fn facet_rows(&self, fi: usize, accept: &dyn Fn(u32) -> bool, out: &mut Bits) {
        let dict = &self.facets[fi];
        for (index, part) in self.parts.iter().enumerate() {
            let reader = self.reader(index);
            if fi >= reader.facet_count() {
                continue;
            }
            let inverted = reader.facet_rows(fi);
            for local in 0..inverted.values() as u32 {
                if dict
                    .to_global(Source::Sealed(index), local)
                    .is_some_and(accept)
                {
                    inverted.union_into(local, part.base as usize, out);
                }
            }
        }
        self.heap_rows(fi, out, &|global| global.is_some_and(accept));
    }

    /// Set in `out` the shard-local slots that hold a value in facet
    /// column `fi`.
    fn facet_present(&self, fi: usize, out: &mut Bits) {
        for (index, part) in self.parts.iter().enumerate() {
            let reader = self.reader(index);
            if fi < reader.facet_count() {
                reader.facet_rows(fi).present_into(part.base as usize, out);
            }
        }
        self.heap_rows(fi, out, &|global| global.is_some());
    }
}

impl SegmentedShard {
    // ---- facets ----

    pub fn facet_count(&self) -> usize {
        self.tail.facet_count()
    }

    pub fn facet_name(&self, fi: usize) -> &str {
        self.tail.facet_name(fi)
    }

    pub fn facet_index(&self, name: &str) -> Option<usize> {
        self.tail.facet_index(name)
    }

    pub fn facet_value_count(&self, fi: usize) -> usize {
        self.facets[fi].values.len()
    }

    pub fn facet_value(&self, fi: usize, ord: u32) -> &str {
        &self.facets[fi].values[ord as usize]
    }

    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        self.facets[fi].ord_of(value)
    }

    pub fn facet_dictionary(&self, fi: usize) -> &[String] {
        &self.facets[fi].values
    }

    pub fn facet_dictionary_sorted(&self, fi: usize) -> bool {
        self.facets[fi].sorted()
    }

    /// The frozen part's and the tail's rows whose global ordinal in facet
    /// column `fi` (`None` when the row holds no value) `keep` admits.
    pub(super) fn heap_rows(&self, fi: usize, out: &mut Bits, keep: &dyn Fn(Option<u32>) -> bool) {
        let dict = &self.facets[fi];
        let len = out.len();
        let mut scan = |store: &Bm25Store, source: Source, base: u32, rows: u32| {
            for local in 0..rows {
                let slot = (base + local) as usize;
                if slot >= len {
                    break;
                }
                let global = store
                    .facet_ord(fi, local)
                    .and_then(|ord| dict.to_global(source, ord));
                if keep(global) {
                    out.set(slot);
                }
            }
        };
        if let Some(frozen) = &self.frozen {
            scan(&frozen.store, Source::Frozen, frozen.base, frozen.rows);
        }
        scan(
            &self.tail,
            Source::Tail,
            self.tail_base,
            self.tail.next_doc_id(),
        );
    }

    pub fn facet_ord(&self, fi: usize, doc: u32) -> Option<u32> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self
                .reader(part)
                .facet_ord(fi, local)
                .and_then(|ord| self.facets[fi].to_global(Source::Sealed(part), ord)),
            Placement::Frozen { local } => self
                .frozen_store()
                .facet_ord(fi, local)
                .and_then(|ord| self.facets[fi].to_global(Source::Frozen, ord)),
            Placement::Tail { local } => self
                .tail
                .facet_ord(fi, local)
                .and_then(|ord| self.facets[fi].to_global(Source::Tail, ord)),
        }
    }

    // ---- numerics, integers, geo ----

    pub fn numeric_count(&self) -> usize {
        self.tail.numeric_count()
    }

    pub fn numeric_name(&self, ni: usize) -> &str {
        self.tail.numeric_name(ni)
    }

    pub fn numeric_index(&self, name: &str) -> Option<usize> {
        self.tail.numeric_index(name)
    }

    pub fn numeric_value(&self, ni: usize, doc: u32) -> Option<f64> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).numeric_value(ni, local),
            Placement::Frozen { local } => self.frozen_store().numeric_value(ni, local),
            Placement::Tail { local } => self.tail.numeric_value(ni, local),
        }
    }

    pub fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        let mut acc = (f64::INFINITY, f64::NEG_INFINITY);
        let fold = |acc: &mut (f64, f64), (lo, hi): (f64, f64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).numeric_min_max(ni));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.numeric_min_max(ni));
        }
        acc
    }

    pub fn integer_count(&self) -> usize {
        self.tail.integer_count()
    }

    pub fn integer_name(&self, ii: usize) -> &str {
        self.tail.integer_name(ii)
    }

    pub fn integer_index(&self, name: &str) -> Option<usize> {
        self.tail.integer_index(name)
    }

    pub fn integer_value(&self, ii: usize, doc: u32) -> Option<i64> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).integer_value(ii, local),
            Placement::Frozen { local } => self.frozen_store().integer_value(ii, local),
            Placement::Tail { local } => self.tail.integer_value(ii, local),
        }
    }

    pub fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        let mut acc = (i64::MAX, i64::MIN);
        let fold = |acc: &mut (i64, i64), (lo, hi): (i64, i64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).integer_min_max(ii));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.integer_min_max(ii));
        }
        acc
    }

    pub fn unsigned_integer_count(&self) -> usize {
        self.tail.unsigned_integer_count()
    }

    pub fn unsigned_integer_name(&self, ii: usize) -> &str {
        self.tail.unsigned_integer_name(ii)
    }

    pub fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.tail.unsigned_integer_index(name)
    }

    pub fn unsigned_integer_value(&self, ii: usize, doc: u32) -> Option<u64> {
        match self.place(doc) {
            Placement::Sealed { part, local } => {
                self.reader(part).unsigned_integer_value(ii, local)
            }
            Placement::Frozen { local } => self.frozen_store().unsigned_integer_value(ii, local),
            Placement::Tail { local } => self.tail.unsigned_integer_value(ii, local),
        }
    }

    pub fn unsigned_integer_min_max(&self, ii: usize) -> (u64, u64) {
        let mut acc = (u64::MAX, 0);
        let fold = |acc: &mut (u64, u64), (lo, hi): (u64, u64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).unsigned_integer_min_max(ii));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.unsigned_integer_min_max(ii));
        }
        acc
    }

    pub fn geo_count(&self) -> usize {
        self.tail.geo_count()
    }

    pub fn geo_name(&self, gi: usize) -> &str {
        self.tail.geo_name(gi)
    }

    pub fn geo_index(&self, name: &str) -> Option<usize> {
        self.tail.geo_index(name)
    }

    pub fn geo_value(&self, gi: usize, doc: u32) -> Option<(f64, f64)> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).geo_value(gi, local),
            Placement::Frozen { local } => self.frozen_store().geo_value(gi, local),
            Placement::Tail { local } => self.tail.geo_value(gi, local),
        }
    }

    pub fn geo_bbox(&self, gi: usize) -> (f64, f64, f64, f64) {
        let mut acc = (
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        );
        let fold = |acc: &mut (f64, f64, f64, f64), b: (f64, f64, f64, f64)| {
            if b.0 <= b.1 && b.2 <= b.3 {
                acc.0 = acc.0.min(b.0);
                acc.1 = acc.1.max(b.1);
                acc.2 = acc.2.min(b.2);
                acc.3 = acc.3.max(b.3);
            }
        };
        for i in 0..self.parts.len() {
            fold(&mut acc, self.reader(i).geo_bbox(gi));
        }
        for (_, heap) in self.heaps() {
            fold(&mut acc, heap.geo_bbox(gi));
        }
        acc
    }
}
