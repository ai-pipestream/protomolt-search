//! Map-valued columns read across every part through the key remaps.

use super::*;

impl SegmentedShard {
    // ---- map facets ----

    pub fn map_facet_count(&self) -> usize {
        self.tail.map_facet_count()
    }

    pub fn map_facet_name(&self, ci: usize) -> &str {
        self.tail.map_facet_name(ci)
    }

    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.tail.map_facet_index(name)
    }

    pub fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_keys[ci].ord_of(key)
    }

    pub fn map_facet_keys(&self, ci: usize) -> &[String] {
        &self.map_keys[ci].values
    }

    pub fn map_facet_value_count(&self, ci: usize) -> usize {
        self.map_values[ci].values.len()
    }

    pub fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        &self.map_values[ci].values[ord as usize]
    }

    pub fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        self.map_values[ci].ord_of(value)
    }

    pub fn map_facet_values(&self, ci: usize) -> &[String] {
        &self.map_values[ci].values
    }

    pub fn map_facet_values_sorted(&self, ci: usize) -> bool {
        self.map_values[ci].sorted()
    }

    /// `(source, reverse-table index, local doc)` for a document id.
    fn locate(&self, doc: u32) -> (Source, usize, u32) {
        match self.place(doc) {
            Placement::Sealed { part, local } => (Source::Sealed(part), part, local),
            Placement::Frozen { local } => (Source::Frozen, self.parts.len(), local),
            Placement::Tail { local } => (Source::Tail, self.parts.len() + 1, local),
        }
    }

    pub fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc: u32) -> Option<u32> {
        let (source, reverse_index, local_doc) = self.locate(doc);
        let local_key = *self.map_key_reverse[ci].per_part[reverse_index].get(key_ord as usize)?;
        if local_key == u32::MAX {
            return None;
        }
        let local_value = match source {
            Source::Sealed(part) => self
                .reader(part)
                .map_facet_value_ord(ci, local_key, local_doc)?,
            Source::Frozen => self
                .frozen_store()
                .map_facet_value_ord(ci, local_key, local_doc)?,
            Source::Tail => self.tail.map_facet_value_ord(ci, local_key, local_doc)?,
        };
        self.map_values[ci].to_global(source, local_value)
    }

    // ---- map numerics ----

    pub fn map_numeric_count(&self) -> usize {
        self.tail.map_numeric_count()
    }

    pub fn map_numeric_name(&self, ci: usize) -> &str {
        self.tail.map_numeric_name(ci)
    }

    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.tail.map_numeric_index(name)
    }

    pub fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_numeric_keys[ci].ord_of(key)
    }

    pub fn map_numeric_keys(&self, ci: usize) -> &[String] {
        &self.map_numeric_keys[ci].values
    }

    pub fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        let mut acc = (f64::INFINITY, f64::NEG_INFINITY);
        let fold = |acc: &mut (f64, f64), (lo, hi): (f64, f64)| {
            if lo <= hi {
                acc.0 = acc.0.min(lo);
                acc.1 = acc.1.max(hi);
            }
        };
        let reverse = &self.map_numeric_key_reverse[ci].per_part;
        for (i, table) in reverse.iter().enumerate().take(self.parts.len()) {
            let local = table[key_ord as usize];
            if local != u32::MAX {
                fold(&mut acc, self.reader(i).map_numeric_key_min_max(ci, local));
            }
        }
        let heap_tables = [self.parts.len(), self.parts.len() + 1];
        let heap_indexes = if self.frozen.is_some() {
            &heap_tables[..]
        } else {
            &heap_tables[1..]
        };
        for (&index, (_, heap)) in heap_indexes.iter().zip(self.heaps()) {
            let local = reverse[index][key_ord as usize];
            if local != u32::MAX {
                fold(&mut acc, heap.map_numeric_key_min_max(ci, local));
            }
        }
        acc
    }

    pub fn map_numeric_value(&self, ci: usize, key_ord: u32, doc: u32) -> Option<f64> {
        let (source, reverse_index, local_doc) = self.locate(doc);
        let local_key =
            *self.map_numeric_key_reverse[ci].per_part[reverse_index].get(key_ord as usize)?;
        if local_key == u32::MAX {
            return None;
        }
        match source {
            Source::Sealed(part) => self
                .reader(part)
                .map_numeric_value(ci, local_key, local_doc),
            Source::Frozen => self
                .frozen_store()
                .map_numeric_value(ci, local_key, local_doc),
            Source::Tail => self.tail.map_numeric_value(ci, local_key, local_doc),
        }
    }

    pub fn map_integer_count(&self) -> usize {
        self.tail.map_integer_count()
    }

    pub fn map_integer_name(&self, ci: usize) -> &str {
        self.tail.map_integer_name(ci)
    }

    pub fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.tail.map_integer_index(name)
    }

    pub fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_integer_keys[ci].ord_of(key)
    }

    pub fn map_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_integer_keys[ci].values
    }

    /// Exact union bounds; a missing key has no bounds.
    pub fn map_integer_key_min_max(&self, ci: usize, key_ord: u32) -> Option<(i64, i64)> {
        let key = self.map_integer_keys[ci].values.get(key_ord as usize)?;
        let mut bounds: Option<(i64, i64)> = None;
        let mut include = |next: Option<(i64, i64)>| {
            if let Some((lo, hi)) = next {
                bounds = Some(bounds.map_or((lo, hi), |(min, max)| (min.min(lo), max.max(hi))));
            }
        };
        for i in 0..self.parts.len() {
            let reader = self.reader(i);
            if let Some(local) = reader.map_integer_key_ord(ci, key) {
                include(reader.map_integer_key_min_max(ci, local));
            }
        }
        for (_, heap) in self.heaps() {
            if let Some(local) = heap.map_integer_key_ord(ci, key) {
                include(heap.map_integer_key_min_max(ci, local));
            }
        }
        bounds
    }

    pub fn map_integer_value(&self, ci: usize, key_ord: u32, doc: u32) -> Option<i64> {
        let (source, reverse_index, local_doc) = self.locate(doc);
        let local_key =
            *self.map_integer_key_reverse[ci].per_part[reverse_index].get(key_ord as usize)?;
        if local_key == u32::MAX {
            return None;
        }
        match source {
            Source::Sealed(part) => self
                .reader(part)
                .map_integer_value(ci, local_key, local_doc),
            Source::Frozen => self
                .frozen_store()
                .map_integer_value(ci, local_key, local_doc),
            Source::Tail => self.tail.map_integer_value(ci, local_key, local_doc),
        }
    }

    pub fn map_unsigned_integer_count(&self) -> usize {
        self.tail.map_unsigned_integer_count()
    }

    pub fn map_unsigned_integer_name(&self, ci: usize) -> &str {
        self.tail.map_unsigned_integer_name(ci)
    }

    pub fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.tail.map_unsigned_integer_index(name)
    }

    pub fn map_unsigned_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_unsigned_integer_keys[ci].ord_of(key)
    }

    pub fn map_unsigned_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_unsigned_integer_keys[ci].values
    }

    /// Exact union bounds; a missing key has no bounds.
    pub fn map_unsigned_integer_key_min_max(&self, ci: usize, key_ord: u32) -> Option<(u64, u64)> {
        let key = self.map_unsigned_integer_keys[ci]
            .values
            .get(key_ord as usize)?;
        let mut bounds: Option<(u64, u64)> = None;
        let mut include = |next: Option<(u64, u64)>| {
            if let Some((lo, hi)) = next {
                bounds = Some(bounds.map_or((lo, hi), |(min, max)| (min.min(lo), max.max(hi))));
            }
        };
        for i in 0..self.parts.len() {
            let reader = self.reader(i);
            if let Some(local) = reader.map_unsigned_integer_key_ord(ci, key) {
                include(reader.map_unsigned_integer_key_min_max(ci, local));
            }
        }
        for (_, heap) in self.heaps() {
            if let Some(local) = heap.map_unsigned_integer_key_ord(ci, key) {
                include(heap.map_unsigned_integer_key_min_max(ci, local));
            }
        }
        bounds
    }

    pub fn map_unsigned_integer_value(&self, ci: usize, key_ord: u32, doc: u32) -> Option<u64> {
        let (source, reverse_index, local_doc) = self.locate(doc);
        let local_key = *self.map_unsigned_integer_key_reverse[ci].per_part[reverse_index]
            .get(key_ord as usize)?;
        if local_key == u32::MAX {
            return None;
        }
        match source {
            Source::Sealed(part) => self
                .reader(part)
                .map_unsigned_integer_value(ci, local_key, local_doc),
            Source::Frozen => self
                .frozen_store()
                .map_unsigned_integer_value(ci, local_key, local_doc),
            Source::Tail => self
                .tail
                .map_unsigned_integer_value(ci, local_key, local_doc),
        }
    }
}
