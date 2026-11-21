//! Open a segmented shard and keep its parts and dictionaries current.

use super::*;

impl SegmentedShard {
    /// Open the catalog under `root` with `tail` as the mutable part.
    /// The tail must be an EMPTY store declared with the same field and
    /// column tables every sealed segment has; a mismatch refuses by
    /// name, because a union over different tables has no meaning.
    pub fn open(root: impl Into<PathBuf>, tail: Bm25Store) -> Result<Self, String> {
        Self::open_with(root, tail, crate::segments::VectorLoad::default())
    }

    /// [`Self::open`] with the way sealed vector images are served.
    pub fn open_with(
        root: impl Into<PathBuf>,
        tail: Bm25Store,
        load: crate::segments::VectorLoad,
    ) -> Result<Self, String> {
        let catalog = SegmentCatalog::open_with(root, load)?;
        Self::from_catalog(catalog, tail)
    }

    /// [`Self::open`] over a catalog the caller already holds — the staged
    /// catalog of a compaction shadow (`SegmentCatalog::open_staged`).
    pub fn open_catalog(catalog: SegmentCatalog, tail: Bm25Store) -> Result<Self, String> {
        Self::from_catalog(catalog, tail)
    }

    fn from_catalog(catalog: SegmentCatalog, tail: Bm25Store) -> Result<Self, String> {
        let set = catalog.snapshot();
        Self::from_snapshot(catalog, set, tail)
    }

    /// Construct a complete serving view before its catalog commits the set.
    /// Only the owning publisher may activate this prepared snapshot.
    pub(crate) fn from_snapshot(
        catalog: SegmentCatalog,
        set: Arc<OpenedSegmentSet>,
        mut tail: Bm25Store,
    ) -> Result<Self, String> {
        if tail.next_doc_id() != 0 {
            return Err("a segmented shard's tail must start empty".to_string());
        }
        if let Some(declaration) = set.generation_declaration() {
            crate::segments::generation::restore_tail(declaration, &mut tail)?;
        }
        if let Some(binding) = set.binding() {
            if tail.binding().is_some_and(|held| held != binding) {
                return Err("segment tail and generation mapped bindings disagree".into());
            }
            tail.set_binding(Some(binding.clone()));
        } else if !set.is_empty() && tail.binding().is_some() {
            return Err("a bound tail cannot label a populated unbound segment set".into());
        }
        let mut shard = SegmentedShard {
            catalog,
            set,
            parts: Vec::new(),
            frozen: None,
            tail,
            tail_base: 0,
            tail_parents_colocated: true,
            facets: Vec::new(),
            map_keys: Vec::new(),
            map_values: Vec::new(),
            map_key_reverse: Vec::new(),
            map_numeric_keys: Vec::new(),
            map_integer_keys: Vec::new(),
            map_unsigned_integer_keys: Vec::new(),
            map_numeric_key_reverse: Vec::new(),
            map_integer_key_reverse: Vec::new(),
            map_unsigned_integer_key_reverse: Vec::new(),
        };
        shard.rebuild()?;
        Ok(shard)
    }

    /// Recompute the parts and dictionaries from the current snapshot.
    pub(super) fn rebuild(&mut self) -> Result<(), String> {
        let set = Arc::clone(&self.set);
        let sealed_end = self.rebuild_parts(&set)?;
        self.tail_base = match self.frozen.as_ref() {
            Some(f) if f.base != sealed_end => {
                return Err(format!(
                    "the frozen part starts at {} but the sealed parts end at {sealed_end}",
                    f.base
                ))
            }
            Some(f) => sealed_end + f.rows,
            None => sealed_end,
        };
        self.rebuild_dictionaries(&set);
        self.freeze_frozen_dictionaries();
        self.sync_tail();
        Ok(())
    }

    /// Place every sealed segment of `set` in one contiguous id space; the
    /// id the sealed parts end at.
    fn rebuild_parts(&mut self, set: &OpenedSegmentSet) -> Result<u32, String> {
        let mut parts = Vec::with_capacity(set.len());
        let mut next: u64 = 0;
        for i in 0..set.len() {
            let m = set.metadata(i);
            if m.base_label != next {
                return Err(format!(
                    "segment {:?} starts at label {} but the previous part ends at {next}; a \
                     segmented shard needs one contiguous id space",
                    m.segment_id, m.base_label
                ));
            }
            let base = u32::try_from(next).map_err(|_| "segment base exceeds u32".to_string())?;
            let rows = u32::try_from(m.rows).map_err(|_| "segment rows exceed u32".to_string())?;
            self.check_tables(set.bm25(i), &m.segment_id)?;
            parts.push(Part { base, rows });
            next += m.rows;
        }
        self.parts = parts;
        let sealed_end = u32::try_from(next).map_err(|_| "segment rows exceed u32".to_string())?;
        Ok(sealed_end)
    }

    /// Global dictionaries per column, over the sealed parts.
    fn rebuild_dictionaries(&mut self, set: &OpenedSegmentSet) {
        let readers: Vec<&crate::postings::Bm25Reader> =
            (0..set.len()).map(|i| set.bm25(i)).collect();
        self.facets = (0..self.tail.facet_count())
            .map(|fi| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.facet_dictionary(fi))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_keys = (0..self.tail.map_facet_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_facet_keys(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_values = (0..self.tail.map_facet_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_facet_values(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_numeric_keys = (0..self.tail.map_numeric_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_numeric_keys(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_integer_keys = (0..self.tail.map_integer_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_integer_keys(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
        self.map_unsigned_integer_keys = (0..self.tail.map_unsigned_integer_count())
            .map(|ci| {
                UnionDict::build(
                    &readers
                        .iter()
                        .map(|r| r.map_unsigned_integer_keys(ci))
                        .collect::<Vec<_>>(),
                )
            })
            .collect();
    }

    /// The frozen part's values enter as the tail's did when it froze,
    /// then move to the frozen remap; the real tail syncs after them.
    fn freeze_frozen_dictionaries(&mut self) {
        if let Some(frozen) = self.frozen.as_ref().map(|f| Arc::clone(&f.store)) {
            for (fi, dict) in self.facets.iter_mut().enumerate() {
                dict.sync_tail(frozen.facet_dictionary(fi));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_keys.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_facet_keys(ci));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_values.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_facet_values(ci));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_numeric_keys.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_numeric_keys(ci));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_integer_keys.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_integer_keys(ci));
                dict.freeze_tail();
            }
            for (ci, dict) in self.map_unsigned_integer_keys.iter_mut().enumerate() {
                dict.sync_tail(frozen.map_unsigned_integer_keys(ci));
                dict.freeze_tail();
            }
        }
    }

    pub(super) fn rebuild_key_reverse(&mut self) {
        let frozen = self.frozen.as_ref().map(|f| &*f.store);
        self.map_key_reverse = self
            .map_keys
            .iter()
            .enumerate()
            .map(|(ci, dict)| {
                KeyReverse::build(
                    dict,
                    frozen.map_or(0, |f| f.map_facet_keys(ci).len()),
                    self.tail.map_facet_keys(ci).len(),
                )
            })
            .collect();
        self.map_numeric_key_reverse = self
            .map_numeric_keys
            .iter()
            .enumerate()
            .map(|(ci, dict)| {
                KeyReverse::build(
                    dict,
                    frozen.map_or(0, |f| f.map_numeric_keys(ci).len()),
                    self.tail.map_numeric_keys(ci).len(),
                )
            })
            .collect();
        self.map_integer_key_reverse = self
            .map_integer_keys
            .iter()
            .enumerate()
            .map(|(ci, dict)| {
                KeyReverse::build(
                    dict,
                    frozen.map_or(0, |f| f.map_integer_keys(ci).len()),
                    self.tail.map_integer_keys(ci).len(),
                )
            })
            .collect();
        self.map_unsigned_integer_key_reverse = self
            .map_unsigned_integer_keys
            .iter()
            .enumerate()
            .map(|(ci, dict)| {
                KeyReverse::build(
                    dict,
                    frozen.map_or(0, |f| f.map_unsigned_integer_keys(ci).len()),
                    self.tail.map_unsigned_integer_keys(ci).len(),
                )
            })
            .collect();
    }

    /// Fold the tail's new dictionary entries into the global ones. Runs
    /// after every ingest under the shard's write lock.
    pub fn sync_tail(&mut self) {
        for (fi, dict) in self.facets.iter_mut().enumerate() {
            dict.sync_tail(self.tail.facet_dictionary(fi));
        }
        for (ci, dict) in self.map_keys.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_facet_keys(ci));
        }
        for (ci, dict) in self.map_values.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_facet_values(ci));
        }
        for (ci, dict) in self.map_numeric_keys.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_numeric_keys(ci));
        }
        for (ci, dict) in self.map_integer_keys.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_integer_keys(ci));
        }
        for (ci, dict) in self.map_unsigned_integer_keys.iter_mut().enumerate() {
            dict.sync_tail(self.tail.map_unsigned_integer_keys(ci));
        }
        self.rebuild_key_reverse();
    }

    /// Freeze the tail for a seal: the tail becomes a read-only part at
    /// its current rows, `fresh` becomes the tail after it, and the
    /// frozen store is returned shared so the seal can write it with no
    /// lock on the shard. Refuses while a frozen part is still waiting
    /// on its publication.
    /// `rows` is the positional span the segment covers: the tail's
    /// documents, or more when the vector side has rows the document
    /// side does not (a vectors-only shard), never fewer.
    pub fn freeze_tail(&mut self, fresh: Bm25Store, rows: u32) -> Result<Arc<Bm25Store>, String> {
        if self.frozen.is_some() {
            return Err("a seal is already in flight on this shard".to_string());
        }
        if fresh.next_doc_id() != 0 {
            return Err("a fresh tail must start empty".to_string());
        }
        if rows < self.tail.next_doc_id() {
            return Err(format!(
                "a seal of {rows} rows cannot cover the tail's {} documents",
                self.tail.next_doc_id()
            ));
        }
        if !same_tables!(&fresh, &self.tail) {
            return Err("a fresh tail must preserve the shard's field and column tables".into());
        }
        let store = Arc::new(std::mem::replace(&mut self.tail, fresh));
        self.frozen = Some(Frozen {
            parents_colocated: std::mem::replace(&mut self.tail_parents_colocated, true),
            base: self.tail_base,
            rows,
            store: Arc::clone(&store),
        });
        self.tail_base += rows;
        for dict in self
            .facets
            .iter_mut()
            .chain(self.map_keys.iter_mut())
            .chain(self.map_values.iter_mut())
            .chain(self.map_numeric_keys.iter_mut())
            .chain(self.map_integer_keys.iter_mut())
            .chain(self.map_unsigned_integer_keys.iter_mut())
        {
            dict.freeze_tail();
        }
        self.rebuild_key_reverse();
        Ok(store)
    }
}
