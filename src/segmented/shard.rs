//! Placement, the field table and per-document reads of a segmented shard.

use super::*;

impl SegmentedShard {
    /// Retain identity metadata for this segment set, frozen seal and tail.
    /// The view does not retain the source blobs or mapped index files.
    pub fn identity_snapshot(&self) -> Result<crate::source_archive::IdentitySnapshot, String> {
        let mut parts = Vec::with_capacity(self.parts.len() + 2);
        for (index, part) in self.parts.iter().enumerate() {
            parts.push((part.base, part.rows, self.reader(index).identity_snapshot()));
        }
        if let Some(frozen) = &self.frozen {
            parts.push((frozen.base, frozen.rows, frozen.store.identity_snapshot()));
        }
        parts.push((
            self.tail_base,
            self.tail.next_doc_id(),
            self.tail.identity_snapshot(),
        ));
        crate::source_archive::IdentitySnapshot::from_parts(parts)
    }

    pub fn document_identity(&self, doc: u32) -> Option<crate::pb::DocumentIdentity> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).document_identity(local),
            Placement::Frozen { local } => self.frozen_store().document_identity(local),
            Placement::Tail { local } => self.tail.document_identity(local),
        }
    }

    pub fn protobuf_source(
        &self,
        doc: u32,
    ) -> std::io::Result<Option<(crate::pb::ProtobufSource, Option<u32>)>> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).protobuf_source(local),
            Placement::Frozen { local } => self.frozen_store().protobuf_source(local),
            Placement::Tail { local } => self.tail.protobuf_source(local),
        }
    }

    /// The frozen part, when a seal is in flight: `(base, rows, store)`.
    pub fn frozen(&self) -> Option<(u32, u32, &Arc<Bm25Store>)> {
        self.frozen.as_ref().map(|f| (f.base, f.rows, &f.store))
    }

    pub(super) fn frozen_store(&self) -> &Bm25Store {
        &self
            .frozen
            .as_ref()
            .expect("a placement named the frozen part")
            .store
    }

    /// The heap parts in row order: the frozen part when one exists,
    /// then the tail, each with its base.
    pub(super) fn heaps(&self) -> impl Iterator<Item = (u32, &Bm25Store)> {
        self.frozen
            .iter()
            .map(|f| (f.base, &*f.store))
            .chain(std::iter::once((self.tail_base, &self.tail)))
    }

    /// Publish binding metadata without dropping a frozen tail or moving rows.
    pub fn publish_binding(&mut self, binding: &StoredBinding) -> Result<(), String> {
        if self.binding().is_some_and(|held| held != binding) {
            return Err("segment shard is bound to another mapping".into());
        }
        let published = self.catalog.publish_binding(binding)?;
        if published.manifest().segments != self.set.manifest().segments {
            return Err("segment rows changed during binding publication".into());
        }
        self.set = published;
        self.tail.set_binding(Some(binding.clone()));
        Ok(())
    }

    pub fn catalog(&self) -> &SegmentCatalog {
        &self.catalog
    }

    pub fn snapshot(&self) -> &Arc<OpenedSegmentSet> {
        &self.set
    }

    pub fn tail(&self) -> &Bm25Store {
        &self.tail
    }

    /// The tail store for ingest; call [`Self::sync_tail`] afterwards.
    pub fn tail_mut(&mut self) -> &mut Bm25Store {
        &mut self.tail
    }

    /// The local id the tail starts at: the rows sealed so far.
    pub fn tail_base(&self) -> u32 {
        self.tail_base
    }

    pub fn sealed_parts(&self) -> usize {
        self.parts.len()
    }

    /// Where local id `doc` lives.
    pub fn place(&self, doc: u32) -> Placement {
        if doc >= self.tail_base {
            return Placement::Tail {
                local: doc - self.tail_base,
            };
        }
        if let Some(f) = self.frozen.as_ref().filter(|f| doc >= f.base) {
            return Placement::Frozen {
                local: doc - f.base,
            };
        }
        let part = match self.parts.binary_search_by(|p| p.base.cmp(&doc)) {
            Ok(i) => i,
            Err(i) => i - 1,
        };
        Placement::Sealed {
            part,
            local: doc - self.parts[part].base,
        }
    }

    pub(super) fn reader(&self, part: usize) -> &crate::postings::Bm25Reader {
        self.set.bm25(part)
    }

    // ---- field table ----

    pub fn field_count(&self) -> usize {
        self.tail.field_count()
    }

    pub fn field_name(&self, f: usize) -> &str {
        self.tail.field_name(f)
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.tail.field_index(name)
    }

    pub fn field_has_positions(&self, f: usize) -> bool {
        self.tail.field_has_positions(f)
    }

    pub fn field_has_sentences(&self, f: usize) -> bool {
        self.tail.field_has_sentences(f)
    }

    /// The analyzer fingerprint of field `f`: the sealed parts' when any
    /// exist (they agree, by the catalog's own check), else the tail's.
    pub fn analysis_fingerprint(&self, f: usize) -> u64 {
        match (self.parts.len(), self.frozen.as_ref()) {
            (0, Some(frozen)) => frozen.store.analysis_fingerprint(f),
            (0, None) => self.tail.analysis_fingerprint(f),
            _ => self.reader(0).analysis_fingerprint(f),
        }
    }

    /// Record field `f`'s analyzer fingerprint on the tail, refusing a
    /// contradiction with the sealed parts or the tail's own record.
    pub fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        let held = match (self.parts.len(), self.frozen.as_ref()) {
            (0, Some(frozen)) => Some(frozen.store.analysis_fingerprint(f)),
            (0, None) => None,
            _ => Some(self.reader(0).analysis_fingerprint(f)),
        };
        if let Some(held) = held {
            if held != 0 && fingerprint != 0 && held != fingerprint {
                return Err(format!(
                    "field {:?} was sealed under analyzer fingerprint {held:#x}; this ingest \
                     analyzes under {fingerprint:#x}",
                    self.tail.field_name(f)
                ));
            }
        }
        self.tail.set_analysis_fingerprint(f, fingerprint)
    }

    pub fn binding(&self) -> Option<&StoredBinding> {
        self.set
            .binding()
            .or_else(|| self.tail.binding())
            .or_else(|| self.frozen.as_ref().and_then(|f| f.store.binding()))
            .or_else(|| {
                (!self.parts.is_empty())
                    .then(|| self.reader(0).binding())
                    .flatten()
            })
    }

    /// The derived-column declaration this shard is written under: the
    /// tail's, which every attached segment was checked against.
    pub fn derived(&self) -> Option<&crate::postings::StoredDerived> {
        self.tail.derived().or_else(|| {
            (!self.parts.is_empty())
                .then(|| self.reader(0).derived())
                .flatten()
        })
    }

    pub fn next_doc_id(&self) -> u32 {
        self.tail_base + self.tail.next_doc_id()
    }

    pub fn doc_count(&self) -> u64 {
        (0..self.parts.len())
            .map(|i| Bm25Index::doc_count(self.reader(i)))
            .sum::<u64>()
            + self.heaps().map(|(_, h)| h.doc_count()).sum::<u64>()
    }

    /// The read surface of field `f` across every part.
    pub fn field(&self, f: usize) -> UnionField<'_> {
        UnionField {
            shard: self,
            fi: f,
            mask: None,
        }
    }

    /// The read surface of field `f` over the sealed parts `mask`
    /// admits (one flag per sealed part, `false` = skipped) plus the
    /// heaps. Walks over postings, impacts,
    /// and dictionary scans skip the masked-out parts; reads addressed
    /// by document id answer for every row, so a hit's text, offsets,
    /// and positions resolve whichever part holds it.
    pub fn field_masked(&self, f: usize, mask: Arc<[bool]>) -> UnionField<'_> {
        debug_assert_eq!(mask.len(), self.parts.len());
        UnionField {
            shard: self,
            fi: f,
            mask: Some(mask),
        }
    }

    /// The whole shard scored as its body field over the parts `mask`
    /// admits: the [`Bm25Index`] the scorers take when a filter has
    /// ruled sealed segments out.
    pub fn masked(&self, mask: Arc<[bool]>) -> MaskedShard<'_> {
        MaskedShard { shard: self, mask }
    }

    /// `(base, rows)` of every sealed part, in catalog order: the local
    /// document id range each segment covers.
    pub fn sealed_ranges(&self) -> Vec<(u32, u32)> {
        self.parts.iter().map(|p| (p.base, p.rows)).collect()
    }

    /// The summary of every sealed part, in catalog order; `None` for a
    /// segment sealed before summaries were written.
    pub fn part_summaries(&self) -> Vec<Option<&SegmentSummary>> {
        (0..self.parts.len())
            .map(|i| self.set.metadata(i).summary.as_ref())
            .collect()
    }

    /// One flag per sealed part: `true` where NONE of `terms` occurs in
    /// field `f` of that part, so a walk over those terms would touch
    /// no posting there. A dictionary lookup per term per part, no
    /// postings read.
    pub fn parts_lacking_terms(&self, f: usize, terms: &[String]) -> Vec<bool> {
        (0..self.parts.len())
            .map(|i| {
                let view = self.reader(i).field(f);
                terms.iter().all(|term| view.df(term) == 0)
            })
            .collect()
    }

    // ---- sentences / text ----

    pub fn field_doc_sentences(&self, f: usize, doc: u32) -> Option<Vec<(u32, u32)>> {
        match self.place(doc) {
            Placement::Sealed { part, local } => self.reader(part).field_doc_sentences(f, local),
            Placement::Frozen { local } => self
                .frozen_store()
                .field_doc_sentences(f, local)
                .map(<[(u32, u32)]>::to_vec),
            Placement::Tail { local } => self
                .tail
                .field_doc_sentences(f, local)
                .map(<[(u32, u32)]>::to_vec),
        }
    }

    pub fn text(&self, doc: u32) -> Option<String> {
        match self.place(doc) {
            Placement::Sealed { part, local } => Bm25Index::text(self.reader(part), local),
            Placement::Frozen { local } => self.frozen_store().text(local).map(str::to_string),
            Placement::Tail { local } => self.tail.text(local).map(str::to_string),
        }
    }

    pub fn lineage(&self, doc: u32) -> Option<DocLineage> {
        match self.place(doc) {
            Placement::Sealed { part, local } => Bm25Index::lineage(self.reader(part), local),
            Placement::Frozen { local } => self.frozen_store().lineage(local),
            Placement::Tail { local } => self.tail.lineage(local),
        }
    }

    // ---- ingest ----

    /// Append one analyzed document at the shard-local id `doc`, which
    /// must be at or past the tail's next id.
    pub fn add_document(
        &mut self,
        doc: u32,
        text: String,
        analyzed: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) -> Result<(), String> {
        let local = doc.checked_sub(self.tail_base).ok_or_else(|| {
            format!(
                "document {doc} falls inside the sealed range (tail starts at {})",
                self.tail_base
            )
        })?;
        self.tail
            .add_document_with_lineage(local, text, analyzed, lineage);
        self.sync_tail();
        Ok(())
    }

    /// The directory a new segment is staged from, under the catalog.
    pub fn stage_dir(&self, tag: &str) -> PathBuf {
        self.set
            .root()
            .join(format!(".seal-{tag}-{}", std::process::id()))
    }

    pub fn root(&self) -> &Path {
        self.set.root()
    }
}
