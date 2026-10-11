//! The store's byte writers: shared text/text_index/lineage sections, the v6/v7
//! ordered writer, the legacy v4 writer, and `read_from`, the in-memory decode
//! entry point that checks the v8 envelope before reading the payload as its
//! base version.

use super::*;

use std::io::{self, Write};

impl Bm25Store {
    /// Sizes of the shared sections (texts, text_index, lineages),
    /// identical bytes in every format version modulo text_index
    /// basing.
    pub(crate) fn shared_section_sizes(&self) -> (u64, u64, u64) {
        let texts_size: u64 = self
            .texts
            .iter()
            .map(|t| 4 + t.as_ref().map_or(0, |s| s.len() as u64))
            .sum();
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        (texts_size, 12 * self.texts.len() as u64, lineages_size)
    }

    /// One field's term list in byte order — the on-disk directory
    /// order, which the heap map already keeps.
    pub(crate) fn sorted_terms(field: &FieldStore) -> Vec<&String> {
        field.postings.keys().collect()
    }

    /// Per-term `(occurrence, skip)` run byte sizes of one field — the
    /// size pass. The skip run's size needs the frontier computation,
    /// so run the same builder over a sink; the write pass re-runs it
    /// for real (deterministic, so the sizes agree).
    pub(crate) fn field_run_sizes(
        field: &FieldStore,
        terms: &[&String],
    ) -> io::Result<Vec<(u64, u64)>> {
        let mut run_sizes: Vec<(u64, u64)> = Vec::with_capacity(terms.len());
        for term in terms {
            let postings = &field.postings[*term];
            let occ_bytes: u64 = postings.iter().map(|p| 8 * p.offsets.len() as u64).sum();
            let mut skip = SkipRunBuilder::new();
            let mut sink = io::sink();
            for p in postings {
                let dl = field.doc_lengths[p.doc_id as usize];
                skip.push(p.tf, dl, p.doc_id, &mut sink)?;
            }
            let (l0_bytes, l1) = skip.finish(&mut sink)?;
            run_sizes.push((occ_bytes, skip_run_size(l0_bytes, &l1)));
        }
        Ok(run_sizes)
    }

    /// Byte size of one field's postings section (v5 shape).
    pub(crate) fn field_postings_size(
        field: &FieldStore,
        terms: &[&String],
        run_sizes: &[(u64, u64)],
    ) -> u64 {
        4 + terms
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let df = field.postings[*t].len() as u64;
                12 * df + 4 + run_sizes[i].0 + run_sizes[i].1
            })
            .sum::<u64>()
    }

    /// Byte size of one field's directory section (v5 shape).
    pub(crate) fn field_directory_size(terms: &[&String]) -> u64 {
        4 + 34 * terms.len() as u64 + terms.iter().map(|t| t.len() as u64).sum::<u64>()
    }

    /// Write the shared sections (texts, text_index, lineages).
    /// `index_base` bases the text_index entries: the absolute texts
    /// section offset in v3/v4/v5 (absolute entries), 0 in v6
    /// (section-relative entries).
    pub(crate) fn write_shared_sections<W: Write>(
        &self,
        w: &mut W,
        index_base: u64,
    ) -> io::Result<()> {
        // texts (+ build the on-disk index)
        let mut text_index: Vec<(u64, u32)> = Vec::with_capacity(self.texts.len());
        let mut cursor = index_base;
        for text in &self.texts {
            match text {
                Some(t) => {
                    write_u32(w, t.len() as u32)?;
                    w.write_all(t.as_bytes())?;
                    text_index.push((cursor + 4, t.len() as u32));
                    cursor += 4 + t.len() as u64;
                }
                None => {
                    write_u32(w, u32::MAX)?;
                    text_index.push((0, u32::MAX));
                    // The absent marker still occupies 4 bytes; skipping
                    // this advance pointed every later text_index entry 4
                    // bytes early per gap slot.
                    cursor += 4;
                }
            }
        }
        // text_index
        for &(offset, len) in &text_index {
            write_u64(w, offset)?;
            write_u32(w, len)?;
        }
        // lineages
        for lineage in &self.lineages {
            match lineage {
                Some(l) => {
                    w.write_all(&[1u8])?;
                    write_u64(w, l.parent_id)?;
                    write_u64(w, l.group_id)?;
                    write_u32(w, l.span_start)?;
                    write_u32(w, l.span_end)?;
                }
                None => w.write_all(&[0u8])?,
            }
        }
        Ok(())
    }

    /// Write one field's doc_lengths section.
    pub(crate) fn write_field_doc_lengths<W: Write>(
        w: &mut W,
        field: &FieldStore,
    ) -> io::Result<()> {
        for &len in &field.doc_lengths {
            write_u32(w, len)?;
        }
        Ok(())
    }

    /// Write one field's postings section (u32 n_terms, then per-term
    /// doc/occurrence/skip runs), returning the directory tuples
    /// `(doc_run_off, skip_run_off, occ_run_off, df)`. Offsets are
    /// based at `run_base`: the absolute postings section offset in v5,
    /// 0 in v6 (section-relative entries).
    ///
    /// The doc run streams straight out; the occurrence run and the
    /// level-0 skip records stage in per-term buffers (the heap store
    /// already holds every posting, so the stage is never the memory
    /// ceiling) and are appended after the sentinel.
    pub(crate) fn write_field_postings<W: Write>(
        w: &mut W,
        field: &FieldStore,
        terms: &[&String],
        run_sizes: &[(u64, u64)],
        run_base: u64,
    ) -> io::Result<Vec<(u64, u64, u64, u32)>> {
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u64, u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = run_base + 4;
        for (i, term) in terms.iter().enumerate() {
            let postings = &field.postings[*term];
            let df = postings.len() as u64;
            let (occ_bytes, skip_bytes) = run_sizes[i];
            let doc_run_off = cursor;
            let occ_run_off = doc_run_off + 12 * df + 4;
            let skip_run_off = occ_run_off + occ_bytes;
            directory.push((doc_run_off, skip_run_off, occ_run_off, df as u32));
            let mut occ_stage: Vec<u8> = Vec::with_capacity(occ_bytes as usize);
            let mut skip_l0: Vec<u8> = Vec::new();
            let mut skip = SkipRunBuilder::new();
            let mut occ_start = 0u32;
            for p in postings {
                let dl = field.doc_lengths[p.doc_id as usize];
                occ_start = push_posting_v5(
                    w,
                    &mut occ_stage,
                    &mut skip,
                    &mut skip_l0,
                    p.doc_id,
                    p.tf,
                    &p.offsets,
                    dl,
                    occ_start,
                )?;
            }
            write_u32(w, occ_start)?; // sentinel
            w.write_all(&occ_stage)?;
            let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
            debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
            debug_assert_eq!(skip_bytes, skip_run_size(l0_bytes, &l1));
            write_skip_run(w, &skip_l0, &l1)?;
            cursor = skip_run_off + skip_bytes;
        }
        Ok(directory)
    }

    /// Write one field's directory section: fixed-stride 34 B entries
    /// (binary-searchable), then the term blob.
    pub(crate) fn write_field_directory<W: Write>(
        w: &mut W,
        terms: &[&String],
        directory: &[(u64, u64, u64, u32)],
    ) -> io::Result<()> {
        write_u32(w, terms.len() as u32)?;
        let mut blob_off = 0u64; // relative to the term blob start
        for (term, &(doc_off, skip_off, occ_off, df)) in terms.iter().zip(directory.iter()) {
            write_u64(w, doc_off)?;
            write_u64(w, skip_off)?;
            write_u64(w, occ_off)?;
            write_u32(w, df)?;
            write_u32(w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
            write_u16(w, term.len() as u16)?;
            blob_off += term.len() as u64;
        }
        for term in terms {
            w.write_all(term.as_bytes())?;
        }
        Ok(())
    }

    /// The v5 layout (see the module docs and `docs/block-max.md`).
    /// Sections in file order, all offsets precomputed and written into
    /// the fixed header so the reader never walks the file to find them:
    ///
    /// ```text
    /// magic "TVBM2505" | header (total_length, section offsets, n_slots)
    /// doc_lengths (n_slots x u32)
    /// texts (n_slots x (u32 len | bytes), len == u32::MAX when absent)
    /// text_index (n_slots x (u64 offset, u32 len))   <- on-disk text directory
    /// lineages (n_slots x (u8 flag + 24B))
    /// postings (u32 n_terms, then per term, sorted by term:
    ///   doc run (df x 12B (doc_id, tf, occ_start) + 4B sentinel occ_start)
    ///   occurrence run (8B (start, end) pairs, posting order)
    ///   skip run (see SkipRunBuilder))
    /// directory (u32 n_terms, then n_terms x 34B fixed-stride entries
    ///   (u64 doc_run_off, u64 skip_run_off, u64 occ_run_off, u32 df,
    ///   u32 blob_off, u16 term_len), then the term blob)
    /// ```
    pub(crate) fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        if !self.map_integers.is_empty() || !self.map_unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve integer map columns",
            ));
        }

        if !self.unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve unsigned integer columns",
            ));
        }
        if !self.sources.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "v5 cannot preserve protobuf sources",
            ));
        }
        if self.fields.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries exactly one field; multi-field stores write v6",
            ));
        }
        if !self.facets.is_empty() || !self.numerics.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries no columns; column-bearing stores write v7",
            ));
        }
        if self.fields[0].positions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries no token positions; positional stores write v7",
            ));
        }
        let field = &self.fields[0];
        let n_slots = self.texts.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let terms = Self::sorted_terms(field);
        let run_sizes = Self::field_run_sizes(field, &terms)?;
        let postings_size = Self::field_postings_size(field, &terms, &run_sizes);

        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + 4 * n_slots;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + postings_size;

        w.write_all(MAGIC_V5)?;
        write_u64(w, field.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, n_slots as u32)?;
        Self::write_field_doc_lengths(w, field)?;
        self.write_shared_sections(w, texts_off)?;
        let directory = Self::write_field_postings(w, field, &terms, &run_sizes, postings_off)?;
        Self::write_field_directory(w, &terms, &directory)?;
        Ok(())
    }

    /// The v6 layout (`TVBM2506`, see `docs/multi-field.md`).
    /// Variable-length header holding an explicit section table, shared
    /// sections, then one doc_lengths/postings/directory group per
    /// field in field-id order. Per-field postings and directory
    /// sections keep the v5 shape, but the directory's run offsets are
    /// RELATIVE to the field's postings section start, and text_index
    /// entries are relative to the texts section start (the v4 blob
    /// lesson generalized: sections survive relocation). Blob offsets
    /// stay blob-relative. Every section is located by an explicit
    /// header offset, nothing derived by arithmetic.
    ///
    /// ```text
    /// magic "TVBM2506"
    /// u32 n_fields | u32 n_slots
    /// u64 texts_off | u64 text_index_off | u64 lineages_off
    /// field table, n_fields entries:
    ///   u16 name_len | name bytes
    ///   u64 analysis_fingerprint | u64 total_length
    ///   u64 doc_lengths_off | u64 postings_off | u64 directory_off
    /// texts | text_index | lineages          <- v5 bytes (index rebased)
    /// per field: doc_lengths | postings | directory   <- v5-shape sections
    /// ```
    ///
    /// A store with declared facet or numeric fields writes v7
    /// (`TVBM2507`) instead: the same layout with a kinded column
    /// table appended to the header (u32 n_columns, then per column:
    /// u16 name_len | name | u8 kind | kind payload — kind 0 facet:
    /// u32 n_values | u64 dict_off | u64 ords_off; kind 1 f64: u64
    /// min_bits | u64 max_bits | u64 vals_off) and, after the last
    /// field group in table order, per facet a dict section (n_values
    /// x (u16 len | value bytes), ordinal order) and an ords section
    /// (n_slots x u32, [`FACET_ABSENT`] = no value), then per numeric
    /// a vals section (n_slots x f64 bits, NaN = no value). Every
    /// column-less byte is identical to the v6 writer's, by
    /// construction (the additions are gated, not forked).
    pub fn write_v6_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v6_ordered(w, true)
    }

    /// [`Self::write_v6_to`] with dictionaries in first-seen order — the
    /// layout every file before 2026-09-02 has. A legacy oracle for the
    /// tests that prove such files still open and serve, and refuse
    /// string ordering by name (`docs/prefix-terms.md`); production
    /// writes always use byte order.
    pub fn write_v6_first_seen_dictionaries<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v6_ordered(w, false)
    }

    pub(crate) fn write_v6_ordered<W: Write>(&self, w: &mut W, byte_order: bool) -> io::Result<()> {
        let n_slots = self.texts.len() as u64;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let has_columns = !self.facets.is_empty()
            || !self.numerics.is_empty()
            || !self.map_facets.is_empty()
            || !self.map_numerics.is_empty()
            || !self.map_integers.is_empty()
            || !self.map_unsigned_integers.is_empty()
            || !self.integers.is_empty()
            || !self.unsigned_integers.is_empty()
            || !self.geos.is_empty()
            || self.binding.is_some()
            || self.derived.is_some()
            || !self.sources.is_empty()
            || self.fields.iter().any(|f| f.positions || f.sentences);
        let column_table_size: u64 = if !has_columns {
            0
        } else {
            4 + self
                .facets
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 1 + 4 + 8 + 8)
                .sum::<u64>()
                + self
                    .numerics
                    .iter()
                    .map(|n| 2 + n.name.len() as u64 + 1 + 8 + 8 + 8)
                    .sum::<u64>()
                + self
                    .map_facets
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 4 + 8 * 4)
                    .sum::<u64>()
                + self
                    .map_numerics
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 8 * 3)
                    .sum::<u64>()
                + self
                    .map_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                    .sum::<u64>()
                + self
                    .map_unsigned_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                    .sum::<u64>()
                + self
                    .integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .unsigned_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .geos
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 5)
                    .sum::<u64>()
                + self
                    .fields
                    .iter()
                    .filter(|f| f.positions)
                    .map(|f| 2 + (POSITIONS_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
                + self
                    .fields
                    .iter()
                    .filter(|f| f.sentences)
                    .map(|f| 2 + (SENTENCES_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
                + binding_entry_size(self.binding.as_ref())
                + derived_entry_size(self.derived.as_ref())
                + if self.sources.is_empty() {
                    0
                } else {
                    SOURCE_ENTRY_BYTES
                }
        };
        let header_size: u64 = 8
            + 4
            + 4
            + 8 * 3
            + self
                .fields
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 8 * 5)
                .sum::<u64>()
            + column_table_size;
        // Size pass per field.
        let mut field_terms: Vec<Vec<&String>> = Vec::with_capacity(self.fields.len());
        let mut field_runs: Vec<Vec<(u64, u64)>> = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let terms = Self::sorted_terms(field);
            let run_sizes = Self::field_run_sizes(field, &terms)?;
            field_terms.push(terms);
            field_runs.push(run_sizes);
        }
        let texts_off = header_size;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let mut cursor = lineages_off + lineages_size;
        // (doc_lengths_off, postings_off, directory_off) per field.
        let mut section_offs: Vec<(u64, u64, u64)> = Vec::with_capacity(self.fields.len());
        for (fi, field) in self.fields.iter().enumerate() {
            let doc_lengths_off = cursor;
            let postings_off = doc_lengths_off + 4 * n_slots;
            let directory_off =
                postings_off + Self::field_postings_size(field, &field_terms[fi], &field_runs[fi]);
            section_offs.push((doc_lengths_off, postings_off, directory_off));
            cursor = directory_off + Self::field_directory_size(&field_terms[fi]);
        }
        // (dict_off, ords_off) per facet, then vals_off per numeric,
        // after the last field group — column-table order.
        let mut facet_offs: Vec<(u64, u64)> = Vec::with_capacity(self.facets.len());
        for facet in &self.facets {
            let dict_off = cursor;
            let dict_size: u64 = facet.dict.iter().map(|v| 2 + v.len() as u64).sum();
            let ords_off = dict_off + dict_size;
            facet_offs.push((dict_off, ords_off));
            cursor = ords_off + 4 * n_slots;
        }
        let mut numeric_offs: Vec<u64> = Vec::with_capacity(self.numerics.len());
        for _ in &self.numerics {
            numeric_offs.push(cursor);
            cursor += 8 * n_slots;
        }
        // (keys_off, values_off, offsets_off, pairs_off) per map-facet
        // column; the offsets section holds n_slots + 1 prefix sums so
        // a document's pair count is off[i+1] - off[i].
        let mut map_facet_offs: Vec<(u64, u64, u64, u64)> =
            Vec::with_capacity(self.map_facets.len());
        for c in &self.map_facets {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64).sum();
            let values_off = keys_off + keys_size;
            let values_size: u64 = c.values.iter().map(|v| 2 + v.len() as u64).sum();
            let offsets_off = values_off + values_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_facet_offs.push((keys_off, values_off, offsets_off, pairs_off));
            cursor = pairs_off + 8 * total_pairs;
        }
        // (keys_off, offsets_off, pairs_off) per map-numeric column;
        // the key dict entries carry per-key min/max bound metadata.
        let mut map_numeric_offs: Vec<(u64, u64, u64)> =
            Vec::with_capacity(self.map_numerics.len());
        for c in &self.map_numerics {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64 + 16).sum();
            let offsets_off = keys_off + keys_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_numeric_offs.push((keys_off, offsets_off, pairs_off));
            cursor = pairs_off + 12 * total_pairs;
        }
        // Integer values followed by their explicit presence bitmap.
        let mut integer_offs: Vec<u64> = Vec::with_capacity(self.integers.len());
        for _ in &self.integers {
            integer_offs.push(cursor);
            cursor += 8 * n_slots + n_slots.div_ceil(8);
        }
        // Geo sections follow the integer groups.
        let mut geo_offs: Vec<u64> = Vec::with_capacity(self.geos.len());
        for _ in &self.geos {
            geo_offs.push(cursor);
            cursor += 16 * n_slots;
        }
        // (field id, section_off, total occurrences) per positional
        // field, last in table order: kind 7 appends after every
        // earlier kind for the same reason kinds 4 and 5 did.
        let mut positions_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, field) in self.fields.iter().enumerate() {
            if !field.positions {
                continue;
            }
            let total: u64 = field_terms[fi]
                .iter()
                .map(|t| {
                    field.postings[*t]
                        .iter()
                        .map(|p| p.positions.len() as u64)
                        .sum::<u64>()
                })
                .sum();
            positions_offs.push((fi, cursor, total));
            cursor += positions_section_size(field_terms[fi].len() as u64, total);
        }
        // Kind-8 sentence sections follow the positions sections, one per
        // sentence field in table order (docs/highlighting.md).
        let mut sentences_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, field) in self.fields.iter().enumerate() {
            if !field.sentences {
                continue;
            }
            let total: u64 = field.doc_sentences.iter().map(|s| s.len() as u64).sum();
            sentences_offs.push((fi, cursor, total));
            cursor += sentences_section_size(n_slots, total);
        }

        let mut unsigned_offs = Vec::with_capacity(self.unsigned_integers.len());
        for _ in &self.unsigned_integers {
            unsigned_offs.push(cursor);
            cursor += 8 * n_slots + n_slots.div_ceil(8);
        }
        let map_integer_sections =
            integer_map_sections(&self.map_integers, n_slots as u32, &mut cursor)?;
        let map_unsigned_sections =
            integer_map_sections(&self.map_unsigned_integers, n_slots as u32, &mut cursor)?;
        let source_section = if self.sources.is_empty() {
            None
        } else {
            Some((cursor, self.sources.encoded_len(n_slots as u32)?))
        };

        w.write_all(if has_columns { MAGIC_V7 } else { MAGIC_V6 })?;
        write_u32(w, self.fields.len() as u32)?;
        write_u32(w, n_slots as u32)?;
        write_u64(w, texts_off)?;
        write_u64(w, text_index_off)?;
        write_u64(w, lineages_off)?;
        for (field, &(dl_off, p_off, d_off)) in self.fields.iter().zip(&section_offs) {
            write_u16(w, field.name.len() as u16)?;
            w.write_all(field.name.as_bytes())?;
            write_u64(w, field.analysis_fingerprint)?;
            write_u64(w, field.total_length)?;
            write_u64(w, dl_off)?;
            write_u64(w, p_off)?;
            write_u64(w, d_off)?;
        }
        if has_columns {
            write_u32(
                w,
                (self.facets.len()
                    + self.numerics.len()
                    + self.map_facets.len()
                    + self.map_numerics.len()
                    + self.map_integers.len()
                    + self.map_unsigned_integers.len()
                    + self.integers.len()
                    + self.unsigned_integers.len()
                    + self.geos.len()
                    + positions_offs.len()
                    + sentences_offs.len()
                    + usize::from(self.binding.is_some())
                    + usize::from(self.derived.is_some())
                    + usize::from(!self.sources.is_empty())) as u32,
            )?;
            for (facet, &(dict_off, ords_off)) in self.facets.iter().zip(&facet_offs) {
                write_u16(w, facet.name.len() as u16)?;
                w.write_all(facet.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_FACET])?;
                write_u32(w, facet.dict.len() as u32)?;
                write_u64(w, dict_off)?;
                write_u64(w, ords_off)?;
            }
            for (numeric, &vals_off) in self.numerics.iter().zip(&numeric_offs) {
                let (min, max) = numeric.min_max();
                write_u16(w, numeric.name.len() as u16)?;
                w.write_all(numeric.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_F64])?;
                write_u64(w, min.to_bits())?;
                write_u64(w, max.to_bits())?;
                write_u64(w, vals_off)?;
            }
            for (c, &(keys_off, values_off, offsets_off, pairs_off)) in
                self.map_facets.iter().zip(&map_facet_offs)
            {
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_MAP_FACET])?;
                write_u32(w, c.keys.len() as u32)?;
                write_u32(w, c.values.len() as u32)?;
                write_u64(w, keys_off)?;
                write_u64(w, values_off)?;
                write_u64(w, offsets_off)?;
                write_u64(w, pairs_off)?;
            }
            for (c, &(keys_off, offsets_off, pairs_off)) in
                self.map_numerics.iter().zip(&map_numeric_offs)
            {
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_MAP_F64])?;
                write_u32(w, c.keys.len() as u32)?;
                write_u64(w, keys_off)?;
                write_u64(w, offsets_off)?;
                write_u64(w, pairs_off)?;
            }
            for (c, &vals_off) in self.integers.iter().zip(&integer_offs) {
                let (min, max) = c.min_max();
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_I64_PRESENT])?;
                write_u64(w, min as u64)?;
                write_u64(w, max as u64)?;
                write_u64(w, vals_off)?;
            }
            for (c, &vals_off) in self.geos.iter().zip(&geo_offs) {
                let (min_lat, max_lat, min_lon, max_lon) = c.bbox();
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_GEO])?;
                write_u64(w, min_lat.to_bits())?;
                write_u64(w, max_lat.to_bits())?;
                write_u64(w, min_lon.to_bits())?;
                write_u64(w, max_lon.to_bits())?;
                write_u64(w, vals_off)?;
            }
            for &(fi, off, total) in &positions_offs {
                let name = format!("{POSITIONS_ENTRY_PREFIX}{}", self.fields[fi].name);
                write_u16(w, name.len() as u16)?;
                w.write_all(name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_POSITIONS])?;
                write_u64(w, off)?;
                write_u64(w, total)?;
            }
            for &(fi, off, total) in &sentences_offs {
                let name = format!("{SENTENCES_ENTRY_PREFIX}{}", self.fields[fi].name);
                write_u16(w, name.len() as u16)?;
                w.write_all(name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_SENTENCES])?;
                write_u64(w, off)?;
                write_u64(w, total)?;
            }
            for (column, &off) in self.unsigned_integers.iter().zip(&unsigned_offs) {
                let (min, max) = column.min_max();
                write_u16(w, column.name.len() as u16)?;
                w.write_all(column.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_U64])?;
                write_u64(w, min)?;
                write_u64(w, max)?;
                write_u64(w, off)?;
            }
            write_integer_map_entries(
                w,
                &self.map_integers,
                &map_integer_sections,
                COLUMN_KIND_MAP_I64,
            )?;
            write_integer_map_entries(
                w,
                &self.map_unsigned_integers,
                &map_unsigned_sections,
                COLUMN_KIND_MAP_U64,
            )?;
            write_binding_entry(w, self.binding.as_ref())?;
            write_derived_entry(w, self.derived.as_ref())?;
            write_source_entry(w, source_section)?;
        }
        self.write_shared_sections(w, 0)?;
        for (fi, field) in self.fields.iter().enumerate() {
            Self::write_field_doc_lengths(w, field)?;
            let directory =
                Self::write_field_postings(w, field, &field_terms[fi], &field_runs[fi], 0)?;
            Self::write_field_directory(w, &field_terms[fi], &directory)?;
        }
        for facet in &self.facets {
            write_facet_column(w, facet, n_slots as usize, byte_order)?;
        }
        for numeric in &self.numerics {
            for slot in 0..n_slots as usize {
                write_u64(
                    w,
                    numeric
                        .vals
                        .get(slot)
                        .copied()
                        .unwrap_or(f64::NAN)
                        .to_bits(),
                )?;
            }
        }
        for c in &self.map_facets {
            write_map_facet_column(w, c, n_slots as usize, byte_order)?;
        }
        for c in &self.map_numerics {
            write_map_numeric_column(w, c, n_slots as usize, byte_order)?;
        }
        for c in &self.integers {
            c.write(w, n_slots as usize)?;
        }
        for c in &self.geos {
            for slot in 0..n_slots as usize {
                let (lat, lon) = c.vals.get(slot).copied().unwrap_or(GEO_ABSENT);
                write_u64(w, lat.to_bits())?;
                write_u64(w, lon.to_bits())?;
            }
        }
        for &(fi, _, total) in &positions_offs {
            let field = &self.fields[fi];
            let terms = &field_terms[fi];
            write_u32(w, terms.len() as u32)?;
            // base[i]: occurrences before directory entry i, in the
            // directory's (sorted) term order — the order the occurrence
            // runs were laid out in, so the two stay parallel.
            let mut base = 0u64;
            for term in terms.iter() {
                write_u32(w, u32::try_from(base).expect("positions exceed u32"))?;
                base += field.postings[*term]
                    .iter()
                    .map(|p| p.positions.len() as u64)
                    .sum::<u64>();
            }
            write_u32(w, u32::try_from(base).expect("positions exceed u32"))?;
            debug_assert_eq!(base, total);
            for term in terms.iter() {
                for p in &field.postings[*term] {
                    for &ordinal in &p.positions {
                        write_u32(w, ordinal)?;
                    }
                }
            }
        }
        for &(fi, _, total) in &sentences_offs {
            let field = &self.fields[fi];
            write_u32(w, n_slots as u32)?;
            let mut base = 0u64;
            for slot in 0..n_slots as usize {
                write_u32(w, u32::try_from(base).expect("sentences exceed u32"))?;
                base += field.doc_sentences.get(slot).map_or(0, |s| s.len() as u64);
            }
            write_u32(w, u32::try_from(base).expect("sentences exceed u32"))?;
            debug_assert_eq!(base, total);
            for slot in 0..n_slots as usize {
                if let Some(spans) = field.doc_sentences.get(slot) {
                    for &(start, end) in spans {
                        write_u32(w, start)?;
                        write_u32(w, end)?;
                    }
                }
            }
        }
        for column in &self.unsigned_integers {
            column.write(w, n_slots as usize)?;
        }
        for column in &self.map_integers {
            column.write(w, n_slots as u32)?;
        }
        for column in &self.map_unsigned_integers {
            column.write(w, n_slots as u32)?;
        }
        if source_section.is_some() {
            self.sources.write(w, n_slots as u32)?;
        }
        Ok(())
    }

    /// Write the v4 format. Exists for benchmarking (v4-vs-v5 scorer
    /// comparisons) and migration checks; new shards are always v5.
    pub fn write_v4_for_bench<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v4_to(w)
    }

    /// The v3/v4 layout. Sections in file order, all offsets precomputed
    /// and written into the fixed header so the reader never walks the
    /// file to find them:
    ///
    /// ```text
    /// magic "TVBM2504" | header (total_length, section offsets, n_slots)
    /// doc_lengths (n_slots x u32)
    /// texts (n_slots x (u32 len | bytes), len == u32::MAX when absent)
    /// text_index (n_slots x (u64 offset, u32 len))   <- on-disk text directory
    /// lineages (n_slots x (u8 flag + 24B))
    /// postings (u32 n_terms, then per-term entries, sorted)
    /// directory (u32 n_terms, then n_terms x 18B fixed-stride entries
    ///   (u64 postings_off, u32 df, u32 blob_off, u16 term_len), then the
    ///   term blob) — binary-searchable by term
    /// ```
    pub(crate) fn write_v4_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        if !self.map_integers.is_empty() || !self.map_unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve integer map columns",
            ));
        }

        if !self.unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve unsigned integer columns",
            ));
        }
        if !self.sources.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy writer cannot preserve protobuf sources",
            ));
        }
        if self.fields.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries exactly one field",
            ));
        }
        if self.fields[0].positions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries no token positions; positional stores write v7",
            ));
        }
        if !self.facets.is_empty() || !self.numerics.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries no columns; column-bearing stores write v7",
            ));
        }
        let field = &self.fields[0];
        let n_slots = self.texts.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_size = 4 * n_slots;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let terms = Self::sorted_terms(field);
        let postings_size: u64 = 4 + terms
            .iter()
            .map(|t| {
                4 + t.len() as u64
                    + 4
                    + field.postings[*t]
                        .iter()
                        .map(|p| 12 + 8 * p.offsets.len() as u64)
                        .sum::<u64>()
            })
            .sum::<u64>();

        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + doc_lengths_size;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + postings_size;

        w.write_all(MAGIC_V4)?;
        write_u64(w, field.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, n_slots as u32)?;
        Self::write_field_doc_lengths(w, field)?;
        self.write_shared_sections(w, texts_off)?;

        // postings (+ directory entries)
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = postings_off + 4;
        for term in &terms {
            let postings = &field.postings[*term];
            directory.push((cursor, postings.len() as u32));
            write_str(w, term)?;
            write_u32(w, postings.len() as u32)?;
            cursor += 4 + term.len() as u64 + 4;
            for p in postings {
                write_u32(w, p.doc_id)?;
                write_u32(w, p.tf)?;
                write_u32(w, p.offsets.len() as u32)?;
                for &(start, end) in &p.offsets {
                    write_u32(w, start)?;
                    write_u32(w, end)?;
                }
                cursor += 12 + 8 * p.offsets.len() as u64;
            }
        }
        // directory: fixed-stride entries (binary-searchable), then the
        // term blob.
        write_u32(w, terms.len() as u32)?;
        let mut blob_off = 0u64; // relative to the term blob start (v4)
        for (term, &(offset, df)) in terms.iter().zip(directory.iter()) {
            write_u64(w, offset)?;
            write_u32(w, df)?;
            write_u32(w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
            write_u16(w, term.len() as u16)?;
            blob_off += term.len() as u64;
        }
        for term in &terms {
            w.write_all(term.as_bytes())?;
        }
        Ok(())
    }

    /// A single-field ("body") store from already-decoded parts — the
    /// shape every pre-v6 read path produces.
    pub(crate) fn from_single_field(
        postings: std::collections::BTreeMap<String, Vec<Posting>>,
        doc_lengths: Vec<u32>,
        total_length: u64,
        texts: Vec<Option<String>>,
        lineages: Vec<Option<DocLineage>>,
    ) -> Self {
        Self {
            fields: vec![FieldStore {
                name: "body".to_string(),
                analysis_fingerprint: 0,
                postings,
                doc_lengths,
                total_length,
                positions: false,
                sentences: false,
                doc_sentences: Vec::new(),
            }],
            texts,
            lineages,
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            map_integers: Vec::new(),
            map_unsigned_integers: Vec::new(),
            integers: Vec::new(),
            unsigned_integers: Vec::new(),
            geos: Vec::new(),
            binding: None,
            derived: None,
            sources: Default::default(),
        }
    }

    pub(crate) fn read_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let original = *r;
        let magic = take(r, 8)?;
        if magic == MAGIC_V8 {
            // Rebuild the full-file view (the trailer's offsets are
            // absolute), check the whole envelope and every CRC — the
            // bytes are all in memory anyway — then read the payload
            // as its base version.
            let mut full = Vec::with_capacity(8 + r.len());
            full.extend_from_slice(MAGIC_V8);
            full.extend_from_slice(r);
            let table = parse_integrity(&full)?;
            for e in &table.entries {
                let got = crate::wal::crc32(&full[e.off as usize..(e.off + e.len) as usize]);
                if got != e.crc {
                    return Err(invalid(&format!(
                        "section {} CRC mismatch: stored {:08x}, computed {got:08x}",
                        e.name, e.crc
                    )));
                }
            }
            validate_structure_v6(&full[..table.payload_len as usize], table.base_v7)?;
            let mut payload = &full[8..table.payload_len as usize];
            return Self::read_v6v7_from(&mut payload, table.base_v7);
        }
        if magic == MAGIC_V7 {
            validate_structure_v6(original, true)?;
            return Self::read_v6v7_from(r, true);
        }
        if magic == MAGIC_V6 {
            validate_structure_v6(original, false)?;
            return Self::read_v6v7_from(r, false);
        }
        if magic == MAGIC_V5 {
            return Self::read_v5_from(r);
        }
        if magic == MAGIC_V4 || magic == MAGIC_V3 {
            return Self::read_v3_from(r);
        }
        let has_lineage = if magic == MAGIC_V2 {
            true
        } else if magic == MAGIC_V1 {
            false
        } else {
            return Err(invalid("bad magic"));
        };
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
        let total_length = read_u64(r)?;
        let n_texts = read_u32(r)? as usize;
        if n_texts != n_slots {
            return Err(invalid("text slots != length slots"));
        }
        let mut texts = Vec::with_capacity(n_texts);
        for _ in 0..n_texts {
            let len = read_u32(r)?;
            if len == u32::MAX {
                texts.push(None);
            } else {
                let bytes = take(r, len as usize)?;
                texts.push(Some(
                    String::from_utf8(bytes.to_vec())
                        .map_err(|_| invalid("invalid utf-8 in doc text"))?,
                ));
            }
        }
        let mut lineages = vec![None; n_texts];
        if has_lineage {
            for lineage in lineages.iter_mut() {
                let present = take(r, 1)?[0];
                if present != 0 {
                    let parent_id = read_u64(r)?;
                    let group_id = read_u64(r)?;
                    let span_start = read_u32(r)?;
                    let span_end = read_u32(r)?;
                    *lineage = Some(DocLineage {
                        parent_id,
                        group_id,
                        span_start,
                        span_end,
                    });
                }
            }
        }
        let n_terms = read_u32(r)? as usize;
        let mut postings = std::collections::BTreeMap::new();
        for _ in 0..n_terms {
            let term_len = read_u32(r)? as usize;
            let term = String::from_utf8(take(r, term_len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in term"))?;
            let n_postings = read_u32(r)? as usize;
            let mut plist = Vec::with_capacity(n_postings);
            for _ in 0..n_postings {
                let doc_id = read_u32(r)?;
                let tf = read_u32(r)?;
                let n_offsets = read_u32(r)? as usize;
                let mut offsets = Vec::with_capacity(n_offsets);
                for _ in 0..n_offsets {
                    offsets.push((read_u32(r)?, read_u32(r)?));
                }
                plist.push(Posting {
                    doc_id,
                    tf,
                    offsets,
                    positions: Vec::new(),
                });
            }
            postings.insert(term, plist);
        }
        if !r.is_empty() {
            return Err(invalid("trailing bytes"));
        }
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
    }
}
