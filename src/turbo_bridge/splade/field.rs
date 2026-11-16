//! Heap-resident SPLADE field index: build, encode, decode and score.

use super::*;

/// Heap-resident quantized SPLADE inverted index for one field on one shard.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpladeFieldIndex {
    /// IEEE-754 `f32::to_bits()` of the baseline quantization scale (`0` when empty).
    pub scale_bits: u32,
    /// Per-document scale overrides; integer impacts are never requantized at merge.
    pub scales: BTreeMap<u32, u32>,
    /// Term -> doc-id-ordered `(doc_id, quantized_impact)` postings.
    pub postings: BTreeMap<String, Vec<(u32, u16)>>,
}

impl SpladeFieldIndex {
    /// Returns `true` when no SPLADE postings have been indexed.
    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    /// Appends one document's quantized SPLADE impacts (`doc_id` strictly ascending).
    pub fn insert_doc(&mut self, doc_id: u32, scale_bits: u32, impacts: &[(String, u16)]) {
        if impacts.is_empty() {
            return;
        }
        let doc_scale = f32::from_bits(scale_bits);
        assert!(
            doc_scale.is_finite() && doc_scale > 0.0,
            "SPLADE impacts require a positive finite scale"
        );
        if self.scale_bits == 0 && doc_scale > 0.0 && doc_scale.is_finite() {
            self.scale_bits = scale_bits;
        }
        if scale_bits != self.scale_bits {
            self.scales.insert(doc_id, scale_bits);
        }
        for (term, raw_q) in impacts {
            if *raw_q == 0 || term.is_empty() {
                continue;
            }
            let q = *raw_q;
            let list = self.postings.entry(term.clone()).or_default();
            if let Some(last) = list.last_mut() {
                if last.0 == doc_id {
                    last.1 = last.1.max(q);
                    continue;
                }
            }
            list.push((doc_id, q));
        }
    }

    /// Per-document quantization scale bits (`f32::to_bits()`).
    pub fn doc_scale_bits(&self, doc: u32) -> u32 {
        self.scales.get(&doc).copied().unwrap_or(self.scale_bits)
    }

    /// Exact serialized byte length of this `TVSPLD02` section.
    pub fn encoded_len(&self) -> u64 {
        self.encoded_len_for_terms(
            self.postings
                .iter()
                .map(|(t, p)| (t.as_str(), p.as_slice())),
        )
    }

    fn encoded_len_for_terms<'a>(
        &self,
        terms: impl Iterator<Item = (&'a str, &'a [(u32, u16)])>,
    ) -> u64 {
        let mut size = 0u64;
        let mut any = false;
        for (term, plist) in terms {
            any = true;
            let n_blocks = plist.len().div_ceil(SPLADE_BLOCK_SIZE) as u64;
            size += 2
                + term.len() as u64
                + 4
                + 4
                + n_blocks * SPLADE_BLOCK_STRIDE as u64
                + (plist.len() as u64) * SPLADE_POSTING_STRIDE as u64;
        }
        if any {
            16 + size
        } else {
            0
        }
    }

    /// Serializes this field's SPLADE postings in canonical `TVSPLD02` format.
    pub fn write<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_terms(
            self.postings.len(),
            self.postings
                .iter()
                .map(|(t, p)| (t.as_str(), p.as_slice())),
            w,
        )
    }

    fn write_terms<'a, W: Write>(
        &self,
        term_count: usize,
        terms: impl Iterator<Item = (&'a str, &'a [(u32, u16)])>,
        w: &mut W,
    ) -> io::Result<()> {
        if term_count == 0 {
            return Ok(());
        }
        w.write_all(SPLADE_SECTION_MAGIC)?;
        w.write_all(&self.scale_bits.to_le_bytes())?;
        w.write_all(&(term_count as u32).to_le_bytes())?;
        for (term, plist) in terms {
            let term_bytes = term.as_bytes();
            w.write_all(&(term_bytes.len() as u16).to_le_bytes())?;
            w.write_all(term_bytes)?;
            w.write_all(&(plist.len() as u32).to_le_bytes())?;
            let n_blocks = plist.len().div_ceil(SPLADE_BLOCK_SIZE);
            w.write_all(&(n_blocks as u32).to_le_bytes())?;
            for chunk in plist.chunks(SPLADE_BLOCK_SIZE) {
                let max_doc = chunk.last().map_or(0, |&(d, _)| d);
                let max_impact = chunk.iter().map(|&(_, q)| q).max().unwrap_or(0);
                let block_len = chunk.len() as u16;
                let max_scale = chunk
                    .iter()
                    .map(|&(d, _)| f32::from_bits(self.doc_scale_bits(d)))
                    .fold(0.0_f32, f32::max);
                w.write_all(&max_doc.to_le_bytes())?;
                w.write_all(&max_impact.to_le_bytes())?;
                w.write_all(&block_len.to_le_bytes())?;
                w.write_all(&max_scale.to_bits().to_le_bytes())?;
            }
            for &(doc_id, q) in plist {
                w.write_all(&doc_id.to_le_bytes())?;
                w.write_all(&q.to_le_bytes())?;
                w.write_all(&self.doc_scale_bits(doc_id).to_le_bytes())?;
            }
        }
        Ok(())
    }

    /// Decodes a `TVSPLD02` SPLADE section back into a [`SpladeFieldIndex`].
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let reader = SpladeSectionReader::open(bytes)?;
        let mut postings = BTreeMap::new();
        let mut scales = BTreeMap::new();
        for entry in &reader.terms {
            let term = std::str::from_utf8(&bytes[entry.term_off..entry.term_off + entry.term_len])
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8 in splade term")
                })?
                .to_string();
            let mut plist = Vec::with_capacity(entry.df as usize);
            for j in 0..entry.df as usize {
                let p = entry.postings_off + j * SPLADE_POSTING_STRIDE;
                let doc_id = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
                let q = u16::from_le_bytes(bytes[p + 4..p + 6].try_into().unwrap());
                let scale = u32::from_le_bytes(bytes[p + 6..p + 10].try_into().unwrap());
                if scale != reader.scale_bits {
                    scales.insert(doc_id, scale);
                }
                plist.push((doc_id, q));
            }
            postings.insert(term, plist);
        }
        Ok(Self {
            scale_bits: reader.scale_bits,
            scales,
            postings,
        })
    }

    /// Evaluates top-`k` documents using 128-doc Block-Max upper bounds and
    /// `query_l1_slack` rounding slack so quantized pruning never drops a true
    /// top-`k` candidate within the calibration bound.
    pub fn score_top_k<F>(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        admitted: F,
    ) -> (Vec<(u32, f32)>, SpladeTraversalStats)
    where
        F: Fn(u32) -> bool,
    {
        let (hits, _, stats) =
            self.score_top_k_with_floor(query_weights, query_l1_slack, 0.0, k, admitted);
        (hits, stats)
    }

    /// Evaluates top-`k` documents seeded with a monotonic cross-shard `initial_floor`.
    pub fn score_top_k_with_floor<F>(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        initial_floor: f32,
        k: usize,
        admitted: F,
    ) -> (Vec<(u32, f32)>, f32, SpladeTraversalStats)
    where
        F: Fn(u32) -> bool,
    {
        let floor = if initial_floor.is_finite() && initial_floor > 0.0 {
            initial_floor
        } else {
            0.0
        };
        if k == 0 || self.postings.is_empty() || query_weights.is_empty() {
            return (Vec::new(), floor, SpladeTraversalStats::default());
        }
        let mut matched_terms: BTreeMap<&str, &[(u32, u16)]> = BTreeMap::new();
        for (term, weight) in query_weights {
            if weight.is_finite() && *weight > 0.0 {
                if let Some(plist) = self.postings.get(term.as_str()) {
                    if !plist.is_empty() {
                        matched_terms.insert(term.as_str(), plist.as_slice());
                    }
                }
            }
        }
        if matched_terms.is_empty() {
            return (Vec::new(), floor, SpladeTraversalStats::default());
        }
        let encoded_len =
            self.encoded_len_for_terms(matched_terms.iter().map(|(&t, &p)| (t, p))) as usize;
        let mut buf = Vec::with_capacity(encoded_len);
        self.write_terms(
            matched_terms.len(),
            matched_terms.iter().map(|(&t, &p)| (t, p)),
            &mut buf,
        )
        .expect("in-memory SpladeFieldIndex query subset encode");
        let reader = SpladeSectionReader::open(&buf).expect("valid encoded SpladeFieldIndex");
        reader.score_top_k_with_floor(query_weights, query_l1_slack, floor, k, admitted)
    }
}
