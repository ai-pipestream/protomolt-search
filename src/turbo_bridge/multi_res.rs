//! Single-pass multi-resolution (Matryoshka) embedding rows.

use tonic::Status;

/// Single-pass multi-resolution document embedding output.
#[derive(Clone, Debug, PartialEq)]
pub struct MultiResVectorOutput {
    /// Matryoshka L2-normalized prefix slices `(prefix_dim, values)`.
    pub prefixes: Vec<(u32, Vec<f32>)>,
    /// Full-dimension vector.
    pub full: Vec<f32>,
    /// Precomputed inverse L2 norm (`1.0 / ||raw||_2`) before normalization.
    pub inv_norm: f32,
    /// Optional packed `TurboQuant` code bytes produced in the same pass.
    pub quantized_codes: Option<Vec<u8>>,
    /// Per-sentence pooled vectors aligned with `AnalyzedField.sentences`.
    pub sentence_vectors: Vec<Vec<f32>>,
    /// Deterministic SHA-256 digest of the canonical little-endian `full` vector bytes.
    pub sha256: [u8; 32],
}

/// Active-tier flat vectors, active dimension, and retained per-slot
/// [`MultiResVectorOutput`] rows produced from a multi-row ingest batch.
pub type MatryoshkaBatchOutput = (Vec<f32>, usize, Vec<(u32, MultiResVectorOutput)>);

impl MultiResVectorOutput {
    /// Derives a multi-resolution output from a single full vector and optional
    /// sentence spans/vectors, computing normalized Matryoshka prefixes and the
    /// streaming SHA-256 row digest in one pass.
    pub fn from_full_and_sentences(
        raw_full: &[f32],
        prefix_dims: &[u32],
        quantized_codes: Option<Vec<u8>>,
        sentence_vectors: Vec<Vec<f32>>,
    ) -> Self {
        let norm_sq: f32 = raw_full.iter().map(|v| v * v).sum();
        let inv_norm = if norm_sq > 0.0 {
            1.0 / norm_sq.sqrt()
        } else {
            0.0
        };
        let full: Vec<f32> = if inv_norm > 0.0 {
            raw_full.iter().map(|v| v * inv_norm).collect()
        } else {
            raw_full.to_vec()
        };
        let mut prefixes = Vec::with_capacity(prefix_dims.len());
        for &dim in prefix_dims {
            let take = (dim as usize).min(full.len());
            let slice = &raw_full[..take];
            let p_sq: f32 = slice.iter().map(|v| v * v).sum();
            let p_inv = if p_sq > 0.0 { 1.0 / p_sq.sqrt() } else { 0.0 };
            let vec: Vec<f32> = slice.iter().map(|v| v * p_inv).collect();
            prefixes.push((take as u32, vec));
        }
        let mut digest = turbo_sha256::Sha256::new();
        for &value in &full {
            digest.update(&value.to_le_bytes());
        }
        let sha256 = digest.finish().0;
        Self {
            prefixes,
            full,
            inv_norm,
            quantized_codes,
            sentence_vectors,
            sha256,
        }
    }

    /// Returns the L2-normalized slice for `target_dim` (matching either a
    /// precomputed prefix tier or the full vector).
    pub fn tier(&self, target_dim: u32) -> Option<&[f32]> {
        if self.full.len() == target_dim as usize {
            return Some(&self.full);
        }
        self.prefixes
            .iter()
            .find(|(d, _)| *d == target_dim)
            .map(|(_, v)| v.as_slice())
    }

    /// Slices a flat row-major embedding batch of `raw_dim` into per-row
    /// [`MultiResVectorOutput`] records and extracts the active indexing tier.
    pub fn project_batch(
        vectors: &[f32],
        raw_dim: usize,
        prefix_dims: &[u32],
        active_dim: Option<u32>,
        start_slot: usize,
    ) -> Result<MatryoshkaBatchOutput, Status> {
        let mut all_prefixes = prefix_dims.to_vec();
        if let Some(ad) = active_dim {
            if ad == 0 || ad as usize > raw_dim {
                return Err(Status::invalid_argument(format!(
                    "matryoshka_active_dim {ad} exceeds input vector dim {raw_dim}"
                )));
            }
            if !all_prefixes.contains(&ad) && (ad as usize) < raw_dim {
                all_prefixes.push(ad);
            }
        }
        let effective_dim = active_dim.map_or(raw_dim, |d| d as usize);
        let row_count = vectors.len() / raw_dim;
        let mut active_flat = Vec::with_capacity(row_count * effective_dim);
        let mut multi_rows = Vec::with_capacity(row_count);
        for (i, row) in vectors.chunks_exact(raw_dim).enumerate() {
            let slot = u32::try_from(start_slot + i)
                .map_err(|_| Status::resource_exhausted("shard local slot overflow"))?;
            let out = Self::from_full_and_sentences(row, &all_prefixes, None, Vec::new());
            let tier = out.tier(effective_dim as u32).ok_or_else(|| {
                Status::invalid_argument(format!(
                    "matryoshka active tier {effective_dim} missing from row"
                ))
            })?;
            active_flat.extend_from_slice(tier);
            multi_rows.push((slot, out));
        }
        Ok((active_flat, effective_dim, multi_rows))
    }
}
