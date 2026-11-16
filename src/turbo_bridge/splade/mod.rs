//! Quantized `u8`/`u16` SPLADE 128-doc Block-Max postings and L1 rounding
//! slack traversal.
//!
//! Stores calibrated integer term impacts (`u8` or `u16` widened to `u16` in
//! 128-posting blocks) inside `.bm25` `COLUMN_KIND_SPLADE` (`19`, named
//! `splade:<field>`), enabling zero-copy memory-mapped Block-Max MaxScore/WAND
//! retrieval with an exact L1 query rounding upper-bound certificate.

use std::collections::BTreeMap;
use std::io::{self, Write};

mod cursor;
mod field;
mod reader;

pub use field::SpladeFieldIndex;
pub use reader::SpladeSectionReader;

/// Postings per SPLADE skip block, matching BM25 level-0 blocks (`128`).
pub const SPLADE_BLOCK_SIZE: usize = 128;

/// Magic bytes of `COLUMN_KIND_SPLADE` sections: 12-byte block headers carry
/// per-block max-scale bits and 10-byte postings carry per-document scale bits.
pub const SPLADE_SECTION_MAGIC: &[u8; 8] = b"TVSPLD02";

const SPLADE_BLOCK_STRIDE: usize = 12;
const SPLADE_POSTING_STRIDE: usize = 10;

// Bound an f32 sum independently of term order. gamma_n accounts for the
// worst-case rounding of the n-1 additions; one extra operation also covers
// the much smaller f64 accumulation and division rounding error.
fn conservative_sum(sum: f64, terms: usize) -> f64 {
    let error = terms as f64 * (f64::from(f32::EPSILON) / 2.0);
    if error >= 1.0 {
        f64::INFINITY
    } else {
        sum / (1.0 - error)
    }
}

/// Telemetry returned by a Block-Max SPLADE query evaluation.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SpladeTraversalStats {
    /// Total 128-doc blocks across queried terms.
    pub total_blocks: u64,
    /// Blocks decoded and scored.
    pub blocks_visited: u64,
    /// Blocks skipped completely via their `block_max_impact` upper bound.
    pub blocks_skipped: u64,
    /// Individual `(doc_id, impact)` postings accumulated.
    pub postings_scored: u64,
}

/// Deterministically quantizes positive float SPLADE weights `(term, weight)`
/// into top-`max_terms` integer impacts (`bits = 8` -> `[1..=255]`, `bits = 16`
/// -> `[1..=65535]`), returning `(scale, quantized_terms, l1_rounding_bound)`.
///
/// The L1 rounding upper bound `sum_i |w_i - q_i * scale|` is exact for the
/// emitted vector and bounds the worst-case inner-product underestimation when
/// paired with unit-bounded or L_inf-bounded query weights.
pub fn quantize_splade_impacts(
    weights: &[(String, f32)],
    max_terms: usize,
    bits: u8,
) -> (f32, Vec<(String, u16)>, f32) {
    let mut positive: Vec<(String, f32)> = weights
        .iter()
        .filter(|(t, w)| !t.is_empty() && w.is_finite() && *w > 0.0)
        .cloned()
        .collect();
    if positive.is_empty() || max_terms == 0 {
        return (0.0, Vec::new(), 0.0);
    }
    positive.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    if positive.len() > max_terms {
        positive.truncate(max_terms);
    }
    let max_val = positive[0].1;
    let levels = if bits <= 8 { 255.0_f32 } else { 65535.0_f32 };
    let q_max = if bits <= 8 { 255u16 } else { u16::MAX };
    let scale = max_val / levels;
    if scale <= 0.0 || !scale.is_finite() {
        return (0.0, Vec::new(), 0.0);
    }

    let mut by_term: BTreeMap<String, (u16, f32)> = BTreeMap::new();
    for (term, w) in positive {
        let q = ((w / scale).round().clamp(1.0, f32::from(q_max))) as u16;
        by_term
            .entry(term)
            .and_modify(|e| {
                if q > e.0 {
                    *e = (q, w);
                }
            })
            .or_insert((q, w));
    }

    let mut l1_err = 0.0_f32;
    let mut out = Vec::with_capacity(by_term.len());
    for (term, (q, w)) in by_term {
        let recon = f32::from(q) * scale;
        l1_err += (w - recon).abs();
        out.push((term, q));
    }
    (scale, out, l1_err)
}

#[cfg(test)]
#[path = "../splade_limits_tests.rs"]
mod limits_tests;
