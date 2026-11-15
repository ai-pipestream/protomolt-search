//! The HyperLogLog cardinality sketch.

use super::hash::hash_base;

/// A HyperLogLog cardinality sketch over 64-bit hashes. Precision p = 14 →
/// 16384 one-byte registers (16 KiB), standard error ~0.8%. Registers merge
/// by cell-wise maximum, which is what makes the novelty rate computable
/// without ever materializing the vocabulary. Not thread-safe.
#[derive(Clone)]
pub struct HyperLogLog {
    precision: usize,
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// Sketch precision: log2 of the register count.
    pub const PRECISION: usize = 14;

    /// A fresh, empty sketch of the standard precision.
    pub fn new() -> Self {
        Self::with_precision(Self::PRECISION)
    }

    /// A sketch with `2^precision` registers; precision in [4, 16].
    pub fn with_precision(precision: usize) -> Self {
        assert!((4..=16).contains(&precision));
        Self {
            precision,
            registers: vec![0; 1 << precision],
        }
    }

    /// Adds one occurrence of `term`. Repeated adds of the same term are
    /// idempotent up to hash collisions.
    pub fn add(&mut self, term: &str) {
        let hash = hash_base(term);
        // The top p bits choose the register; the rank is the number of
        // leading zeros of the remaining 64 - p bits, plus one.
        let index = (hash >> (64 - self.precision)) as usize;
        let rank = ((hash << self.precision) | (1u64 << (self.precision - 1))).leading_zeros() + 1;
        let register = &mut self.registers[index];
        if rank as u8 > *register {
            *register = rank as u8;
        }
    }

    /// The estimated number of distinct terms added. Small cardinalities
    /// use linear counting; 64-bit hashes make the large-range correction
    /// irrelevant at any corpus scale this engine will see.
    pub fn estimate(&self) -> f64 {
        let mut inverse_sum = 0.0f64;
        let mut zeros = 0u32;
        for &register in &self.registers {
            inverse_sum += 2.0f64.powi(-i32::from(register));
            if register == 0 {
                zeros += 1;
            }
        }
        let register_count = self.registers.len() as f64;
        let alpha = 0.7213 / (1.0 + 1.079 / register_count);
        let raw = alpha * register_count * register_count / inverse_sum;
        if raw <= 2.5 * register_count && zeros > 0 {
            return register_count * (register_count / f64::from(zeros)).ln();
        }
        raw
    }

    /// Merges `other` into this sketch, cell-wise maximum. Only meaningful
    /// for sketches of the same precision.
    pub fn merge(&mut self, other: &Self) -> Result<(), String> {
        if other.precision != self.precision {
            return Err(format!(
                "precisions differ: {} vs {}",
                other.precision, self.precision
            ));
        }
        for (register, &other_register) in self.registers.iter_mut().zip(&other.registers) {
            *register = (*register).max(other_register);
        }
        Ok(())
    }

    /// The precision this sketch was built with.
    pub fn precision(&self) -> usize {
        self.precision
    }

    /// The registers in register order — the `hll_registers` encoding of
    /// `VocabChannelSnapshot` (a copy, one byte per register).
    pub fn to_bytes(&self) -> Vec<u8> {
        self.registers.clone()
    }

    /// Restores a sketch from [`Self::to_bytes`] output.
    pub fn from_bytes(precision: usize, bytes: &[u8]) -> Result<Self, String> {
        let sketch = Self::with_precision(precision);
        if bytes.len() != sketch.registers.len() {
            return Err(format!(
                "register byte length {} does not match precision {}",
                bytes.len(),
                precision
            ));
        }
        Ok(Self {
            precision,
            registers: bytes.to_vec(),
        })
    }
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}
