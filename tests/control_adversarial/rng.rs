//! Deterministic xorshift64 RNG for the adversarial harness. Seeded per test
//! so every trace reproduces exactly; no external crates.

/// xorshift64* is not used here; plain xorshift64 with the classic
/// 13/7/17 shifts, nonzero seed required.
pub struct XorShift64(u64);

impl XorShift64 {
    pub fn new(seed: u64) -> Self {
        assert!(seed != 0, "xorshift64 needs a nonzero seed");
        Self(seed)
    }

    pub fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    /// Uniform value in `0..n` (n > 0).
    pub fn below(&mut self, n: u64) -> u64 {
        assert!(n > 0);
        self.next() % n
    }

    /// True with `pct` percent probability.
    pub fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }
}
