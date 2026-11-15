//! Path code arithmetic.

use super::CODE_BITS;

/// The path code: index per level in a fixed field, root highest.
pub fn encode(path: &[u32], level_bits: u32) -> i64 {
    let mut code: i64 = 0;
    for (level, index) in path.iter().enumerate() {
        let shift = CODE_BITS - level_bits * (level as u32 + 1);
        code |= (i64::from(*index)) << shift;
    }
    code
}

/// The inclusive code range of every path that extends `path`.
pub fn subtree_range(path: &[u32], level_bits: u32) -> (i64, i64) {
    let lo = encode(path, level_bits);
    let used = level_bits * path.len() as u32;
    let width = CODE_BITS - used;
    let span = if width >= 63 {
        i64::MAX
    } else {
        (1i64 << width) - 1
    };
    (lo, lo | span)
}

/// The index at `level` (0 = root) of `code`.
pub fn index_at(code: i64, level: u32, level_bits: u32) -> u32 {
    let shift = CODE_BITS - level_bits * (level + 1);
    ((code >> shift) & ((1i64 << level_bits) - 1)) as u32
}
