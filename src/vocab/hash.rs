//! The hashes every sketch shares, ported exactly from the Java reference
//! so the sketch bytes match.

/// The splitmix64 increment (the golden ratio as a 64-bit odd constant).
const GOLDEN: u64 = 0x9E37_79B9_7F4A_7C15;
/// FNV-1a 64-bit offset basis.
pub(super) const FNV_OFFSET: u64 = 0xCBF2_9CE4_8422_2325;
/// FNV-1a 64-bit prime.
pub(super) const FNV_PRIME: u64 = 0x0000_0100_0000_01B3;

/// The murmur3 64-bit finalizer: full avalanche in three xorshift-multiply
/// rounds. Ported exactly from the Java reference (wrapping arithmetic, so
/// the hashes — and therefore every sketch byte — match).
pub(super) fn fmix64(mut value: u64) -> u64 {
    value ^= value >> 33;
    value = value.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    value ^= value >> 33;
    value = value.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    value ^= value >> 33;
    value
}

/// The base 64-bit hash of a term: FNV-1a over its UTF-8 bytes, avalanched
/// with the murmur3 finalizer.
pub(super) fn hash_base(term: &str) -> u64 {
    let mut hash = FNV_OFFSET;
    for &b in term.as_bytes() {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    fmix64(hash)
}

/// The hash function of count-min row `row`: one splitmix64 step over the
/// base hash seeded with a per-row stride.
pub(super) fn hash_row(base: u64, row: usize) -> u64 {
    fmix64(base.wrapping_add(GOLDEN.wrapping_mul(row as u64 + 1)))
}
