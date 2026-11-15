//! Address and range normalization shared by the section parsers.

/// Normalize one node address the same way at startup and during hot reload.
pub fn normalize_addr(addr: String) -> String {
    normalize_addrs(vec![addr]).remove(0)
}

/// Both bounds or neither; an inverted range is refused.
pub(super) fn parse_hash_range(
    lo: Option<u64>,
    hi: Option<u64>,
    what: &str,
) -> Result<Option<(u64, u64)>, String> {
    match (lo, hi) {
        (Some(lo), Some(hi)) if lo <= hi => Ok(Some((lo, hi))),
        (Some(lo), Some(hi)) => Err(format!("{what}: range {lo}..={hi} is inverted")),
        (None, None) => Ok(None),
        _ => Err(format!("{what}: pass both bounds or neither")),
    }
}

pub(super) fn normalize_addrs(addrs: Vec<String>) -> Vec<String> {
    addrs
        .into_iter()
        .map(|s| {
            if s.starts_with("http://") || s.starts_with("https://") {
                s
            } else {
                format!("http://{s}")
            }
        })
        .collect()
}

pub(super) fn normalize_analysis_backend(value: String) -> String {
    if matches!(value.trim(), "native" | "native://") {
        crate::analyzer::NATIVE_ANALYSIS_BACKEND.to_string()
    } else {
        normalize_addrs(vec![value]).remove(0)
    }
}
