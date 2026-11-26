//! Bit-plane shortlist eligibility mirror for [`super::EmbeddedTurboVec`],
//! latching `TURBOVEC_*` environment gates once per process via [`OnceLock`]
//! to match `turbovec::pack::planes_wanted`.

use std::sync::OnceLock;

/// Rows at which turbovec searches an owned image through its bit-plane
/// cache, on a host and geometry whose kernels read it.
pub(super) const STAGED_MIN_ROWS: usize = 32_768;

fn vector_major_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        if std::env::var("TURBOVEC_NO_VECTOR_MAJOR").is_ok_and(|v| v != "0")
            || std::env::var("TURBOVEC_NO_VNNI").is_ok_and(|v| v != "0")
        {
            return false;
        }
        #[cfg(target_arch = "x86_64")]
        {
            std::arch::is_x86_feature_detected!("avx512vbmi")
                && std::arch::is_x86_feature_detected!("avx512vnni")
                && std::arch::is_x86_feature_detected!("avx512bw")
                && std::arch::is_x86_feature_detected!("avx512f")
        }
        #[cfg(target_arch = "aarch64")]
        {
            std::arch::is_aarch64_feature_detected!("dotprod")
        }
        #[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
        {
            false
        }
    })
}

fn vm8_2bit_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| std::env::var_os("TURBOVEC_2BIT_VM8").is_some_and(|v| v == "1"))
}

fn planes_2bit_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| !std::env::var_os("TURBOVEC_2BIT_PLANES").is_some_and(|v| v == "0"))
}

fn planes_4bit_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| !std::env::var_os("TURBOVEC_4BIT_PLANES").is_some_and(|v| v == "0"))
}

/// Mirror `turbovec::pack::planes_wanted` so `staged_top_k` is advertised and
/// reported only when the engine's bit-plane shortlist path actually runs.
pub(super) fn bit_planes_eligible(bits: usize, dim: usize) -> bool {
    if dim == 0 || !dim.is_multiple_of(8) {
        return false;
    }
    let n_byte_groups = (bits * dim) / 8;
    let sign_scans = if cfg!(target_arch = "x86_64") {
        vector_major_enabled()
    } else {
        cfg!(target_arch = "aarch64") && !vm8_2bit_enabled()
    };
    match bits {
        2 => n_byte_groups.is_multiple_of(8) && planes_2bit_enabled() && sign_scans,
        4 => {
            n_byte_groups.is_multiple_of(16)
                && planes_4bit_enabled()
                && sign_scans
                && vector_major_enabled()
        }
        _ => false,
    }
}
