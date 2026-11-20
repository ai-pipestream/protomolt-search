//! The hardware capability profile a policy's speed evidence was measured on.

use std::io::Read;

use prost::Message;

use crate::pb::DenseHostProfile;

/// The SIMD features the dense kernels dispatch on, detected at runtime.
fn simd_features() -> Vec<String> {
    let mut features: Vec<&str> = Vec::new();
    #[cfg(target_arch = "x86_64")]
    {
        let detected = [
            ("avx2", std::arch::is_x86_feature_detected!("avx2")),
            ("fma", std::arch::is_x86_feature_detected!("fma")),
            ("avx512f", std::arch::is_x86_feature_detected!("avx512f")),
            ("avx512bw", std::arch::is_x86_feature_detected!("avx512bw")),
            (
                "avx512vbmi",
                std::arch::is_x86_feature_detected!("avx512vbmi"),
            ),
            (
                "avx512vnni",
                std::arch::is_x86_feature_detected!("avx512vnni"),
            ),
        ];
        features.extend(detected.iter().filter(|(_, on)| *on).map(|(name, _)| *name));
    }
    #[cfg(target_arch = "aarch64")]
    {
        let detected = [
            ("neon", std::arch::is_aarch64_feature_detected!("neon")),
            (
                "dotprod",
                std::arch::is_aarch64_feature_detected!("dotprod"),
            ),
            ("i8mm", std::arch::is_aarch64_feature_detected!("i8mm")),
            ("fp16", std::arch::is_aarch64_feature_detected!("fp16")),
            ("sve", std::arch::is_aarch64_feature_detected!("sve")),
        ];
        features.extend(detected.iter().filter(|(_, on)| *on).map(|(name, _)| *name));
    }
    features.sort_unstable();
    features.dedup();
    features.into_iter().map(str::to_string).collect()
}

/// Extract a CPU model identifier from `/proc/cpuinfo` text across both x86
/// (`model name`) and mainline ARM64 Linux (`CPU implementer` / `CPU part`).
fn parse_cpuinfo_model(text: &str) -> Option<String> {
    let mut implementer = None;
    let mut arch = None;
    let mut variant = None;
    let mut part = None;
    let mut revision = None;
    for line in text.lines() {
        let Some((raw_key, raw_val)) = line.split_once(':') else {
            continue;
        };
        let key = raw_key.trim();
        let val = raw_val.trim();
        if val.is_empty() {
            continue;
        }
        if key.eq_ignore_ascii_case("model name") {
            return Some(val.to_string());
        }
        match key {
            "CPU implementer" if implementer.is_none() => implementer = Some(val),
            "CPU architecture" if arch.is_none() => arch = Some(val),
            "CPU variant" if variant.is_none() => variant = Some(val),
            "CPU part" if part.is_none() => part = Some(val),
            "CPU revision" if revision.is_none() => revision = Some(val),
            _ => {}
        }
    }
    if implementer.is_some() || part.is_some() {
        return Some(format!(
            "arm64-impl={}-arch={}-var={}-part={}-rev={}",
            implementer.unwrap_or("unknown"),
            arch.unwrap_or("unknown"),
            variant.unwrap_or("unknown"),
            part.unwrap_or("unknown"),
            revision.unwrap_or("unknown"),
        ));
    }
    None
}

fn cpu_model() -> String {
    std::fs::File::open("/proc/cpuinfo")
        .ok()
        .and_then(|file| {
            let mut text = String::new();
            file.take(64 * 1024).read_to_string(&mut text).ok()?;
            parse_cpuinfo_model(&text)
        })
        .unwrap_or_else(|| std::env::consts::ARCH.to_string())
}

/// Whether `host` carries a real shard hardware profile rather than an empty
/// placeholder.
pub fn is_profiled_host(host: &DenseHostProfile) -> bool {
    !host.cpu_model.trim().is_empty() && host.logical_cores > 0
}

/// Canonicalize a `DenseHostProfile` so feature ordering and outer whitespace
/// never perturb equality or fingerprinting.
pub fn normalize_host(host: &DenseHostProfile) -> DenseHostProfile {
    let mut features: Vec<String> = host
        .simd_features
        .iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    features.sort_unstable();
    features.dedup();
    DenseHostProfile {
        cpu_model: host.cpu_model.trim().to_string(),
        simd_features: features,
        logical_cores: host.logical_cores,
    }
}

/// This process's host, detected once: every dense preflight reports it.
pub fn local_host_profile() -> DenseHostProfile {
    static PROFILE: std::sync::OnceLock<DenseHostProfile> = std::sync::OnceLock::new();
    PROFILE
        .get_or_init(|| {
            normalize_host(&DenseHostProfile {
                cpu_model: cpu_model(),
                simd_features: simd_features(),
                logical_cores: std::thread::available_parallelism()
                    .map_or(1, |cores| u32::try_from(cores.get()).unwrap_or(u32::MAX)),
            })
        })
        .clone()
}

/// SHA-256 over the fleet's canonical per-shard host capability profiles,
/// excluding `node_count` so homogeneous replica scale-out preserves the
/// policy while any CPU/SIMD/core change or unprofiled shard invalidates it.
pub fn fleet_fingerprint(hosts: &[DenseHostProfile]) -> String {
    let mut hasher = turbo_sha256::Sha256::new();
    if hosts.is_empty() || hosts.iter().any(|h| !is_profiled_host(h)) {
        hasher.update(b"dense-hw-unprofiled-v2");
        let mut raw: Vec<Vec<u8>> = hosts.iter().map(Message::encode_to_vec).collect();
        raw.sort_unstable();
        raw.dedup();
        for host in raw {
            hasher.update(&(host.len() as u64).to_le_bytes());
            hasher.update(&host);
        }
        return hasher.finish().to_hex();
    }
    hasher.update(b"dense-hw-profile-v2");
    let mut encoded: Vec<Vec<u8>> = hosts
        .iter()
        .map(|h| normalize_host(h).encode_to_vec())
        .collect();
    encoded.sort_unstable();
    encoded.dedup();
    for host in encoded {
        hasher.update(&(host.len() as u64).to_le_bytes());
        hasher.update(&host);
    }
    hasher.finish().to_hex()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fleet_fingerprint_preserves_replica_scale_out_and_separates_distinct_or_unprofiled_hosts() {
        let a = local_host_profile();
        assert!(is_profiled_host(&a));
        let mut b = a.clone();
        b.logical_cores += 1;
        assert_eq!(
            fleet_fingerprint(std::slice::from_ref(&a)),
            fleet_fingerprint(&[a.clone(), a.clone(), a.clone()]),
            "scaling homogeneous replica count must preserve hardware_fingerprint"
        );
        assert_ne!(
            fleet_fingerprint(std::slice::from_ref(&a)),
            fleet_fingerprint(std::slice::from_ref(&b))
        );
        assert_ne!(
            fleet_fingerprint(std::slice::from_ref(&a)),
            fleet_fingerprint(&[a.clone(), b.clone()])
        );
        assert_eq!(
            fleet_fingerprint(&[a.clone(), b.clone()]),
            fleet_fingerprint(&[b, a.clone()])
        );
        assert_ne!(
            fleet_fingerprint(std::slice::from_ref(&a)),
            fleet_fingerprint(&[DenseHostProfile::default()])
        );
    }

    #[test]
    fn cpuinfo_parser_handles_x86_model_name_and_arm64_implementer_part_fields() {
        let x86 = "processor\t: 0\nmodel name\t: AMD EPYC 9654 96-Core Processor\n";
        assert_eq!(
            parse_cpuinfo_model(x86).as_deref(),
            Some("AMD EPYC 9654 96-Core Processor")
        );
        let arm = "processor\t: 0\nBogoMIPS\t: 108.00\nCPU implementer\t: 0x41\n\
                   CPU architecture: 8\nCPU variant\t: 0x0\nCPU part\t: 0xd0b\nCPU revision\t: 1\n";
        let cortex_a76 = parse_cpuinfo_model(arm).unwrap();
        assert_eq!(
            cortex_a76,
            "arm64-impl=0x41-arch=8-var=0x0-part=0xd0b-rev=1"
        );
        let arm_other = arm.replace("0xd0b", "0xd40");
        assert_ne!(parse_cpuinfo_model(&arm_other).unwrap(), cortex_a76);
    }
}
