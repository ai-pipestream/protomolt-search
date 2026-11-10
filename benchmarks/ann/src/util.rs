//! Small shared pieces: arguments, bitsets, FP32 kernels, top-k, files.

use std::cmp::Ordering;
use std::path::Path;

pub type Error = Box<dyn std::error::Error + Send + Sync>;

/// `--key=value`, or `default`.
pub fn arg(key: &str, default: &str) -> String {
    let prefix = format!("--{key}=");
    std::env::args()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
        .unwrap_or_else(|| default.to_string())
}

pub fn list_usize(key: &str, default: &str) -> Result<Vec<usize>, Error> {
    let raw = arg(key, default);
    if raw.trim().is_empty() {
        return Ok(Vec::new());
    }
    let mut values = raw
        .split(',')
        .map(|part| part.trim().parse::<usize>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("--{key}: {e}"))?;
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

pub fn list_str(key: &str, default: &str) -> Vec<String> {
    arg(key, default)
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

/// A fixed-size bitset over rows. The allow mask and every engine's view of
/// it are this type, built from the column index inside the timed region.
#[derive(Clone)]
pub struct Bitset {
    words: Vec<u64>,
    len: usize,
}

impl Bitset {
    pub fn new(len: usize) -> Self {
        Self {
            words: vec![0; len.div_ceil(64)],
            len,
        }
    }

    #[inline]
    pub fn set(&mut self, i: usize) {
        self.words[i >> 6] |= 1 << (i & 63);
    }

    #[inline]
    pub fn get(&self, i: usize) -> bool {
        (self.words[i >> 6] >> (i & 63)) & 1 == 1
    }

    pub fn len(&self) -> usize {
        self.len
    }

    /// The packed words, bit `i` of the set at bit `i & 63` of word `i >> 6`.
    pub fn words(&self) -> &[u64] {
        &self.words
    }

    pub fn count(&self) -> usize {
        self.words.iter().map(|w| w.count_ones() as usize).sum()
    }

    /// Set bits in `start..end`, ascending.
    pub fn ones_in(&self, start: usize, end: usize) -> impl Iterator<Item = usize> + '_ {
        let first = start >> 6;
        let last = end.div_ceil(64).min(self.words.len());
        (first..last).flat_map(move |w| {
            let mut word = self.words[w];
            std::iter::from_fn(move || {
                while word != 0 {
                    let bit = word.trailing_zeros() as usize;
                    word &= word - 1;
                    let i = (w << 6) | bit;
                    if i >= start && i < end {
                        return Some(i);
                    }
                }
                None
            })
        })
    }
}

/// FP32 inner product, eight independent lanes so the compiler vectorizes it.
#[inline]
pub fn dot(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0f32; 8];
    let chunks = a.len() / 8;
    for c in 0..chunks {
        let x = &a[c * 8..c * 8 + 8];
        let y = &b[c * 8..c * 8 + 8];
        for l in 0..8 {
            acc[l] += x[l] * y[l];
        }
    }
    let mut sum: f32 = acc.iter().sum();
    for i in chunks * 8..a.len() {
        sum += a[i] * b[i];
    }
    sum
}

/// Squared L2 distance, same lane layout.
#[inline]
pub fn l2sq(a: &[f32], b: &[f32]) -> f32 {
    let mut acc = [0f32; 8];
    let chunks = a.len() / 8;
    for c in 0..chunks {
        let x = &a[c * 8..c * 8 + 8];
        let y = &b[c * 8..c * 8 + 8];
        for l in 0..8 {
            let d = x[l] - y[l];
            acc[l] += d * d;
        }
    }
    let mut sum: f32 = acc.iter().sum();
    for i in chunks * 8..a.len() {
        let d = a[i] - b[i];
        sum += d * d;
    }
    sum
}

pub fn normalize(row: &mut [f32]) {
    let norm = row.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        row.iter_mut().for_each(|v| *v /= norm);
    }
}

/// Best first by score, ties by row id.
pub fn by_score_desc(a: &(u32, f32), b: &(u32, f32)) -> Ordering {
    b.1.total_cmp(&a.1).then(a.0.cmp(&b.0))
}

/// A bounded min-heap over `(row, score)` that keeps the best `k`.
pub struct TopK {
    k: usize,
    heap: Vec<(u32, f32)>,
}

impl TopK {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            heap: Vec::with_capacity(k + 1),
        }
    }

    #[inline]
    pub fn threshold(&self) -> f32 {
        if self.heap.len() < self.k {
            f32::NEG_INFINITY
        } else {
            self.heap[0].1
        }
    }

    #[inline]
    pub fn push(&mut self, row: u32, score: f32) {
        if self.heap.len() < self.k {
            self.heap.push((row, score));
            self.sift_up(self.heap.len() - 1);
        } else if score > self.heap[0].1 {
            self.heap[0] = (row, score);
            self.sift_down(0);
        }
    }

    fn sift_up(&mut self, mut i: usize) {
        while i > 0 {
            let p = (i - 1) / 2;
            if self.heap[i].1 < self.heap[p].1 {
                self.heap.swap(i, p);
                i = p;
            } else {
                break;
            }
        }
    }

    fn sift_down(&mut self, mut i: usize) {
        let n = self.heap.len();
        loop {
            let l = 2 * i + 1;
            let r = l + 1;
            let mut m = i;
            if l < n && self.heap[l].1 < self.heap[m].1 {
                m = l;
            }
            if r < n && self.heap[r].1 < self.heap[m].1 {
                m = r;
            }
            if m == i {
                break;
            }
            self.heap.swap(i, m);
            i = m;
        }
    }

    pub fn into_sorted(mut self) -> Vec<(u32, f32)> {
        self.heap.sort_unstable_by(by_score_desc);
        self.heap
    }
}

/// Splits `0..n` across the host's threads and runs `f` on each part.
pub fn par_map<T: Send, F>(n: usize, threads: usize, f: F) -> Vec<T>
where
    F: Fn(usize) -> T + Sync,
{
    let threads = threads.max(1);
    let next = std::sync::atomic::AtomicUsize::new(0);
    let mut out: Vec<(usize, T)> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..threads)
            .map(|_| {
                scope.spawn(|| {
                    let mut local = Vec::new();
                    loop {
                        let i = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        if i >= n {
                            break;
                        }
                        local.push((i, f(i)));
                    }
                    local
                })
            })
            .collect();
        handles
            .into_iter()
            .flat_map(|h| h.join().expect("worker thread"))
            .collect()
    });
    out.sort_unstable_by_key(|(i, _)| *i);
    out.into_iter().map(|(_, t)| t).collect()
}

pub fn host_threads() -> usize {
    std::thread::available_parallelism().map_or(1, |n| n.get())
}

pub fn rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status.lines().find_map(|line| {
                let kib = line.strip_prefix("VmRSS:")?.trim().strip_suffix(" kB")?;
                kib.trim().parse::<u64>().ok()
            })
        })
        .map_or(0, |kib| kib * 1024)
}

pub fn write_json(path: &Path, value: &serde_json::Value) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_vec_pretty(value)?)?;
    Ok(())
}

pub fn read_json(path: &Path) -> Result<serde_json::Value, Error> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub fn write_f32(path: &Path, values: &[f32]) -> Result<(), Error> {
    let mut bytes = Vec::with_capacity(values.len() * 4);
    for v in values {
        bytes.extend_from_slice(&v.to_le_bytes());
    }
    std::fs::write(path, bytes)?;
    Ok(())
}

pub fn read_f32(path: &Path) -> Result<Vec<f32>, Error> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    if bytes.len() % 4 != 0 {
        return Err(format!("{}: length is not a multiple of 4", path.display()).into());
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

pub fn write_u32(path: &Path, values: &[u32]) -> Result<(), Error> {
    let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
    std::fs::write(path, bytes)?;
    Ok(())
}

pub fn read_u32(path: &Path) -> Result<Vec<u32>, Error> {
    let bytes = std::fs::read(path).map_err(|e| format!("{}: {e}", path.display()))?;
    Ok(bytes
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

/// The commit the harness ran at and when the run started: provenance the
/// gate requires of every result file. The commit comes from
/// `ANN_BENCH_COMMIT` (run-matrix.sh sets it), read at run time and, failing
/// that, as it was at build time; the bench crate and the product crate share
/// one repository, so one commit names both.
pub fn harness() -> serde_json::Value {
    let commit = std::env::var("ANN_BENCH_COMMIT")
        .ok()
        .filter(|c| !c.is_empty())
        .or_else(|| option_env!("ANN_BENCH_COMMIT").map(str::to_string))
        .unwrap_or_default();
    let started_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    serde_json::json!({
        "runner": "ann_bench",
        "version": env!("CARGO_PKG_VERSION"),
        "commit": commit,
        "started_at": started_at,
    })
}

/// Host facts recorded in every result file.
pub fn host() -> serde_json::Value {
    let cpu = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find(|l| l.starts_with("model name") || l.starts_with("Model"))
                .and_then(|l| l.split(':').nth(1))
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_default();
    let hostname = std::env::var("ANN_BENCH_HOST")
        .ok()
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .unwrap_or_default();
    let mut features = Vec::new();
    #[cfg(target_arch = "x86_64")]
    {
        for (name, on) in [
            ("sse4.2", std::arch::is_x86_feature_detected!("sse4.2")),
            ("avx2", std::arch::is_x86_feature_detected!("avx2")),
            ("fma", std::arch::is_x86_feature_detected!("fma")),
            ("avx512f", std::arch::is_x86_feature_detected!("avx512f")),
        ] {
            if on {
                features.push(name);
            }
        }
    }
    serde_json::json!({
        "name": hostname,
        "cpu": cpu,
        "arch": std::env::consts::ARCH,
        "kernel": std::fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|s| s.trim().to_string())
            .unwrap_or_default(),
        "threads": host_threads(),
        "cpu_features": features,
    })
}
