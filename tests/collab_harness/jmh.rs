//! Forked warmup/measurement trials in the style of JMH.
//!
//! A [`Bench`] runs one benchmark in `forks` child processes, each this
//! test binary re-executed through [`std::env::current_exe`] with
//! `--exact <test path>`, so allocator and page-cache state never carry
//! from one fork to the next. A child sees [`FORK_ENV`] naming its
//! benchmark, builds its state, calibrates a batch size, runs the warmup
//! and measurement iterations, writes its samples as JSON to the file
//! named by [`FORK_OUT_ENV`] and returns. The parent merges the samples
//! of every fork, computes the statistics and writes a receipt.
//!
//! One iteration times a batch of `batch_ops` calls with
//! `CLOCK_MONOTONIC_RAW` (wall) and `CLOCK_THREAD_CPUTIME_ID` (CPU of the
//! calling thread only: work a call hands to other threads is not
//! counted, so a ratio well under 1.0 means the call blocked or
//! offloaded). The batch is doubled until it lasts `target_batch_ns`,
//! which keeps clock overhead out of sub-microsecond calls. Every input
//! and result passes through [`std::hint::black_box`].
//!
//! Plain `cargo test` measures the unoptimized test profile, which the
//! receipt records as `debug_assertions: true`; use `cargo test --release`
//! for numbers worth comparing. Environment overrides for full sweeps: `COLLAB_JMH_FORKS`,
//! `COLLAB_JMH_WARMUP`, `COLLAB_JMH_ITERATIONS`,
//! `COLLAB_JMH_TARGET_BATCH_NS`, and `COLLAB_JMH_RECEIPT_DIR` to keep the
//! receipts somewhere other than a temporary directory.

use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::{json, Value};

/// Names the benchmark a re-executed child must measure.
pub const FORK_ENV: &str = "COLLAB_JMH_FORK";
/// Names the file a child writes its samples to.
pub const FORK_OUT_ENV: &str = "COLLAB_JMH_FORK_OUT";
/// Receipt schema identifier.
pub const SCHEMA: &str = "collab-jmh/v1";
/// Upper bound on the calibrated batch size.
const MAX_BATCH_OPS: u64 = 1 << 24;

/// Shape of one trial.
#[derive(Clone, Debug)]
pub struct TrialConfig {
    pub forks: usize,
    pub warmup: usize,
    pub iterations: usize,
    pub target_batch_ns: u64,
}

impl TrialConfig {
    /// The `cargo test` default: 2 forks, 2 warmup and 5 measured
    /// iterations of at least 1 ms each.
    pub fn smoke() -> Self {
        Self {
            forks: 2,
            warmup: 2,
            iterations: 5,
            target_batch_ns: 1_000_000,
        }
    }

    /// `defaults` with any `COLLAB_JMH_*` overrides applied.
    pub fn from_env(defaults: Self) -> Self {
        Self {
            forks: env_u64("COLLAB_JMH_FORKS", defaults.forks as u64).max(1) as usize,
            warmup: env_u64("COLLAB_JMH_WARMUP", defaults.warmup as u64) as usize,
            iterations: env_u64("COLLAB_JMH_ITERATIONS", defaults.iterations as u64).max(2)
                as usize,
            target_batch_ns: env_u64("COLLAB_JMH_TARGET_BATCH_NS", defaults.target_batch_ns),
        }
    }
}

fn env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn clock_ns(clock: libc::clockid_t) -> u64 {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `ts` is a valid, writable timespec for the whole call.
    let rc = unsafe { libc::clock_gettime(clock, &mut ts) };
    assert_eq!(rc, 0, "clock_gettime: {}", std::io::Error::last_os_error());
    ts.tv_sec as u64 * 1_000_000_000 + ts.tv_nsec as u64
}

/// Wall and thread-CPU nanoseconds of one batch.
fn time_batch<S, R>(state: &S, op: &mut impl FnMut(&S) -> R, ops: u64) -> (u64, u64) {
    let wall = clock_ns(libc::CLOCK_MONOTONIC_RAW);
    let cpu = clock_ns(libc::CLOCK_THREAD_CPUTIME_ID);
    for _ in 0..ops {
        black_box(op(black_box(state)));
    }
    let cpu = clock_ns(libc::CLOCK_THREAD_CPUTIME_ID) - cpu;
    (clock_ns(libc::CLOCK_MONOTONIC_RAW) - wall, cpu)
}

/// The samples of one fork, in nanoseconds per operation.
#[derive(Clone, Debug, PartialEq)]
pub struct ForkSamples {
    pub batch_ops: u64,
    pub wall_ns_per_op: Vec<f64>,
    pub cpu_ns_per_op: Vec<f64>,
}

impl ForkSamples {
    fn to_json(&self) -> Value {
        json!({
            "batch_ops": self.batch_ops,
            "wall_ns_per_op": self.wall_ns_per_op,
            "cpu_ns_per_op": self.cpu_ns_per_op,
        })
    }

    fn from_json(value: &Value) -> Option<Self> {
        let floats = |key: &str| -> Option<Vec<f64>> {
            value[key].as_array()?.iter().map(Value::as_f64).collect()
        };
        Some(Self {
            batch_ops: value["batch_ops"].as_u64()?,
            wall_ns_per_op: floats("wall_ns_per_op")?,
            cpu_ns_per_op: floats("cpu_ns_per_op")?,
        })
    }
}

/// Calibrate, warm up and measure one fork in this process.
pub fn measure<S, R>(config: &TrialConfig, state: &S, mut op: impl FnMut(&S) -> R) -> ForkSamples {
    let mut batch_ops = 1;
    while batch_ops < MAX_BATCH_OPS {
        if time_batch(state, &mut op, batch_ops).0 >= config.target_batch_ns {
            break;
        }
        batch_ops *= 2;
    }
    for _ in 0..config.warmup {
        time_batch(state, &mut op, batch_ops);
    }
    let mut samples = ForkSamples {
        batch_ops,
        wall_ns_per_op: Vec::with_capacity(config.iterations),
        cpu_ns_per_op: Vec::with_capacity(config.iterations),
    };
    for _ in 0..config.iterations {
        let (wall, cpu) = time_batch(state, &mut op, batch_ops);
        samples.wall_ns_per_op.push(wall as f64 / batch_ops as f64);
        samples.cpu_ns_per_op.push(cpu as f64 / batch_ops as f64);
    }
    samples
}

/// Summary statistics over every measured iteration of every fork.
#[derive(Clone, Debug, PartialEq)]
pub struct Summary {
    pub n: usize,
    pub mean: f64,
    pub stddev: f64,
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub ci99: (f64, f64),
}

/// Two-sided 99% Student t critical values for 1 to 30 degrees of freedom.
const T99: [f64; 30] = [
    63.657, 9.925, 5.841, 4.604, 4.032, 3.707, 3.499, 3.355, 3.250, 3.169, 3.106, 3.055, 3.012,
    2.977, 2.947, 2.921, 2.898, 2.878, 2.861, 2.845, 2.831, 2.819, 2.807, 2.797, 2.787, 2.779,
    2.771, 2.763, 2.756, 2.750,
];

fn t99(degrees_of_freedom: usize) -> f64 {
    T99.get(degrees_of_freedom.saturating_sub(1))
        .copied()
        .unwrap_or(2.576)
}

/// Nearest-rank percentile of an ascending, non-empty slice.
fn percentile(sorted: &[f64], q: f64) -> f64 {
    let rank = (q * sorted.len() as f64).ceil() as usize;
    sorted[rank.clamp(1, sorted.len()) - 1]
}

/// Mean, sample standard deviation, nearest-rank percentiles and the 99%
/// confidence interval of the mean. Needs at least two samples.
pub fn summarize(samples: &[f64]) -> Summary {
    assert!(samples.len() >= 2, "a summary needs two or more samples");
    let mut sorted = samples.to_vec();
    sorted.sort_by(f64::total_cmp);
    let n = sorted.len();
    let mean = sorted.iter().sum::<f64>() / n as f64;
    let variance = sorted.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1) as f64;
    let stddev = variance.sqrt();
    let half = t99(n - 1) * stddev / (n as f64).sqrt();
    Summary {
        n,
        mean,
        stddev,
        p50: percentile(&sorted, 0.50),
        p95: percentile(&sorted, 0.95),
        p99: percentile(&sorted, 0.99),
        ci99: (mean - half, mean + half),
    }
}

fn summary_json(s: &Summary) -> Value {
    json!({
        "n": s.n, "mean": s.mean, "stddev": s.stddev,
        "p50": s.p50, "p95": s.p95, "p99": s.p99,
        "ci99_low": s.ci99.0, "ci99_high": s.ci99.1,
    })
}

/// Where receipts go when the caller has no preference: the directory in
/// `COLLAB_JMH_RECEIPT_DIR`, else a per-process temporary directory.
pub fn default_receipt_dir() -> PathBuf {
    std::env::var_os("COLLAB_JMH_RECEIPT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join(format!("collab-jmh-{}", std::process::id())))
}

/// Build-host and source facts recorded in every receipt.
pub fn provenance() -> Value {
    let git_sha = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string());
    let cpu_model = std::fs::read_to_string("/proc/cpuinfo")
        .ok()
        .and_then(|info| {
            info.lines()
                .find(|line| line.starts_with("model name"))
                .and_then(|line| line.split_once(':'))
                .map(|(_, model)| model.trim().to_string())
        });
    json!({
        "git_sha": git_sha,
        "cpu_model": cpu_model,
        "rustflags": std::env::var("RUSTFLAGS").ok().or(option_env!("RUSTFLAGS").map(String::from)),
        "available_parallelism": std::thread::available_parallelism().map_or(0, |n| n.get()),
        "measurement_threads": 1,
        "debug_assertions": cfg!(debug_assertions),
    })
}

/// One forked benchmark. `test_path` is the libtest name of the test
/// function that calls [`Bench::run`], without the crate prefix.
pub struct Bench {
    pub name: String,
    pub test_path: String,
    pub config: TrialConfig,
    pub receipt_dir: PathBuf,
}

impl Bench {
    /// In a child forked for this benchmark: build the state, measure,
    /// write the samples and return `None`. In a child forked for another
    /// benchmark: return `None` without running anything. In the parent:
    /// fork, aggregate, write the receipt and return its path and body.
    pub fn run<S, R>(
        &self,
        setup: impl FnOnce() -> S,
        op: impl FnMut(&S) -> R,
    ) -> Option<(PathBuf, Value)> {
        match std::env::var(FORK_ENV) {
            Ok(target) if target == self.name => {
                let samples = measure(&self.config, &setup(), op);
                let out = std::env::var(FORK_OUT_ENV).expect("fork output path");
                std::fs::write(out, samples.to_json().to_string()).expect("write fork samples");
                None
            }
            Ok(_) => None,
            Err(_) => Some(self.run_forks()),
        }
    }

    fn run_forks(&self) -> (PathBuf, Value) {
        std::fs::create_dir_all(&self.receipt_dir).expect("create receipt dir");
        let forks: Vec<ForkSamples> = (0..self.config.forks).map(|i| self.fork(i)).collect();
        let receipt = self.receipt(&forks);
        let path = self
            .receipt_dir
            .join(format!("{}-{}.json", self.name, std::process::id()));
        let text = serde_json::to_string_pretty(&receipt).unwrap();
        std::fs::write(&path, text).expect("write receipt");
        (path, receipt)
    }

    fn fork(&self, index: usize) -> ForkSamples {
        let out = self.receipt_dir.join(format!(
            ".{}-fork{index}-{}.json",
            self.name,
            std::process::id()
        ));
        let child = Command::new(std::env::current_exe().expect("test binary path"))
            .args([
                self.test_path.as_str(),
                "--exact",
                "--test-threads=1",
                "--quiet",
            ])
            .env(FORK_ENV, &self.name)
            .env(FORK_OUT_ENV, &out)
            .output()
            .expect("spawn fork");
        assert!(
            child.status.success(),
            "fork {index} of {} failed: {}\n{}{}",
            self.name,
            child.status,
            String::from_utf8_lossy(&child.stdout),
            String::from_utf8_lossy(&child.stderr),
        );
        let samples = read_samples(&out);
        let _ = std::fs::remove_file(&out);
        samples.unwrap_or_else(|| panic!("fork {index} of {} wrote no samples", self.name))
    }

    fn receipt(&self, forks: &[ForkSamples]) -> Value {
        let wall: Vec<f64> = forks
            .iter()
            .flat_map(|f| f.wall_ns_per_op.clone())
            .collect();
        let cpu: Vec<f64> = forks.iter().flat_map(|f| f.cpu_ns_per_op.clone()).collect();
        let (wall_s, cpu_s) = (summarize(&wall), summarize(&cpu));
        json!({
            "schema": SCHEMA,
            "benchmark": self.name,
            "provenance": provenance(),
            "config": {
                "forks": self.config.forks,
                "warmup": self.config.warmup,
                "iterations": self.config.iterations,
                "target_batch_ns": self.config.target_batch_ns,
            },
            "wall_ns_per_op": summary_json(&wall_s),
            "cpu_ns_per_op": summary_json(&cpu_s),
            "cpu_to_wall_ratio": cpu_s.mean / wall_s.mean,
            "forks": forks.iter().map(ForkSamples::to_json).collect::<Vec<_>>(),
        })
    }
}

fn read_samples(path: &Path) -> Option<ForkSamples> {
    let text = std::fs::read_to_string(path).ok()?;
    ForkSamples::from_json(&serde_json::from_str(&text).ok()?)
}

#[test]
fn summary_statistics_match_hand_computed_values() {
    let s = summarize(&[1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]);
    assert_eq!(
        (s.n, s.mean, s.p50, s.p95, s.p99),
        (10, 5.5, 5.0, 10.0, 10.0)
    );
    let stddev = (82.5f64 / 9.0).sqrt();
    assert!((s.stddev - stddev).abs() < 1e-12);
    let half = 3.250 * stddev / 10f64.sqrt();
    assert!((s.ci99.0 - (5.5 - half)).abs() < 1e-12);
    assert!((s.ci99.1 - (5.5 + half)).abs() < 1e-12);
    assert_eq!(t99(1), 63.657);
    assert_eq!(t99(500), 2.576);
}

#[test]
fn measure_calibrates_the_batch_and_samples_every_iteration() {
    let config = TrialConfig {
        forks: 1,
        warmup: 1,
        iterations: 3,
        target_batch_ns: 200_000,
    };
    let samples = measure(&config, &7u64, |x| x.wrapping_mul(31));
    assert!(samples.batch_ops > 1, "a multiply is far below 200 us");
    assert_eq!(samples.wall_ns_per_op.len(), 3);
    assert_eq!(samples.cpu_ns_per_op.len(), 3);
    assert!(samples.wall_ns_per_op.iter().all(|ns| *ns > 0.0));
    let round = ForkSamples::from_json(&samples.to_json());
    assert_eq!(round.as_ref(), Some(&samples));
}
