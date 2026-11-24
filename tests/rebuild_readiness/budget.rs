//! The readiness budgets on a clock the test controls. `readiness.sh` is
//! sourced as `rebuild.sh` sources it, with a `sleep` that advances the
//! shell's `SECONDS` and a `timeout` that records the limit it was given
//! and reports the attempt as failed. Every attempt must run under a
//! positive limit no longer than what is left of its budget: `timeout 0`
//! is no limit at all, and a negative one is not a limit.

use std::process::Command;

use super::scratch::scratch_path;

/// What `rebuild.sh` supplies, and the controlled clock. `timeout` runs
/// inside a command substitution, so only `sleep` moves the clock.
const HARNESS: &str = r#"
set -euo pipefail
die() { echo "rebuild: $*" >&2; exit 1; }
say() { echo "== $*"; }
source "$READINESS"
sleep() { SECONDS=$((SECONDS + STEP)); }
timeout() { echo "$2" >>"$SCRATCH/limits"; return "$ATTEMPT_STATUS"; }
kill() { return 0; }
port_open() { return 0; }
COORD_PORT=1 SHARDS=1 DIM=1 CLIENT_ARG_LIST=() VERIFY=verifier PROBE=probe
SIDECAR_ADDR=http://127.0.0.1:1 SIDECAR_PORT=1 SIDECAR_BIN=$(type -P true)
EMBEDDINGS_DIR=$SCRATCH RUN=$SCRATCH LOGS=$SCRATCH
SECONDS=0
"#;

/// Run `call` in the harness and return the limits its attempts ran
/// under, with what the shell printed on its way out.
fn limits(name: &str, env: &[(&str, &str)], call: &str) -> (Vec<i64>, String) {
    let scratch = scratch_path(&format!("rebuild-readiness-budget-{name}"));
    let _ = std::fs::remove_dir_all(&*scratch);
    std::fs::create_dir_all(&*scratch).unwrap();
    let readiness = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/deploy/v7-rebuild/readiness.sh"
    );
    let output = Command::new("bash")
        .arg("-c")
        .arg(format!("{HARNESS}{call}"))
        .env("READINESS", readiness)
        .env("SCRATCH", &*scratch)
        .envs(env.iter().copied())
        .output()
        .expect("run bash");
    assert!(!output.status.success(), "the wait reported success");
    let limits = std::fs::read_to_string(scratch.join("limits"))
        .unwrap_or_default()
        .lines()
        .map(|line| line.parse().expect("a numeric limit"))
        .collect();
    (limits, String::from_utf8_lossy(&output.stderr).into_owned())
}

/// Budgets and attempt cadences that put the last attempt at the deadline
/// and past it.
#[test]
fn every_readiness_attempt_runs_under_what_is_left_of_the_budget() {
    for (budget, step) in [("9", "2"), ("10", "3"), ("1", "5")] {
        let env = [
            ("READY_TIMEOUT_S", budget),
            ("STEP", step),
            ("ATTEMPT_STATUS", "3"),
        ];
        let (limits, stderr) = limits("fleet", &env, "wait_fleet_ready 1");
        let budget: i64 = budget.parse().unwrap();
        assert!(
            limits.len() > 1 || budget == 1,
            "{budget}/{step}: {limits:?}"
        );
        assert!(
            limits.iter().all(|limit| (1..=budget).contains(limit)),
            "{budget}/{step}: {limits:?}\n{stderr}"
        );
        assert!(
            limits.windows(2).all(|pair| pair[1] < pair[0]),
            "{limits:?}"
        );
        assert!(
            stderr.contains(&format!("fleet NOT ready after {budget}s")),
            "{budget}/{step}: {stderr}"
        );
    }
}

/// A started sidecar has one budget to pass a probe, not one per probe.
#[test]
fn every_probe_of_a_started_sidecar_runs_under_what_is_left_of_its_budget() {
    let env = [
        ("EMBED_TIMEOUT_S", "7"),
        ("STEP", "2"),
        ("ATTEMPT_STATUS", "1"),
    ];
    let (limits, stderr) = limits("sidecar", &env, "start_sidecar");
    assert!(limits.len() > 1, "{limits:?}\n{stderr}");
    assert!(
        limits.iter().all(|limit| (1..=7).contains(limit)),
        "{limits:?}"
    );
    assert!(
        limits.windows(2).all(|pair| pair[1] < pair[0]),
        "{limits:?}"
    );
    assert!(stderr.contains("did not embed within 7s"), "{stderr}");
}
