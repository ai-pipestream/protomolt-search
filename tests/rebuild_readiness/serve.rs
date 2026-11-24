//! The `serve` stage's fleet-readiness wait.

use super::rig::Rig;

/// A verifier that reports shard 0 unreachable on every call, with the
/// status that means "not ready yet".
const NEVER_READY: &str = r#"echo "  ready   TIMED OUT at 0/1 shards after 2s"
echo "  not ready  shard 0 at 127.0.0.1:1 unreachable: connection refused"
exit 3"#;

fn serving(name: &str, verifier: &str, budget_s: &str) -> Rig {
    let mut rig = Rig::new(name);
    rig.ready_to_serve();
    rig.stub("verify", verifier);
    rig.set("READY_TIMEOUT_S", budget_s);
    rig
}

#[test]
fn a_verifier_that_never_confirms_fails_the_stage_with_its_last_report() {
    let rig = serving("never-ready", NEVER_READY, "1");
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("fleet NOT ready after 1s"),
        "{}",
        run.transcript()
    );
    assert!(
        run.stderr
            .contains("not ready  shard 0 at 127.0.0.1:1 unreachable: connection refused"),
        "{}",
        run.transcript()
    );
    assert!(
        !rig.recorded("serve"),
        "a failed stage left a ledger record"
    );
}

#[test]
fn readiness_is_reported_once_the_verifier_confirms_the_shards() {
    let ready_on_third_call = r#"if [ "$(wc -l <"$CALLS/verify")" -lt 3 ]; then
  echo "  ready   TIMED OUT at 0/1 shards after 2s"
  exit 3
fi
echo "  ready   1/1 shards after 0s""#;
    let rig = serving("eventually-ready", ready_on_third_call, "600");
    let run = rig.run("serve");
    assert!(run.ok, "{}", run.transcript());
    assert!(run.stdout.contains("fleet ready"), "{}", run.transcript());
    let calls = rig.calls("verify");
    assert_eq!(calls.len(), 3, "{calls:?}");
    assert!(
        calls[0].contains("--shards=1 --ready-only --wait-ready=2"),
        "{calls:?}"
    );
    assert!(rig.recorded("serve"));
}

#[test]
fn a_missing_verifier_fails_the_stage_before_anything_starts() {
    let mut rig = Rig::new("no-verifier");
    rig.ready_to_serve();
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("no v7_verify at"),
        "{}",
        run.transcript()
    );
    assert!(rig.calls("bin").is_empty(), "a node or coordinator started");
}

#[test]
fn a_verifier_lost_during_the_wait_fails_the_stage() {
    let rig = serving("verifier-lost", "rm -- \"$0\"\nexit 3", "20");
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("failed (exit 127)"),
        "{}",
        run.transcript()
    );
    assert_eq!(rig.calls("verify").len(), 1);
}

#[test]
fn a_verifier_usage_error_fails_the_stage_at_once() {
    let usage = "echo '--ready-only needs --wait-ready=<seconds>' >&2\nexit 2";
    let rig = serving("verifier-usage", usage, "20");
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(
        run.stderr.contains("failed (exit 2)")
            && run.stderr.contains("--ready-only needs --wait-ready"),
        "{}",
        run.transcript()
    );
    assert_eq!(rig.calls("verify").len(), 1);
}

/// Status 1 is the verifier saying it can never succeed as invoked: an
/// unreadable token, a bad flag, refused credentials, a TLS failure.
#[test]
fn a_verifier_that_cannot_succeed_fails_the_stage_at_once() {
    let unreadable_token =
        "echo 'Error: \"read bearer token file /absent: No such file\"' >&2\nexit 1";
    let rig = serving("verifier-cannot-succeed", unreadable_token, "600");
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("failed (exit 1)")
            && run.stderr.contains("read bearer token file /absent"),
        "{}",
        run.transcript()
    );
    assert_eq!(
        rig.calls("verify").len(),
        1,
        "a permanent error was retried"
    );
}

/// Status 4 is the verifier saying the fleet as configured cannot become
/// ready: `SHARDS` disagrees with the coordinator's listing, or the shards
/// do not score in one space. The stage ends at once with the whole
/// report, since no later attempt can differ.
#[test]
fn a_fleet_that_cannot_become_ready_fails_the_stage_at_once_with_the_whole_report() {
    let cannot_become_ready = r#"echo "  ready   NOT POSSIBLE at 2/1 shards: waiting cannot clear this"
echo "  not ready  coordinator lists 2 primary shards, expected 1"
echo "  not ready  shard 1 at 127.0.0.1:2 answers, but: node serves collection a"
echo "  not ready  provider mismatch: hnsw/a: shard 0; flat/b: shard 1"
echo "  (end of report)"
exit 4"#;
    let rig = serving("cannot-become-ready", cannot_become_ready, "600");
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("waiting cannot change that")
            && run
                .stderr
                .contains("coordinator lists 2 primary shards, expected 1")
            && run.stderr.contains("provider mismatch: hnsw/a"),
        "{}",
        run.transcript()
    );
    assert_eq!(
        rig.calls("verify").len(),
        1,
        "a standing blocker was retried"
    );
    assert!(!rig.recorded("serve"));
}

/// The coordinator answers the first attempt and then stops answering: the
/// wait still ends with the budget, and keeps the report it did get.
#[test]
fn a_verifier_left_without_an_answer_on_a_later_attempt_fails_the_stage() {
    let answers_once = format!(
        "if [ \"$(wc -l <\"$CALLS/verify\")\" -lt 2 ]; then\n{NEVER_READY}\nfi\n{}",
        Rig::stays_up()
    );
    let rig = serving("verifier-hangs-later", &answers_once, "2");
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("fleet NOT ready after 2s")
            && run.stderr.contains("shard 0 at 127.0.0.1:1 unreachable")
            && run.stderr.contains("gave the verifier no answer"),
        "{}",
        run.transcript()
    );
    assert_eq!(rig.calls("verify").len(), 2);
}

/// The coordinator accepts the verifier's connection and never answers.
#[test]
fn a_verifier_left_without_an_answer_fails_the_stage_when_the_budget_ends() {
    let rig = serving("verifier-hangs", &Rig::stays_up(), "1");
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("fleet NOT ready after 1s")
            && run.stderr.contains("gave the verifier no answer"),
        "{}",
        run.transcript()
    );
    assert_eq!(rig.calls("verify").len(), 1);
}

#[test]
fn a_coordinator_that_exits_during_the_wait_fails_the_stage() {
    let rig = serving("coordinator-exits", NEVER_READY, "20");
    let nodes_stay_up = format!(
        "case \"$*\" in *--role=coordinator*) exit 3 ;; esac\n{}",
        Rig::stays_up()
    );
    rig.stub("bin", &nodes_stay_up);
    let run = rig.run("serve");
    assert!(!run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    assert!(
        run.stderr.contains("the coordinator exited"),
        "{}",
        run.transcript()
    );
}

/// A shard-only host starts no coordinator and waits on nothing, so it
/// needs no verifier and never claims the fleet is ready.
#[test]
fn a_shard_only_host_serves_without_a_verifier_or_a_readiness_claim() {
    let mut rig = Rig::new("shard-only");
    rig.ready_to_serve();
    rig.set("RUN_COORD", "0");
    let run = rig.run("serve");
    assert!(run.ok, "{}", run.transcript());
    assert!(!run.stdout.contains("fleet ready"), "{}", run.transcript());
    let started = rig.calls_once_recorded("bin", 1);
    assert_eq!(started.len(), 1, "one node: {started:?}");
    // The script records a coordinator's pid before it returns, so this
    // does not depend on when a background stub logs its call.
    assert!(
        !rig.recorded_pid("coordinator"),
        "a coordinator was started on a shard-only host"
    );
}
