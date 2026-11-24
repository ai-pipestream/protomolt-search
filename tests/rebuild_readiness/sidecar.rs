//! Adopting an embedding provider that already exists: a remote sidecar,
//! a sidecar already listening on this host, or in-process analysis.

use super::rig::{Rig, PROBE_CANNOT_EMBED, PROBE_EMBEDS};

/// A sidecar binary that must never be launched.
const MUST_NOT_START: &str = "exit 9";

fn assert_refused(rig: &Rig, expected: &str) {
    let run = rig.run("sidecar");
    assert!(!run.ok, "{}", run.transcript());
    assert!(run.stderr.contains(expected), "{}", run.transcript());
    assert!(
        !rig.recorded("sidecar"),
        "a failed stage left a ledger record"
    );
    assert!(rig.calls("sidecar").is_empty(), "a sidecar was launched");
}

fn assert_adopted(rig: &Rig, expected: &str) {
    let run = rig.run("sidecar");
    assert!(run.ok, "{}", run.transcript());
    assert!(run.stdout.contains(expected), "{}", run.transcript());
    assert!(rig.calls("sidecar").is_empty(), "a sidecar was launched");
}

/// A sidecar already listening on a port this process holds.
fn listening_sidecar(name: &str) -> Rig {
    let mut rig = Rig::new(name);
    let port = rig.listening_port();
    rig.local_sidecar(port);
    rig.stub("sidecar", MUST_NOT_START);
    rig
}

#[test]
fn a_remote_sidecar_is_refused_without_a_probe_executable() {
    let rig = Rig::new("remote-no-probe");
    assert_refused(&rig, "no analyze_probe at");
}

#[test]
fn a_remote_sidecar_that_cannot_embed_is_refused_with_the_probe_diagnostic() {
    let rig = Rig::new("remote-cannot-embed");
    rig.stub("probe", PROBE_CANNOT_EMBED);
    assert_refused(
        &rig,
        "failed the embed probe: embedding: FAILED -- no TASK_EMBED bundle loaded",
    );
}

/// The endpoint accepts the call and never answers.
#[test]
fn a_remote_sidecar_whose_embedding_never_answers_is_refused() {
    let mut rig = Rig::new("remote-hangs");
    rig.set("EMBED_TIMEOUT_S", "1");
    rig.stub("probe", &Rig::stays_up());
    assert_refused(&rig, "failed the embed probe: no answer within 1s");
}

#[test]
fn a_remote_sidecar_is_adopted_after_it_embeds() {
    let rig = Rig::new("remote-embeds");
    rig.stub("probe", PROBE_EMBEDS);
    assert_adopted(&rig, "using the remote analysis sidecar");
    let probes = rig.calls("probe");
    assert_eq!(
        probes,
        ["--addr=http://sidecar.invalid:59202 --text=probe --embed --expect-dim=1"]
    );
}

#[test]
fn a_listening_local_sidecar_is_refused_without_a_probe_executable() {
    let rig = listening_sidecar("local-no-probe");
    assert_refused(&rig, "no analyze_probe at");
}

#[test]
fn a_listening_local_sidecar_started_elsewhere_that_cannot_embed_is_refused() {
    let rig = listening_sidecar("local-foreign-cannot-embed");
    rig.stub("probe", PROBE_CANNOT_EMBED);
    assert_refused(&rig, "cannot embed and was not started here");
}

/// The recorded pid is gone but the port still answers and still cannot
/// embed: nothing here can stop that listener.
#[test]
fn a_recorded_local_sidecar_that_cannot_embed_and_will_not_stop_is_refused() {
    let rig = listening_sidecar("local-recorded-cannot-embed");
    rig.stub("probe", PROBE_CANNOT_EMBED);
    rig.record_stopped_sidecar();
    assert_refused(&rig, "will not stop");
}

#[test]
fn a_listening_local_sidecar_is_reused_after_it_embeds() {
    let rig = listening_sidecar("local-embeds");
    rig.stub("probe", PROBE_EMBEDS);
    assert_adopted(&rig, "reusing");
}

/// The probe of a provider whose model has 768 dimensions: it embeds,
/// and only the index dimension it is asked about can refuse it.
const PROBE_EMBEDS_768: &str = r#"case "$*" in
  *--expect-dim=768) ;;
  *--expect-dim=*)
    echo "embedding: FAILED -- dim 768 where the index expects ${*##*--expect-dim=}"
    exit 1 ;;
esac
echo "embedding: dim 768, L2 norm 1.0000""#;

#[test]
fn a_sidecar_whose_model_has_another_dimension_than_the_index_is_refused() {
    let rig = Rig::new("remote-wrong-dim");
    rig.stub("probe", PROBE_EMBEDS_768);
    assert_refused(
        &rig,
        "failed the embed probe: embedding: FAILED -- dim 768 where the index expects 1",
    );
}

#[test]
fn a_sidecar_whose_model_has_the_index_dimension_is_adopted() {
    let mut rig = Rig::new("remote-right-dim");
    // A 12-byte header plus one dim-768 record.
    rig.set("DIM", "768");
    rig.set("EMB_BYTES", &(12 + 12 + 768 * 4).to_string());
    rig.stub("probe", PROBE_EMBEDS_768);
    assert_adopted(&rig, "using the remote analysis sidecar");
}

/// In-process analysis with no embedding bundle cannot embed a query. That
/// is a deployment only when it is declared; otherwise it is a forgotten
/// bundle.
#[test]
fn in_process_analysis_without_a_bundle_is_refused_unless_declared_lexical_only() {
    for backend in ["turbo", "native"] {
        let mut rig = Rig::new(&format!("lexical-only-{backend}"));
        rig.set("SIDECAR_ADDR", backend);
        rig.stub("probe", PROBE_CANNOT_EMBED);
        rig.stub("sidecar", MUST_NOT_START);
        assert_refused(&rig, "LEXICAL_ONLY=1");
        rig.set("LEXICAL_ONLY", "1");
        assert_adopted(&rig, "lexical-only");
        assert!(rig.calls("probe").is_empty(), "{backend} was probed");
    }
}

#[test]
fn lexical_only_is_refused_for_a_deployment_that_provides_embeddings() {
    let mut rig = Rig::new("lexical-only-contradicted");
    rig.set("LEXICAL_ONLY", "1");
    rig.stub("probe", PROBE_EMBEDS);
    assert_refused(&rig, "LEXICAL_ONLY=1, but");
    rig.set("SIDECAR_ADDR", "turbo");
    rig.set("TURBO_EMBED_BUNDLE", &rig.path("bundles"));
    assert_refused(&rig, "LEXICAL_ONLY=1, but");
    assert!(rig.calls("probe").is_empty());
}

#[test]
fn in_process_embedding_must_pass_the_probe() {
    let mut rig = Rig::new("in-process-bundle");
    rig.set("SIDECAR_ADDR", "turbo");
    rig.set("TURBO_EMBED_BUNDLE", &rig.path("bundles"));
    assert_refused(&rig, "no analyze_probe at");
    rig.stub("probe", PROBE_CANNOT_EMBED);
    assert_refused(&rig, "failed the embed probe");
    rig.stub("probe", PROBE_EMBEDS);
    assert_adopted(&rig, "in-process embedding from");
}
