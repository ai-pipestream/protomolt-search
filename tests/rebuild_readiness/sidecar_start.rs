//! Starting a sidecar on this host: its port opening is not the stage's
//! success, its first embedding is.

use tokio::net::TcpSocket;

use super::rig::{Rig, Run, PROBE_CANNOT_EMBED};

/// Run the `sidecar` stage against a free local port that starts
/// listening only once the script has launched `sidecar`, the way the real
/// server's port opens after its process starts. `budget_s` bounds the
/// wait for the first embedding.
fn start_sidecar(name: &str, sidecar: &str, probe: &str, budget_s: &str) -> (Rig, Run) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()
        .unwrap();
    let _reactor = runtime.enter();
    // Bound but not listening: the port is reserved and refuses connections.
    let socket = TcpSocket::new_v4().unwrap();
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();

    let mut rig = Rig::new(name);
    rig.local_sidecar(socket.local_addr().unwrap().port());
    rig.set("EMBED_TIMEOUT_S", budget_s);
    rig.stub("sidecar", &format!(": >\"$CALLS/../launched\"\n{sidecar}"));
    rig.stub("probe", probe);
    rig.pace_waits();

    let mut child = rig.spawn("sidecar");
    rig.await_marker(&mut child, "launched");
    let _listener = socket.listen(8).unwrap();
    let run = rig.finish(child);
    (rig, run)
}

#[test]
fn a_started_sidecar_that_binds_its_port_but_cannot_embed_fails_the_stage() {
    let (rig, run) = start_sidecar(
        "start-cannot-embed",
        &Rig::stays_up(),
        PROBE_CANNOT_EMBED,
        "1",
    );
    assert!(!run.ok, "{}", run.transcript());
    assert!(
        run.stderr.contains("did not embed within 1s")
            && run.stderr.contains("no TASK_EMBED bundle loaded"),
        "{}",
        run.transcript()
    );
    assert!(!rig.recorded("sidecar"));
}

#[test]
fn a_started_sidecar_that_exits_before_embedding_fails_the_stage() {
    let (rig, run) = start_sidecar("start-exits", "exit 1", PROBE_CANNOT_EMBED, "60");
    assert!(!run.ok, "{}", run.transcript());
    assert!(
        run.stderr.contains("exited before it could embed"),
        "{}",
        run.transcript()
    );
    assert!(!rig.recorded("sidecar"));
}

#[test]
fn a_started_sidecar_is_reported_once_its_bundle_loads_and_it_embeds() {
    let embeds_on_third_call = r#"if [ "$(wc -l <"$CALLS/probe")" -lt 3 ]; then
  echo "embedding: FAILED -- model not ready"
  exit 1
fi
echo "embedding: dim 4, L2 norm 1.0000""#;
    let (rig, run) = start_sidecar("start-embeds", &Rig::stays_up(), embeds_on_third_call, "60");
    assert!(run.ok, "{}", run.transcript());
    assert!(
        run.stdout.contains("embed probe passed"),
        "{}",
        run.transcript()
    );
    assert_eq!(rig.calls("probe").len(), 3);
    assert_eq!(rig.calls_once_recorded("sidecar", 1).len(), 1);
    assert!(rig.recorded("sidecar"));
}
