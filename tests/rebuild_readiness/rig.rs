//! A throwaway deployment for `deploy/v7-rebuild/rebuild.sh`. Stub
//! executables stand in for the node binary, the verifier, the embed
//! probe, and the sidecar, and the test process holds the listening
//! ports, so the script's own decisions run with no fleet behind them.

use std::fs;
use std::net::TcpListener;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Mutex, MutexGuard};
use std::time::{Duration, Instant};

use super::scratch::{scratch_path, ScratchPath};

/// An embed probe that succeeds.
pub const PROBE_EMBEDS: &str = r#"echo "embedding: dim 4, L2 norm 1.0000""#;

/// An embed probe against an endpoint that analyzes but has no model.
pub const PROBE_CANNOT_EMBED: &str = r#"echo "embedding: FAILED -- no TASK_EMBED bundle loaded"
exit 1"#;

/// What one run of the script printed and how it exited.
pub struct Run {
    pub ok: bool,
    pub stdout: String,
    pub stderr: String,
}

impl Run {
    fn of(output: Output) -> Self {
        Self {
            ok: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        }
    }

    /// Both streams, for assertion messages.
    pub fn transcript(&self) -> String {
        format!(
            "ok={}\n--- stdout\n{}--- stderr\n{}",
            self.ok, self.stdout, self.stderr
        )
    }
}

pub struct Rig {
    root: ScratchPath,
    env: Vec<(String, String)>,
    held: Vec<TcpListener>,
}

impl Rig {
    /// A one-shard deployment whose executables are all missing until a
    /// test stubs them, pointed at a remote sidecar address nothing dials.
    pub fn new(name: &str) -> Self {
        let root = scratch_path(&format!("rebuild-readiness-{name}"));
        let _ = fs::remove_dir_all(&*root);
        for dir in ["stubs", "path", "calls", "bundles"] {
            fs::create_dir_all(root.join(dir)).unwrap();
        }
        let mut rig = Self {
            root,
            env: Vec::new(),
            held: Vec::new(),
        };
        // The script's waits pause through `sleep`; a no-op first on PATH
        // makes every wait immediate without touching the script.
        write_script(&rig.root.join("path/sleep"), "exit 0");
        let path = std::env::var("PATH").unwrap();
        rig.set("PATH", &format!("{}:{path}", rig.path("path")));
        rig.set("CALLS", &rig.path("calls"));
        rig.set("OUT", &rig.path("out"));
        // A 12-byte header plus one dim-1 record: a one-chunk corpus.
        for (key, value) in [("EMB_BYTES", "28"), ("DIM", "1"), ("BLOCK", "1")] {
            rig.set(key, value);
        }
        rig.set("EMB", &rig.path("no-embeddings.bin"));
        rig.set("SHARDS", "1");
        rig.set("NICE", "0");
        rig.set("EMBEDDINGS_DIR", &rig.path("bundles"));
        rig.set("SIDECAR_ADDR", "http://sidecar.invalid:59202");
        for (key, name) in STUB_VARIABLES {
            rig.set(key, &rig.path(&format!("stubs/{name}")));
        }
        rig
    }

    pub fn path(&self, relative: &str) -> String {
        self.root.join(relative).to_str().unwrap().to_string()
    }

    pub fn set(&mut self, key: &str, value: &str) {
        self.env.retain(|(k, _)| k != key);
        self.env.push((key.to_string(), value.to_string()));
    }

    /// Install the executable the script reaches through `name`'s variable
    /// (`bin`, `verify`, `probe`, or `sidecar`). Every call is recorded
    /// with its arguments before `body` runs.
    pub fn stub(&self, name: &str, body: &str) {
        assert!(STUB_VARIABLES.iter().any(|(_, n)| *n == name), "{name}");
        let log = format!("echo \"$*\" >>\"$CALLS/{name}\"\n{body}");
        write_script(&self.root.join("stubs").join(name), &log);
    }

    /// A stub body that stays alive the way a server does, without the
    /// no-op `sleep` this rig puts on PATH.
    pub fn stays_up() -> String {
        format!("exec {} 120", real_sleep().display())
    }

    /// Replace the no-op `sleep` with a short real one, for a case where
    /// the script waits on something this process does in response to it.
    pub fn pace_waits(&self) {
        let pause = format!("exec {} 0.05", real_sleep().display());
        write_script(&self.root.join("path/sleep"), &pause);
    }

    /// Record, as the sidecar this script started, a process that has
    /// already exited.
    pub fn record_stopped_sidecar(&self) {
        let mut finished = {
            let _exclusive = no_stub_open_for_writing();
            Command::new("true").spawn().unwrap()
        };
        finished.wait().unwrap();
        fs::create_dir_all(self.root.join("out/run")).unwrap();
        fs::write(
            self.root.join("out/run/sidecar.pid"),
            finished.id().to_string(),
        )
        .unwrap();
    }

    /// The argument lines of every call `name` received.
    pub fn calls(&self, name: &str) -> Vec<String> {
        fs::read_to_string(self.root.join("calls").join(name))
            .map(|text| text.lines().map(str::to_string).collect())
            .unwrap_or_default()
    }

    /// Hold an open loopback port for the script to find listening.
    pub fn listening_port(&mut self) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        self.held.push(listener);
        port
    }

    /// Point the deployment at a sidecar on this host's `port`.
    pub fn local_sidecar(&mut self, port: u16) {
        self.set("SIDECAR_PORT", &port.to_string());
        self.set("SIDECAR_ADDR", &format!("http://127.0.0.1:{port}"));
    }

    /// A built shard, listening node and coordinator ports, a node binary
    /// that stays up, and a sidecar that embeds: everything `serve` needs
    /// short of a verifier.
    pub fn ready_to_serve(&mut self) {
        let catalog = self.root.join("out/shard-0.tv.segments");
        fs::create_dir_all(&catalog).unwrap();
        fs::write(catalog.join("segments.json"), "{}").unwrap();
        let node = self.listening_port();
        self.set("PORT_BASE", &node.to_string());
        let coordinator = self.listening_port();
        self.set("COORD_PORT", &coordinator.to_string());
        self.stub("bin", &Self::stays_up());
        self.stub("probe", PROBE_EMBEDS);
    }

    pub fn spawn(&self, stage: &str) -> Child {
        let script = Path::new(env!("CARGO_MANIFEST_DIR")).join("deploy/v7-rebuild/rebuild.sh");
        let _exclusive = no_stub_open_for_writing();
        Command::new("bash")
            .arg(script)
            .arg(stage)
            .env_clear()
            .envs(self.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn bash")
    }

    pub fn run(&self, stage: &str) -> Run {
        Run::of(self.spawn(stage).wait_with_output().unwrap())
    }

    /// Wait for `child` to create `marker` under the rig, or to exit
    /// without doing so, then return.
    pub fn await_marker(&self, child: &mut Child, marker: &str) {
        let deadline = Instant::now() + Duration::from_secs(30);
        while !self.root.join(marker).exists() && child.try_wait().unwrap().is_none() {
            assert!(Instant::now() < deadline, "{marker} never appeared");
            std::thread::sleep(Duration::from_millis(5));
        }
    }

    pub fn finish(&self, child: Child) -> Run {
        Run::of(child.wait_with_output().unwrap())
    }

    /// True when the stage ledger records a completed `stage`.
    pub fn recorded(&self, stage: &str) -> bool {
        fs::read_to_string(self.root.join("out/timings.jsonl"))
            .is_ok_and(|ledger| ledger.contains(&format!("\"stage\":\"{stage}\"")))
    }
}

impl Drop for Rig {
    /// Stop whatever stand-in processes the script left running.
    fn drop(&mut self) {
        let Ok(entries) = fs::read_dir(self.root.join("out/run")) else {
            return;
        };
        let _exclusive = no_stub_open_for_writing();
        for entry in entries.flatten() {
            if let Ok(pid) = fs::read_to_string(entry.path()) {
                let _ = Command::new("kill")
                    .arg(pid.trim())
                    .stderr(Stdio::null())
                    .status();
            }
        }
    }
}

/// The script's variable for each stub, and the stub's file name.
const STUB_VARIABLES: [(&str, &str); 4] = [
    ("BIN", "bin"),
    ("VERIFY", "verify"),
    ("PROBE", "probe"),
    ("SIDECAR_BIN", "sidecar"),
];

/// Held while a stub is written and while this process starts a child.
/// A child started in between would hold the stub's write descriptor
/// until its own exec, and the kernel refuses to run a file that is open
/// for writing.
fn no_stub_open_for_writing() -> MutexGuard<'static, ()> {
    static GATE: Mutex<()> = Mutex::new(());
    GATE.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn write_script(path: &Path, body: &str) {
    let _exclusive = no_stub_open_for_writing();
    fs::write(path, format!("#!/bin/sh\n{body}\n")).unwrap();
    fs::set_permissions(path, fs::Permissions::from_mode(0o755)).unwrap();
}

/// The system `sleep`, by absolute path.
fn real_sleep() -> PathBuf {
    let path = std::env::var_os("PATH").expect("PATH");
    std::env::split_paths(&path)
        .map(|dir| dir.join("sleep"))
        .find(|candidate| candidate.is_file())
        .expect("a sleep executable on PATH")
}
