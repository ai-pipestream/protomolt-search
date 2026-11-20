use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

#[derive(Debug)]
pub struct TurboOutcome {
    pub baseline: (u32, u32, u32),
    pub case_count: usize,
    pub tie_entry: u32,
    pub fingerprint: u64,
    pub reversed_fingerprint: u64,
}

struct ProbeDir(PathBuf);

impl ProbeDir {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "turbo-glossary-probe-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for ProbeDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn find_glossary_rlib(deps: &Path) -> Option<PathBuf> {
    let mut candidates: Vec<_> = std::fs::read_dir(deps)
        .ok()?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .filter(|path| {
            let name = path.file_name().unwrap_or_default().to_string_lossy();
            name.starts_with("libturbo_glossary-") && name.ends_with(".rlib")
        })
        .collect();
    candidates.sort_by_key(|path| path.metadata().unwrap().modified().unwrap());
    candidates.pop()
}

fn dependency_dir() -> PathBuf {
    let exe = std::env::current_exe().unwrap();
    let parent = exe.parent().unwrap();
    if find_glossary_rlib(parent).is_some() {
        parent.to_path_buf()
    } else {
        parent.join("deps")
    }
}

fn compile_probe(source: &Path, deps: &Path, binary: &Path) {
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| "rustc".into());
    let rlib = find_glossary_rlib(deps).expect("turbo_glossary rlib is built");
    let output = Command::new(rustc)
        .arg("--edition=2021")
        .arg(source)
        .arg("-L")
        .arg(format!("dependency={}", deps.display()))
        .arg("--extern")
        .arg(format!("turbo_glossary={}", rlib.display()))
        .arg("-o")
        .arg(binary)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "glossary probe build failed:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_probe(source: &Path) -> BTreeMap<String, String> {
    let probe_dir = ProbeDir::new();
    let binary = probe_dir.0.join("turbo_glossary_probe");
    compile_probe(source, &dependency_dir(), &binary);
    let output = Command::new(&binary).output().unwrap();
    assert!(
        output.status.success(),
        "glossary probe failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).unwrap();
    stdout
        .lines()
        .map(|line| {
            let (key, value) = line.split_once(' ').unwrap();
            (key.to_string(), value.to_string())
        })
        .collect()
}

fn observe() -> TurboOutcome {
    let source = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/turbo_native_parity_gate/glossary_probe.rs");
    let output = run_probe(&source);
    let baseline: Vec<u32> = output["BASELINE"]
        .split(' ')
        .map(|field| field.parse().unwrap())
        .collect();
    TurboOutcome {
        baseline: (baseline[0], baseline[1], baseline[2]),
        case_count: output["CASE"].parse().unwrap(),
        tie_entry: output["TIE"].parse().unwrap(),
        fingerprint: output["FINGERPRINT"].parse().unwrap(),
        reversed_fingerprint: output["REVERSED"].parse().unwrap(),
    }
}

pub fn outcome() -> &'static TurboOutcome {
    static OUTCOME: OnceLock<TurboOutcome> = OnceLock::new();
    OUTCOME.get_or_init(observe)
}
