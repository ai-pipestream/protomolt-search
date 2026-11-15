use std::path::PathBuf;

mod columns;
mod files;
mod option;
mod raft;
mod roles;

/// The arguments of one test plus `--allow-plaintext`: these tests bind the
/// non-loopback defaults and are about other options, and plaintext off
/// loopback needs the explicit flag.
fn args(pairs: &[&str]) -> Vec<String> {
    let mut v = args_raw(pairs);
    if !pairs
        .iter()
        .any(|p| p.starts_with("--tls-") || *p == "--allow-plaintext")
    {
        v.push("--allow-plaintext".to_string());
    }
    v
}

fn args_raw(pairs: &[&str]) -> Vec<String> {
    pairs.iter().map(|s| s.to_string()).collect()
}

/// `pairs` with `extra` appended.
fn with(pairs: &[&str], extra: &[&str]) -> Vec<String> {
    let mut all = pairs.to_vec();
    all.extend_from_slice(extra);
    args(&all)
}

/// A file under the crate's `target/tmp`, unique to this process and `stem`.
fn temp_file(stem: &str, contents: &str) -> PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!("{stem}_{}.toml", std::process::id()));
    std::fs::write(&path, contents).unwrap();
    path
}

/// `--config=<path>` for a file written by [`temp_file`].
fn config_flag(path: &std::path::Path) -> String {
    format!("--config={}", path.display())
}
