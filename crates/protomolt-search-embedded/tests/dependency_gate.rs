//! The mobile link gate (`docs/embedded-mobile.md`): the embedded crate
//! must not link tonic's HTTP/2 transport, `h2`, `hyper`, or Tokio's
//! networking. `cargo tree` on this package, resolved on its own, is the
//! evidence; the test fails the moment one of them comes back.

use std::process::Command;

fn cargo_tree(args: &[&str]) -> String {
    let cargo = std::env::var("CARGO").unwrap_or_else(|_| "cargo".into());
    let output = Command::new(cargo)
        .args([
            "tree",
            "-p",
            "protomolt-search-embedded",
            "--edges",
            "normal",
        ])
        .args(args)
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo tree runs");
    assert!(
        output.status.success(),
        "cargo tree failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("cargo tree prints UTF-8")
}

#[test]
fn the_embedded_crate_links_no_network_stack() {
    let tree = cargo_tree(&["--prefix", "none", "--no-dedupe"]);
    let mut live: Vec<&str> = Vec::new();
    for line in tree.lines() {
        let name = line.split_whitespace().next().unwrap_or("");
        if matches!(
            name,
            "h2" | "hyper" | "hyper-util" | "axum" | "tonic-web" | "rustls"
        ) {
            live.push(line);
        }
    }
    assert!(
        live.is_empty(),
        "the embedded crate links a network stack it does not use:\n{}",
        live.join("\n")
    );
    // tonic itself is expected, for Status and the codec; its transport
    // feature is not.
    let features = cargo_tree(&[
        "--edges", "features", "--format", "{p} {f}", "--prefix", "none",
    ]);
    for line in features.lines() {
        let mut words = line.split_whitespace();
        let (Some(name), Some(_version)) = (words.next(), words.next()) else {
            continue;
        };
        let enabled: Vec<&str> = words.flat_map(|w| w.split(',')).collect();
        match name {
            "tokio" => assert!(
                !enabled.iter().any(|f| *f == "net" || *f == "signal"),
                "tokio carries networking features: {line}"
            ),
            "tonic" => assert!(
                !enabled
                    .iter()
                    .any(|f| matches!(*f, "transport" | "channel" | "server" | "router" | "tls")),
                "tonic carries its transport: {line}"
            ),
            "pipestream-search" => assert!(
                !enabled.iter().any(|f| *f == "net" || *f == "tls"),
                "the search crate is built with its network stack: {line}"
            ),
            _ => {}
        }
    }
}
