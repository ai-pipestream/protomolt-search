//! Scripted smoke for `cluster_sweep`'s lexical mode (docs/block-max.md,
//! stage 4): stands up a loopback fleet — the mock analysis sidecar
//! plus two v5-resident two-shard clusters over the same corpus, one
//! with `--block-max=false` semantics via `NodeConfig.block_max` — then
//! invokes the `cluster_sweep` binary (sibling of this executable) for
//! the {block-max on, off} x {unseeded, seeded} factorial and asserts
//! the correctness gate passes (exit 0).
//!
//! ```text
//! cargo build --release --examples
//! cargo run --release --example lexical_sweep_smoke
//! ```

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::harness::mock_analysis::start_mock_analysis;
use turbovec_search::harness::start_empty_node;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{AddDocumentsRequest, FlushRequest};

const SHARD_DOCS: [&[&str]; 2] = [
    &["rust search rust fast", "vector search rust"],
    &["search engines love rust", "vector vector vector"],
];

async fn add_documents(addr: &str, texts: &[&str]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in texts {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
        })
        .await
        .unwrap();
    }
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("lex_smoke_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)?;

    let (analysis, mock) = start_mock_analysis().await;
    let mut clusters = Vec::new();
    let mut handles = Vec::new();
    for (tag, block_max) in [("on", true), ("off", false)] {
        let mut addrs = Vec::new();
        for (i, _) in SHARD_DOCS.iter().enumerate() {
            let (addr, handle) = start_empty_node(NodeConfig {
                slot_offset: (i * 2) as u64,
                analysis_addr: Some(analysis.clone()),
                index_path: Some(dir.join(format!("shard-{tag}-{i}.tv"))),
                block_max,
                ..Default::default()
            })
            .await;
            addrs.push(addr);
            handles.push(handle);
        }
        for (i, docs) in SHARD_DOCS.iter().enumerate() {
            add_documents(&addrs[i], docs).await;
            let mut client = NodeServiceClient::connect(addrs[i].clone()).await.unwrap();
            assert!(client.flush(FlushRequest {}).await?.into_inner().written);
        }
        clusters.push(addrs.join(","));
    }

    // The sweep binary is this executable's sibling in the target dir.
    let sweep = std::env::current_exe()?
        .parent()
        .expect("exe has a parent dir")
        .join("cluster_sweep");
    let out = std::process::Command::new(&sweep)
        .args([
            "--bm25-terms=rust,search",
            &format!("--analysis={analysis}"),
            &format!("--nodes-sharing={}", clusters[0]),
            &format!("--nodes-nosharing={}", clusters[1]),
            "--k=4",
            "--queries=5",
        ])
        .output()?;
    println!(
        "--- cluster_sweep stdout ---\n{}",
        String::from_utf8_lossy(&out.stdout)
    );
    println!(
        "--- cluster_sweep stderr ---\n{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.status.success(),
        "cluster_sweep exited with {} (gate failure)",
        out.status
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
    println!("lexical factorial smoke: OK");
    Ok(())
}
