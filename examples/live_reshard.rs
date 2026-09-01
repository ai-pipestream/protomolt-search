//! Operator control for a stable-key live reshard.
//!
//! The offline baseline comes from:
//!
//! ```text
//! cargo run --release --example reshard -- \
//!   --log=/data/source.tv.wal --split=2 --stable-routing \
//!   --out-dir=/data/next --slot-base=0 --slot-stride=25000000
//! ```
//!
//! Start the emitted child images on the addresses in a staging map, then:
//!
//! ```text
//! live_reshard init --source=http://old:50051 \
//!   --cutoff=/data/next/live-cutoff.toml --old-generation=7 \
//!   --children-map=/data/next/staging-map.toml --state=/data/next/live.toml
//! live_reshard catch-up --state=/data/next/live.toml
//! live_reshard cutover --state=/data/next/live.toml \
//!   --coordinator=http://coordinator:50050 \
//!   --publish-map=/etc/protomolt-search/shard-map.toml
//! ```

use std::path::{Path, PathBuf};

use pipestream_search::replication::{LiveChild, LiveReshardState};
use serde::Deserialize;

#[derive(Deserialize)]
struct CutoffFile {
    generation: u64,
    high_watermark: u64,
}

fn option(name: &str) -> Result<String, String> {
    let prefix = format!("--{name}=");
    std::env::args()
        .find_map(|argument| argument.strip_prefix(&prefix).map(str::to_string))
        .ok_or_else(|| format!("missing --{name}=..."))
}

fn state_path() -> Result<PathBuf, String> {
    option("state").map(PathBuf::from)
}

fn usage() -> &'static str {
    "usage:\n  live_reshard init --source=URL --cutoff=FILE --old-generation=N \\\n+     --children-map=FILE --state=FILE\n  live_reshard catch-up --state=FILE\n  \
     live_reshard cutover --state=FILE --coordinator=URL --publish-map=FILE"
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let command = std::env::args().nth(1).ok_or_else(usage)?;
    match command.as_str() {
        "init" => {
            let source = pipestream_search::config::normalize_addr(option("source")?);
            let cutoff_path = option("cutoff")?;
            let cutoff: CutoffFile = toml::from_str(&std::fs::read_to_string(&cutoff_path)?)?;
            let old_topology_generation: u64 = option("old-generation")?.parse()?;
            let children_map =
                pipestream_search::config::load_shard_map(Path::new(&option("children-map")?))?;
            let children = children_map
                .shards
                .into_iter()
                .map(|shard| {
                    let (hash_lo, hash_hi) = shard
                        .hash_lo
                        .zip(shard.hash_hi)
                        .ok_or("every child needs hash_lo and hash_hi")?;
                    Ok(LiveChild {
                        addr: pipestream_search::config::normalize_addr(shard.addr),
                        replica: shard.replica.map(pipestream_search::config::normalize_addr),
                        hash_lo,
                        hash_hi,
                        slot_offset: shard.slot_offset,
                        base_vectors: 0,
                        base_document_slots: 0,
                        applied_vectors: 0,
                        applied_documents: 0,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            let state = pipestream_search::replication::initialize_live_reshard(
                source,
                pipestream_search::reshard::WalCutoff {
                    generation: cutoff.generation,
                    high_watermark: cutoff.high_watermark,
                },
                old_topology_generation,
                children_map.generation,
                children,
            )
            .await?;
            let path = state_path()?;
            state.write(&path)?;
            println!(
                "initialized {} child(s) at source WAL generation {} clock {}; wrote {}",
                state.children.len(),
                state.source_wal_generation,
                state.source_clock,
                path.display()
            );
        }
        "catch-up" => {
            let path = state_path()?;
            let state = LiveReshardState::load(&path)?;
            let updated = pipestream_search::replication::catch_up_children_once(&state).await?;
            updated.write(&path)?;
            println!(
                "caught children up through source clock {}; wrote {}",
                updated.source_clock,
                path.display()
            );
        }
        "cutover" => {
            let path = state_path()?;
            let state = LiveReshardState::load(&path)?;
            let coordinator = pipestream_search::config::normalize_addr(option("coordinator")?);
            let publish_map = PathBuf::from(option("publish-map")?);
            let updated = pipestream_search::replication::atomic_live_cutover(
                &coordinator,
                &state,
                &path,
                &publish_map,
            )
            .await?;
            println!(
                "published topology generation {} at source clock {}; map {}",
                updated.new_topology_generation,
                updated.source_clock,
                publish_map.display()
            );
        }
        _ => return Err(usage().into()),
    }
    Ok(())
}
