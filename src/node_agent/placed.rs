//! Serving shards the worker placed under `--data-dir`, each on a
//! listener of its own.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::node::NodeServiceImpl;

use super::{NodeAgent, PlacedShard, ServedShard};

impl NodeAgent {
    /// Reopen and serve every shard the worker placed under the data
    /// directory before (a restart resumes them where they were).
    pub async fn open_placed(&self) -> Result<Vec<String>, String> {
        let data_dir = &self.inner.config.data_dir;
        std::fs::create_dir_all(data_dir)
            .map_err(|e| format!("create data dir {}: {e}", data_dir.display()))?;
        let mut opened = Vec::new();
        let mut entries: Vec<PathBuf> = std::fs::read_dir(data_dir)
            .map_err(|e| format!("read data dir {}: {e}", data_dir.display()))?
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| PlacedShard::path(path).exists())
            .collect();
        entries.sort();
        for dir in entries {
            let placed = PlacedShard::load(&dir)?.expect("checked above");
            if placed.collection != self.inner.config.collection {
                continue;
            }
            self.serve_placed(placed).await?;
            opened.push(
                dir.file_name()
                    .map(|n| n.to_string_lossy().into_owned())
                    .unwrap_or_default(),
            );
        }
        Ok(opened)
    }

    /// Open (or create) the placed shard's files and put them on a
    /// listener of their own; idempotent for a shard already served.
    pub(super) async fn serve_placed(&self, mut placed: PlacedShard) -> Result<String, String> {
        if let Some(shard) = self.shards().get(&placed.shard_id) {
            if shard.placed.is_none() {
                return Err(format!(
                    "shard {:?} is configured statically on node {:?}; the plan cannot place \
                     a copy of it here",
                    placed.shard_id, self.inner.config.node_id
                ));
            }
            return Ok(shard.addr.clone());
        }
        let dir = self.shard_dir(&placed.shard_id);
        let node = self.open_placed_node(&placed, &dir)?;
        let listener = self.bind_replica_listener(&placed).await?;
        let bound = listener
            .local_addr()
            .map_err(|e| format!("listener addr: {e}"))?;
        placed.port = bound.port();
        placed.write(&dir)?;
        node.spawn_floor_listener(bound);
        let scheme = if self.inner.config.tls.is_some() {
            "https"
        } else {
            "http"
        };
        let host = &self.inner.config.advertise_host;
        let addr = format!("{scheme}://{host}:{}", bound.port());
        let server = self.spawn_server(node.clone(), listener)?;
        eprintln!(
            "node {:?}: shard {:?} placed at {addr} ({})",
            self.inner.config.node_id,
            placed.shard_id,
            dir.display()
        );
        let shard = ServedShard {
            shard_id: placed.shard_id.clone(),
            node,
            addr: addr.clone(),
            hash_range: Some((placed.hash_lo, placed.hash_hi)),
            placed: Some(placed),
            server: Some(server),
        };
        self.shards().insert(shard.shard_id.clone(), shard);
        Ok(addr)
    }

    /// The placed shard's node, opened from the template configuration
    /// with its own index path, slot offset, collection, and WAL.
    fn open_placed_node(
        &self,
        placed: &PlacedShard,
        dir: &std::path::Path,
    ) -> Result<NodeServiceImpl, String> {
        let mut config = self.inner.config.template.clone();
        config.index_path = Some(dir.join("shard"));
        config.slot_offset = placed.slot_offset;
        config.collection = placed.collection.clone();
        config.wal = true;
        std::fs::create_dir_all(dir).map_err(|e| format!("create {}: {e}", dir.display()))?;
        Ok(
            NodeServiceImpl::open(config, self.inner.config.phrase_index.clone())?
                .with_flush_notify(Arc::clone(&self.inner.flush_notify)),
        )
    }

    /// The shard's listener: the port it bound before, else the next one
    /// above the configured base (or any, when the base port is 0).
    async fn bind_replica_listener(
        &self,
        placed: &PlacedShard,
    ) -> Result<tokio::net::TcpListener, String> {
        let base = self.inner.config.replica_listen;
        let port = if placed.port != 0 {
            placed.port
        } else if base.port() == 0 {
            0
        } else {
            let taken = self
                .shards()
                .values()
                .filter(|shard| shard.placed.is_some())
                .count() as u16;
            base.port().saturating_add(taken)
        };
        tokio::net::TcpListener::bind(SocketAddr::new(base.ip(), port))
            .await
            .map_err(|e| {
                format!(
                    "bind replica listener {}:{port} for shard {:?}: {e}",
                    base.ip(),
                    placed.shard_id
                )
            })
    }

    fn spawn_server(
        &self,
        node: NodeServiceImpl,
        listener: tokio::net::TcpListener,
    ) -> Result<tokio::task::JoinHandle<()>, String> {
        let max = self.inner.config.max_message_bytes;
        let builder = match &self.inner.config.tls {
            None => tonic::transport::Server::builder(),
            #[cfg(feature = "tls")]
            Some(tls) => tonic::transport::Server::builder()
                .tls_config(tls.server_config(true))
                .map_err(|e| format!("replica listener TLS: {e}"))?,
            #[cfg(not(feature = "tls"))]
            Some(_) => return Err("this build has no TLS support (feature `tls` is off)".into()),
        };
        let server = builder
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW)
            .add_service(NodeServiceImpl::into_server(node, max))
            .serve_with_incoming(crate::harness::nodelay_incoming(listener));
        Ok(tokio::spawn(async move {
            if let Err(e) = server.await {
                eprintln!("replica listener ended: {e}");
            }
        }))
    }

    /// Stop serving a removed placed shard and delete its directory.
    pub(super) fn remove_placed(
        &self,
        shard: ServedShard,
        shard_id: &str,
    ) -> Result<PathBuf, String> {
        if let Some(server) = shard.server {
            server.abort();
        }
        drop(shard.node);
        let dir = self.shard_dir(shard_id);
        std::fs::remove_dir_all(&dir).map_err(|e| format!("remove {}: {e}", dir.display()))?;
        Ok(dir)
    }
}
