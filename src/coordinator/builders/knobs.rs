//! Live and fixed diagnostic knobs, fan-out limits, and shard layout diagnostics.

use super::*;

impl CoordinatorServiceImpl {
    /// Builders run before the coordinator is shared, so a knob change
    /// at build time replaces the set; the live values carry over.
    pub(in crate::coordinator) fn rebuild_knobs(
        &mut self,
        max_k: u32,
        hedge_delay: Option<Duration>,
        shard_pruning: bool,
        signal_batch: u32,
    ) {
        self.knobs = Arc::new(crate::diagnostics::Knobs::coordinator(
            self.knobs.process().to_string(),
            crate::diagnostics::CoordinatorKnobValues {
                max_k,
                hedge_delay_ms: hedge_delay.map_or(0, |d| d.as_millis() as u64),
                shard_pruning,
                signal_batch,
            },
            Vec::new(),
        ));
    }

    /// The read-at-startup settings listed beside the live knobs.
    pub(in crate::coordinator) fn refresh_fixed_knobs(&self) {
        use crate::diagnostics::FixedKnob;
        use crate::pb::KnobKind;
        self.knobs.set_fixed(vec![
            FixedKnob {
                name: "collection",
                kind: KnobKind::String,
                value: self.collection.clone(),
                description: "The collection this coordinator serves.",
            },
            FixedKnob {
                name: "nodes",
                kind: KnobKind::Int,
                value: self.node_addrs.len().to_string(),
                description: "Shard nodes in the construction-time shard map.",
            },
            FixedKnob {
                name: "replicas",
                kind: KnobKind::Int,
                value: self
                    .replica_addrs
                    .iter()
                    .filter(|r| r.is_some())
                    .count()
                    .to_string(),
                description: "Shards with a replica configured.",
            },
            FixedKnob {
                name: "stream_search",
                kind: KnobKind::Bool,
                value: self.stream_search.to_string(),
                description: "Vector legs over the streaming shard search (--stream-search).",
            },
            FixedKnob {
                name: "bm25_stream",
                kind: KnobKind::Bool,
                value: self.bm25_stream.to_string(),
                description: "Lexical legs over the streaming BM25 query (--bm25-stream).",
            },
            FixedKnob {
                name: "max_rerank_bytes",
                kind: KnobKind::Int,
                value: self.max_rerank_bytes.to_string(),
                description: "Largest exact-rerank pool in FP32 bytes (--max-rerank-bytes).",
            },
            FixedKnob {
                name: "shard_deadline_ms",
                kind: KnobKind::Int,
                value: self
                    .limits
                    .shard_deadline
                    .map_or(0, |d| d.as_millis() as u64)
                    .to_string(),
                description: "Bound on one shard's attempt; 0 is none (--shard-deadline-ms).",
            },
            FixedKnob {
                name: "dense_execution_policy",
                kind: KnobKind::Bool,
                value: self.dense_execution_policy.current().is_some().to_string(),
                description: "A dense execution policy is installed (--dense-execution-policy).",
            },
        ]);
    }

    /// The knobs this coordinator reads at request time.
    pub fn knobs(&self) -> &Arc<crate::diagnostics::Knobs> {
        &self.knobs
    }

    /// Fan-out limits with the live hedge delay.
    pub(in crate::coordinator) fn limits(&self) -> FanoutLimits {
        FanoutLimits {
            shard_deadline: self.limits.shard_deadline,
            hedge_delay: self.knobs.hedge_delay(),
        }
    }

    /// Collect layout diagnostics across shard nodes: in-process nodes answer
    /// directly and remote ones via `DiagnosticsService`, recording RPC errors
    /// on unserved entries.
    pub async fn shard_diagnostics(
        &self,
        only: Option<u32>,
    ) -> Vec<crate::pb::ShardLayoutDiagnostics> {
        let mut out = Vec::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let shard = shard as u32;
            if only.is_some_and(|s| s != shard) {
                continue;
            }
            let layout = match self.node_client(addr) {
                Ok(crate::link::NodeLink::Local(node)) => {
                    node.shard_diagnostics(shard, addr.clone())
                }
                #[cfg(feature = "net")]
                Ok(crate::link::NodeLink::Remote(_)) => match self.connect(addr) {
                    Ok(channel) => {
                        let mut client =
                            crate::pb::diagnostics_service_client::DiagnosticsServiceClient::new(
                                channel,
                            );
                        match tokio::time::timeout(
                            Duration::from_secs(5),
                            client.get_shard_diagnostics(crate::pb::ShardDiagnosticsRequest {
                                shard: None,
                            }),
                        )
                        .await
                        {
                            Ok(Ok(reply)) => match reply.into_inner().shards.into_iter().next() {
                                Some(mut layout) => {
                                    layout.shard = shard;
                                    layout.address = addr.clone();
                                    layout
                                }
                                None => crate::diagnostics::unserved_layout(
                                    shard,
                                    addr.clone(),
                                    "EMPTY: the node reported no shard",
                                ),
                            },
                            Ok(Err(status)) => crate::diagnostics::unserved_layout(
                                shard,
                                addr.clone(),
                                &format!("{:?}: {}", status.code(), status.message()),
                            ),
                            Err(_) => crate::diagnostics::unserved_layout(
                                shard,
                                addr.clone(),
                                "DEADLINE_EXCEEDED: diagnostics probe timed out",
                            ),
                        }
                    }
                    Err(status) => crate::diagnostics::unserved_layout(
                        shard,
                        addr.clone(),
                        &format!("{:?}: {}", status.code(), status.message()),
                    ),
                },
                Err(status) => crate::diagnostics::unserved_layout(
                    shard,
                    addr.clone(),
                    &format!("{:?}: {}", status.code(), status.message()),
                ),
            };
            out.push(layout);
        }
        out
    }
}
