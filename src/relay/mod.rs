//! Relay coordinators: a coordinator process that presents itself to its
//! parent as ONE shard.
//!
//! The parent calls `NodeService` routes on a relay exactly as it calls
//! them on a node, and the relay answers the read routes by fanning out
//! to its children (shard nodes or further relays) and merging their
//! answers exactly. Every write, administration, and per-shard fusion
//! route refuses UNIMPLEMENTED by name (`refused`). Each decision pins
//! the [`MapSource`] reading it was made under and refuses by name when
//! that reading is no longer current.

mod bitmaps;
mod bm25_legs;
mod bm25_stream;
mod boolean;
mod diagnostics;
mod fanout;
mod fetch;
mod health;
mod lexical;
mod map;
mod merge_browse;
mod merge_dictionary;
mod merge_lexical;
mod merge_util;
mod merge_values;
mod rescore;
mod service;
mod shard_legs;
mod shard_scan;
mod shard_search;
mod signals;
mod stream_forward;
mod stream_identity;
mod stream_search;
mod stream_state;
#[cfg(test)]
mod tests;
mod values;
mod vector;

pub use self::bitmaps::{concat_bitmaps, ChildBitmap};
pub use self::diagnostics::RelayDiagnostics;
pub use self::health::{merge_health, merge_shard_layouts};
pub use self::map::{
    CoordinatorMapSource, MapSnapshot, MapSource, RelayMap, TokenRegistry, TokenTuple,
    RETAINED_TOKENS,
};
pub use self::merge_browse::merge_browse_pages;
pub use self::merge_dictionary::{merge_prefix_expansions, merge_suggest_terms};
pub use self::merge_lexical::{merge_bm25_responses, merge_boolean_responses, merge_term_stats};
pub use self::merge_values::merge_aggregate_shares;
pub use self::vector::merge_vector_backend;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tonic::Status;

use crate::coordinator::CoordinatorServiceImpl;
use crate::node::STALE_STATS_EPOCH;
use crate::pb::node_service_server::NodeServiceServer;
use crate::stats_identity::{StatsClaim, StatsIncarnation};

use self::signals::RelaySignals;

/// What a relay holds: the base coordinator (links, keys, limits), the
/// map source, the token registry, the parent-facing signal registry,
/// and the identity the parent sees.
pub struct RelayInner {
    base: Arc<CoordinatorServiceImpl>,
    map: Arc<dyn MapSource>,
    collection: String,
    tokens: Mutex<TokenRegistry>,
    stats_incarnation: StatsIncarnation,
    signals: Arc<Mutex<HashMap<u64, Arc<RelaySignals>>>>,
}

impl RelayInner {
    /// The current map reading and a coordinator frozen over exactly
    /// its children: the pair every decision starts from.
    fn pin(&self) -> (MapSnapshot, CoordinatorServiceImpl) {
        let snapshot = self.map.current();
        let frozen = self.base.frozen_over(
            snapshot.topology_generation,
            &snapshot.map.routes,
            snapshot.map.placement.clone(),
        );
        (snapshot, frozen)
    }

    /// Refuse by name when the map moved since `pinned` was taken.
    fn still_current(&self, pinned: &MapSnapshot, route: &str) -> Result<(), Status> {
        let now = self.map.current();
        if now.control_revision != pinned.control_revision {
            return Err(Status::failed_precondition(format!(
                "relay: the shard map moved from revision {} (generation {}) to revision {} \
                 (generation {}) during {route}; the answer was made under the older map and \
                 is not served; retry under the current one",
                pinned.control_revision,
                pinned.topology_generation,
                now.control_revision,
                now.topology_generation
            )));
        }
        Ok(())
    }
}

/// A relay coordinator's node-facing service. Cheap to clone: the server
/// takes one clone and a test or the process keeps another to read the
/// token registry or run the startup check.
#[derive(Clone)]
pub struct RelayService {
    inner: Arc<RelayInner>,
}

impl std::ops::Deref for RelayService {
    type Target = RelayInner;

    fn deref(&self) -> &RelayInner {
        &self.inner
    }
}

impl RelayService {
    /// A relay over `coordinator`'s shard set. The coordinator supplies
    /// the child links, the UDP key, the placement mask, and the stream
    /// fan-out; the relay adds the node-facing surface.
    pub fn new(coordinator: Arc<CoordinatorServiceImpl>) -> Self {
        let map = Arc::new(CoordinatorMapSource::new(Arc::clone(&coordinator)));
        Self::with_map(coordinator, map)
    }

    /// A relay whose shard map comes from `map` (a replicated control
    /// state, say) while `base` supplies the child links, the UDP key,
    /// and the fan-out limits.
    pub fn with_map(base: Arc<CoordinatorServiceImpl>, map: Arc<dyn MapSource>) -> Self {
        let collection = base.collection().to_string();
        let incarnation = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1)
            & 0xffff_ffff;
        RelayService {
            inner: Arc::new(RelayInner {
                base,
                map,
                collection,
                tokens: Mutex::new(TokenRegistry::new(incarnation | 1)),
                stats_incarnation: Default::default(),
                signals: Arc::new(Mutex::new(HashMap::new())),
            }),
        }
    }

    pub fn into_server(self, max_message_bytes: usize) -> NodeServiceServer<Self> {
        NodeServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// The coordinator the relay fans out through: the child links, the
    /// UDP key, the limits, and (under the file-polled source) the map.
    pub fn base(&self) -> &Arc<CoordinatorServiceImpl> {
        &self.inner.base
    }

    /// The current map reading.
    pub fn map(&self) -> MapSnapshot {
        self.inner.map.current()
    }

    /// The children of the current map, in shard order.
    pub fn children(&self) -> Vec<String> {
        self.map().map.children()
    }

    /// The tuple a token stands for, or the stale-epoch refusal: unknown
    /// tokens, and tokens issued under a map revision that is no longer
    /// current, both refuse, because a claim on a child set the map no
    /// longer names is not a claim this relay can translate.
    pub fn token_tuple(&self, token: u64) -> Result<TokenTuple, Status> {
        let tuple = {
            let registry = self.tokens.lock().expect("relay token registry poisoned");
            registry.lookup(token).cloned()
        }
        .ok_or_else(|| {
            Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: relay token {token} is not one this relay issued and \
                 still retains (a child's statistics moved, the token is older than the \
                 {RETAINED_TOKENS} newest, or the relay restarted); refetch TermStats"
            ))
        })?;
        let now = self.map();
        if now.control_revision != tuple.control_revision {
            return Err(Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: relay token {token} was issued under map revision {} and \
                 the map is at revision {} now; refetch TermStats",
                tuple.control_revision, now.control_revision
            )));
        }
        Ok(tuple)
    }

    /// The per-child epoch claims a parent's token stands for, parallel
    /// to the token's children (the current map's, or the token would
    /// have refused). Token 0 is no claim and translates to no claim on
    /// any child, exactly as a node reads it.
    pub fn translate_epoch(&self, token: u64) -> Result<Vec<StatsClaim>, Status> {
        if token == 0 {
            return Ok(vec![StatsClaim::default(); self.children().len()]);
        }
        Ok(self.token_tuple(token)?.epochs)
    }

    /// One request per child with the parent's claim translated into
    /// that child's, or the stale-epoch refusal when the token is not
    /// one this relay can translate under the current map.
    pub(super) fn child_claims(
        &self,
        token: u64,
        incarnation: &[u8],
        children: usize,
    ) -> Result<Vec<StatsClaim>, Status> {
        self.stats_incarnation.check(token, token, incarnation)?;
        let claims = self.translate_epoch(token)?;
        if claims.len() != children {
            return Err(Status::failed_precondition(format!(
                "{STALE_STATS_EPOCH}: relay token {token} names {} children and the map has \
                 {children}; refetch TermStats",
                claims.len()
            )));
        }
        Ok(claims)
    }
}

/// The gRPC deadline of a request, from its `grpc-timeout` header.
pub(crate) fn grpc_timeout(metadata: &tonic::metadata::MetadataMap) -> Option<Duration> {
    let text = metadata.get("grpc-timeout")?.to_str().ok()?;
    let (digits, unit) = text.split_at(text.len().checked_sub(1)?);
    let value: u64 = digits.parse().ok()?;
    let duration = match unit {
        "H" => Duration::from_secs(value.checked_mul(3600)?),
        "M" => Duration::from_secs(value.checked_mul(60)?),
        "S" => Duration::from_secs(value),
        "m" => Duration::from_millis(value),
        "u" => Duration::from_micros(value),
        "n" => Duration::from_nanos(value),
        _ => return None,
    };
    Some(duration)
}

/// The refusal every route outside the relay's scope answers with.
fn refused(route: &str) -> Status {
    Status::unimplemented(format!(
        "relay: NodeService.{route} is not served by a relay coordinator; a relay forwards \
         the read routes only: StreamSearch, SearchShard, TermStats, Health, \
         GetVectorBackend, the keyword leg (Bm25Query, Bm25PhraseQuery, Bm25QueryStream, \
         Bm25Rescore, ShardLegs), VectorRescore, ExactVectorRescore, the bitmap routes \
         (ResolveFilterBitmap, ResolveLexicalBitmap, ResolveVectorBitmap), the \
         dictionaries (ExpandTermPrefix, SuggestTerms), the fetches by id (GetDocuments, \
         ResolveParents, FetchValues, BrowseShard), the folds (AggregateShard, \
         QuantileCounts, EvaluateBoolean)"
    ))
}
