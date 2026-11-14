//! A relay's view of its children: the shard map, its source, and the token registry.

use std::collections::VecDeque;
use std::sync::Arc;

use tokio::sync::watch;
use tonic::Status;

use crate::coordinator::{CoordinatorServiceImpl, TopologyRoute};
use crate::stats_identity::StatsClaim;

/// Relay tokens retained per relay; the oldest is forgotten first. A
/// parent holding an older token gets the stale-epoch refusal and
/// refetches, which is the same thing it does when a child's epoch moves.
pub const RETAINED_TOKENS: usize = 256;

/// What one relay token stands for: the exact children and the epoch
/// each reported when the token was allocated. The parent's claim on
/// the token translates into one claim per child, which the child
/// enforces itself.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TokenTuple {
    pub collection: String,
    /// The map revision the children were read under.
    pub control_revision: u64,
    pub topology_generation: u64,
    pub children: Vec<String>,
    pub epochs: Vec<StatsClaim>,
}

/// Relay tokens (`docs/relay-coordinators.md`, "The epoch token"): a
/// nonzero allocation bound to a [`TokenTuple`], reused while the tuple
/// repeats so a parent's stats cache keeps hitting, replaced the moment
/// any child's lifetime or epoch moves, and retained for [`RETAINED_TOKENS`]
/// tuples. The relay's separate 32-byte identity fences a restart; the legacy
/// numeric clock prefix below is not sufficient to identify a lifetime.
pub struct TokenRegistry {
    incarnation: u64,
    pub(super) counter: u32,
    pub(super) entries: VecDeque<(u64, TokenTuple)>,
}

impl TokenRegistry {
    pub(super) fn new(incarnation: u64) -> Self {
        Self {
            incarnation,
            counter: 0,
            entries: VecDeque::new(),
        }
    }

    pub(super) fn allocate(&mut self, tuple: TokenTuple) -> Result<u64, Status> {
        if let Some(position) = self.entries.iter().position(|(_, t)| *t == tuple) {
            let entry = self.entries.remove(position).expect("position from iter");
            let token = entry.0;
            self.entries.push_back(entry);
            return Ok(token);
        }
        self.counter = self
            .counter
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("relay statistics token space exhausted"))?;
        let token = (self.incarnation << 32) | u64::from(self.counter);
        self.entries.push_back((token, tuple));
        while self.entries.len() > RETAINED_TOKENS {
            self.entries.pop_front();
        }
        Ok(token)
    }

    pub(super) fn lookup(&self, token: u64) -> Option<&TokenTuple> {
        self.entries
            .iter()
            .rev()
            .find(|(t, _)| *t == token)
            .map(|(_, tuple)| tuple)
    }
}

/// One immutable reading of a relay's shard map: the children in shard
/// order with their placement codes, and the placement tree of that
/// generation when the map has one.
#[derive(Debug, Clone)]
pub struct RelayMap {
    pub routes: Vec<TopologyRoute>,
    pub placement: Option<Arc<crate::placement::Placement>>,
}

impl RelayMap {
    /// Child addresses in shard order.
    pub fn children(&self) -> Vec<String> {
        self.routes.iter().map(|route| route.addr.clone()).collect()
    }
}

/// A map reading stamped with where it came from. `control_revision` is
/// the number a decision pins; `topology_generation` is what the shards
/// and the parent see on the wire. Under the file-polled source the two
/// are the same number.
#[derive(Debug, Clone)]
pub struct MapSnapshot {
    pub control_revision: u64,
    pub topology_generation: u64,
    pub map: Arc<RelayMap>,
}

/// Where a relay's shard map comes from (`docs/relay-coordinators.md`,
/// "Map interface"). Minimal on purpose: the current reading, and a
/// receiver whose value changes whenever the current revision does.
pub trait MapSource: Send + Sync {
    /// The current reading. Cheap; called once per decision.
    fn current(&self) -> MapSnapshot;
    /// A receiver that changes whenever the current revision changes;
    /// its value is the new control revision.
    fn changes(&self) -> watch::Receiver<u64>;
}

/// The file-polled map as a [`MapSource`]: the coordinator's live
/// topology (or its static shard set), with the topology generation as
/// the control revision and the coordinator's publication watch as the
/// change notification. Every reading is one frozen snapshot, so the
/// children, their codes, and the generation come from one publication.
pub struct CoordinatorMapSource {
    coordinator: Arc<CoordinatorServiceImpl>,
}

impl CoordinatorMapSource {
    pub fn new(coordinator: Arc<CoordinatorServiceImpl>) -> Self {
        Self { coordinator }
    }
}

impl MapSource for CoordinatorMapSource {
    fn current(&self) -> MapSnapshot {
        let frozen = self
            .coordinator
            .request_snapshot()
            .unwrap_or_else(|| self.coordinator.as_ref().clone());
        let generation = frozen.current_topology_generation();
        MapSnapshot {
            control_revision: generation,
            topology_generation: generation,
            map: Arc::new(RelayMap {
                routes: frozen.current_topology_routes(),
                placement: frozen.current_placement(),
            }),
        }
    }

    fn changes(&self) -> watch::Receiver<u64> {
        self.coordinator.topology_changes()
    }
}
