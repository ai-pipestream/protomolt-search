//! The committed map as a relay's map source (docs/map-feed.md,
//! docs/relay-coordinators.md "Map interface"): a read-only consumer of
//! `published_map` that wakes on the applied watch, validates every frame
//! through `MapConsumer`, and hands the relay one frozen `MapSnapshot` per
//! accepted revision. It grants no admission and carries no lease; a
//! relay routes on it and admits nothing by it.
use super::*;
use crate::coordinator::TopologyRoute;
use crate::placement::{Placement, PlacementTreeConfig};
use crate::relay::{MapSnapshot, MapSource, RelayMap};

type StoreHandle = Box<dyn Fn() -> Result<SourceAuthorityStore, Status> + Send + Sync>;

struct Inner {
    store: StoreHandle,
    principal: String,
    key: LogicalSourceOwner,
    consumer: Mutex<MapConsumer>,
    current: RwLock<MapSnapshot>,
    changes: watch::Sender<u64>,
    // The last frame the consumer refused, kept beside the held map: the
    // relay keeps routing on the last accepted map and the refusal is
    // readable by name.
    fault: Mutex<Option<Status>>,
}

/// A relay map fed from a source authority's committed rows. `attach`
/// reads the current map once (a resource with no applied control state
/// refuses), then a task follows the applied watch; `current` is the last
/// accepted reading and `changes` moves with its control revision.
pub struct AuthorityMapSource {
    inner: Arc<Inner>,
    task: tokio::task::JoinHandle<()>,
}

impl AuthorityMapSource {
    /// `store` yields the current handle on every read, so a store
    /// replaced by a snapshot install is followed; the applied watch moves
    /// with the replacement and the subscription taken here stays live.
    pub fn attach(
        store: impl Fn() -> Result<SourceAuthorityStore, Status> + Send + Sync + 'static,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<Self, Status> {
        contract::principal(principal)?;
        contract::key(key, false)?;
        let handle = store()?;
        let mut applied = handle.subscribe_applied();
        let map = handle.published_map(principal, key)?;
        drop(handle);
        let mut consumer = MapConsumer::new();
        consumer.offer(map.clone())?;
        let snapshot = snapshot_of(&map)?;
        let inner = Arc::new(Inner {
            store: Box::new(store),
            principal: principal.to_string(),
            key: key.clone(),
            consumer: Mutex::new(consumer),
            changes: watch::Sender::new(snapshot.control_revision),
            current: RwLock::new(snapshot),
            fault: Mutex::new(None),
        });
        let follower = Arc::clone(&inner);
        let task = tokio::spawn(async move {
            while applied.changed().await.is_ok() {
                follower.refresh();
            }
        });
        Ok(Self { inner, task })
    }

    /// The last frame the consumer refused since the last accepted one.
    pub fn fault(&self) -> Option<Status> {
        self.inner.fault.lock().unwrap().clone()
    }

    /// Read and offer the current map now (the task does this on every
    /// applied change; tests call it directly).
    pub fn refresh(&self) {
        self.inner.refresh();
    }
}

impl Drop for AuthorityMapSource {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Inner {
    fn refresh(&self) {
        let result = (self.store)()
            .and_then(|store| store.published_map(&self.principal, &self.key))
            .and_then(|map| {
                let mut consumer = self.consumer.lock().unwrap();
                if consumer.offer(map.clone())? {
                    let snapshot = snapshot_of(&map)?;
                    let revision = snapshot.control_revision;
                    *self.current.write().unwrap() = snapshot;
                    self.changes.send_replace(revision);
                }
                Ok(())
            });
        *self.fault.lock().unwrap() = result.err();
    }
}

impl MapSource for AuthorityMapSource {
    fn current(&self) -> MapSnapshot {
        self.inner.current.read().unwrap().clone()
    }

    fn changes(&self) -> watch::Receiver<u64> {
        self.inner.changes.subscribe()
    }
}

/// One frozen relay reading of a published map: routes in committed order
/// with their placement codes, and the validated tree when the map has
/// one. A tree the placement module refuses is a refusal here too.
fn snapshot_of(map: &PublishedMap) -> Result<MapSnapshot, Status> {
    let placement = match map.tree.as_ref() {
        Some(tree) => Some(Arc::new(
            Placement::validate(&PlacementTreeConfig::from_proto(tree)).map_err(|e| {
                Status::failed_precondition(format!("published map placement tree: {e}"))
            })?,
        )),
        None => None,
    };
    if map.codes_available && map.codes.len() != map.routes.len() {
        return Err(Status::failed_precondition(
            "published map codes do not cover its routes",
        ));
    }
    let routes = map
        .routes
        .iter()
        .enumerate()
        .map(|(index, route)| {
            let placement = map
                .codes_available
                .then(|| map.codes.get(index))
                .flatten()
                .filter(|code| code.has_placement)
                .map(|code| code.placement as i64);
            if placement.is_some() != map.tree.is_some() {
                return Err(Status::failed_precondition(
                    "published map routes carry placement codes without a tree, or a tree without codes",
                ));
            }
            Ok(TopologyRoute {
                addr: route.addr.clone(),
                replica: route.replica.clone(),
                hash_range: Some((route.hash_lo, route.hash_hi)),
                placement,
            })
        })
        .collect::<Result<Vec<_>, Status>>()?;
    Ok(MapSnapshot {
        control_revision: map.control_revision,
        topology_generation: map.topology_generation,
        map: Arc::new(RelayMap { routes, placement }),
    })
}
