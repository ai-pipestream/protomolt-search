//! Request admission, topology checks, and node client access.

use super::*;

impl CoordinatorServiceImpl {
    /// Bind an authority decision to a private execution clone. Every public
    /// route calls this before work; uncertified restricted routes refuse.
    pub(crate) fn for_access(
        &self,
        access: Option<&crate::pb::AccessDecision>,
        route: &str,
    ) -> Result<Self, Status> {
        let mut scoped = self.clone();
        let view = access.and_then(|a| a.document_visibility.as_ref());
        let fields = access.and_then(|a| a.field_permissions.as_ref());
        if view.is_none() && fields.is_none() {
            return Ok(scoped);
        }
        crate::visibility::VisibilityScope::new(view)?;
        scoped.field_permissions = fields
            .map(|fields| {
                crate::field_permissions::FieldScope::new(fields)
                    .map(|scope| scope.with_derived(self.derived.clone()))
            })
            .transpose()
            .map_err(|_| Status::permission_denied("invalid field permission decision"))?;
        if access.is_none_or(|a| a.action != crate::pb::AccessAction::Search as i32) {
            return Err(Status::permission_denied(
                "restricted grants require search authorization",
            ));
        }
        if !matches!(
            route,
            "query" | "query_stream" | "bm25_search" | "suggest" | "term_suggest" | "aggregate"
        ) {
            return Err(Status::permission_denied(
                "this route does not yet enforce document or field grants",
            ));
        }
        if self.allow_network
            || self.has_clustered_vectors()
            || self.live_topology.is_some()
            || self
                .links
                .lock()
                .map_err(|_| Status::internal("node link cache lock poisoned"))?
                .values()
                .any(|link| !link.is_local())
        {
            return Err(Status::failed_precondition(
                "restricted grants currently require private in-process shards",
            ));
        }
        scoped.document_visibility = view.cloned();
        Ok(scoped)
    }

    pub(crate) fn for_vector_field(&self, field: &str) -> Result<Self, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        let mut scoped = self.clone();
        scoped.vector_read_field = (!field.is_empty()).then(|| field.to_string());
        Ok(scoped)
    }

    pub(crate) fn scoped_vector_scan(&self) -> bool {
        self.vector_read_field.is_some()
            || self.document_visibility.is_some()
            || self.field_permissions.is_some()
    }

    pub(super) fn check_vector_scan(
        &self,
        filters: &RequestFilters,
        collapse: bool,
    ) -> Result<(), Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(self.vector_read_field.as_deref().unwrap_or(""))?;
            fields.filter(&filters.geo, filters.tree.as_ref())?;
            if collapse {
                fields.dictionary("parent_id")?;
            }
        }
        if self.scoped_vector_scan() && self.has_clustered_vectors() {
            return Err(Status::failed_precondition(
                "scoped vector scans require product-node read receipts",
            ));
        }
        if filters.floor_prune == Some(false)
            && (filters.floor_greediness_ppm.is_some() || filters.floor_clamp_min.is_some())
        {
            return Err(Status::invalid_argument(
                "floor_greediness_ppm and floor_clamp_min require floor_prune = true",
            ));
        }
        if let Some(greediness) = filters.floor_greediness_ppm {
            if greediness > 1_000_000 {
                return Err(Status::invalid_argument(format!(
                    "floor_greediness_ppm {greediness} must be in 0..=1000000"
                )));
            }
        }
        if let Some(share) = filters.parent_floor_share_ppm {
            if share > 1_000_000 {
                return Err(Status::invalid_argument(format!(
                    "floor_share_ppm {share} must be in 0..=1000000"
                )));
            }
        }
        if self.has_clustered_vectors()
            && (filters.floor_prune == Some(true)
                || (filters.floor_prune.is_none()
                    && (filters.floor_greediness_ppm.is_some()
                        || filters.floor_clamp_min.is_some())))
        {
            return Err(Status::failed_precondition(
                "vector backend \"clustered-turbovec\" does not support graph floor pruning \
                 (it does not advertise floor_pruning)",
            ));
        }
        Ok(())
    }

    pub(super) fn vector_read_barrier(
        &self,
    ) -> Result<Option<Arc<crate::vector_read::VectorReadBarrier>>, Status> {
        if !self.scoped_vector_scan() {
            return Ok(None);
        }
        let claims = self
            .query_read_versions
            .as_ref()
            .filter(|claims| claims.len() == self.node_addrs.len())
            .ok_or_else(|| {
                Status::failed_precondition("vector scans require an admitted physical read set")
            })?;
        crate::vector_read::VectorReadBarrier::new(
            self.vector_read_field.clone().unwrap_or_default(),
            self.document_visibility.clone(),
            claims.as_ref().clone(),
        )
        .map(Some)
    }

    pub(super) fn vector_scan_mask(
        &self,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Option<crate::placement::ShardMask> {
        if self.scoped_vector_scan() {
            None
        } else {
            self.shard_mask(filter)
        }
    }

    pub(super) fn visible_filter(
        &self,
        user: Option<crate::pb::FilterExpr>,
    ) -> Result<Option<crate::pb::FilterExpr>, Status> {
        crate::visibility::intersect_filter(self.document_visibility.as_ref(), user)
    }

    pub(super) fn check_visibility_columns(&self, known: &[bool]) -> Result<(), Status> {
        if known.iter().any(|known| !known) {
            return Err(Status::failed_precondition(
                "document grant references a column unavailable in this collection",
            ));
        }
        Ok(())
    }

    /// True only for coordinators that may create network transports.
    pub fn allows_network(&self) -> bool {
        self.allow_network
    }

    /// Whether dense search runs on a clustered TurboVec backend rather
    /// than the shard fleet.
    pub(super) fn has_clustered_vectors(&self) -> bool {
        #[cfg(feature = "net")]
        {
            self.clustered_vectors.is_some()
        }
        #[cfg(not(feature = "net"))]
        {
            false
        }
    }

    pub fn max_k(&self) -> u32 {
        self.knobs.max_k()
    }

    /// The term-stats cache, exposed for tests (`fetch_count` is how a
    /// test proves the hit path issued no RPCs).
    pub fn stats_cache(&self) -> &crate::stats_cache::StatsCache {
        &self.stats_cache
    }

    /// The shard addresses this coordinator fans out to, in shard order.
    pub fn node_addresses(&self) -> &[String] {
        &self.node_addrs
    }

    /// Admit a request only for this coordinator's collection: an empty
    /// name gets to the unnamed dataset, or a named collection through a
    /// [`crate::collections::CollectionSet`] that written the name; any
    /// other name refuses rather than answering from the wrong dataset.
    pub(super) fn admit(&self, requested: &str) -> Result<(), Status> {
        if requested.is_empty() || requested == self.collection {
            return Ok(());
        }
        Err(if self.collection.is_empty() {
            Status::invalid_argument(format!(
                "unknown collection {requested:?}: this coordinator serves one unnamed dataset"
            ))
        } else {
            Status::invalid_argument(format!(
                "this coordinator serves collection {:?}, not {requested:?}",
                self.collection
            ))
        })
    }

    /// Ask every shard which collection it serves and refuse when any
    /// answer differs from this coordinator's: a shard belongs to precisely
    /// one collection, and a fleet that disagrees never serves.
    pub async fn verify_collection_membership(&self) -> Result<(), Status> {
        let mut addrs: Vec<String> = self.node_addrs.clone();
        addrs.extend(self.replica_addrs.iter().flatten().cloned());
        for addr in addrs {
            let mut client = self.node_client(&addr)?;
            let health = client
                .health(HealthRequest {})
                .await
                .map_err(|e| {
                    Status::unavailable(format!(
                        "collection {:?}: node {addr} did not answer a health probe: {}",
                        self.collection,
                        e.message()
                    ))
                })?
                .into_inner();
            if health.collection != self.collection {
                return Err(Status::failed_precondition(format!(
                    "node {addr} serves collection {:?}, but this coordinator is {:?}; a shard \
                     belongs to only one collection",
                    health.collection, self.collection
                )));
            }
        }
        Ok(())
    }

    /// Dial a node across the network (the `net` feature): a lazy channel
    /// with the engine's window sizes and the process's client TLS.
    #[cfg(feature = "net")]
    pub(super) fn connect(&self, addr: &str) -> Result<Channel, Status> {
        let tls = self
            .client_tls
            .as_ref()
            .or(crate::security::process_client_tls());
        let endpoint = Endpoint::from_shared(crate::security::secure_url(addr, tls))
            .map_err(|e| Status::unavailable(format!("invalid node address {addr}: {e}")))?
            .tcp_nodelay(true)
            // The client end is the RECEIVER of stream batches, so these
            // windows are what let a shard's pre-floor burst flow without
            // window-update round trips (see H2_STREAM_WINDOW).
            .initial_stream_window_size(crate::H2_STREAM_WINDOW)
            .initial_connection_window_size(crate::H2_CONN_WINDOW);
        let endpoint = crate::security::apply_client_tls(endpoint, tls)
            .map_err(Status::failed_precondition)?;
        Ok(endpoint.connect_lazy())
    }

    /// The link to a node: the cached one (a local node, or a channel
    /// dialed before), else a new channel when the network is allowed.
    pub(crate) fn node_client(&self, addr: &str) -> Result<crate::link::NodeLink, Status> {
        #[cfg_attr(not(feature = "net"), allow(unused_mut))]
        let mut cache = self.links.lock().expect("node link cache mutex poisoned");
        if let Some(link) = cache.get(addr) {
            return Ok(link.clone());
        }
        if !self.allow_network {
            return Err(Status::failed_precondition(format!(
                "in-process coordinator has no link for {addr}; network fallback is disabled"
            )));
        }
        #[cfg(feature = "net")]
        {
            let link = crate::link::NodeLink::remote(self.connect(addr)?);
            cache.insert(addr.to_string(), link.clone());
            Ok(link)
        }
        #[cfg(not(feature = "net"))]
        {
            Err(Status::failed_precondition(format!(
                "this build has no network transport (feature `net` is off); no link for {addr}"
            )))
        }
    }
}
