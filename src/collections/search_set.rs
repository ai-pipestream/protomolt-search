//! The search surface over named collections: membership, routing by
//! name, principals and quotas, and the recent-request ring.

use super::*;

/// The public search surface over one or more collections.
#[derive(Clone)]
pub struct CollectionSet {
    pub(super) members: Members<CoordinatorServiceImpl>,
    /// Bearer principals (`docs/security.md`); `None` serves anonymous
    /// callers with no quotas.
    pub(super) principals: Option<Arc<Principals>>,
    /// The recent-request ring the diagnostics service reads
    /// (docs/diagnostics.md).
    pub(super) ring: Arc<crate::diagnostics::RecentRing>,
}

impl CollectionSet {
    /// One unnamed dataset: the shape every pre-collection deployment has.
    pub fn single(coordinator: CoordinatorServiceImpl) -> Self {
        CollectionSet {
            members: Members::single(coordinator),
            principals: None,
            ring: Arc::new(crate::diagnostics::RecentRing::default()),
        }
    }

    /// Require a bearer token on every public call and apply each
    /// principal's quotas (`docs/security.md`).
    pub fn with_principals(mut self, principals: Arc<Principals>) -> Self {
        self.principals = Some(principals);
        self
    }

    /// Authenticate a request: its principal, or `None` when no
    /// principals are configured (anonymous service).
    pub(super) fn authenticate<T>(
        &self,
        request: &Request<T>,
    ) -> Result<Option<Arc<Principal>>, Status> {
        match &self.principals {
            Some(principals) => principals.authenticate(request.metadata()).map(Some),
            None => Ok(None),
        }
    }

    pub(super) fn authorize(
        &self,
        principal: Option<&Arc<Principal>>,
        collection: &str,
        action: AccessAction,
    ) -> Result<Option<AccessPermit>, Status> {
        match principal {
            Some(principal) => self
                .principals
                .as_ref()
                .expect("authenticated above")
                .authorize(principal, collection, action)
                .map(Some)
                .map_err(crate::error_disclosure::authorization_status),
            None => Ok(None),
        }
    }

    pub(super) fn resolve_authorized<'a>(
        &'a self,
        principal: Option<&Arc<Principal>>,
        collection: &str,
        action: AccessAction,
    ) -> Result<(String, &'a CoordinatorServiceImpl, Option<AccessPermit>), Status> {
        let (name, target) = self.members.resolve(collection).map_err(|error| {
            if principal.is_some() {
                crate::error_disclosure::authorization_status(Status::permission_denied(
                    "operation is not authorized for this collection",
                ))
            } else {
                error
            }
        })?;
        let permit = self.authorize(principal, &name, action)?;
        Ok((name, target, permit))
    }

    /// Admit an authenticated request under its principal's quotas: `k`
    /// against `max_k`, and one in-flight slot, held by the returned
    /// permit for the request's life. An unset `k` keeps its meaning —
    /// the target coordinator's default, `default_k` — and is refused
    /// when that resolved value is above the cap; the request is never
    /// rewritten to the cap.
    pub(super) fn admit_under<T: RequestK>(
        principal: Option<&Arc<Principal>>,
        request: &Request<T>,
        default_k: u32,
    ) -> Result<Option<Permit>, Status> {
        let Some(principal) = principal else {
            return Ok(None);
        };
        match request.get_ref().k() {
            Some(0) => principal.admit_default_k(default_k)?,
            Some(k) => principal.admit_k(k)?,
            None => {}
        }
        principal.admit_request().map(Some)
    }

    /// [`Self::authenticate`] then [`Self::admit_under`] for requests
    /// without a `k`.
    pub(super) fn admit<T: RequestK>(
        &self,
        request: &Request<T>,
    ) -> Result<(Option<Permit>, Option<Arc<Principal>>), Status> {
        let principal = self.authenticate(request)?;
        let permit = Self::admit_under(principal.as_ref(), request, 0)?;
        Ok((permit, principal))
    }

    /// Named collections. Each coordinator must have been built for the
    /// name it is registered under (`with_collection`), no node address
    /// may serve two collections, and the default, when given, must be a
    /// member.
    pub fn named(
        members: Vec<(String, CoordinatorServiceImpl)>,
        default: Option<String>,
    ) -> Result<Self, String> {
        for (name, coordinator) in &members {
            if coordinator.collection() != name {
                return Err(format!(
                    "collection {name:?}: its coordinator was built for {:?}",
                    coordinator.collection()
                ));
            }
        }
        let mut owners: BTreeMap<String, String> = BTreeMap::new();
        for (name, coordinator) in &members {
            for addr in coordinator.node_addresses() {
                if let Some(other) = owners.insert(addr.clone(), name.clone()) {
                    if other != *name {
                        return Err(format!(
                            "node {addr} is listed under collections {other:?} and {name:?}; a \
                             shard belongs to only one collection"
                        ));
                    }
                }
            }
        }
        Ok(CollectionSet {
            members: Members::named(members, default, "collections")?,
            principals: None,
            ring: Arc::new(crate::diagnostics::RecentRing::default()),
        })
    }

    /// The collection names this set serves (empty for the unnamed set).
    pub fn names(&self) -> Vec<&str> {
        self.members.names()
    }

    /// The coordinator a request naming `collection` gets to.
    pub fn resolve(&self, collection: &str) -> Result<&CoordinatorServiceImpl, Status> {
        self.members.resolve(collection).map(|(_, member)| member)
    }

    /// Every member's coordinator, by name ("" for the unnamed dataset).
    pub fn members(&self) -> Vec<(&str, &CoordinatorServiceImpl)> {
        let mut out: Vec<(&str, &CoordinatorServiceImpl)> = self
            .members
            .named
            .iter()
            .map(|(name, member)| (name.as_str(), member))
            .collect();
        if let Some(member) = &self.members.unnamed {
            out.push(("", member));
        }
        out
    }

    /// Ask every node of every collection which collection it serves and
    /// refuse the set when any answer disagrees with the coordinator that
    /// lists it. Run at startup so a misconfigured fleet never serves.
    /// The diagnostics service over this set (docs/diagnostics.md):
    /// the members' knobs and layouts, this set's principals for the
    /// admin rule, and the ring of recent requests.
    pub fn diagnostics(&self) -> crate::diagnostics::CoordinatorDiagnostics {
        let members = self
            .members()
            .into_iter()
            .map(|(name, member)| (name.to_string(), member.clone()))
            .collect();
        crate::diagnostics::CoordinatorDiagnostics::new(
            members,
            self.principals.clone(),
            Arc::clone(&self.ring),
        )
    }

    /// One ring entry per public request, pushed after the handler
    /// answers; the figures come from the response without cloning it.
    pub(super) fn record(
        &self,
        route: &'static str,
        collection: &str,
        k: u32,
        principal: String,
        started: std::time::Instant,
        outcome: Result<crate::diagnostics::RecentFigures, tonic::Code>,
    ) {
        const PUBLIC: [&str; 10] = [
            "search",
            "bm25_search",
            "phrase_search",
            "hybrid_search",
            "variant_search",
            "query",
            "query_stream",
            "aggregate",
            "suggest",
            "term_suggest",
        ];
        if !PUBLIC.contains(&route) {
            return;
        }
        let (status, figures) = match outcome {
            Ok(figures) => ("OK".to_string(), figures),
            Err(code) => (format!("{code:?}"), Default::default()),
        };
        self.ring.push(crate::pb::RecentQuery {
            unix_ms: crate::diagnostics::unix_ms(),
            request_id: String::new(),
            route: route.to_string(),
            executed: figures.executed,
            k,
            total_ms: started.elapsed().as_secs_f32() * 1000.0,
            status,
            principal,
            segments_total: figures.segments_total,
            segments_skipped: figures.segments_skipped,
            candidates_collected: figures.candidates_collected,
            hits: figures.hits,
            collection: collection.to_string(),
        });
    }

    pub async fn verify_membership(&self) -> Result<(), Status> {
        for (_, member) in self.members() {
            member.verify_collection_membership().await?;
        }
        Ok(())
    }

    pub fn into_server(self, max_message_bytes: usize) -> SearchServiceServer<Self> {
        SearchServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }
}
