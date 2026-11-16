//! Authorized reads of committed policy, owners and decisions.
use super::*;

impl SourceAuthorityStore {
    /// The current committed decision for an actor on one collection: the
    /// same evaluation the general authorizer performs on a policy snapshot.
    pub fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        contract::principal(principal)?;
        self.guarded(|| {
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let header: SourceAuthorityHeader = contract::decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| missing("header"))?
                    .value(),
            )?;
            recovery::header(&header, &self.inner.identity)?;
            let policy: AccessPolicy = contract::decode(
                meta.get("policy")
                    .map_err(storage)?
                    .ok_or_else(|| missing("policy"))?
                    .value(),
            )?;
            contract::policy(&policy).map_err(corrupt)?;
            crate::authorization::authorize_policy_snapshot(&policy, principal, collection, action)
        })
    }

    pub(crate) fn with_resource_admin<T>(
        &self,
        principal: &str,
        identity: &SourceAuthorityIdentity,
        key: &LogicalSourceOwner,
        run: impl FnOnce() -> Result<T, Status>,
    ) -> Result<T, Status> {
        self.guarded(|| {
            if identity != &self.inner.identity {
                return Err(Status::failed_precondition(
                    "managed source authority identity differs",
                ));
            }
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            Ok(run())
        })?
    }

    pub fn owner(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
    ) -> Result<PreparedSourceOwner, Status> {
        self.guarded(|| {
            contract::key(key, true)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            self.read_policy(&tx, principal, key)?;
            let owners = tx.open_table(OWNERS).map_err(storage)?;
            let bytes = key.encode_to_vec();
            let owner: PreparedSourceOwner = contract::decode(
                owners
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("source authority owner is not prepared"))?
                    .value(),
            )?;
            contract::owner(&owner).map_err(corrupt)?;
            if owner.key.as_ref() != Some(key) {
                return Err(corrupt("owner key differs from table key"));
            }
            Ok(owner)
        })
    }

    pub fn decision(
        &self,
        principal: &str,
        key: &LogicalSourceOwner,
        command_id: &[u8],
    ) -> Result<SourceAuthorityDecision, Status> {
        self.guarded(|| {
            contract::operation_id(command_id)?;
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let policy = self.read_policy(&tx, principal, key)?;
            let meta = tx.open_table(META).map_err(storage)?;
            let header: SourceAuthorityHeader = contract::decode(
                meta.get("header")
                    .map_err(storage)?
                    .ok_or_else(|| missing("header"))?
                    .value(),
            )?;
            let key = contract::operation_key(principal, key, command_id);
            let bytes = key.encode_to_vec();
            let decisions = tx.open_table(DECISIONS).map_err(storage)?;
            let operation: SourceAuthorityOperation = contract::decode(
                decisions
                    .get(bytes.as_slice())
                    .map_err(storage)?
                    .ok_or_else(|| Status::not_found("source authority command has no decision"))?
                    .value(),
            )?;
            recovery::operation(&key, &operation, &header, &policy)?;
            operation.decision.ok_or_else(|| missing("decision"))
        })
    }

    /// A current policy view restricted to one authorized resource. No grants or
    /// ownership metadata from another collection are returned.
    pub fn policy(
        &self,
        principal: &str,
        workspace: &str,
        collection: &str,
    ) -> Result<AccessPolicy, Status> {
        self.guarded(|| {
            let key = LogicalSourceOwner {
                workspace: workspace.into(),
                collection: collection.into(),
                owner_id: Vec::new(),
            };
            let tx = self.inner.database().begin_read().map_err(storage)?;
            let mut policy = self.read_policy(&tx, principal, &key)?;
            policy.resources.retain(|resource| {
                resource.workspace == workspace && resource.collection == collection
            });
            policy
                .grants
                .retain(|grant| grant.workspace == workspace && grant.collection == collection);
            Ok(policy)
        })
    }
}
