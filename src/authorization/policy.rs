//! The validated in-memory form of one access policy and its decisions.

use super::POLICY_FORMAT;
use crate::pb::{AccessAction, AccessDecision, AccessPolicy};
use std::collections::{BTreeMap, BTreeSet};
use tonic::Status;

fn require_current_format(found: u32) -> Result<(), String> {
    (found == POLICY_FORMAT).then_some(()).ok_or_else(|| {
        format!("unsupported access policy format_version {found}; expected {POLICY_FORMAT}")
    })
}

#[derive(Debug)]
pub(super) struct Policy {
    pub(super) revision: u64,
    resources: BTreeMap<String, String>,
    grants: BTreeMap<(String, String), BTreeSet<i32>>,
    views: BTreeMap<(String, String), crate::pb::DocumentVisibility>,
    fields: BTreeMap<(String, String), crate::pb::FieldPermissions>,
}

impl Policy {
    pub(super) fn validate(input: AccessPolicy) -> Result<Self, String> {
        require_current_format(input.format_version)?;
        if input.revision == 0 {
            return Err("access policy revision must be nonzero".into());
        }
        let mut resources = BTreeMap::new();
        for resource in input.resources {
            if resource.workspace.is_empty() {
                return Err("access policy workspace must be nonempty".into());
            }
            crate::collections::validate_name(&resource.workspace)?;
            if !resource.collection.is_empty() {
                crate::collections::validate_name(&resource.collection)?;
            }
            if resources
                .insert(resource.collection, resource.workspace)
                .is_some()
            {
                return Err("access policy repeats a collection binding".into());
            }
        }
        let mut grants = BTreeMap::new();
        let mut views = BTreeMap::new();
        let mut fields = BTreeMap::new();
        for grant in input.grants {
            if grant.principal.is_empty() {
                return Err("access grant principal must be nonempty".into());
            }
            if resources.get(&grant.collection) != Some(&grant.workspace) {
                return Err("access grant does not match a workspace/collection binding".into());
            }
            if grant.actions.is_empty() {
                return Err("access grant must name at least one action".into());
            }
            let mut actions = BTreeSet::new();
            for action in grant.actions {
                match AccessAction::try_from(action) {
                    Ok(AccessAction::Search | AccessAction::Ingest | AccessAction::Admin) => {}
                    _ => return Err(format!("unknown access action {action}")),
                }
                if !actions.insert(action) {
                    return Err("access grant repeats an action".into());
                }
            }
            if let Some(view) = grant.document_visibility {
                if !actions.contains(&(AccessAction::Search as i32)) {
                    return Err("document visibility requires an explicit search action".into());
                }
                crate::visibility::VisibilityScope::new(Some(&view))
                    .map_err(|error| format!("invalid document grant: {}", error.message()))?;
                views.insert((grant.principal.clone(), grant.collection.clone()), view);
            }
            if let Some(permissions) = grant.field_permissions {
                if !actions.contains(&(AccessAction::Search as i32)) {
                    return Err("field permissions require an explicit search action".into());
                }
                crate::field_permissions::FieldScope::new(&permissions)?;
                fields.insert(
                    (grant.principal.clone(), grant.collection.clone()),
                    permissions,
                );
            }
            if grants
                .insert((grant.principal, grant.collection), actions)
                .is_some()
            {
                return Err("access policy repeats a principal/collection grant".into());
            }
        }
        Ok(Self {
            revision: input.revision,
            resources,
            grants,
            views,
            fields,
        })
    }
}

// Reuse the same policy semantics in deterministic control-state application.
// The caller supplies committed state; this never reads a live policy or clock.
#[cfg(feature = "net")]
pub(crate) fn validate_policy_snapshot(input: &AccessPolicy) -> Result<(), String> {
    Policy::validate(input.clone()).map(|_| ())
}

#[cfg(feature = "net")]
pub(crate) fn authorize_policy_snapshot(
    input: &AccessPolicy,
    principal: &str,
    collection: &str,
    action: AccessAction,
) -> Result<AccessDecision, Status> {
    Policy::validate(input.clone())
        .map_err(|error| Status::data_loss(format!("committed control policy: {error}")))?
        .authorize(principal, collection, action)
}

impl Policy {
    pub(super) fn authorize(
        &self,
        principal: &str,
        collection: &str,
        action: AccessAction,
    ) -> Result<AccessDecision, Status> {
        let policy = self;
        let allowed = policy
            .grants
            .get(&(principal.to_owned(), collection.to_owned()))
            .is_some_and(|actions| actions.contains(&(action as i32)));
        if !allowed {
            return Err(Status::permission_denied(
                "operation is not authorized for this collection",
            ));
        }
        Ok(AccessDecision {
            policy_revision: policy.revision,
            principal: principal.into(),
            collection: collection.into(),
            workspace: policy.resources[collection].clone(),
            action: action as i32,
            field_permissions: if action == AccessAction::Search {
                policy
                    .fields
                    .get(&(principal.to_owned(), collection.to_owned()))
                    .cloned()
            } else {
                None
            },
            document_visibility: if action == AccessAction::Search {
                policy
                    .views
                    .get(&(principal.to_owned(), collection.to_owned()))
                    .cloned()
            } else {
                None
            },
        })
    }
}
