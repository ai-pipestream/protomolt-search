//! Bearer principals: the configured public clients, their policy file,
//! and token authentication.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use serde::Deserialize;
use tonic::metadata::MetadataMap;
use tonic::Status;

use super::quota::Principal;
use super::udp::constant_time_eq;

/// One public client, as configured: a name, its bearer token, and its
/// quotas. A quota of 0 means "no limit of that kind".
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct PrincipalConfig {
    pub name: String,
    pub token: String,
    /// The largest `k` this principal may ask for; 0 means the
    /// coordinator's own `max_k`.
    pub max_k: u32,
    /// Requests in flight at once; 0 means unlimited.
    pub concurrency: u32,
    /// Documents per second through routed ingest, as a token bucket
    /// with one second of burst; 0 means unlimited.
    pub ingest_docs_per_sec: u32,
    /// Enable diagnostics eligibility. Calls also require an Admin grant
    /// for every served collection. Off by default.
    pub admin: bool,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PrincipalsFile {
    principals: Vec<PrincipalConfig>,
    policy: Option<PolicyConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyConfig {
    format_version: u32,
    revision: u64,
    #[serde(default)]
    resources: Vec<ResourceConfig>,
    #[serde(default)]
    grants: Vec<GrantConfig>,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResourceConfig {
    workspace: String,
    collection: String,
}
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct GrantConfig {
    principal: String,
    workspace: String,
    collection: String,
    actions: Vec<String>,
}
impl PolicyConfig {
    fn into_policy(self) -> Result<crate::pb::AccessPolicy, String> {
        let mut grants = Vec::new();
        for grant in self.grants {
            let mut actions = Vec::new();
            for action in grant.actions {
                actions.push(match action.as_str() {
                    "search" => crate::pb::AccessAction::Search as i32,
                    "ingest" => crate::pb::AccessAction::Ingest as i32,
                    "admin" => crate::pb::AccessAction::Admin as i32,
                    _ => return Err(format!("unknown access action {action:?}")),
                });
            }
            grants.push(crate::pb::CollectionGrant {
                field_permissions: None,
                document_visibility: None,
                principal: grant.principal,
                workspace: grant.workspace,
                collection: grant.collection,
                actions,
            });
        }
        Ok(crate::pb::AccessPolicy {
            format_version: self.format_version,
            revision: self.revision,
            resources: self
                .resources
                .into_iter()
                .map(|r| crate::pb::CollectionResource {
                    workspace: r.workspace,
                    collection: r.collection,
                })
                .collect(),
            grants,
        })
    }
}

/// The configured principals, looked up by bearer token.
#[derive(Debug, Clone)]
pub struct Principals {
    by_token: HashMap<String, Arc<Principal>>,
    authorizer: Option<Arc<dyn crate::authorization::Authorizer>>,
}

impl Principals {
    /// Load `[[principals]]` from a TOML file.
    pub fn load(path: &Path) -> Result<Self, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("read bearer tokens {}: {e}", path.display()))?;
        let file: PrincipalsFile = toml::from_str(&text)
            .map_err(|e| format!("parse bearer tokens {}: {e}", path.display()))?;
        let policy = file.policy.ok_or(
            "bearer tokens require an explicit [policy]; credentials do not grant capabilities",
        )?;
        Self::from_configs(&file.principals)?.with_policy(policy.into_policy()?)
    }

    pub fn from_configs(configs: &[PrincipalConfig]) -> Result<Self, String> {
        if configs.is_empty() {
            return Err("bearer tokens: no [[principals]] declared".to_string());
        }
        let mut by_token = HashMap::with_capacity(configs.len());
        let mut names: Vec<&str> = Vec::with_capacity(configs.len());
        for (i, c) in configs.iter().enumerate() {
            if c.name.is_empty() {
                return Err(format!("principals[{i}]: name is empty"));
            }
            if c.token.len() < 16 {
                return Err(format!(
                    "principals[{i}] ({:?}): token is shorter than 16 bytes",
                    c.name
                ));
            }
            if names.contains(&c.name.as_str()) {
                return Err(format!("principals[{i}]: name {:?} repeats", c.name));
            }
            names.push(&c.name);
            if by_token
                .insert(c.token.clone(), Arc::new(Principal::new(c)))
                .is_some()
            {
                return Err(format!(
                    "principals[{i}] ({:?}): token repeats another principal's",
                    c.name
                ));
            }
        }
        Ok(Principals {
            by_token,
            authorizer: None,
        })
    }

    /// Install a validated protobuf policy. Principals without a grant are denied.
    pub fn with_policy(self, policy: crate::pb::AccessPolicy) -> Result<Self, String> {
        for grant in &policy.grants {
            if !self.by_token.values().any(|p| p.name == grant.principal) {
                return Err(format!(
                    "access grant names unknown principal {:?}",
                    grant.principal
                ));
            }
        }
        Ok(
            self.with_authorizer(Arc::new(crate::authorization::PolicyAuthority::new(
                policy,
            )?)),
        )
    }

    /// Integrate an ecosystem authority without changing token authentication.
    pub fn with_authorizer(
        mut self,
        authorizer: Arc<dyn crate::authorization::Authorizer>,
    ) -> Self {
        self.authorizer = Some(authorizer);
        self
    }

    pub fn authorize(
        &self,
        principal: &Principal,
        collection: &str,
        action: crate::pb::AccessAction,
    ) -> Result<crate::authorization::AccessPermit, Status> {
        let authority = self
            .authorizer
            .clone()
            .ok_or_else(|| Status::permission_denied("no access policy is configured"))?;
        crate::authorization::AccessPermit::acquire(authority, &principal.name, collection, action)
    }

    /// The principal behind a request's `authorization: Bearer <token>`
    /// header, or `UNAUTHENTICATED` naming what was missing.
    pub fn authenticate(&self, metadata: &MetadataMap) -> Result<Arc<Principal>, Status> {
        let header = metadata
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization: Bearer <token>"))?;
        let value = header
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not ASCII"))?;
        let token = value
            .strip_prefix("Bearer ")
            .or_else(|| value.strip_prefix("bearer "))
            .ok_or_else(|| Status::unauthenticated("authorization is not a Bearer token"))?
            .trim();
        // Constant-time over the configured tokens: the comparison cost
        // does not depend on how far a guess matches.
        let mut found: Option<Arc<Principal>> = None;
        for (known, principal) in &self.by_token {
            if constant_time_eq(known.as_bytes(), token.as_bytes()) {
                found = Some(Arc::clone(principal));
            }
        }
        found.ok_or_else(|| Status::unauthenticated("bearer token is not recognized"))
    }

    pub fn len(&self) -> usize {
        self.by_token.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_token.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principals_authenticate_and_refuse_by_name() {
        let principals = Principals::from_configs(&[
            PrincipalConfig {
                name: "console".into(),
                token: "console-token-0123456789".into(),
                max_k: 50,
                concurrency: 1,
                ingest_docs_per_sec: 2,
                admin: false,
            },
            PrincipalConfig {
                name: "batch".into(),
                token: "batch-token-0123456789".into(),
                ..Default::default()
            },
        ])
        .unwrap();
        let mut md = MetadataMap::new();
        assert_eq!(
            principals.authenticate(&md).unwrap_err().code(),
            tonic::Code::Unauthenticated
        );
        md.insert(
            "authorization",
            "Bearer nope-nope-nope-nope".parse().unwrap(),
        );
        assert!(principals
            .authenticate(&md)
            .unwrap_err()
            .message()
            .contains("not recognized"));
        md.insert("authorization", "Basic abc".parse().unwrap());
        assert!(principals
            .authenticate(&md)
            .unwrap_err()
            .message()
            .contains("not a Bearer"));
        md.insert(
            "authorization",
            "Bearer console-token-0123456789".parse().unwrap(),
        );
        let console = principals.authenticate(&md).unwrap();
        assert_eq!(console.name, "console");
        assert!(console.admit_k(50).is_ok());
        let error = console.admit_k(51).unwrap_err();
        assert_eq!(error.code(), tonic::Code::ResourceExhausted);
        assert!(error.message().contains("k=51 exceeds its max_k=50"));
        let first = console.admit_request().unwrap();
        let error = console.admit_request().unwrap_err();
        assert!(error.message().contains("concurrency limit of 1"));
        drop(first);
        assert!(console.admit_request().is_ok());
        assert!(console.admit_ingest(2).is_ok());
        let error = console.admit_ingest(1).unwrap_err();
        assert!(error.message().contains("exceeds its rate of 2"));
        md.insert(
            "authorization",
            "Bearer batch-token-0123456789".parse().unwrap(),
        );
        let batch = principals.authenticate(&md).unwrap();
        assert!(batch.admit_k(1_000_000).is_ok());
        assert!(batch.admit_ingest(1_000_000).is_ok());
        // Configuration refusals name the entry.
        assert!(Principals::from_configs(&[]).is_err());
        assert!(Principals::from_configs(&[PrincipalConfig {
            name: "x".into(),
            token: "short".into(),
            ..Default::default()
        }])
        .unwrap_err()
        .contains("shorter than 16"));
    }
}
