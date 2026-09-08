//! Independent reference model of the source-authority control rules,
//! written from `docs/source-authority-storage.md` and
//! `docs/control-import.md` ONLY (docs/control-import.md names
//! `src/source_authority/import.rs`, but this model is deliberately not
//! derived from that code). Where a doc does not name an exact gRPC code,
//! the most defensible reading is chosen and marked `AMBIGUOUS`; a fuzz
//! mismatch on such a point is a reportable finding, not a model fix.
//!
//! Scope: the vocabulary the differential fuzzer exercises — Prepare, Cancel,
//! ReplaceGrants on owner/collection keys, and the staged import
//! Begin/Chunk/Commit/Abort on one collection. Capacity accounting, malformed
//! envelopes, supplement validation and cross-resource rules are out of
//! scope (the generator only emits well-formed envelopes).
//!
//! Shared between the harness targets, each of which uses a subset.
#![allow(dead_code)]

use std::collections::{HashMap, HashSet};
use tonic::Code;

/// The actor the retirement holder binds Begin to (the kit retires as alice);
/// docs/control-import.md Begin row: "the holder's operation must name this
/// actor".
pub const RETIREMENT_ACTOR: &str = "alice";
/// Every import in the harness declares three chunks.
pub const CHUNK_COUNT: u32 = 3;

#[derive(Debug)]
pub enum ModelOutcome {
    /// A `Status` error: nothing was recorded. Covers failed admission /
    /// current-permission and retry-content conflicts
    /// (docs/source-authority-storage.md: "Malformed envelopes, failed
    /// admission ... return a Status instead of a recorded decision").
    Status(Code),
    /// A durable decision returned to the caller (`code` may be nonzero for
    /// recorded business refusals; revisions are the model's post-state for
    /// accepts and current state for recorded refusals).
    Decision {
        code: u32,
        control_revision: u64,
        policy_revision: u64,
    },
    /// A policy command that revokes its own issuer: the decision commits
    /// (revisions advance, journal entry recorded) but the response is
    /// suppressed as PermissionDenied
    /// (docs/source-authority-storage.md: "A policy command that revokes its
    /// own issuer still commits its decision, but the response is suppressed
    /// until the issuer again has current administration permission").
    Suppressed {
        control_revision: u64,
        policy_revision: u64,
    },
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ModelCommand {
    Prepare {
        workflow: Vec<u8>,
    },
    Cancel {
        workflow: Vec<u8>,
    },
    /// Full replacement of this collection's Admin set.
    ReplaceGrants {
        admins: Vec<String>,
    },
    Begin {
        workflow: Vec<u8>,
    },
    Chunk {
        workflow: Vec<u8>,
        ordinal: u32,
        bytes: Vec<u8>,
    },
    Commit {
        workflow: Vec<u8>,
    },
    Abort {
        workflow: Vec<u8>,
    },
}

impl ModelCommand {
    /// Import commands live in the import namespace; the docs say a command
    /// id "used by one path cannot be reused by the other"
    /// (docs/control-import.md, "The staged protocol").
    pub fn is_import(&self) -> bool {
        matches!(
            self,
            ModelCommand::Begin { .. }
                | ModelCommand::Chunk { .. }
                | ModelCommand::Commit { .. }
                | ModelCommand::Abort { .. }
        )
    }
}

/// One operation against the model. `content` is the canonical encoded
/// command; it is the retry-content fingerprint (byte equality).
pub struct ModelOp {
    pub actor: String,
    /// Empty owner id = the collection resource; otherwise a logical owner.
    pub owner_id: Vec<u8>,
    pub command_id: Vec<u8>,
    pub expected_control: u64,
    pub expected_policy: u64,
    pub expected_generation: u64,
    pub command: ModelCommand,
    pub content: Vec<u8>,
}

struct JournalEntry {
    content: Vec<u8>,
    code: u32,
    control_revision: u64,
    policy_revision: u64,
}

#[derive(Clone, PartialEq, Eq)]
struct Owner {
    workflow: Vec<u8>,
    generation: u64,
    prepared: bool,
}

#[derive(Clone, PartialEq, Eq)]
struct Import {
    actor: String,
    /// `Some` while staging (ordinal -> staged bytes); `None` once terminal.
    staging: Option<HashMap<u32, Vec<u8>>>,
}

/// One recorded decision for the verification probes: actor, key, id,
/// namespace, code and revisions at record time.
#[derive(Clone, Debug)]
pub struct JournalView {
    pub actor: String,
    pub owner_id: Vec<u8>,
    pub command_id: Vec<u8>,
    pub import: bool,
    pub code: u32,
    pub control_revision: u64,
    pub policy_revision: u64,
}

pub struct Model {
    admins: HashSet<String>,
    policy_revision: u64,
    control_revision: u64,
    /// (actor, owner_id, command_id, import_namespace).
    journal: HashMap<(String, Vec<u8>, Vec<u8>, bool), JournalEntry>,
    owners: HashMap<Vec<u8>, Owner>,
    /// (owner_id, workflow): owner workflows are never reusable.
    used_workflows: HashSet<(Vec<u8>, Vec<u8>)>,
    /// workflow id -> import state (single collection).
    imports: HashMap<Vec<u8>, Import>,
    applied_import: bool,
}

impl Model {
    /// Grants matching the kit bootstrap policy: alice and bob are Admins of
    /// `books`; revisions start at 1 like a fresh store header.
    pub fn new() -> Self {
        Self {
            admins: ["alice", "bob"].into_iter().map(str::to_owned).collect(),
            policy_revision: 1,
            control_revision: 1,
            journal: HashMap::new(),
            owners: HashMap::new(),
            used_workflows: HashSet::new(),
            imports: HashMap::new(),
            applied_import: false,
        }
    }

    pub fn control_revision(&self) -> u64 {
        self.control_revision
    }

    pub fn policy_revision(&self) -> u64 {
        self.policy_revision
    }

    pub fn is_admin(&self, actor: &str) -> bool {
        self.admins.contains(actor)
    }

    pub fn admins(&self) -> Vec<String> {
        let mut admins: Vec<String> = self.admins.iter().cloned().collect();
        admins.sort();
        admins
    }

    pub fn some_admin(&self) -> Option<String> {
        self.admins().first().cloned()
    }

    pub fn owner_generation(&self, owner_id: &[u8]) -> u64 {
        self.owners.get(owner_id).map_or(0, |o| o.generation)
    }

    /// (generation, workflow, prepared) for the plausible-command generator.
    pub fn owner_view(&self, owner_id: &[u8]) -> Option<(u64, Vec<u8>, bool)> {
        self.owners
            .get(owner_id)
            .map(|o| (o.generation, o.workflow.clone(), o.prepared))
    }

    pub fn staging_workflow(&self) -> Option<Vec<u8>> {
        self.imports
            .iter()
            .find(|(_, i)| i.staging.is_some())
            .map(|(wf, _)| wf.clone())
    }

    pub fn journal_lookup(
        &self,
        actor: &str,
        owner_id: &[u8],
        command_id: &[u8],
        import: bool,
    ) -> Option<(u32, u64, u64)> {
        self.journal
            .get(&(
                actor.to_owned(),
                owner_id.to_vec(),
                command_id.to_vec(),
                import,
            ))
            .map(|e| (e.code, e.control_revision, e.policy_revision))
    }

    pub fn journal_entries(&self) -> Vec<JournalView> {
        self.journal
            .iter()
            .map(|((actor, owner_id, command_id, import), e)| JournalView {
                actor: actor.clone(),
                owner_id: owner_id.clone(),
                command_id: command_id.clone(),
                import: *import,
                code: e.code,
                control_revision: e.control_revision,
                policy_revision: e.policy_revision,
            })
            .collect()
    }

    /// Record `code` at the current revisions and return the decision.
    fn record(&mut self, op: &ModelOp, code: u32) -> ModelOutcome {
        self.journal.insert(
            (
                op.actor.clone(),
                op.owner_id.clone(),
                op.command_id.clone(),
                op.command.is_import(),
            ),
            JournalEntry {
                content: op.content.clone(),
                code,
                control_revision: self.control_revision,
                policy_revision: self.policy_revision,
            },
        );
        ModelOutcome::Decision {
            code,
            control_revision: self.control_revision,
            policy_revision: self.policy_revision,
        }
    }

    pub fn apply(&mut self, op: &ModelOp) -> ModelOutcome {
        let import = op.command.is_import();
        // 1. Current permission first (docs/source-authority-storage.md:
        // "Every operation checks that actor's current explicit Admin grant";
        // docs/control-import.md: "every step, including an exact retry,
        // requires current resource Admin"). This also gates retry
        // disclosure: "Current permission is checked before looking up a
        // retry. Revocation therefore prevents disclosure of its saved
        // result."
        if !self.admins.contains(&op.actor) {
            return ModelOutcome::Status(Code::PermissionDenied);
        }

        // The Begin holder binds the retiring actor
        // (docs/control-import.md: "its operation must name this actor").
        if import
            && matches!(&op.command, ModelCommand::Begin { .. })
            && op.actor != RETIREMENT_ACTOR
        {
            return ModelOutcome::Status(Code::PermissionDenied);
        }

        // 2. Cross-namespace id reuse refuses as a Status (docs say the id
        // "cannot be reused by the other" without naming a code; treated like
        // the changed-content refusal below, which the docs also only say
        // "refuses" — AMBIGUOUS, most defensible: same as changed content).
        if self.journal.contains_key(&(
            op.actor.clone(),
            op.owner_id.clone(),
            op.command_id.clone(),
            !import,
        )) {
            return ModelOutcome::Status(Code::FailedPrecondition);
        }

        // Retry lookup by (actor, key, command_id): identical content returns
        // the stored decision verbatim; changed content refuses as a Status
        // (docs/source-authority-storage.md: "Exact retries return the
        // original response without reapplying it. Changed content under a
        // used actor/resource/command key refuses." — the slice-1 gate
        // confirmed the refusal is NOT a recorded decision).
        if let Some(saved) = self.journal.get(&(
            op.actor.clone(),
            op.owner_id.clone(),
            op.command_id.clone(),
            import,
        )) {
            if saved.content == op.content {
                return ModelOutcome::Decision {
                    code: saved.code,
                    control_revision: saved.control_revision,
                    policy_revision: saved.policy_revision,
                };
            }
            return ModelOutcome::Status(Code::FailedPrecondition);
        }

        // 3. CAS: every step carries expected revisions against the current
        // values; a stale step is a RECORDED refusal
        // (docs/control-import.md: "a stale step is a recorded refusal").
        // AMBIGUOUS: the docs never name the code; FailedPrecondition is the
        // canonical stale-precondition code.
        if op.expected_control != self.control_revision
            || op.expected_policy != self.policy_revision
        {
            return self.record(op, Code::FailedPrecondition as u32);
        }

        // Owner commands also carry the expected ownership generation
        // (docs/source-authority-storage.md: "A command carries ... expected
        // ownership generation"). Zero for collection keys, where no owner
        // exists. AMBIGUOUS: code unnamed; FailedPrecondition.
        let generation = self.owners.get(&op.owner_id).map_or(0, |o| o.generation);
        if op.expected_generation != generation {
            return self.record(op, Code::FailedPrecondition as u32);
        }

        // 4. Transition rules.
        match &op.command {
            ModelCommand::Prepare { workflow } => {
                // "A new preparation after cancellation advances the
                // generation and needs a new workflow ID. Used workflows are
                // never released for reuse." AMBIGUOUS: the reuse code;
                // AlreadyExists matches the import workflow tombstones.
                if self
                    .used_workflows
                    .contains(&(op.owner_id.clone(), workflow.clone()))
                {
                    return self.record(op, Code::AlreadyExists as u32);
                }
                match self.owners.get(&op.owner_id) {
                    // Preparing over a still-pending owner is not the
                    // documented cancel-then-prepare path. AMBIGUOUS: code
                    // unnamed; FailedPrecondition.
                    Some(owner) if owner.prepared => {
                        self.record(op, Code::FailedPrecondition as u32)
                    }
                    _ => {
                        self.used_workflows
                            .insert((op.owner_id.clone(), workflow.clone()));
                        self.owners.insert(
                            op.owner_id.clone(),
                            Owner {
                                workflow: workflow.clone(),
                                generation: generation + 1,
                                prepared: true,
                            },
                        );
                        self.control_revision += 1;
                        self.record(op, 0)
                    }
                }
            }
            ModelCommand::Cancel { workflow } => {
                // "CancelPreparedSourceOwner retains its generation and
                // history." AMBIGUOUS: the docs name no refusal code; a
                // cancel on an owner that was never prepared reads most
                // naturally as NotFound (the resource does not exist), while
                // a wrong workflow on an existing preparation is a
                // precondition conflict: FailedPrecondition.
                let matches_pending = match self.owners.get(&op.owner_id) {
                    Some(owner) => owner.prepared && owner.workflow == *workflow,
                    None => return self.record(op, Code::NotFound as u32),
                };
                if !matches_pending {
                    return self.record(op, Code::FailedPrecondition as u32);
                }
                self.owners
                    .get_mut(&op.owner_id)
                    .expect("checked above")
                    .prepared = false;
                self.control_revision += 1;
                self.record(op, 0)
            }
            ModelCommand::ReplaceGrants { admins } => {
                // "The policy and control revisions advance in the same
                // transaction." Self-revocation commits but suppresses the
                // response.
                self.admins = admins.iter().cloned().collect();
                self.control_revision += 1;
                self.policy_revision += 1;
                if self.admins.contains(&op.actor) {
                    self.record(op, 0)
                } else {
                    self.journal.insert(
                        (
                            op.actor.clone(),
                            op.owner_id.clone(),
                            op.command_id.clone(),
                            import,
                        ),
                        JournalEntry {
                            content: op.content.clone(),
                            code: 0,
                            control_revision: self.control_revision,
                            policy_revision: self.policy_revision,
                        },
                    );
                    ModelOutcome::Suppressed {
                        control_revision: self.control_revision,
                        policy_revision: self.policy_revision,
                    }
                }
            }
            ModelCommand::Begin { workflow } => {
                // Begin refusals, docs/control-import.md Begin row: used
                // workflow id, resource already holds an applied import,
                // another workflow is pending, in that order of specificity.
                // AMBIGUOUS: only the first two read naturally as
                // AlreadyExists ("a second applied import" parallels the
                // recorded AlreadyExists of slice 1); the pending-workflow
                // refusal is a precondition conflict: FailedPrecondition.
                if self.imports.contains_key(workflow) {
                    return self.record(op, Code::AlreadyExists as u32);
                }
                if self.applied_import {
                    return self.record(op, Code::AlreadyExists as u32);
                }
                if self.imports.values().any(|i| i.staging.is_some()) {
                    return self.record(op, Code::FailedPrecondition as u32);
                }
                self.imports.insert(
                    workflow.clone(),
                    Import {
                        actor: op.actor.clone(),
                        staging: Some(HashMap::new()),
                    },
                );
                self.control_revision += 1;
                self.record(op, 0)
            }
            ModelCommand::Chunk {
                workflow,
                ordinal,
                bytes,
            } => {
                let Some(import) = self.imports.get(workflow) else {
                    // AMBIGUOUS: the docs name no code for an unknown
                    // workflow; NotFound is the canonical choice.
                    return self.record(op, Code::NotFound as u32);
                };
                // "Another Admin cannot stage, commit or abort it (recorded
                // FailedPrecondition)" — named in the doc.
                if import.actor != op.actor {
                    return self.record(op, Code::FailedPrecondition as u32);
                }
                let Some(staging) = import.staging.clone() else {
                    // "a late chunk, a second commit and an abort after a
                    // terminal step are recorded refusals" — FailedPrecondition
                    // per the slice-1 gate.
                    return self.record(op, Code::FailedPrecondition as u32);
                };
                if let Some(staged) = staging.get(ordinal) {
                    // "An identical restage under the same ordinal is a
                    // recorded AlreadyExists; different content under a
                    // staged ordinal is a recorded FailedPrecondition."
                    if staged == bytes {
                        return self.record(op, Code::AlreadyExists as u32);
                    }
                    return self.record(op, Code::FailedPrecondition as u32);
                }
                let import = self.imports.get_mut(workflow).expect("checked above");
                import
                    .staging
                    .as_mut()
                    .expect("staging checked above")
                    .insert(*ordinal, bytes.clone());
                self.control_revision += 1;
                self.record(op, 0)
            }
            ModelCommand::Commit { workflow } => {
                let Some(import) = self.imports.get(workflow) else {
                    return self.record(op, Code::NotFound as u32); // AMBIGUOUS, as Chunk.
                };
                if import.actor != op.actor {
                    return self.record(op, Code::FailedPrecondition as u32);
                }
                let Some(staging) = &import.staging else {
                    return self.record(op, Code::FailedPrecondition as u32); // terminal
                };
                if staging.len() < CHUNK_COUNT as usize {
                    // "Requires every chunk." AMBIGUOUS: code unnamed;
                    // FailedPrecondition.
                    return self.record(op, Code::FailedPrecondition as u32);
                }
                let import = self.imports.get_mut(workflow).expect("checked above");
                import.staging = None;
                self.applied_import = true;
                self.control_revision += 1;
                self.record(op, 0)
            }
            ModelCommand::Abort { workflow } => {
                let staging = match self.imports.get(workflow) {
                    None => {
                        // AMBIGUOUS: the docs name no code for an unknown
                        // workflow; NotFound is the canonical choice.
                        return self.record(op, Code::NotFound as u32);
                    }
                    // "Another Admin cannot stage, commit or abort it
                    // (recorded FailedPrecondition)" — named in the doc.
                    Some(import) if import.actor != op.actor => {
                        return self.record(op, Code::FailedPrecondition as u32);
                    }
                    Some(import) => import.staging.is_some(),
                };
                if !staging {
                    // "a late chunk, a second commit and an abort after a
                    // terminal step are recorded refusals".
                    return self.record(op, Code::FailedPrecondition as u32);
                }
                self.imports
                    .get_mut(workflow)
                    .expect("checked above")
                    .staging = None;
                self.control_revision += 1;
                self.record(op, 0)
            }
        }
    }
}
