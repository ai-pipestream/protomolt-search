//! Seeded operation-trace differential fuzzer: an independent reference
//! model of the documented control-authority rules (see
//! `control_adversarial::model`, written from the contract docs only) driven
//! against the real `SourceAuthorityStore` through the slice-1 kit.
//!
//! Deterministic: seeds 1..=24 (seed = 0x5EED_0000 + n). Set
//! `PSEARCH_ADV_SEEDS=<end>` to extend the range past 24; the committed test
//! never requires it. Set `PSEARCH_ADV_TRACE_DIR=<dir>` to keep per-seed op
//! traces. Every mismatch panics with the seed, op index and the full
//! serialized trace so it can be minimized into a regression test.

mod control_adversarial;

use std::time::Instant;

use control_adversarial::kit;
use control_adversarial::model::{Model, ModelCommand, ModelOp, ModelOutcome};
use control_adversarial::rng::XorShift64;
use pipestream_search::pb::storage::source_authority_command::Action;
use pipestream_search::pb::storage::{
    ControlInstallCommand, LogicalSourceOwner, SourceAuthorityCommand,
};
use prost::Message;
use tonic::Code;

const OPS_PER_TRACE: usize = 60;
const RECOVERY_POINTS: [usize; 2] = [20, 40];
const ID_POOL: usize = 24;
const OWNER_IDS: [&[u8]; 2] = [b"phone-owner", b"tablet-owner"];
const WORKFLOW_POOL: [&[u8]; 4] = [b"wf-a", b"wf-b", b"wf-c", b"wf-d"];

#[derive(Clone, Debug)]
enum IssuedCmd {
    Source(Box<SourceAuthorityCommand>),
    Install(Box<ControlInstallCommand>),
}

#[derive(Debug)]
enum Real {
    Status(Code),
    Decision {
        code: u32,
        control_revision: u64,
        policy_revision: u64,
    },
}

struct Generated {
    actor: String,
    cmd: IssuedCmd,
    op: ModelOp,
    desc: String,
}

fn owner_key(owner_id: &[u8]) -> LogicalSourceOwner {
    LogicalSourceOwner {
        owner_id: owner_id.to_vec(),
        ..kit::key()
    }
}

fn off_by_one(value: u64) -> u64 {
    // Deterministic "wrong" revision adjacent to the right one; stays >= 1.
    if value > 1 {
        value - 1
    } else {
        value + 1
    }
}

/// Build the model op from the final command (single source of truth: the
/// encoded command bytes are the retry fingerprint).
fn model_op_for_source(actor: &str, cmd: &SourceAuthorityCommand) -> ModelOp {
    let key = cmd.key.clone().unwrap();
    let command = match cmd.action.clone().unwrap() {
        Action::Prepare(request) => ModelCommand::Prepare {
            workflow: request.workflow_id,
        },
        Action::Cancel(request) => ModelCommand::Cancel {
            workflow: request.workflow_id,
        },
        Action::Activate(request) => ModelCommand::Activate {
            workflow: request.workflow_id,
        },
        Action::ReplaceGrants(grants) => ModelCommand::ReplaceGrants {
            admins: grants.grants.iter().map(|g| g.principal.clone()).collect(),
        },
        other => panic!("model has no rule for source action {other:?}"),
    };
    ModelOp {
        actor: actor.to_owned(),
        owner_id: key.owner_id,
        command_id: cmd.command_id.clone(),
        expected_control: cmd.expected_control_revision,
        expected_policy: cmd.expected_policy_revision,
        expected_generation: cmd.expected_ownership_generation,
        command,
        content: cmd.encode_to_vec(),
    }
}

fn model_op_for_install(actor: &str, cmd: &ControlInstallCommand) -> ModelOp {
    ModelOp {
        actor: actor.to_owned(),
        owner_id: cmd.key.clone().unwrap().owner_id,
        command_id: cmd.command_id.clone(),
        expected_control: cmd.expected_control_revision,
        expected_policy: cmd.expected_policy_revision,
        expected_generation: 0,
        command: ModelCommand::Install,
        content: cmd.encode_to_vec(),
    }
}

/// Execute one generated op against the real store.
fn run_real(
    store: &pipestream_search::source_authority::SourceAuthorityStore,
    g: &Generated,
) -> Real {
    match &g.cmd {
        IssuedCmd::Source(command) => match store.execute(&g.actor, command) {
            Ok(decision) => Real::Decision {
                code: decision.code,
                control_revision: decision.control_revision,
                policy_revision: decision.policy_revision,
            },
            Err(status) => Real::Status(status.code()),
        },
        IssuedCmd::Install(command) => match store.install_control(&g.actor, command) {
            Ok(decision) => Real::Decision {
                code: decision.code,
                control_revision: decision.control_revision,
                policy_revision: decision.policy_revision,
            },
            Err(status) => Real::Status(status.code()),
        },
    }
}

/// Compare the real outcome with the model's; on mismatch dump the trace.
fn compare(
    seed: u64,
    index: usize,
    g: &Generated,
    real: &Real,
    expected: &ModelOutcome,
    trace: &[String],
) -> String {
    let matches = match (real, expected) {
        (Real::Status(c), ModelOutcome::Status(m)) => c == m,
        (Real::Status(c), ModelOutcome::Suppressed { .. }) => *c == Code::PermissionDenied,
        (
            Real::Decision {
                code,
                control_revision,
                policy_revision,
            },
            ModelOutcome::Decision {
                code: mc,
                control_revision: mcr,
                policy_revision: mpr,
            },
        ) => code == mc && control_revision == mcr && policy_revision == mpr,
        _ => false,
    };
    let outcome = match expected {
        ModelOutcome::Suppressed {
            control_revision,
            policy_revision,
        } => format!("SuppressedPermissionDenied(cr={control_revision}, pr={policy_revision})"),
        _ => format!("{real:?}"),
    };
    if !matches {
        panic!(
            "seed {seed:#x} op #{index} ({} {}):\n  real:   {real:?}\n  model:  {expected:?}\n\
             full trace:\n{}",
            g.actor,
            g.desc,
            trace.join("\n")
        );
    }
    outcome
}

/// Generate a fresh (non-retry) op. ~65% plausible (model-guided revisions,
/// ids and phases), ~35% adversarial (stale revisions, wrong actor, reused
/// ids, out-of-phase steps).
fn gen_fresh(
    rng: &mut XorShift64,
    model: &Model,
    authority: &pipestream_search::pb::storage::SourceAuthorityIdentity,
) -> Generated {
    let plausible = rng.chance(65);
    let admins = model.admins();
    assert!(!admins.is_empty(), "grant sets always retain an admin");
    let actor = if !plausible && rng.chance(50) {
        "mallory".to_owned()
    } else {
        admins[rng.below(admins.len() as u64) as usize].clone()
    };
    let id = format!("cmd-{:02}", rng.below(ID_POOL as u64));
    let (mut ecr, mut epr) = (model.control_revision(), model.policy_revision());
    if !plausible {
        match rng.below(3) {
            0 => ecr = off_by_one(ecr),
            1 => epr = off_by_one(epr),
            _ => {
                ecr = off_by_one(ecr);
                epr = off_by_one(epr);
            }
        }
    }

    let roll = rng.below(100);
    match roll {
        // Prepare: 25%
        0..=24 => {
            let owner = &OWNER_IDS[rng.below(OWNER_IDS.len() as u64) as usize];
            let wf = WORKFLOW_POOL[rng.below(WORKFLOW_POOL.len() as u64) as usize].to_vec();
            let generation = model.owner_generation(owner);
            let eg = if plausible {
                generation
            } else {
                off_by_one(generation)
            };
            let cmd = kit::source_command(
                authority,
                &owner_key(owner),
                &id,
                ecr,
                epr,
                eg,
                kit::prepare_action(&wf),
            );
            let op = model_op_for_source(&actor, &cmd);
            Generated {
                actor,
                cmd: IssuedCmd::Source(Box::new(cmd)),
                op,
                desc: format!(
                    "prepare owner={} wf={} gen={eg} ecr={ecr} epr={epr} plausible={plausible}",
                    String::from_utf8_lossy(owner),
                    String::from_utf8_lossy(&wf),
                ),
            }
        }
        // ReplaceGrants: 20%
        25..=44 => {
            let mut set: Vec<String> = model.admins();
            if rng.chance(40) {
                toggle(&mut set, "mallory");
            }
            if rng.chance(40) {
                toggle(&mut set, "bob");
            }
            if rng.chance(25) {
                toggle(&mut set, "alice");
            }
            if !set.iter().any(|p| p == "alice" || p == "bob") {
                set.push("alice".to_owned());
            }
            set.sort();
            let grants: Vec<pipestream_search::pb::CollectionGrant> =
                set.iter().map(|p| kit::grant(p, kit::COLLECTION)).collect();
            let cmd = kit::source_command(
                authority,
                &kit::key(),
                &id,
                ecr,
                epr,
                0,
                kit::replace_grants_action(grants),
            );
            let op = model_op_for_source(&actor, &cmd);
            Generated {
                actor,
                cmd: IssuedCmd::Source(Box::new(cmd)),
                op,
                desc: format!(
                    "replace-grants set={set:?} ecr={ecr} epr={epr} plausible={plausible}"
                ),
            }
        }
        // Cancel: 20%
        45..=64 => {
            let owner = &OWNER_IDS[rng.below(OWNER_IDS.len() as u64) as usize];
            let wf = if plausible {
                match model.owner_view(owner) {
                    Some((_, wf, true)) => wf,
                    _ => WORKFLOW_POOL[rng.below(WORKFLOW_POOL.len() as u64) as usize].to_vec(),
                }
            } else {
                WORKFLOW_POOL[rng.below(WORKFLOW_POOL.len() as u64) as usize].to_vec()
            };
            let generation = model.owner_generation(owner);
            let eg = if plausible {
                generation
            } else {
                off_by_one(generation)
            };
            let cmd = kit::source_command(
                authority,
                &owner_key(owner),
                &id,
                ecr,
                epr,
                eg,
                kit::cancel_action(&wf),
            );
            let op = model_op_for_source(&actor, &cmd);
            Generated {
                actor,
                cmd: IssuedCmd::Source(Box::new(cmd)),
                op,
                desc: format!(
                    "cancel owner={} wf={} ecr={ecr} epr={epr} plausible={plausible}",
                    String::from_utf8_lossy(owner),
                    String::from_utf8_lossy(&wf),
                ),
            }
        }
        // Activate: 15%. From tests only the refusal paths are reachable
        // (READY needs the pub(crate) readiness proof); plausible rolls
        // target a prepared owner with its own workflow.
        65..=79 => {
            let owner = &OWNER_IDS[rng.below(OWNER_IDS.len() as u64) as usize];
            let wf = if plausible {
                match model.owner_view(owner) {
                    Some((_, wf, true)) => wf,
                    _ => WORKFLOW_POOL[rng.below(WORKFLOW_POOL.len() as u64) as usize].to_vec(),
                }
            } else {
                WORKFLOW_POOL[rng.below(WORKFLOW_POOL.len() as u64) as usize].to_vec()
            };
            let generation = model.owner_generation(owner);
            let eg = if plausible {
                generation
            } else {
                off_by_one(generation)
            };
            let cmd = kit::source_command(
                authority,
                &owner_key(owner),
                &id,
                ecr,
                epr,
                eg,
                kit::activate_action(&wf),
            );
            let op = model_op_for_source(&actor, &cmd);
            Generated {
                actor,
                cmd: IssuedCmd::Source(Box::new(cmd)),
                op,
                desc: format!(
                    "activate owner={} wf={} ecr={ecr} epr={epr} plausible={plausible}",
                    String::from_utf8_lossy(owner),
                    String::from_utf8_lossy(&wf),
                ),
            }
        }
        // Install: 20%. Adversarial rolls vary the state, which changes the
        // retry fingerprint but never the model outcome.
        _ => {
            let routes = if plausible || rng.chance(50) { 2 } else { 3 };
            let cmd =
                kit::install_command_policy(authority, &id, ecr, epr, kit::install_state(routes));
            let op = model_op_for_install(&actor, &cmd);
            Generated {
                actor,
                cmd: IssuedCmd::Install(Box::new(cmd)),
                op,
                desc: format!("install routes={routes} ecr={ecr} epr={epr} plausible={plausible}"),
            }
        }
    }
}

fn toggle(set: &mut Vec<String>, principal: &str) {
    if let Some(index) = set.iter().position(|p| p == principal) {
        set.remove(index);
    } else {
        set.push(principal.to_owned());
    }
}

/// Generate an exact or mutated retry of an earlier issued op.
fn gen_retry(rng: &mut XorShift64, issued: &[Generated]) -> Generated {
    let index = rng.below(issued.len() as u64) as usize;
    let base = &issued[index];
    let actor = base.actor.clone();
    if rng.chance(45) {
        // Exact retry: same actor, same bytes.
        let (cmd, op) = match &base.cmd {
            IssuedCmd::Source(c) => (IssuedCmd::Source(c.clone()), model_op_for_source(&actor, c)),
            IssuedCmd::Install(c) => (
                IssuedCmd::Install(c.clone()),
                model_op_for_install(&actor, c),
            ),
        };
        return Generated {
            actor,
            cmd,
            op,
            desc: format!("retry-of-#{index} exact"),
        };
    }
    // Mutated retry: same command_id, changed content (revision or workflow).
    match base.cmd.clone() {
        IssuedCmd::Source(mut c) => {
            if rng.chance(50) {
                c.expected_control_revision = off_by_one(c.expected_control_revision);
            } else {
                match c.action.as_mut() {
                    Some(Action::Prepare(request)) => request.workflow_id.push(b'!'),
                    Some(Action::Cancel(request)) => request.workflow_id.push(b'!'),
                    Some(Action::Activate(request)) => request.workflow_id.push(b'!'),
                    _ => c.expected_policy_revision = off_by_one(c.expected_policy_revision),
                }
            }
            let op = model_op_for_source(&actor, &c);
            Generated {
                actor,
                cmd: IssuedCmd::Source(c),
                op,
                desc: format!("retry-of-#{index} mutated"),
            }
        }
        IssuedCmd::Install(mut c) => {
            if rng.chance(50) {
                c.expected_control_revision = off_by_one(c.expected_control_revision);
            } else {
                c.state = Some(kit::install_state(4));
            }
            let op = model_op_for_install(&actor, &c);
            Generated {
                actor,
                cmd: IssuedCmd::Install(c),
                op,
                desc: format!("retry-of-#{index} mutated"),
            }
        }
    }
}

/// Verify that decisions recorded under a revoked actor stay undisclosed and
/// reappear (with the recorded code and revisions) once the actor is
/// regranted.
fn drain_suppressed(
    store: &pipestream_search::source_authority::SourceAuthorityStore,
    model: &Model,
    pending: &mut Vec<(String, Vec<u8>, Vec<u8>, bool)>,
) {
    pending.retain(|(actor, owner_id, command_id, install)| {
        if !model.is_admin(actor) {
            return true;
        }
        let expected = model
            .journal_lookup(actor, owner_id, command_id, *install)
            .expect("suppressed entry is recorded");
        let actual = if *install {
            let d = store
                .control_install_decision(actor, &kit::key(), command_id)
                .unwrap();
            (d.code, d.control_revision, d.policy_revision)
        } else {
            let d = store
                .decision(actor, &owner_key(owner_id), command_id)
                .unwrap();
            (d.code, d.control_revision, d.policy_revision)
        };
        assert_eq!(
            actual, expected,
            "suppressed decision for {actor}/{command_id:?} did not reappear verbatim after regrant"
        );
        false
    });
}

/// Sampled journal consistency: an actor with current Admin reads its
/// recorded decisions verbatim; any other actor is refused with
/// PermissionDenied even though the decision exists.
fn verify_journal(
    rng: &mut XorShift64,
    store: &pipestream_search::source_authority::SourceAuthorityStore,
    model: &Model,
) {
    let entries = model.journal_entries();
    if entries.is_empty() {
        return;
    }
    let mut order: Vec<usize> = (0..entries.len()).collect();
    order.sort_by_cached_key(|_| rng.next());
    for index in order.into_iter().take(6) {
        let entry = &entries[index];
        if model.is_admin(&entry.actor) {
            let actual = if entry.install {
                let d = store
                    .control_install_decision(&entry.actor, &kit::key(), &entry.command_id)
                    .unwrap();
                (d.code, d.control_revision, d.policy_revision)
            } else {
                let d = store
                    .decision(&entry.actor, &owner_key(&entry.owner_id), &entry.command_id)
                    .unwrap();
                (d.code, d.control_revision, d.policy_revision)
            };
            assert_eq!(
                actual,
                (entry.code, entry.control_revision, entry.policy_revision),
                "journal entry {} {}/{:?} disagrees with the model",
                entry.actor,
                if entry.install { "install" } else { "source" },
                entry.command_id,
            );
        } else {
            let error = if entry.install {
                store
                    .control_install_decision(&entry.actor, &kit::key(), &entry.command_id)
                    .unwrap_err()
            } else {
                store
                    .decision(&entry.actor, &owner_key(&entry.owner_id), &entry.command_id)
                    .unwrap_err()
            };
            assert_eq!(
                error.code(),
                Code::PermissionDenied,
                "a revoked actor must not read its stored decision"
            );
        }
    }
    // A principal that never held a grant is refused on a known id.
    let entry = &entries[0];
    let error = if entry.install {
        store
            .control_install_decision("nobody", &kit::key(), &entry.command_id)
            .unwrap_err()
    } else {
        store
            .decision("nobody", &owner_key(&entry.owner_id), &entry.command_id)
            .unwrap_err()
    };
    assert_eq!(error.code(), Code::PermissionDenied);
}

fn seeds() -> Vec<u64> {
    let mut end = 24u64;
    if let Some(raw) = std::env::var_os("PSEARCH_ADV_SEEDS") {
        if let Some(text) = raw.to_str() {
            if let Ok(parsed) = text.trim().parse::<u64>() {
                end = end.max(parsed);
            }
        }
    }
    (1..=end).collect()
}

fn run_seed(n: u64) {
    let seed = 0x5EED_0000 + n;
    eprintln!("model fuzz: seed {seed:#x} (n={n})");
    let start = Instant::now();
    let mut rng = XorShift64::new(seed);
    let dir = kit::TestDir::new(&format!("model-{seed:#x}"));
    let (mut store, authority) = kit::create_store(&dir);

    let mut model = Model::new();
    let mut issued: Vec<Generated> = Vec::new();
    let mut trace: Vec<String> = Vec::new();
    let mut suppressed: Vec<(String, Vec<u8>, Vec<u8>, bool)> = Vec::new();

    for index in 0..OPS_PER_TRACE {
        if RECOVERY_POINTS.contains(&index) {
            // If alice is revoked, a current admin regrants her first so
            // every seed keeps a stable admin; that op is a real compared
            // step.
            if !model.is_admin("alice") {
                let actor = model.some_admin().expect("an admin remains");
                let mut set = model.admins();
                set.push("alice".to_owned());
                set.sort();
                let grants: Vec<pipestream_search::pb::CollectionGrant> =
                    set.iter().map(|p| kit::grant(p, kit::COLLECTION)).collect();
                let cmd = kit::source_command(
                    &authority,
                    &kit::key(),
                    &format!("regrant-recover-{index}"),
                    model.control_revision(),
                    model.policy_revision(),
                    0,
                    kit::replace_grants_action(grants),
                );
                let g = Generated {
                    actor: actor.clone(),
                    cmd: IssuedCmd::Source(Box::new(cmd.clone())),
                    op: model_op_for_source(&actor, &cmd),
                    desc: format!("recovery regrant alice set={set:?}"),
                };
                let expected = model.apply(&g.op);
                let real = run_real(&store, &g);
                let outcome = compare(seed, index, &g, &real, &expected, &trace);
                trace.push(format!("#{index:02} {} {} -> {outcome}", g.actor, g.desc));
                issued.push(g);
                drain_suppressed(&store, &model, &mut suppressed);
            }
            // Drop every clone and reopen.
            drop(store);
            store = kit::open_store(&dir, &authority);
            trace.push(format!("#{index:02} recovery: store reopened"));
            verify_journal(&mut rng, &store, &model);
        }

        let generated = if !issued.is_empty() && rng.chance(15) {
            gen_retry(&mut rng, &issued)
        } else {
            gen_fresh(&mut rng, &model, &authority)
        };
        let expected = model.apply(&generated.op);
        let real = run_real(&store, &generated);
        let outcome = compare(seed, index, &generated, &real, &expected, &trace);
        if matches!(expected, ModelOutcome::Suppressed { .. }) {
            suppressed.push((
                generated.actor.clone(),
                generated.op.owner_id.clone(),
                generated.op.command_id.clone(),
                generated.op.command.is_install(),
            ));
        }
        trace.push(format!(
            "#{index:02} {} {} -> {outcome}",
            generated.actor, generated.desc
        ));
        issued.push(generated);
        drain_suppressed(&store, &model, &mut suppressed);
    }

    verify_journal(&mut rng, &store, &model);
    if let Some(dir) = std::env::var_os("PSEARCH_ADV_TRACE_DIR") {
        let path = std::path::PathBuf::from(dir).join(format!("trace-{seed:#x}.txt"));
        std::fs::write(&path, trace.join("\n") + "\n").expect("write trace file");
    }
    eprintln!(
        "model fuzz: seed {seed:#x} done in {:?} ({} ops, journal {})",
        start.elapsed(),
        trace.len(),
        model.journal_entries().len()
    );
}

#[test]
fn model_differential_fuzz_matches_store() {
    for n in seeds() {
        run_seed(n);
    }
}
