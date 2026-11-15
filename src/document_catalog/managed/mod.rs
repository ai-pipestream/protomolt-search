//! Durable managed ownership starts closed. No writable adapter exists here.
use super::*;
use crate::pb::storage::{PreparedSourceOwnerPhase, SourceManagedActivation, SourceManagedBinding};

#[cfg(all(any(test, feature = "fault-injection"), feature = "net"))]
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Clone, Copy)]
#[allow(clippy::enum_variant_names)]
pub(super) enum BindFault {
    BeforeCommit,
    AfterCommit,
    ExitBeforeCommit,
    ExitAfterCommit,
}

#[cfg(all(any(test, feature = "fault-injection"), feature = "net"))]
fn inject(fault: Option<BindFault>, after: bool) -> Result<(), Status> {
    match (fault, after) {
        (Some(BindFault::BeforeCommit), false) => Err(Status::aborted(
            "injected source binding BeforeCommit failure",
        )),
        (Some(BindFault::AfterCommit), true) => {
            Err(storage("injected source binding AfterCommit failure"))
        }
        (Some(BindFault::ExitBeforeCommit), false) | (Some(BindFault::ExitAfterCommit), true) => {
            std::process::exit(87)
        }
        _ => Ok(()),
    }
}

/// Read a catalog file's header without opening the catalog: its format
/// with the persisted binding and activation. Recovery presents these
/// exact binding bytes to the committed fence; nothing is reconstructed.
/// A file with no managed binding is not a managed source.
#[cfg(all(feature = "net", feature = "raft"))]
pub(crate) fn header_binding(
    path: &Path,
) -> Result<(u32, SourceManagedBinding, Option<SourceManagedActivation>), Status> {
    if !path.is_file() {
        return Err(Status::not_found(format!(
            "managed catalog {} is missing; restore it rather than creating a fresh authority",
            path.display()
        )));
    }
    let database = Database::open(path).map_err(storage)?;
    let transaction = database.begin_read().map_err(storage)?;
    let metadata = transaction.open_table(META).map_err(storage)?;
    let bytes = metadata
        .get("header")
        .map_err(storage)?
        .ok_or_else(|| Status::data_loss("existing document catalog header missing"))?
        .value()
        .to_vec();
    let header = decode_header(&bytes)?;
    let binding = header.managed_binding.ok_or_else(|| {
        Status::failed_precondition(format!(
            "catalog {} has no managed binding; bind it under a committed preparation first",
            path.display()
        ))
    })?;
    Ok((header.format_version, binding, header.managed_activation))
}

pub(super) fn validate_binding(binding: &SourceManagedBinding) -> Result<(), Status> {
    if binding.format_version != 1 {
        return Err(Status::invalid_argument(
            "managed source binding requires format 1",
        ));
    }
    let authority = binding
        .authority
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("managed source authority identity missing"))?;
    crate::source_owner::identity(authority)?;
    let preparation = binding
        .preparation
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("managed source preparation missing"))?;
    crate::source_owner::owner(preparation).map_err(|e| Status::invalid_argument(e.message()))?;
    if preparation.phase != PreparedSourceOwnerPhase::Prepared as i32 {
        return Err(Status::invalid_argument(
            "managed source binding requires a prepared owner",
        ));
    }
    Ok(())
}

pub(super) fn validate_activation(
    activation: &SourceManagedActivation,
    binding: &SourceManagedBinding,
) -> Result<(), Status> {
    let preparation = binding.preparation.as_ref().expect("validated preparation");
    if activation.format_version != 1
        || activation.authority != binding.authority
        || activation.ownership_generation != preparation.ownership_generation
        || activation.write_epoch == 0
        || activation.activated_control_revision == 0
    {
        return Err(Status::invalid_argument(
            "managed activation requires format 1, the binding's authority and generation, and a nonzero fence",
        ));
    }
    Ok(())
}

pub(super) fn validate_header(header: &DocumentCatalogHeader) -> Result<(), Status> {
    let Some(binding) = &header.managed_binding else {
        return if matches!(
            header.format_version,
            MANAGED_FORMAT | ACTIVE_MANAGED_FORMAT
        ) || header.managed_activation.is_some()
        {
            Err(Status::data_loss(
                "managed catalog owner binding is missing",
            ))
        } else {
            Ok(())
        };
    };
    match (header.format_version, header.managed_activation.as_ref()) {
        (MANAGED_FORMAT, None) => {}
        (ACTIVE_MANAGED_FORMAT, Some(activation)) => {
            validate_binding(binding).map_err(|e| Status::data_loss(e.message()))?;
            validate_activation(activation, binding).map_err(|e| Status::data_loss(e.message()))?;
            if activation.activated_at_sequence > header.accepted_sequence {
                return Err(Status::data_loss(
                    "managed activation sequence is ahead of the accepted sequence",
                ));
            }
        }
        _ => {
            return Err(Status::data_loss(
                "managed source binding requires catalog format 9 without activation or format 11 with it",
            ))
        }
    }
    validate_binding(binding).map_err(|e| Status::data_loss(e.message()))?;
    let prepared = binding.preparation.as_ref().expect("validated preparation");
    let key = prepared.key.as_ref().expect("validated owner key");
    let target = prepared.target.as_ref().expect("validated target");
    let resource = header
        .resource_binding
        .as_ref()
        .ok_or_else(|| Status::data_loss("managed catalog resource binding missing"))?;
    if key.workspace != resource.workspace
        || key.collection != resource.collection
        || target.history_id != header.history_id
        || binding.bound_at_sequence > header.accepted_sequence
    {
        return Err(Status::data_loss(
            "managed source owner, resource, history or accepted sequence differs",
        ));
    }
    Ok(())
}

#[cfg(feature = "net")]
mod adapter;

#[cfg(feature = "net")]
pub use adapter::{Acceptance, ActiveManagedCatalog, PreparedManagedCatalog, Settlement};
#[cfg(all(test, feature = "net"))]
mod tests;
