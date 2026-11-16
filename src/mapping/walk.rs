//! The plan walk: one message's fields in declaration order, expanded into
//! planned fields.

use super::MappingError;
use prost_types::FieldDescriptorProto;

use super::hint_bytes::HintMap;
use super::hint_resolve::{is_repeated, resolve_hint, shape, wrapper_kind, ResolvedHint, Shape};
use super::hint_validate::validate_hint;
use super::type_index::{MsgEntry, TypeIndex};
use super::{refuse_at, MAX_DEPTH};
use crate::pb;

/// Well-known message types that plan as leaves rather than expanding.
pub(super) fn well_known_leaf(full: &str) -> bool {
    wrapper_kind(full).is_some()
        || matches!(
            full,
            "google.protobuf.Timestamp" | "google.protobuf.Struct" | "google.protobuf.Value"
        )
}

/// The column plane one planned field lands on. `NONE` is a visible
/// outcome of the dry run, never a silent drop at ingest: the facet,
/// i64, u64, and f64 planes hold one value per document slot, so repeated
/// scalars do not land, and this engine does not guess a collapse rule.
pub(super) fn family(kind: pb::MappedKind, repeated: bool) -> pb::ColumnFamily {
    use pb::MappedKind as K;
    match kind {
        K::Vector => pb::ColumnFamily::Vector,
        K::Object | K::Nested | K::Binary | K::Unspecified => pb::ColumnFamily::None,
        _ if repeated => pb::ColumnFamily::None,
        K::Text => pb::ColumnFamily::TextField,
        K::Keyword | K::Boolean => pb::ColumnFamily::Facet,
        K::Int32 | K::Int64 | K::Date => pb::ColumnFamily::I64,
        K::Uint32 | K::Uint64 => pb::ColumnFamily::U64,
        K::Float | K::Double => pb::ColumnFamily::F64,
    }
}

pub(super) fn planned(
    path: &str,
    name: &str,
    field: &FieldDescriptorProto,
    shape: &Shape<'_>,
    hint: &ResolvedHint,
) -> pb::MappedField {
    let map = matches!(shape, Shape::Message { map: true, .. });
    let repeated = is_repeated(field) || map;
    pb::MappedField {
        path: path.to_string(),
        name: name.to_string(),
        kind: hint.kind as i32,
        repeated,
        role: hint.role as i32,
        vector_dims: hint.vector_dims,
        analyzer: hint.analyzer.clone(),
        search_analyzer: hint.search_analyzer.clone(),
        family: if map {
            match hint.kind {
                pb::MappedKind::Keyword | pb::MappedKind::Boolean => pb::ColumnFamily::MapFacet,
                pb::MappedKind::Float | pb::MappedKind::Double => pb::ColumnFamily::MapF64,
                pb::MappedKind::Int32 | pb::MappedKind::Int64 => pb::ColumnFamily::MapI64,
                pb::MappedKind::Uint32 | pb::MappedKind::Uint64 => pb::ColumnFamily::MapU64,
                _ => pb::ColumnFamily::None,
            }
        } else {
            family(hint.kind, repeated)
        } as i32,
    }
}

/// The walk over a root message: the planned fields so far, and the message
/// names on the current branch so a recursive type stops expanding instead
/// of looping.
pub(super) struct PlanWalk<'a> {
    index: &'a TypeIndex<'a>,
    hint_map: &'a HintMap,
    out: Vec<pb::MappedField>,
    visiting: Vec<String>,
}

impl<'a> PlanWalk<'a> {
    pub(super) fn new(index: &'a TypeIndex<'a>, hint_map: &'a HintMap) -> Self {
        Self {
            index,
            hint_map,
            out: Vec::new(),
            visiting: Vec::new(),
        }
    }

    pub(super) fn into_fields(self) -> Vec<pb::MappedField> {
        self.out
    }

    /// Walks one message's fields, appending planned fields in declaration
    /// order.
    pub(super) fn message(
        &mut self,
        entry: &MsgEntry<'_>,
        path_prefix: &str,
        name_prefix: &str,
        depth: usize,
    ) -> Result<(), MappingError> {
        self.visiting.push(entry.full.clone());
        for (field_idx, field) in entry.desc.field.iter().enumerate() {
            let path = join_path(path_prefix, field.name());
            let field_shape = shape(field, self.index, &path)?;
            let explicit = self.hint_map.get(&(entry.key.clone(), field_idx));
            let hint = resolve_hint(field, &field_shape, explicit, &path)?;
            if hint.skip {
                continue;
            }
            let qualified = if name_prefix.is_empty() {
                field.name().to_string()
            } else {
                format!("{name_prefix}_{}", field.name())
            };
            let name = if hint.name_override.is_empty() {
                qualified
            } else {
                hint.name_override.clone()
            };
            validate_hint(field, &field_shape, &hint, &path)?;
            if self.expand(field, &field_shape, &hint, &path, &name, depth)? {
                continue;
            }
            self.out
                .push(planned(&path, &name, field, &field_shape, &hint));
        }
        self.visiting.pop();
        Ok(())
    }

    /// Expands a message-typed field into its children where the plan calls
    /// for it. Returns whether the field is fully handled, its own entry
    /// included.
    fn expand(
        &mut self,
        field: &FieldDescriptorProto,
        field_shape: &Shape<'_>,
        hint: &ResolvedHint,
        path: &str,
        name: &str,
        depth: usize,
    ) -> Result<bool, MappingError> {
        let Shape::Message {
            full,
            entry: child,
            map,
        } = field_shape
        else {
            return Ok(false);
        };
        if (wrapper_kind(full).is_some() || hint.kind == pb::MappedKind::Date)
            && family(hint.kind, is_repeated(field)) != pb::ColumnFamily::None
            && child
                .desc
                .field
                .iter()
                .enumerate()
                .any(|(i, _)| self.hint_map.contains_key(&(child.key.clone(), i)))
        {
            return Err(refuse_at(path, "indexing hints on well-known value components are unsupported; put the hint on the containing field"));
        }
        let blocked = depth >= MAX_DEPTH || self.visiting.iter().any(|n| n == full);
        if hint.role == pb::MappedRole::Chunks {
            // The chunk scope keeps its container entry and expands its
            // children as unprefixed fields: within a block the children
            // are their own documents, not properties of the parent.
            self.out.push(planned(path, name, field, field_shape, hint));
            if !blocked {
                self.message(child, path, "", depth + 1)?;
            }
            return Ok(true);
        }
        let expandable = !is_repeated(field)
            && !map
            && !well_known_leaf(full)
            && !matches!(
                hint.kind,
                pb::MappedKind::Object | pb::MappedKind::Nested if hint.explicit_kind
            );
        if expandable && !blocked {
            self.message(child, path, name, depth + 1)?;
            return Ok(true);
        }
        Ok(false)
    }
}

fn join_path(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}.{segment}")
    }
}
