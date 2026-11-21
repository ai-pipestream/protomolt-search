//! Column table declaration: placement, derived, and source tables.

use crate::node::*;

/// Ensure `config.placement_column` is registered in `integer_fields` so the
/// column table and catalog agree across all constructors.
pub(in crate::node) fn declare_placement_column(config: &mut NodeConfig) {
    if let Some(column) = config.placement_column.as_deref() {
        if !config.integer_fields.iter().any(|name| name == column) {
            config.integer_fields.push(column.to_string());
        }
    }
}

/// The index's SOURCE column tables: the configured lists without the
/// derived names, which is what a derived expression or a request's
/// own materialize spec may read (outputs are not inputs).
pub(in crate::node) fn source_tables(config: &NodeConfig) -> crate::reshard::ColumnTables {
    let without = |names: &[String]| -> Vec<String> {
        names
            .iter()
            .filter(|name| {
                config
                    .derived
                    .as_ref()
                    .is_none_or(|declaration| !declaration.is_derived(name))
            })
            .cloned()
            .collect()
    };
    crate::reshard::ColumnTables {
        facets: config.facet_fields.clone(),
        numerics: without(&config.numeric_fields),
        map_facets: config.map_facet_fields.clone(),
        map_numerics: config.map_numeric_fields.clone(),
        map_integers: config.map_integer_fields.clone(),
        map_unsigned_integers: config.map_unsigned_integer_fields.clone(),
        integers: without(&config.integer_fields),
        unsigned_integers: without(&config.unsigned_integer_fields),
        geo: config.geo_fields.clone(),
    }
}

/// Validate `config.derived` against source column tables (verifying source-only
/// inputs, matching target kinds, and non-colliding names) and append derived
/// columns in declaration order. Idempotent across constructors.
pub(in crate::node) fn declare_derived_columns(config: &mut NodeConfig) -> Result<(), String> {
    let Some(declaration) = config.derived.clone() else {
        return Ok(());
    };
    // A derived name already in a table is either this function's own
    // earlier append (the derived names of the kind, in order, at the
    // table's end) or a collision with a source column.
    for (kind, table) in [
        (crate::pb::MaterializeKind::F64, &config.numeric_fields),
        (crate::pb::MaterializeKind::I64, &config.integer_fields),
        (
            crate::pb::MaterializeKind::U64,
            &config.unsigned_integer_fields,
        ),
    ] {
        let names = declaration.names_of(kind);
        let appended = table.ends_with(&names);
        if !appended {
            if let Some(name) = names.iter().find(|name| table.contains(name)) {
                return Err(format!(
                    "derived columns: derived column {name:?} collides with a source column \
                     of the same name"
                ));
            }
        }
    }
    let sources = source_tables(config);
    declaration
        .check_tables(&sources.numeric_types())
        .map_err(|e| format!("derived columns: {e}"))?;
    for (kind, table) in [
        (crate::pb::MaterializeKind::F64, &mut config.numeric_fields),
        (crate::pb::MaterializeKind::I64, &mut config.integer_fields),
        (
            crate::pb::MaterializeKind::U64,
            &mut config.unsigned_integer_fields,
        ),
    ] {
        for name in declaration.names_of(kind) {
            if !table.contains(&name) {
                table.push(name);
            }
        }
    }
    Ok(())
}

/// A placement tree's predicates read columns this shard declares,
/// source or derived; a predicate naming any other column would send
/// rows by a value no row carries, so it is refused at startup.
pub(in crate::node) fn check_placement_columns(config: &NodeConfig) -> Result<(), String> {
    let Some(pinned) = config.placement_tree.as_ref() else {
        return Ok(());
    };
    for column in pinned.placement().predicate_columns() {
        let declared = config.facet_fields.contains(&column)
            || config.numeric_fields.contains(&column)
            || config.map_facet_fields.contains(&column)
            || config.map_numeric_fields.contains(&column)
            || config.integer_fields.contains(&column)
            || config.unsigned_integer_fields.contains(&column)
            || config.geo_fields.contains(&column);
        if !declared {
            return Err(format!(
                "placement tree: a predicate reads column {column:?}, which this shard \
                 declares in no table, source or derived"
            ));
        }
    }
    Ok(())
}

/// The store form of the configured declaration.
pub(crate) fn stored_derived(config: &NodeConfig) -> Option<crate::postings::StoredDerived> {
    config
        .derived
        .as_deref()
        .map(crate::postings::StoredDerived::of)
}

/// A store or catalog attached to this shard was written under this
/// shard's declaration, or under none while still empty; anything else
/// is an index compatibility event, refused by name.
pub(crate) fn check_attached_derived(
    config: &NodeConfig,
    held: Option<&crate::postings::StoredDerived>,
    rows: u64,
    what: &str,
) -> Result<(), String> {
    match (held, config.derived.as_deref()) {
        (None, None) => Ok(()),
        (Some(held), Some(wanted)) if held.fingerprint == wanted.fingerprint() => Ok(()),
        (Some(held), Some(wanted)) => Err(format!(
            "{what} was written under derived-column declaration {:?}, this node declares \
             {:?}; a changed declaration is a rebuild through the reshard tool",
            held.fingerprint,
            wanted.fingerprint()
        )),
        (Some(held), None) => Err(format!(
            "{what} was written under derived-column declaration {:?}, which this node does \
             not declare (--derived-columns)",
            held.fingerprint
        )),
        (None, Some(_)) if rows == 0 => Ok(()),
        (None, Some(wanted)) => Err(format!(
            "{what} holds {rows} rows written without derived columns, this node declares \
             {:?}; backfill the rows through the reshard tool (--derive)",
            wanted.fingerprint()
        )),
    }
}

// Rust and embedded callers bypass the CLI parser. Enforce the same column
// namespace before they create or open storage, and before a fresh ingest.
pub(in crate::node) fn validate_column_tables(config: &NodeConfig) -> Result<(), String> {
    let mut names = std::collections::HashSet::new();
    for name in config
        .facet_fields
        .iter()
        .chain(&config.numeric_fields)
        .chain(&config.map_facet_fields)
        .chain(&config.map_numeric_fields)
        .chain(&config.map_integer_fields)
        .chain(&config.map_unsigned_integer_fields)
        .chain(&config.integer_fields)
        .chain(&config.unsigned_integer_fields)
        .chain(&config.geo_fields)
    {
        if !names.insert(name) {
            return Err(format!(
                "column {name:?} is declared more than once; each name has one column kind"
            ));
        }
    }
    Ok(())
}
