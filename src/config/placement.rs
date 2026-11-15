//! The placement leaf a shard serves and the derived columns it computes.

use std::path::Path;
use std::sync::Arc;

use super::file::FileConfig;
use super::option::Options;
use super::shard_map::load_placement_tree;

/// The placement code column, the pinned leaf, and the derived-column declaration.
pub(super) struct Placement {
    pub(super) placement_column: Option<String>,
    pub(super) placement_leaf: Option<i64>,
    pub(super) placement_tree: Option<Arc<crate::placement::PinnedLeaf>>,
    pub(super) derived: Option<Arc<crate::derived::Declaration>>,
}

/// A placement code: a non-negative path code.
fn leaf_code(o: Options, file: &FileConfig) -> Result<Option<i64>, String> {
    let file_leaf = file.placement_leaf.map(|code| code.to_string());
    let Some(raw) = o.trimmed("placement-leaf", file_leaf.as_deref()) else {
        return Ok(None);
    };
    let code = raw
        .parse::<i64>()
        .map_err(|e| super::option::invalid("placement-leaf", &raw, e))?;
    if code < 0 {
        return Err(format!(
            "placement leaf {code} is negative; codes are non-negative path codes"
        ));
    }
    Ok(Some(code))
}

/// The tree checked on the leaf this shard pins.
fn pinned_tree(
    path: &str,
    column: Option<&str>,
    leaf: Option<i64>,
) -> Result<Arc<crate::placement::PinnedLeaf>, String> {
    let (Some(column), Some(code)) = (column, leaf) else {
        return Err(
            "--placement-tree needs --placement-leaf (and --placement-column): the \
             tree is checked on the leaf this shard pins"
                .to_string(),
        );
    };
    let tree = load_placement_tree(Path::new(path))?;
    let pinned = crate::placement::PinnedLeaf::pin(&tree, column, code)
        .map_err(|e| format!("placement tree {path}: {e}"))?;
    Ok(Arc::new(pinned))
}

/// The derived-column declaration: a file (the coordinator's shard map or a
/// bare `[[derived]]` table) or the config file's own `[[derived]]` table,
/// never both.
fn derived_columns(
    o: Options,
    file: &FileConfig,
) -> Result<Option<Arc<crate::derived::Declaration>>, String> {
    let path = o.trimmed("derived-columns", file.derived_columns.as_deref());
    let spec = match (path, file.derived.is_empty()) {
        (Some(_), false) => {
            return Err(
                "--derived-columns names a file and the config file has its own [[derived]] \
                 table; declare the columns in one place"
                    .to_string(),
            );
        }
        (Some(path), true) => Some(
            crate::derived::load_declaration(Path::new(&path))
                .map_err(|e| format!("derived columns {path}: {e}"))?,
        ),
        (None, false) => Some(crate::derived::spec_from_config(&file.derived)?),
        (None, true) => None,
    };
    spec.map(|spec| {
        crate::derived::Declaration::compile(&spec)
            .map(Arc::new)
            .map_err(|e| format!("derived columns: {e}"))
    })
    .transpose()
}

pub(super) fn resolve_placement(o: Options, file: &FileConfig) -> Result<Placement, String> {
    let placement_column = o.trimmed("placement-column", file.placement_column.as_deref());
    let placement_leaf = leaf_code(o, file)?;
    if placement_leaf.is_some() && placement_column.is_none() {
        return Err(
            "--placement-leaf needs --placement-column, the column that holds the code".to_string(),
        );
    }
    let placement_tree = o
        .trimmed("placement-tree", file.placement_tree.as_deref())
        .map(|path| pinned_tree(&path, placement_column.as_deref(), placement_leaf))
        .transpose()?;
    Ok(Placement {
        placement_column,
        placement_leaf,
        placement_tree,
        derived: derived_columns(o, file)?,
    })
}
