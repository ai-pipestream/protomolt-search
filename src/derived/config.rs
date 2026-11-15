//! The TOML `[[derived]]` form of a declaration.

use super::*;

/// One `[[derived]]` entry of a node config or shard map: the TOML
/// form of [`pb::DerivedColumn`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DerivedColumnConfig {
    /// The column name.
    pub name: String,
    /// `f64`, `i64`, or `u64`.
    pub kind: String,
    /// The CEL value expression.
    pub expression: String,
    /// `inputs` or `own`.
    pub disclosure: String,
}

/// The protobuf contract of a `[[derived]]` table. Kind and disclosure
/// names are exact and lowercase; anything else refuses by name.
pub fn spec_from_config(entries: &[DerivedColumnConfig]) -> Result<pb::DerivedColumns, String> {
    let mut columns = Vec::with_capacity(entries.len());
    for entry in entries {
        let kind = match entry.kind.as_str() {
            "f64" => pb::MaterializeKind::F64,
            "i64" => pb::MaterializeKind::I64,
            "u64" => pb::MaterializeKind::U64,
            other => {
                return Err(format!(
                    "derived column {:?}: kind {other:?} is not one of f64, i64, u64",
                    entry.name
                ))
            }
        };
        let disclosure = match entry.disclosure.as_str() {
            "inputs" => pb::DerivedDisclosure::Inputs,
            "own" => pb::DerivedDisclosure::Own,
            other => {
                return Err(format!(
                    "derived column {:?}: disclosure {other:?} is not one of inputs, own",
                    entry.name
                ))
            }
        };
        columns.push(pb::DerivedColumn {
            name: entry.name.clone(),
            expression: entry.expression.clone(),
            kind: kind as i32,
            disclosure: disclosure as i32,
        });
    }
    Ok(pb::DerivedColumns { columns })
}

/// The TOML form of a contract.
pub fn config_from_spec(spec: &pb::DerivedColumns) -> Vec<DerivedColumnConfig> {
    spec.columns
        .iter()
        .map(|column| DerivedColumnConfig {
            name: column.name.clone(),
            kind: kind_name(
                pb::MaterializeKind::try_from(column.kind)
                    .unwrap_or(pb::MaterializeKind::Unspecified),
            )
            .to_string(),
            expression: column.expression.clone(),
            disclosure: disclosure_name(
                pb::DerivedDisclosure::try_from(column.disclosure)
                    .unwrap_or(pb::DerivedDisclosure::Unspecified),
            )
            .to_string(),
        })
        .collect()
}

/// A file holding a `[[derived]]` table: the coordinator's shard map,
/// or a file with just the table.
#[derive(Debug, Deserialize)]
pub(super) struct DeclarationFile {
    #[serde(default)]
    pub(super) derived: Vec<DerivedColumnConfig>,
}

/// Read a declaration for `--derived-columns=<file>`: the shard map's
/// `[[derived]]` table or a file holding just that table. A file
/// without the table refuses by name.
pub fn load_declaration(path: &std::path::Path) -> Result<pb::DerivedColumns, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read derived columns {}: {e}", path.display()))?;
    let file: DeclarationFile = toml::from_str(&text)
        .map_err(|e| format!("parse derived columns {}: {e}", path.display()))?;
    if file.derived.is_empty() {
        return Err(format!(
            "derived columns {}: the file has no [[derived]] table",
            path.display()
        ));
    }
    spec_from_config(&file.derived)
}

/// The TOML text of a `[[derived]]` table, for the reshard tool's
/// shard map and for reports.
pub fn declaration_toml(spec: &pb::DerivedColumns) -> Result<String, String> {
    #[derive(Serialize)]
    struct Out<'a> {
        derived: &'a [DerivedColumnConfig],
    }
    let entries = config_from_spec(spec);
    toml::to_string(&Out { derived: &entries }).map_err(|e| e.to_string())
}
