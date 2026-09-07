//! Derived columns declared on the index (`docs/derived-columns.md`):
//! CEL value columns computed once at index time from each document's
//! own values, stored as ordinary typed columns, and usable as the
//! physical shard key.
//!
//! The declaration is a protobuf contract (`DerivedColumns`), which the
//! TOML `[[derived]]` table configures field for field. Its fingerprint
//! identifies what an index means: every shard, the coordinator's shard
//! map, the write-ahead log, the store file (kind-15 entry), and the
//! segment catalog carry it, and a mismatch refuses by name. A changed
//! expression is a rebuild through the reshard tool, never a mutation
//! in place.
//!
//! At ingest the node is the only writer of derived values: a request
//! that carries a value under a derived name, or a fingerprint, is
//! refused as forged; the node evaluates the declaration over the
//! document's own values, pushes the results into the ordinary value
//! lists, stamps the fingerprint, and logs that. A logged record
//! replays with its recorded values and never evaluates twice.

use crate::pb::{self, AddDocumentsRequest};
use crate::values::{self, IngestEnv, IngestVal, ValueType};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashSet};
use tonic::Status;

/// Domain separator of the declaration fingerprint. Bumped only when
/// the meaning of an unchanged declaration changes.
const FINGERPRINT_DOMAIN: &[u8] = b"protomolt-search.derived-columns.v1\0";

/// One compiled derived column.
#[derive(Debug, Clone)]
pub struct CompiledColumn {
    /// The column name.
    pub name: String,
    /// The compiled expression.
    pub expr: pb::ValueExpr,
    /// The stored family.
    pub kind: pb::MaterializeKind,
    /// The disclosure rule.
    pub disclosure: pb::DerivedDisclosure,
    /// The column names the expression reads (facets included).
    pub inputs: Vec<String>,
    /// Whether the expression reads the stable routing key.
    pub reads_stable_key: bool,
}

/// A validated declaration.
#[derive(Debug, Clone)]
pub struct Declaration {
    spec: pb::DerivedColumns,
    fingerprint: String,
    columns: Vec<CompiledColumn>,
}

impl PartialEq for Declaration {
    fn eq(&self, other: &Self) -> bool {
        self.fingerprint == other.fingerprint
    }
}
impl Eq for Declaration {}

/// The fingerprint of a declaration: SHA-256 over a domain separator
/// and the canonical protobuf encoding, lowercase hex. Independent of
/// the TOML spelling, so a shard map and a node config that mean the
/// same declaration agree.
pub fn fingerprint_of(spec: &pb::DerivedColumns) -> String {
    let mut hasher = crate::sha256::Sha256::new();
    hasher.update(FINGERPRINT_DOMAIN);
    hasher.update(&spec.encode_to_vec());
    crate::sha256::to_hex(&hasher.finalize())
}

/// Decode a stored canonical declaration.
pub fn decode_canonical(bytes: &[u8]) -> Result<pb::DerivedColumns, String> {
    pb::DerivedColumns::decode(bytes).map_err(|e| format!("derived-column declaration: {e}"))
}

/// The wire kind's name in the TOML form.
fn kind_name(kind: pb::MaterializeKind) -> &'static str {
    match kind {
        pb::MaterializeKind::F64 => "f64",
        pb::MaterializeKind::I64 => "i64",
        pb::MaterializeKind::U64 => "u64",
        pb::MaterializeKind::Unspecified => "unspecified",
    }
}

fn disclosure_name(disclosure: pb::DerivedDisclosure) -> &'static str {
    match disclosure {
        pb::DerivedDisclosure::Inputs => "inputs",
        pb::DerivedDisclosure::Own => "own",
        pb::DerivedDisclosure::Unspecified => "unspecified",
    }
}

impl Declaration {
    /// Compile and validate a declaration on its own: names, kinds,
    /// disclosure rules, and expressions that compile to the declared
    /// static type where the expression pins one. The column tables
    /// are checked separately by [`Self::check_tables`].
    pub fn compile(spec: &pb::DerivedColumns) -> Result<Declaration, String> {
        let mut names: HashSet<&str> = HashSet::new();
        let mut columns = Vec::with_capacity(spec.columns.len());
        for column in &spec.columns {
            let name = column.name.trim();
            if name.is_empty() || name != column.name {
                return Err(format!(
                    "derived column {:?}: a name is non-empty with no surrounding whitespace",
                    column.name
                ));
            }
            if !names.insert(name) {
                return Err(format!("derived column {name:?} is declared twice"));
            }
            let kind = match pb::MaterializeKind::try_from(column.kind) {
                Ok(pb::MaterializeKind::F64) => pb::MaterializeKind::F64,
                Ok(pb::MaterializeKind::I64) => pb::MaterializeKind::I64,
                Ok(pb::MaterializeKind::U64) => pb::MaterializeKind::U64,
                _ => {
                    return Err(format!(
                        "derived column {name:?} declares no kind; kinds are explicit (f64, \
                         i64, or u64), never inferred from data"
                    ))
                }
            };
            let disclosure = match pb::DerivedDisclosure::try_from(column.disclosure) {
                Ok(pb::DerivedDisclosure::Inputs) => pb::DerivedDisclosure::Inputs,
                Ok(pb::DerivedDisclosure::Own) => pb::DerivedDisclosure::Own,
                _ => {
                    return Err(format!(
                        "derived column {name:?} declares no disclosure rule; it is explicit: \
                         `inputs` (the column needs the grants of everything it reads) or \
                         `own` (its own grant suffices)"
                    ))
                }
            };
            if column.expression.trim().is_empty() {
                return Err(format!("derived column {name:?} has an empty expression"));
            }
            let expr = crate::cel::compile_value(&column.expression)
                .map_err(|e| format!("derived column {name:?}: {}", e.message()))?;
            let mut leaves = Vec::new();
            values::column_leaves(&expr, &mut leaves);
            let inputs: BTreeSet<String> = leaves
                .into_iter()
                .map(|leaf| match leaf {
                    values::ValueLeaf::Column(column) | values::ValueLeaf::Map { column, .. } => {
                        column
                    }
                })
                .collect();
            columns.push(CompiledColumn {
                name: name.to_string(),
                expr: expr.clone(),
                kind,
                disclosure,
                inputs: inputs.into_iter().collect(),
                reads_stable_key: values::reads_stable_key(&expr),
            });
        }
        // A later column naming an earlier one would read nothing: the
        // outputs are not inputs, by contract. Refuse the spelling
        // instead of storing absence.
        for (i, column) in columns.iter().enumerate() {
            for input in &column.inputs {
                if columns[..i].iter().any(|earlier| &earlier.name == input) {
                    return Err(format!(
                        "derived column {:?} reads derived column {input:?}; outputs are not \
                         inputs, write the earlier expression inline",
                        column.name
                    ));
                }
            }
        }
        Ok(Declaration {
            spec: spec.clone(),
            fingerprint: fingerprint_of(spec),
            columns,
        })
    }

    /// Check the declaration against the index's SOURCE column tables:
    /// every input names a column of the index (no expression is
    /// absent on every document), the static type matches the declared
    /// kind, and no derived name collides with a source column.
    pub fn check_tables(&self, tables: &values::NumericTypes<'_>) -> Result<(), String> {
        for column in &self.columns {
            let name = &column.name;
            if tables.numerics.contains(name)
                || tables.integers.contains(name)
                || tables.unsigned_integers.contains(name)
                || tables.map_numerics.contains(name)
                || tables.facets.contains(name)
            {
                return Err(format!(
                    "derived column {name:?} collides with a source column of the same name"
                ));
            }
            let mut leaves = Vec::new();
            values::column_leaves(&column.expr, &mut leaves);
            for leaf in &leaves {
                if !values::leaf_known(leaf, tables) {
                    let what = match leaf {
                        values::ValueLeaf::Column(column) => format!("column {column:?}"),
                        values::ValueLeaf::Map { column, key } => {
                            format!("map column {column:?} (key {key:?})")
                        }
                    };
                    return Err(format!(
                        "derived column {name:?} reads {what}, which this index does not \
                         declare; a derived expression reads source columns only"
                    ));
                }
            }
            let (_, vt) = values::resolve(&column.expr, tables)
                .map_err(|e| format!("derived column {name:?}: {}", e.message()))?;
            let expected = match column.kind {
                pb::MaterializeKind::F64 => ValueType::Double,
                pb::MaterializeKind::I64 => ValueType::Int,
                pb::MaterializeKind::U64 => ValueType::Uint,
                pb::MaterializeKind::Unspecified => unreachable!("compile checked the kind"),
            };
            if vt != expected {
                let hint = match vt {
                    ValueType::Bool => {
                        "wrap the boolean in a ternary that yields the declared kind"
                    }
                    ValueType::Str => "a bare facet read stores nothing; hash it or compare it",
                    ValueType::Unknown => "every input is missing from the index",
                    _ => "align the kind with the expression; double(...) converts to f64",
                };
                return Err(format!(
                    "derived column {name:?} declares {} but its expression resolves to {}; {hint}",
                    kind_name(column.kind),
                    vt.name()
                ));
            }
        }
        Ok(())
    }

    /// The declaration's fingerprint.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    /// The declaration as declared.
    pub fn spec(&self) -> &pb::DerivedColumns {
        &self.spec
    }

    /// The canonical protobuf encoding, what the fingerprint covers and
    /// what the store and the log persist.
    pub fn canonical(&self) -> Vec<u8> {
        self.spec.encode_to_vec()
    }

    /// The compiled columns in declaration order.
    pub fn columns(&self) -> &[CompiledColumn] {
        &self.columns
    }

    /// One column by name.
    pub fn column(&self, name: &str) -> Option<&CompiledColumn> {
        self.columns.iter().find(|column| column.name == name)
    }

    /// Whether `name` is a derived column here.
    pub fn is_derived(&self, name: &str) -> bool {
        self.column(name).is_some()
    }

    /// The names of one kind, in declaration order.
    pub fn names_of(&self, kind: pb::MaterializeKind) -> Vec<String> {
        self.columns
            .iter()
            .filter(|column| column.kind == kind)
            .map(|column| column.name.clone())
            .collect()
    }

    /// Whether any column reads the stable routing key.
    pub fn reads_stable_key(&self) -> bool {
        self.columns.iter().any(|column| column.reads_stable_key)
    }

    /// The `(name, expression, kind)` triples the materialization
    /// evaluator takes.
    fn triples(&self) -> Vec<(String, pb::ValueExpr, pb::MaterializeKind)> {
        self.columns
            .iter()
            .map(|column| (column.name.clone(), column.expr.clone(), column.kind))
            .collect()
    }

    /// A fresh request must not carry a derived value or a fingerprint:
    /// the node computes them. Refused by name as forged.
    pub fn refuse_carried(&self, doc: &AddDocumentsRequest) -> Result<(), Status> {
        if !doc.derived_fingerprint.is_empty() {
            return Err(Status::invalid_argument(format!(
                "the request stamps derived fingerprint {:?}; derived columns are computed \
                 by the index from its declaration, and a client supplies neither the values \
                 nor the fingerprint",
                doc.derived_fingerprint
            )));
        }
        for column in &self.columns {
            let carried = doc.numerics.iter().any(|v| v.field == column.name)
                || doc.integers.iter().any(|v| v.field == column.name)
                || doc.unsigned_integers.iter().any(|v| v.field == column.name)
                || doc.timestamps.iter().any(|v| v.field == column.name);
            if carried {
                return Err(Status::invalid_argument(format!(
                    "the request carries a value under derived column {:?}; derived columns \
                     are computed by the index from its declaration, and a supplied value is \
                     refused as forged",
                    column.name
                )));
            }
        }
        Ok(())
    }

    /// A logged record already carries its derived values: its
    /// fingerprint must be this declaration's.
    pub fn check_logged(&self, doc: &AddDocumentsRequest) -> Result<(), Status> {
        if doc.derived_fingerprint != self.fingerprint {
            return Err(Status::failed_precondition(format!(
                "the record was derived under declaration {:?}, this index declares {:?}; \
                 rebuild the rows through the reshard tool (docs/derived-columns.md)",
                doc.derived_fingerprint, self.fingerprint
            )));
        }
        Ok(())
    }

    /// Evaluate every column over the document's own values, push the
    /// results into its value lists, and stamp the fingerprint. A
    /// column that reads the stable key refuses a document without one
    /// instead of storing absence.
    pub fn apply(
        &self,
        doc: AddDocumentsRequest,
        stable_key: Option<&[u8]>,
    ) -> Result<AddDocumentsRequest, Status> {
        self.refuse_carried(&doc)?;
        if self.reads_stable_key() && stable_key.is_none_or(<[u8]>::is_empty) {
            let column = self
                .columns
                .iter()
                .find(|column| column.reads_stable_key)
                .expect("reads_stable_key found one");
            return Err(Status::invalid_argument(format!(
                "derived column {:?} reads the stable routing key and this document arrived \
                 without one; route it through the coordinator, which stamps the key",
                column.name
            )));
        }
        let env = ingest_env(&doc, stable_key)?;
        let mut doc = materialize_into(doc, &self.triples(), &env, true)?;
        doc.derived_fingerprint = self.fingerprint.clone();
        Ok(doc)
    }

    /// Recompute columns on a row the reshard tool rebuilds
    /// (`--derive`, docs/derived-columns.md). The row's fingerprint says
    /// what it carries: this declaration's, and `names` must be empty
    /// (nothing to recompute); none, the row was never derived, and
    /// `names` must cover every declared column; another declaration's,
    /// and the named columns are recomputed in place of the values they
    /// carry while the rest are carried through. Every path restamps.
    pub fn rederive(
        &self,
        doc: AddDocumentsRequest,
        names: &[String],
        stable_key: Option<&[u8]>,
    ) -> Result<AddDocumentsRequest, Status> {
        if doc.derived_fingerprint == self.fingerprint {
            if names.is_empty() {
                return Ok(doc);
            }
            return Err(Status::failed_precondition(format!(
                "the row is already derived under this declaration ({}); --derive names \
                 {names:?}, which have nothing to recompute",
                self.fingerprint
            )));
        }
        for name in names {
            if !self.is_derived(name) {
                return Err(Status::invalid_argument(format!(
                    "{name:?} is not a column of the declaration"
                )));
            }
        }
        if doc.derived_fingerprint.is_empty() {
            let missing: Vec<&str> = self
                .columns
                .iter()
                .filter(|column| !names.contains(&column.name))
                .map(|column| column.name.as_str())
                .collect();
            if !missing.is_empty() {
                return Err(Status::failed_precondition(format!(
                    "the row was never derived; --derive must name every declared column, and \
                     {missing:?} are not named"
                )));
            }
        }
        let mut triples = Vec::with_capacity(names.len());
        for name in names {
            let column = self.column(name).expect("checked above");
            if column.reads_stable_key && stable_key.is_none_or(<[u8]>::is_empty) {
                return Err(Status::failed_precondition(format!(
                    "derived column {name:?} reads the stable routing key, which this row \
                     does not carry; split from the logs, which record it"
                )));
            }
            triples.push((column.name.clone(), column.expr.clone(), column.kind));
        }
        // The inputs are the row's source columns: every derived column
        // is hidden from the environment (outputs are not inputs), and
        // the named ones are dropped from the row so the recomputed
        // value takes their place.
        let mut source = doc.clone();
        let mut doc = doc;
        for column in &self.columns {
            source.numerics.retain(|v| v.field != column.name);
            source.integers.retain(|v| v.field != column.name);
            source.unsigned_integers.retain(|v| v.field != column.name);
            if names.contains(&column.name) {
                doc.numerics.retain(|v| v.field != column.name);
                doc.integers.retain(|v| v.field != column.name);
                doc.unsigned_integers.retain(|v| v.field != column.name);
            }
        }
        let env = ingest_env(&source, stable_key)?;
        let mut doc = materialize_into(doc, &triples, &env, true)?;
        doc.derived_fingerprint = self.fingerprint.clone();
        Ok(doc)
    }

    /// The TOML form of the declaration.
    pub fn config(&self) -> Vec<DerivedColumnConfig> {
        config_from_spec(&self.spec)
    }
}

/// The value environment of one document: its numeric families,
/// timestamps as epoch-micro integers under their column names, facet
/// strings, map numerics, and the stable routing key. A name arriving
/// twice in one family, or as both an integer and a timestamp, refuses
/// the document the way the store would.
pub fn ingest_env(
    doc: &AddDocumentsRequest,
    stable_key: Option<&[u8]>,
) -> Result<IngestEnv, Status> {
    let mut env = IngestEnv::default();
    for nv in &doc.numerics {
        if env.numerics.insert(nv.field.clone(), nv.value).is_some() {
            return Err(Status::invalid_argument(format!(
                "numeric field {:?} repeats in one document",
                nv.field
            )));
        }
    }
    for iv in &doc.integers {
        if env.integers.insert(iv.field.clone(), iv.value).is_some() {
            return Err(Status::invalid_argument(format!(
                "integer field {:?} repeats in one document",
                iv.field
            )));
        }
    }
    for tv in &doc.timestamps {
        let Some(instant) = tv.value.as_ref() else {
            return Err(Status::invalid_argument(format!(
                "timestamp field {:?} carries no instant; omit the entry instead",
                tv.field
            )));
        };
        let micros = crate::node::timestamp_to_epoch_micros(&tv.field, instant)?;
        if env.integers.insert(tv.field.clone(), micros).is_some() {
            return Err(Status::invalid_argument(format!(
                "integer field {:?} repeats in one document (integers and timestamps name \
                 the same columns)",
                tv.field
            )));
        }
    }
    for uv in &doc.unsigned_integers {
        if env
            .unsigned_integers
            .insert(uv.field.clone(), uv.value)
            .is_some()
        {
            return Err(Status::invalid_argument(format!(
                "unsigned integer field {:?} repeats in one document",
                uv.field
            )));
        }
    }
    for entry in &doc.map_numerics {
        env.map_numerics
            .insert((entry.field.clone(), entry.key.clone()), entry.value);
    }
    for fv in &doc.facets {
        if env
            .facets
            .insert(fv.field.clone(), fv.value.clone())
            .is_some()
        {
            return Err(Status::invalid_argument(format!(
                "facet field {:?} repeats in one document",
                fv.field
            )));
        }
    }
    env.stable_key = stable_key.filter(|key| !key.is_empty()).map(<[u8]>::to_vec);
    Ok(env)
}

/// Evaluate compiled `(name, expression, kind)` columns over `env` and
/// push the results into the document's ordinary value lists, refusing
/// a result whose type is not the declared kind. Shared by the index
/// declaration and a request's own `MaterializeSpec`. With `strict`
/// (the declaration's paths), the integer operations stock CEL calls
/// errors refuse the document instead of evaluating absent
/// (`values::eval_ingest_derived`); the per-request spec keeps the
/// Kleene rule (`values::eval_ingest`).
pub fn materialize_into(
    mut doc: AddDocumentsRequest,
    columns: &[(String, pb::ValueExpr, pb::MaterializeKind)],
    env: &IngestEnv,
    strict: bool,
) -> Result<AddDocumentsRequest, Status> {
    for (name, expr, kind) in columns {
        let value = if strict {
            values::eval_ingest_derived(expr, env)
        } else {
            values::eval_ingest(expr, env)
        }
        .map_err(|e| {
            Status::invalid_argument(format!("materialize: column {name:?}: {}", e.message()))
        })?;
        match value {
            None => {}
            Some(IngestVal::Bool(_)) => {
                return Err(Status::invalid_argument(format!(
                    "materialize: column {name:?} evaluated a boolean; a stored \
                     column holds numbers — wrap the expression in a ternary \
                     (`cond ? 1 : 0`)"
                )));
            }
            Some(IngestVal::Double(v)) => {
                if *kind != pb::MaterializeKind::F64 {
                    return Err(Status::invalid_argument(format!(
                        "materialize: column {name:?} declares {kind:?} but its \
                         expression evaluated double on this document; stock CEL \
                         does not coerce — align the kind or the expression"
                    )));
                }
                doc.numerics.push(pb::NumericValue {
                    field: name.clone(),
                    value: v,
                });
            }
            Some(IngestVal::Int(v)) => {
                if *kind != pb::MaterializeKind::I64 {
                    return Err(Status::invalid_argument(format!(
                        "materialize: column {name:?} declares {kind:?} but its \
                         expression evaluated int on this document; write \
                         double(...) to land it in the f64 family"
                    )));
                }
                doc.integers.push(pb::IntegerValue {
                    field: name.clone(),
                    value: v,
                });
            }
            Some(IngestVal::Uint(v)) => {
                if *kind != pb::MaterializeKind::U64 {
                    return Err(Status::invalid_argument(format!(
                        "materialize: column {name:?} declares {kind:?} but evaluated uint; align the kind or convert explicitly with double()"
                    )));
                }
                doc.unsigned_integers.push(pb::UnsignedIntegerValue {
                    field: name.clone(),
                    value: v,
                });
            }
        }
    }
    Ok(doc)
}

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
struct DeclarationFile {
    #[serde(default)]
    derived: Vec<DerivedColumnConfig>,
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

#[cfg(test)]
mod tests {
    use super::*;

    fn column(name: &str, kind: pb::MaterializeKind, expression: &str) -> pb::DerivedColumn {
        pb::DerivedColumn {
            name: name.into(),
            expression: expression.into(),
            kind: kind as i32,
            disclosure: pb::DerivedDisclosure::Inputs as i32,
        }
    }

    fn tables<'a>(integers: &'a [String], facets: &'a [String]) -> values::NumericTypes<'a> {
        values::NumericTypes {
            numerics: &[],
            integers,
            unsigned_integers: &[],
            map_numerics: &[],
            facets,
        }
    }

    #[test]
    fn the_fingerprint_follows_the_canonical_encoding_not_the_spelling() {
        let a = pb::DerivedColumns {
            columns: vec![column(
                "y",
                pb::MaterializeKind::I64,
                "calendar.year(decided)",
            )],
        };
        let mut b = a.clone();
        b.columns[0].expression = "calendar.year( decided )".into();
        assert_ne!(fingerprint_of(&a), fingerprint_of(&b));
        let toml = declaration_toml(&a).unwrap();
        let file: DeclarationFile = toml::from_str(&toml).unwrap();
        assert_eq!(spec_from_config(&file.derived).unwrap(), a);
        assert_eq!(
            fingerprint_of(&spec_from_config(&file.derived).unwrap()),
            fingerprint_of(&a)
        );
    }

    #[test]
    fn compile_refuses_by_name() {
        let refused = |spec: pb::DerivedColumns, needle: &str| {
            let err = Declaration::compile(&spec).unwrap_err();
            assert!(err.contains(needle), "{err}");
        };
        refused(
            pb::DerivedColumns {
                columns: vec![column("", pb::MaterializeKind::I64, "year")],
            },
            "non-empty",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![
                    column("y", pb::MaterializeKind::I64, "year"),
                    column("y", pb::MaterializeKind::I64, "year"),
                ],
            },
            "declared twice",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![column("y", pb::MaterializeKind::Unspecified, "year")],
            },
            "no kind",
        );
        let mut no_disclosure = column("y", pb::MaterializeKind::I64, "year");
        no_disclosure.disclosure = 0;
        refused(
            pb::DerivedColumns {
                columns: vec![no_disclosure],
            },
            "no disclosure rule",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![
                    column("a", pb::MaterializeKind::I64, "year / 10"),
                    column("b", pb::MaterializeKind::I64, "a * 10"),
                ],
            },
            "outputs are not inputs",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![column("y", pb::MaterializeKind::I64, "stable_key()")],
            },
            "hash.fnv64(stable_key())",
        );
    }

    #[test]
    fn check_tables_refuses_unknown_inputs_collisions_and_type_mismatches() {
        let integers = vec!["year".to_string(), "decided".to_string()];
        let facets = vec!["court".to_string()];
        let t = tables(&integers, &facets);
        let ok = Declaration::compile(&pb::DerivedColumns {
            columns: vec![
                column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
                column("court_hash", pb::MaterializeKind::U64, "hash.fnv64(court)"),
                column(
                    "scotus",
                    pb::MaterializeKind::I64,
                    "court == \"scotus\" ? 1 : 0",
                ),
                column(
                    "key_bucket",
                    pb::MaterializeKind::U64,
                    "hash.fnv64(stable_key()) % 64u",
                ),
            ],
        })
        .unwrap();
        ok.check_tables(&t).unwrap();
        assert!(ok.reads_stable_key());
        assert_eq!(
            ok.column("scotus").unwrap().inputs,
            vec!["court".to_string()]
        );
        let refused = |spec: pb::DerivedColumns, needle: &str| {
            let err = Declaration::compile(&spec)
                .unwrap()
                .check_tables(&t)
                .unwrap_err();
            assert!(err.contains(needle), "{err}");
        };
        refused(
            pb::DerivedColumns {
                columns: vec![column("year", pb::MaterializeKind::I64, "decided")],
            },
            "collides with a source column",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![column("y", pb::MaterializeKind::I64, "yeer")],
            },
            "does not declare",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![column("y", pb::MaterializeKind::F64, "year")],
            },
            "declares f64 but its expression resolves to int",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![column("y", pb::MaterializeKind::I64, "year > 1990")],
            },
            "wrap the boolean",
        );
        refused(
            pb::DerivedColumns {
                columns: vec![column("y", pb::MaterializeKind::I64, "court")],
            },
            "bare facet read",
        );
    }

    #[test]
    fn apply_computes_stamps_and_refuses_forgeries() {
        let decl = Declaration::compile(&pb::DerivedColumns {
            columns: vec![
                column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
                column("court_hash", pb::MaterializeKind::U64, "hash.fnv64(court)"),
                column(
                    "key_bucket",
                    pb::MaterializeKind::U64,
                    "hash.fnv64(stable_key()) % 64u",
                ),
            ],
        })
        .unwrap();
        let doc = AddDocumentsRequest {
            timestamps: vec![pb::TimestampValue {
                field: "decided".into(),
                value: Some(prost_types::Timestamp {
                    seconds: 1_420_070_400, // 2015-01-01T00:00:00Z
                    nanos: 0,
                }),
            }],
            facets: vec![pb::FacetValue {
                field: "court".into(),
                value: "scotus".into(),
            }],
            ..Default::default()
        };
        let out = decl.apply(doc.clone(), Some(b"case-1")).unwrap();
        assert_eq!(out.derived_fingerprint, decl.fingerprint());
        assert_eq!(
            out.integers,
            vec![pb::IntegerValue {
                field: "y".into(),
                value: 2015
            }]
        );
        assert_eq!(
            out.unsigned_integers,
            vec![
                pb::UnsignedIntegerValue {
                    field: "court_hash".into(),
                    value: values::fnv1a64_bytes(b"scotus"),
                },
                pb::UnsignedIntegerValue {
                    field: "key_bucket".into(),
                    value: values::fnv1a64_bytes(b"case-1") % 64,
                },
            ]
        );
        let err = decl.apply(doc.clone(), None).unwrap_err();
        assert!(
            err.message().contains("stable routing key"),
            "{}",
            err.message()
        );
        let mut forged = doc.clone();
        forged.integers.push(pb::IntegerValue {
            field: "y".into(),
            value: 1999,
        });
        let err = decl.apply(forged, Some(b"k")).unwrap_err();
        assert!(
            err.message().contains("refused as forged"),
            "{}",
            err.message()
        );
        let mut stamped = doc;
        stamped.derived_fingerprint = "abc".into();
        let err = decl.apply(stamped, Some(b"k")).unwrap_err();
        assert!(
            err.message().contains("stamps derived fingerprint"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn rederive_follows_the_row_fingerprint() {
        let decl = Declaration::compile(&pb::DerivedColumns {
            columns: vec![
                column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
                column("m", pb::MaterializeKind::I64, "calendar.month(decided)"),
            ],
        })
        .unwrap();
        let decided = pb::IntegerValue {
            field: "decided".into(),
            value: 1_420_070_400_000_000,
        };
        let y = |value: i64| pb::IntegerValue {
            field: "y".into(),
            value,
        };
        let m = |value: i64| pb::IntegerValue {
            field: "m".into(),
            value,
        };
        // Never derived: every column must be named.
        let fresh = AddDocumentsRequest {
            integers: vec![decided.clone()],
            ..Default::default()
        };
        let err = decl
            .rederive(fresh.clone(), &["y".to_string()], None)
            .unwrap_err();
        assert!(err.message().contains("never derived"), "{}", err.message());
        let out = decl
            .rederive(fresh, &["y".to_string(), "m".to_string()], None)
            .unwrap();
        assert_eq!(out.derived_fingerprint, decl.fingerprint());
        assert_eq!(out.integers, vec![decided.clone(), y(2015), m(1)]);
        // Derived under another declaration: the named column is
        // recomputed in place, the other carried through.
        let older = AddDocumentsRequest {
            integers: vec![decided.clone(), y(1999), m(7)],
            derived_fingerprint: "older".into(),
            ..Default::default()
        };
        let out = decl.rederive(older, &["y".to_string()], None).unwrap();
        assert_eq!(out.derived_fingerprint, decl.fingerprint());
        assert_eq!(out.integers, vec![decided.clone(), m(7), y(2015)]);
        // Already this declaration: nothing to recompute.
        let current = AddDocumentsRequest {
            integers: vec![decided.clone(), y(2015), m(1)],
            derived_fingerprint: decl.fingerprint().to_string(),
            ..Default::default()
        };
        assert_eq!(decl.rederive(current.clone(), &[], None).unwrap(), current);
        let err = decl
            .rederive(current, &["y".to_string()], None)
            .unwrap_err();
        assert!(
            err.message().contains("already derived"),
            "{}",
            err.message()
        );
        let err = decl
            .rederive(AddDocumentsRequest::default(), &["z".to_string()], None)
            .unwrap_err();
        assert!(err.message().contains("not a column"), "{}", err.message());
    }

    /// The civil date the declaration computes for one epoch-micro
    /// input, through the full evaluator.
    fn civil_at(decl: &Declaration, micros: i64) -> (i64, i64, i64) {
        let doc = AddDocumentsRequest {
            integers: vec![pb::IntegerValue {
                field: "decided".into(),
                value: micros,
            }],
            ..Default::default()
        };
        let out = decl.apply(doc, None).unwrap();
        let get = |name: &str| {
            out.integers
                .iter()
                .find(|v| v.field == name)
                .unwrap_or_else(|| panic!("no {name} computed for {micros}"))
                .value
        };
        (get("y"), get("m"), get("d"))
    }

    #[test]
    fn calendar_functions_are_exact_at_the_boundaries() {
        let decl = Declaration::compile(&pb::DerivedColumns {
            columns: vec![
                column("y", pb::MaterializeKind::I64, "calendar.year(decided)"),
                column("m", pb::MaterializeKind::I64, "calendar.month(decided)"),
                column("d", pb::MaterializeKind::I64, "calendar.day(decided)"),
            ],
        })
        .unwrap();
        const DAY: i64 = 86_400_000_000;
        let at = |y: i64, m: u32, d: u32| crate::calendar::days_from_civil(y, m, d) * DAY;
        // The proleptic Gregorian edge: the first day of year 1.
        assert_eq!(civil_at(&decl, at(1, 1, 1)), (1, 1, 1));
        assert_eq!(civil_at(&decl, at(1, 6, 15) + DAY / 2), (1, 6, 15));
        // The epoch exactly, and the microsecond before it.
        assert_eq!(civil_at(&decl, 0), (1970, 1, 1));
        assert_eq!(civil_at(&decl, -1), (1969, 12, 31));
        assert_eq!(civil_at(&decl, at(1969, 12, 31) + DAY - 1), (1969, 12, 31));
        // 2000 is a leap year by the 400-rule; 1900 is not by the
        // 100-rule, so the microsecond after 1900-02-28 is 1900-03-01.
        assert_eq!(civil_at(&decl, at(2000, 2, 29)), (2000, 2, 29));
        assert_eq!(civil_at(&decl, at(2000, 2, 29) + DAY - 1), (2000, 2, 29));
        assert_eq!(civil_at(&decl, at(1900, 2, 28) + DAY - 1), (1900, 2, 28));
        assert_eq!(civil_at(&decl, at(1900, 2, 28) + DAY), (1900, 3, 1));
        // The last microsecond of a UTC day keeps the day.
        assert_eq!(civil_at(&decl, at(2024, 12, 31) + DAY - 1), (2024, 12, 31));
        // The ends of the i64 range are exact proleptic dates, not a
        // wrap into a wrong sign and not a panic: i64::MIN micros is
        // 290309 BCE (year -290308), i64::MAX is 294247 CE.
        assert_eq!(civil_at(&decl, i64::MIN), (-290308, 12, 21));
        assert_eq!(civil_at(&decl, i64::MAX), (294247, 1, 10));
    }

    #[test]
    fn integer_overflow_refuses_the_document_and_boundaries_stay_exact() {
        let int_decl = |expression: &str| {
            Declaration::compile(&pb::DerivedColumns {
                columns: vec![column("edge", pb::MaterializeKind::I64, expression)],
            })
            .unwrap()
        };
        let uint_decl = |expression: &str| {
            Declaration::compile(&pb::DerivedColumns {
                columns: vec![column("edge", pb::MaterializeKind::U64, expression)],
            })
            .unwrap()
        };
        let with_int = |n: i64| AddDocumentsRequest {
            integers: vec![pb::IntegerValue {
                field: "n".into(),
                value: n,
            }],
            ..Default::default()
        };
        let with_uint = |u: u64| AddDocumentsRequest {
            unsigned_integers: vec![pb::UnsignedIntegerValue {
                field: "u".into(),
                value: u,
            }],
            ..Default::default()
        };
        // Correct values at the edges still compute. The input column
        // sits in the same list, so read the derived one by name.
        let edge_int = |out: &AddDocumentsRequest| {
            out.integers
                .iter()
                .find(|v| v.field == "edge")
                .map(|v| v.value)
        };
        let edge_uint = |out: &AddDocumentsRequest| {
            out.unsigned_integers
                .iter()
                .find(|v| v.field == "edge")
                .map(|v| v.value)
        };
        let out = int_decl("n + 1")
            .apply(with_int(i64::MAX - 1), None)
            .unwrap();
        assert_eq!(edge_int(&out), Some(i64::MAX));
        let out = int_decl("n + 1").apply(with_int(i64::MIN), None).unwrap();
        assert_eq!(edge_int(&out), Some(i64::MIN + 1));
        let out = int_decl("n - 1")
            .apply(with_int(i64::MIN + 1), None)
            .unwrap();
        assert_eq!(edge_int(&out), Some(i64::MIN));
        let out = int_decl("-n").apply(with_int(i64::MIN + 1), None).unwrap();
        assert_eq!(edge_int(&out), Some(i64::MAX));
        let out = uint_decl("u % 64u")
            .apply(with_uint(u64::MAX), None)
            .unwrap();
        assert_eq!(edge_uint(&out), Some(63));
        let out = uint_decl("u % 64u").apply(with_uint(0), None).unwrap();
        assert_eq!(edge_uint(&out), Some(0));
        // The impossible states refuse the document, naming the column
        // and the cause; they are not the absence a missing input
        // stores.
        for (expression, n, needle) in [
            ("n + 1", i64::MAX, "overflow"),
            ("n - 1", i64::MIN, "overflow"),
            ("-n", i64::MIN, "overflow"),
            ("n / -1", i64::MIN, "overflow"),
            ("n / 0", 42, "division by zero"),
            ("n % 0", 42, "division by zero"),
        ] {
            let err = int_decl(expression).apply(with_int(n), None).unwrap_err();
            assert!(
                err.message().contains("\"edge\"") && err.message().contains(needle),
                "{expression} at {n}: {}",
                err.message()
            );
        }
        let err = uint_decl("u + 1u")
            .apply(with_uint(u64::MAX), None)
            .unwrap_err();
        assert!(
            err.message().contains("\"edge\"") && err.message().contains("overflow"),
            "{}",
            err.message()
        );
        // A genuinely missing input still stores absence, and the
        // reshard path's rederive refuses an impossible row the same
        // way (the tool's own error adds the row's source id).
        let decl = Declaration::compile(&pb::DerivedColumns {
            columns: vec![
                column("plus_one", pb::MaterializeKind::I64, "n + 1"),
                column(
                    "bucket",
                    pb::MaterializeKind::U64,
                    "hash.fnv64(court) % 64u",
                ),
            ],
        })
        .unwrap();
        let out = decl.apply(AddDocumentsRequest::default(), None).unwrap();
        assert!(out.integers.is_empty() && out.unsigned_integers.is_empty());
        let err = decl
            .rederive(
                with_int(i64::MAX),
                &["plus_one".to_string(), "bucket".to_string()],
                None,
            )
            .unwrap_err();
        assert!(
            err.message().contains("\"plus_one\"") && err.message().contains("overflow"),
            "{}",
            err.message()
        );
    }

    #[test]
    fn fnv64_is_one_function_for_ingest_and_routing() {
        // The published FNV-1a 64 vectors pin the function itself.
        assert_eq!(values::fnv1a64_bytes(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(values::fnv1a64_bytes(b"a"), 0xaf63_dc4c_8601_ec8c);
        assert_eq!(values::fnv1a64_bytes(b"foobar"), 0x8594_4171_f739_67e8);
        // The coordinator's routing hash and the derived-column hash
        // are the same function over the same bytes, binary input
        // included: an ingest-time key bucket and the coordinator's
        // routing cannot drift apart.
        let long = [0xabu8; 1024];
        let inputs: [&[u8]; 4] = [b"", b"case-1", &[0x00, 0xff, 0x7f, 0x80], &long];
        for input in inputs {
            assert_eq!(
                crate::coordinator::stable_routing_hash(input),
                values::fnv1a64_bytes(input),
                "{input:?}"
            );
        }
        // The declaration path stores exactly that function's output.
        let decl = Declaration::compile(&pb::DerivedColumns {
            columns: vec![
                column("court_hash", pb::MaterializeKind::U64, "hash.fnv64(court)"),
                column(
                    "key_bucket",
                    pb::MaterializeKind::U64,
                    "hash.fnv64(stable_key()) % 64u",
                ),
            ],
        })
        .unwrap();
        let doc = AddDocumentsRequest {
            facets: vec![pb::FacetValue {
                field: "court".into(),
                value: "scotus".into(),
            }],
            ..Default::default()
        };
        let out = decl.apply(doc, Some(b"case-1")).unwrap();
        assert_eq!(
            out.unsigned_integers,
            vec![
                pb::UnsignedIntegerValue {
                    field: "court_hash".into(),
                    value: crate::coordinator::stable_routing_hash(b"scotus"),
                },
                pb::UnsignedIntegerValue {
                    field: "key_bucket".into(),
                    value: crate::coordinator::stable_routing_hash(b"case-1") % 64,
                },
            ]
        );
    }
}
