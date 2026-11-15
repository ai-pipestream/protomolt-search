//! Compiling, checking, and applying a derived-column declaration.

use super::*;

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
                    values::ValueLeaf::Column(column)
                    | values::ValueLeaf::Map { column, .. }
                    | values::ValueLeaf::TypedMap { column, .. } => column,
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
                || tables.map_integers.contains(name)
                || tables.map_unsigned_integers.contains(name)
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
                        values::ValueLeaf::Map { column, key }
                        | values::ValueLeaf::TypedMap { column, key } => {
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
    pub(super) fn triples(&self) -> Vec<(String, pb::ValueExpr, pb::MaterializeKind)> {
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
    /// (`--derive`). The row's fingerprint says
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
