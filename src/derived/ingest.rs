//! The per-document value environment and column materialization.

use super::*;

/// The value environment of one document: its numeric families,
/// timestamps as epoch-micro integers under their column names, facet
/// strings, map numerics, and the stable routing key. A name arriving
/// twice in one family, or as both an integer and a timestamp, refuses
/// the document the way the store would.
pub fn ingest_env(
    doc: &AddDocumentsRequest,
    stable_key: Option<&[u8]>,
) -> Result<IngestEnv, DerivedError> {
    let mut env = IngestEnv::default();
    scalar_env(doc, &mut env)?;
    map_env(doc, &mut env)?;
    env.stable_key = stable_key.filter(|key| !key.is_empty()).map(<[u8]>::to_vec);
    Ok(env)
}

fn repeats(family: &str, field: &str) -> DerivedError {
    DerivedError::invalid_argument(format!("{family} {field:?} repeats in one document"))
}

/// Numerics, integers, timestamps (as epoch-micro integers), and
/// unsigned integers.
fn scalar_env(doc: &AddDocumentsRequest, env: &mut IngestEnv) -> Result<(), DerivedError> {
    for nv in &doc.numerics {
        if env.numerics.insert(nv.field.clone(), nv.value).is_some() {
            return Err(repeats("numeric field", &nv.field));
        }
    }
    for iv in &doc.integers {
        if env.integers.insert(iv.field.clone(), iv.value).is_some() {
            return Err(repeats("integer field", &iv.field));
        }
    }
    for tv in &doc.timestamps {
        let Some(instant) = tv.value.as_ref() else {
            return Err(DerivedError::invalid_argument(format!(
                "timestamp field {:?} carries no instant; omit the entry instead",
                tv.field
            )));
        };
        let micros = crate::node::timestamp_to_epoch_micros(&tv.field, instant)
            .map_err(DerivedError::foreign)?;
        if env.integers.insert(tv.field.clone(), micros).is_some() {
            return Err(DerivedError::invalid_argument(format!(
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
            return Err(repeats("unsigned integer field", &uv.field));
        }
    }
    Ok(())
}

/// Map numerics, signed and unsigned integer maps, and facets.
fn map_env(doc: &AddDocumentsRequest, env: &mut IngestEnv) -> Result<(), DerivedError> {
    for entry in &doc.map_numerics {
        env.map_numerics
            .insert((entry.field.clone(), entry.key.clone()), entry.value);
    }
    for entry in &doc.map_integers {
        if env
            .map_integers
            .insert((entry.field.clone(), entry.key.clone()), entry.value)
            .is_some()
        {
            return Err(DerivedError::invalid_argument(format!(
                "signed map {:?}[{:?}] repeats in one document",
                entry.field, entry.key
            )));
        }
    }
    for entry in &doc.map_unsigned_integers {
        if env
            .map_unsigned_integers
            .insert((entry.field.clone(), entry.key.clone()), entry.value)
            .is_some()
        {
            return Err(DerivedError::invalid_argument(format!(
                "unsigned map {:?}[{:?}] repeats in one document",
                entry.field, entry.key
            )));
        }
    }
    for fv in &doc.facets {
        if env
            .facets
            .insert(fv.field.clone(), fv.value.clone())
            .is_some()
        {
            return Err(repeats("facet field", &fv.field));
        }
    }
    Ok(())
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
) -> Result<AddDocumentsRequest, DerivedError> {
    for (name, expr, kind) in columns {
        let value = if strict {
            values::eval_ingest_derived(expr, env)
        } else {
            values::eval_ingest(expr, env)
        }
        .map_err(|e| {
            DerivedError::invalid_argument(format!("materialize: column {name:?}: {}", e.message()))
        })?;
        match value {
            None => {}
            Some(IngestVal::Bool(_)) => {
                return Err(DerivedError::invalid_argument(format!(
                    "materialize: column {name:?} evaluated a boolean; a stored \
                     column holds numbers — wrap the expression in a ternary \
                     (`cond ? 1 : 0`)"
                )));
            }
            Some(IngestVal::Double(v)) => {
                if *kind != pb::MaterializeKind::F64 {
                    return Err(DerivedError::invalid_argument(format!(
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
                    return Err(DerivedError::invalid_argument(format!(
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
                    return Err(DerivedError::invalid_argument(format!(
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
