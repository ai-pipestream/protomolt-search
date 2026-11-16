//! The schema and vector backend state a rewrite must leave unchanged.

use super::*;

#[derive(PartialEq, Eq)]
pub(super) struct Schema {
    tables: SourceTables,
    derived: Option<crate::postings::StoredDerived>,
}

pub(super) fn schema(set: &OpenedSegmentSet) -> Result<Option<Schema>, String> {
    let mut schema = set
        .generation_declaration()
        .map(|declaration| {
            let columns = declaration
                .columns
                .as_ref()
                .expect("validated generation columns");
            Ok::<_, String>(Schema {
                tables: SourceTables {
                    fields: declaration
                        .text_fields
                        .iter()
                        .map(|f| f.name.clone())
                        .collect(),
                    fingerprints: declaration
                        .text_fields
                        .iter()
                        .map(|f| f.analysis_fingerprint.unwrap_or(0))
                        .collect(),
                    position_fields: declaration
                        .text_fields
                        .iter()
                        .filter(|f| f.positions)
                        .map(|f| f.name.clone())
                        .collect(),
                    sentence_fields: declaration
                        .text_fields
                        .iter()
                        .filter(|f| f.sentences)
                        .map(|f| f.name.clone())
                        .collect(),
                    columns: crate::reshard::ColumnTables {
                        facets: columns.facets.clone(),
                        numerics: columns.numerics.clone(),
                        integers: columns.integers.clone(),
                        unsigned_integers: columns.unsigned_integers.clone(),
                        geo: columns.geo.clone(),
                        map_facets: columns.map_facets.clone(),
                        map_numerics: columns.map_numerics.clone(),
                        map_integers: columns.map_integers.clone(),
                        map_unsigned_integers: columns.map_unsigned_integers.clone(),
                    },
                },
                derived: declaration
                    .derived
                    .as_ref()
                    .map(|d| {
                        crate::derived::Declaration::compile(d)
                            .map(|d| crate::postings::StoredDerived::of(&d))
                    })
                    .transpose()?,
            })
        })
        .transpose()?;
    for i in 0..set.len() {
        let reader = set.bm25(i);
        let this = Schema {
            tables: segment_tables(reader),
            derived: reader.derived().cloned(),
        };
        if schema.as_ref().is_some_and(|expected| expected != &this) {
            return Err(err(
                "segment schema or derived declaration differs within a catalog",
            ));
        }
        schema = Some(this);
    }
    Ok(schema)
}

pub(super) fn schema_hash(set: &OpenedSegmentSet, schema: Option<&Schema>) -> [u8; 32] {
    let mut hash = Sha256::new();
    part(&mut hash, b"protomolt.source-rewrite.schema.v1");
    if let Some(binding) = &set.manifest().binding {
        part(&mut hash, &binding.protobuf);
    } else {
        part(&mut hash, &[]);
    }
    hash.update(&[u8::from(schema.is_some())]);
    if let Some(schema) = schema {
        let t = &schema.tables;
        names(&mut hash, &t.fields);
        for value in &t.fingerprints {
            hash.update(&value.to_le_bytes());
        }
        names(&mut hash, &t.position_fields);
        names(&mut hash, &t.sentence_fields);
        let c = &t.columns;
        for names_ in [
            &c.facets,
            &c.numerics,
            &c.integers,
            &c.unsigned_integers,
            &c.geo,
            &c.map_facets,
            &c.map_numerics,
            &c.map_integers,
            &c.map_unsigned_integers,
        ] {
            names(&mut hash, names_);
        }
        hash.update(&[u8::from(schema.derived.is_some())]);
        if let Some(derived) = &schema.derived {
            part(&mut hash, &derived.declaration);
            part(&mut hash, derived.fingerprint.as_bytes());
        }
    }
    hash.finalize()
}

pub(super) fn backend(
    set: &OpenedSegmentSet,
) -> Result<Option<(usize, crate::vector::VectorBackendConfig)>, String> {
    let mut expected = set.generation_declaration().and_then(generation::backend);
    for i in 0..set.len() {
        if let Some(vector) = set.vector(i) {
            let this = (
                vector
                    .dim_opt()
                    .ok_or_else(|| err("vector dimension is absent"))?,
                vector.backend_config().map_err(err)?,
            );
            if expected.as_ref().is_some_and(|expected| expected != &this) {
                return Err(err("vector backend states differ within a catalog"));
            }
            if set.exact_vectors(i).is_none() {
                return Err(err("vector segment has no exact FP32 rows"));
            }
            expected = Some(this);
        } else if set.exact_vectors(i).is_some() {
            return Err(err("orphan exact-vector rows"));
        }
    }
    Ok(expected)
}
