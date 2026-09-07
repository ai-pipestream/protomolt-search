//! Generation declarations outlive row files, including the final segment.
use super::*;
use crate::pb::storage::{
    GenerationColumns, GenerationTextField, GenerationVectorState, IndexGenerationDeclaration,
};
use crate::postings::{Bm25Store, StoredDerived};
use crate::vector::VectorBackendConfig;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SegmentGenerationDeclaration {
    pub protobuf: Vec<u8>,
    pub sha256: String,
}
impl SegmentGenerationDeclaration {
    pub fn encode(declaration: &IndexGenerationDeclaration) -> Result<Self, String> {
        let protobuf = declaration.encode_to_vec();
        let encoded = Self {
            sha256: crate::sha256::hex_digest(&protobuf),
            protobuf,
        };
        encoded.decode()?;
        Ok(encoded)
    }
    pub fn decode(&self) -> Result<IndexGenerationDeclaration, String> {
        let declaration = IndexGenerationDeclaration::decode(self.protobuf.as_slice())
            .map_err(|error| format!("generation declaration: {error}"))?;
        if self.sha256 != crate::sha256::hex_digest(&self.protobuf)
            || declaration.encode_to_vec() != self.protobuf
        {
            return Err("generation declaration is noncanonical or its checksum differs".into());
        }
        validate(&declaration)?;
        Ok(declaration)
    }
}
fn validate_names<'a>(names: impl Iterator<Item = &'a str>) -> Result<(), String> {
    let mut seen = BTreeSet::new();
    for name in names {
        if name.is_empty() || !seen.insert(name) {
            return Err(format!(
                "generation declaration has an empty or duplicate name {name:?}"
            ));
        }
    }
    Ok(())
}
fn validate(declaration: &IndexGenerationDeclaration) -> Result<(), String> {
    if declaration.format_version != 1 {
        return Err("unsupported generation declaration format".into());
    }
    validate_names(
        declaration
            .text_fields
            .iter()
            .map(|field| field.name.as_str()),
    )?;
    if declaration
        .text_fields
        .iter()
        .any(|field| field.analysis_fingerprint == Some(0))
    {
        return Err(
            "generation analyzer fingerprint zero is unset, not an established fingerprint".into(),
        );
    }
    let columns = declaration
        .columns
        .as_ref()
        .ok_or("generation declaration is missing its column tables")?;
    for table in [
        &columns.facets,
        &columns.numerics,
        &columns.integers,
        &columns.unsigned_integers,
        &columns.geo,
        &columns.map_facets,
        &columns.map_numerics,
        &columns.map_integers,
        &columns.map_unsigned_integers,
    ] {
        validate_names(table.iter().map(String::as_str))?;
    }
    if let Some(derived) = &declaration.derived {
        if derived.columns.is_empty() {
            return Err("generation derived declaration has no columns".into());
        }
        let compiled = crate::derived::Declaration::compile(derived)?;
        if compiled.canonical() != derived.encode_to_vec() {
            return Err("generation derived declaration is noncanonical".into());
        }
    }
    if let Some(vector) = &declaration.vector {
        let config = vector
            .config
            .as_ref()
            .ok_or("generation vector state has no backend configuration")?;
        if vector.dimension == 0
            || config.backend_kind.is_empty()
            || config.config_format.is_empty()
        {
            return Err(
                "generation vector state has an invalid dimension or backend identity".into(),
            );
        }
    }
    Ok(())
}

pub(crate) fn backend(
    declaration: &IndexGenerationDeclaration,
) -> Option<(usize, VectorBackendConfig)> {
    declaration.vector.as_ref().map(|vector| {
        let config = vector
            .config
            .as_ref()
            .expect("validated generation vector state");
        (
            vector.dimension as usize,
            VectorBackendConfig {
                backend_kind: config.backend_kind.clone(),
                config_format: config.config_format.clone(),
                payload: config.payload.clone(),
            },
        )
    })
}
pub(crate) fn vector_state(
    value: Option<(usize, VectorBackendConfig)>,
) -> Result<Option<GenerationVectorState>, String> {
    value
        .map(|(dimension, config)| {
            Ok(GenerationVectorState {
                dimension: u32::try_from(dimension)
                    .map_err(|_| "generation vector dimension exceeds u32")?,
                config: Some(crate::pb::VectorBackendConfig {
                    backend_kind: config.backend_kind,
                    config_format: config.config_format,
                    payload: config.payload,
                }),
            })
        })
        .transpose()
}

macro_rules! describe {
    ($store:expr, $backend:expr) => {{
        let store = $store;
        Ok(IndexGenerationDeclaration {
            format_version: 1,
            text_fields: (0..store.field_count())
                .map(|f| GenerationTextField {
                    name: store.field_name(f).to_string(),
                    analysis_fingerprint: match store.analysis_fingerprint(f) {
                        0 => None,
                        value => Some(value),
                    },
                    positions: store.field_has_positions(f),
                    sentences: store.field_has_sentences(f),
                })
                .collect(),
            columns: Some(GenerationColumns {
                facets: (0..store.facet_count())
                    .map(|i| store.facet_name(i).to_string())
                    .collect(),
                numerics: (0..store.numeric_count())
                    .map(|i| store.numeric_name(i).to_string())
                    .collect(),
                integers: (0..store.integer_count())
                    .map(|i| store.integer_name(i).to_string())
                    .collect(),
                unsigned_integers: (0..store.unsigned_integer_count())
                    .map(|i| store.unsigned_integer_name(i).to_string())
                    .collect(),
                geo: (0..store.geo_count())
                    .map(|i| store.geo_name(i).to_string())
                    .collect(),
                map_facets: (0..store.map_facet_count())
                    .map(|i| store.map_facet_name(i).to_string())
                    .collect(),
                map_numerics: (0..store.map_numeric_count())
                    .map(|i| store.map_numeric_name(i).to_string())
                    .collect(),
                map_integers: (0..store.map_integer_count())
                    .map(|i| store.map_integer_name(i).to_string())
                    .collect(),
                map_unsigned_integers: (0..store.map_unsigned_integer_count())
                    .map(|i| store.map_unsigned_integer_name(i).to_string())
                    .collect(),
            }),
            derived: store.derived().map(StoredDerived::validate).transpose()?,
            vector: vector_state($backend)?,
        })
    }};
}
pub(crate) fn from_store(
    store: &Bm25Store,
    backend: Option<(usize, VectorBackendConfig)>,
) -> Result<IndexGenerationDeclaration, String> {
    describe!(store, backend)
}
pub(crate) fn from_reader(
    store: &Bm25Reader,
    backend: Option<(usize, VectorBackendConfig)>,
) -> Result<IndexGenerationDeclaration, String> {
    describe!(store, backend)
}

/// Source writes may establish previously unset semantics. Rewrites must use
/// strict equality instead: they are never an opportunity to rebind a schema.
pub(crate) fn check_upgrade(
    before: &IndexGenerationDeclaration,
    after: &IndexGenerationDeclaration,
) -> Result<(), String> {
    validate(before)?;
    validate(after)?;
    if before.columns != after.columns
        || before.derived != after.derived
        || before.text_fields.len() != after.text_fields.len()
        || before
            .text_fields
            .iter()
            .zip(&after.text_fields)
            .any(|(a, b)| {
                a.name != b.name
                    || a.positions != b.positions
                    || a.sentences != b.sentences
                    || a.analysis_fingerprint
                        .is_some_and(|fp| b.analysis_fingerprint != Some(fp))
            })
        || before
            .vector
            .as_ref()
            .is_some_and(|vector| after.vector.as_ref() != Some(vector))
    {
        return Err("source publication changes an established generation declaration".into());
    }
    Ok(())
}
pub(crate) fn check_reader(
    declaration: &IndexGenerationDeclaration,
    reader: &Bm25Reader,
    vector: Option<&VectorIndex>,
) -> Result<(), String> {
    let actual = from_reader(
        reader,
        vector
            .map(|vector| {
                Ok::<_, String>((
                    vector
                        .dim_opt()
                        .ok_or("segment vector dimension is absent")?,
                    vector.backend_config().map_err(|e| e.to_string())?,
                ))
            })
            .transpose()?,
    )?;
    if actual.text_fields != declaration.text_fields
        || actual.columns != declaration.columns
        || actual.derived != declaration.derived
        || actual
            .vector
            .as_ref()
            .is_some_and(|v| declaration.vector.as_ref() != Some(v))
    {
        return Err("segment differs from its generation declaration".into());
    }
    Ok(())
}
pub(crate) fn restore_tail(
    declaration: &IndexGenerationDeclaration,
    tail: &mut Bm25Store,
) -> Result<(), String> {
    let actual = from_store(tail, None)?;
    let mut expected = declaration.clone();
    expected.vector = None;
    check_upgrade(&actual, &expected).map_err(|_| {
        "tail configuration differs from the persisted generation declaration".to_string()
    })?;
    for (i, field) in expected.text_fields.iter().enumerate() {
        if let Some(fp) = field.analysis_fingerprint {
            if tail.analysis_fingerprint(i) == 0 && tail.doc_count() != 0 {
                return Err(
                    "a populated tail cannot adopt missing generation analysis metadata".into(),
                );
            }
            tail.set_analysis_fingerprint(i, fp)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn declaration() -> IndexGenerationDeclaration {
        from_store(
            &Bm25Store::with_fields(&["body"]).with_unsigned_integers(&["empty"]),
            None,
        )
        .unwrap()
    }

    #[test]
    fn declarations_require_canonical_complete_metadata() {
        let value = declaration();
        let encoded = SegmentGenerationDeclaration::encode(&value).unwrap();
        assert_eq!(encoded.decode().unwrap(), value);
        let mut corrupt = encoded.clone();
        corrupt.sha256 = "00".repeat(32);
        assert!(corrupt.decode().is_err());
        let mut unknown = encoded.clone();
        unknown.protobuf.extend_from_slice(&[0x78, 1]);
        unknown.sha256 = crate::sha256::hex_digest(&unknown.protobuf);
        assert!(unknown.decode().is_err());
        for change in 0..8 {
            let mut bad = value.clone();
            match change {
                0 => bad.format_version = 2,
                1 => bad.columns = None,
                2 => bad.text_fields.push(bad.text_fields[0].clone()),
                3 => bad
                    .columns
                    .as_mut()
                    .unwrap()
                    .unsigned_integers
                    .push("empty".into()),
                4 => bad.text_fields[0].analysis_fingerprint = Some(0),
                5 => {
                    bad.vector = Some(GenerationVectorState {
                        dimension: 8,
                        config: None,
                    })
                }
                6 => bad.text_fields[0].name.clear(),
                7 => bad.derived = Some(crate::pb::DerivedColumns::default()),
                _ => unreachable!(),
            }
            assert!(
                SegmentGenerationDeclaration::encode(&bad).is_err(),
                "case {change}"
            );
        }
    }

    #[test]
    fn first_source_establishes_semantics_but_later_sources_cannot_rebind() {
        let initial = declaration();
        let mut established = initial.clone();
        established.text_fields[0].analysis_fingerprint = Some(17);
        established.vector = Some(GenerationVectorState {
            dimension: 8,
            config: Some(crate::pb::VectorBackendConfig {
                backend_kind: "provider".into(),
                config_format: "v1".into(),
                payload: vec![1],
            }),
        });
        check_upgrade(&initial, &established).unwrap();
        check_upgrade(&established, &established).unwrap();
        assert!(check_upgrade(&established, &initial).is_err());
        for change in 0..5 {
            let mut changed = established.clone();
            match change {
                0 => changed.text_fields[0].analysis_fingerprint = Some(18),
                1 => changed
                    .vector
                    .as_mut()
                    .unwrap()
                    .config
                    .as_mut()
                    .unwrap()
                    .payload
                    .push(2),
                2 => changed.columns.as_mut().unwrap().unsigned_integers.clear(),
                3 => changed.text_fields[0].positions = true,
                4 => changed.vector.as_mut().unwrap().dimension = 16,
                _ => unreachable!(),
            }
            assert!(
                check_upgrade(&established, &changed).is_err(),
                "case {change}"
            );
        }
    }

    #[test]
    fn empty_tail_restores_analysis_without_losing_empty_columns() {
        let mut persisted = declaration();
        persisted.text_fields[0].analysis_fingerprint = Some(17);
        let mut tail = Bm25Store::with_fields(&["body"]).with_unsigned_integers(&["empty"]);
        restore_tail(&persisted, &mut tail).unwrap();
        assert_eq!(from_store(&tail, None).unwrap(), persisted);
        let mut wrong = Bm25Store::with_fields(&["body"]);
        assert!(restore_tail(&persisted, &mut wrong).is_err());
        let mut changed = persisted.clone();
        changed.text_fields[0].analysis_fingerprint = Some(18);
        assert!(restore_tail(&changed, &mut tail).is_err());
    }
}
