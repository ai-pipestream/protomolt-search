//! Resolve and pin the explicit analysis contract before a mapped stream mutates a shard.
use std::collections::BTreeMap;

use prost::Message;
use tonic::Status;

use crate::pb::{AnalysisSpec, ColumnFamily, MappedBind};

pub(crate) struct MappedAnalysis {
    pub body: Option<AnalysisSpec>,
    pub fields: BTreeMap<String, AnalysisSpec>,
    pub digest: String,
    pub contract: Vec<u8>,
}

impl MappedAnalysis {
    pub fn resolve(
        bind: &MappedBind,
        extractor: &crate::mapping::Extractor,
    ) -> Result<Self, Status> {
        if bind.field_analysis.is_empty() {
            return Ok(Self {
                body: bind.analysis.clone(),
                fields: BTreeMap::new(),
                digest: String::new(),
                contract: Vec::new(),
            });
        }
        if bind.analysis.is_some() {
            return Err(Status::invalid_argument(
                "field_analysis and legacy body analysis are mutually exclusive",
            ));
        }
        let text_fields: BTreeMap<_, _> = extractor
            .plan()
            .fields
            .iter()
            .filter(|field| field.family == ColumnFamily::TextField as i32)
            .map(|field| (field.path.as_str(), field.name.as_str()))
            .collect();
        let mut specs = BTreeMap::new();
        for field in &bind.field_analysis {
            if !text_fields.contains_key(field.path.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field_analysis path {:?} is not a projected TEXT path",
                    field.path
                )));
            }
            let spec = field.analysis.as_ref().ok_or_else(|| {
                Status::invalid_argument(format!(
                    "field_analysis path {:?} requires an AnalysisSpec",
                    field.path
                ))
            })?;
            validate_spec(spec)?;
            if specs.insert(field.path.as_str(), spec).is_some() {
                return Err(Status::invalid_argument(format!(
                    "duplicate field_analysis path {:?}",
                    field.path
                )));
            }
        }
        for path in text_fields.keys() {
            if !specs.contains_key(path) {
                return Err(Status::invalid_argument(format!(
                    "field_analysis is missing projected TEXT path {path:?}"
                )));
            }
        }
        let mut contract = crate::pb::MappedAnalysisContract::default();
        let mut body = None;
        let mut fields = BTreeMap::new();
        for (path, spec) in specs {
            contract.fields.push(crate::pb::MappedAnalysisColumn {
                path: path.to_owned(),
                name: if path == extractor.body_path() {
                    "body".into()
                } else {
                    text_fields[path].to_owned()
                },
                analysis: Some(spec.clone()),
            });
            if path == extractor.body_path() {
                body = Some(spec.clone());
            } else {
                fields.insert(text_fields[path].to_owned(), spec.clone());
            }
        }
        Ok(Self {
            body,
            fields,
            digest: contract_digest(&contract.encode_to_vec()),
            contract: contract.encode_to_vec(),
        })
    }

    pub fn validate_native(&self) -> Result<(), Status> {
        // Legacy mode keeps its existing body-only/default behavior. Explicit mode
        // checks every declared field, including those absent from all documents.
        if !self.digest.is_empty() {
            for spec in self.body.iter().chain(self.fields.values()) {
                crate::analyzer::validate_native_spec(spec)?;
            }
        }
        Ok(())
    }
}

fn validate_spec(spec: &AnalysisSpec) -> Result<(), Status> {
    use crate::pb::analysis::{analysis_options, term_vector_options};
    if spec.tokenizer == 0
        || spec.stemmer == 0
        || spec.term_vector_mode == 0
        || spec.term_vector_source == 0
        || spec.char_filters.contains(&0)
        || (spec.char_filters.is_empty() && spec.term_vector_source != 2)
    {
        return Err(Status::invalid_argument("field_analysis requires explicit tokenizer, stemmer, mode, source and normalizer steps; server defaults are not a pinned contract"));
    }
    if matches!(spec.term_vector_source, 2 | 3) && spec.stemmer == 1 {
        return Err(Status::invalid_argument(
            "field_analysis STEMS sources require a stemmer",
        ));
    }
    if analysis_options::Tokenizer::try_from(spec.tokenizer).is_err()
        || analysis_options::Stemmer::try_from(spec.stemmer).is_err()
        || term_vector_options::Mode::try_from(spec.term_vector_mode).is_err()
        || term_vector_options::Source::try_from(spec.term_vector_source).is_err()
        || spec
            .char_filters
            .iter()
            .any(|step| term_vector_options::NormalizerStep::try_from(*step).is_err())
    {
        return Err(Status::invalid_argument(
            "field_analysis contains an unknown AnalysisSpec enum value",
        ));
    }
    Ok(())
}

pub(crate) fn valid_digest(digest: &str) -> bool {
    digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::analyzer::{body_spec, cased_body_spec};
    use crate::pb::MappedFieldAnalysis;

    fn bind() -> MappedBind {
        MappedBind {
            descriptor_set: include_bytes!("../tests/fixtures/wrapper-mapping/descriptor.bin")
                .to_vec(),
            message_type: "wrapper_fixture.Record".into(),
            body_path: "body".into(),
            field_analysis: vec![
                MappedFieldAnalysis {
                    path: "body".into(),
                    analysis: Some(body_spec()),
                },
                MappedFieldAnalysis {
                    path: "nested.caption".into(),
                    analysis: Some(cased_body_spec()),
                },
            ],
            ..Default::default()
        }
    }
    fn resolve(bind: &MappedBind) -> Result<MappedAnalysis, Status> {
        let extractor = crate::mapping::Extractor::new(
            &bind.descriptor_set,
            &bind.message_type,
            &bind.body_path,
        )?;
        MappedAnalysis::resolve(bind, &extractor)
    }
    #[test]
    fn all_text_paths_resolve_and_hash_independently_of_list_order() {
        let mut bind = bind();
        let resolved = resolve(&bind).unwrap();
        resolved.validate_native().unwrap();
        assert_eq!(resolved.body, Some(body_spec()));
        assert_eq!(resolved.fields["nested_caption"], cased_body_spec());
        assert!(valid_digest(&resolved.digest));
        bind.field_analysis.reverse();
        assert_eq!(resolve(&bind).unwrap().digest, resolved.digest);
        bind.field_analysis[0].analysis = Some(body_spec());
        assert_ne!(resolve(&bind).unwrap().digest, resolved.digest);
        let ordered = resolve(&bind).unwrap().digest;
        bind.field_analysis[0]
            .analysis
            .as_mut()
            .unwrap()
            .char_filters
            .reverse();
        assert_ne!(resolve(&bind).unwrap().digest, ordered);
        // A different body selection keeps path-based settings, independent of names.
        bind.body_path = "nested.caption".into();
        let swapped = resolve(&bind).unwrap();
        assert_eq!(swapped.body, bind.field_analysis[0].analysis);
        assert_eq!(swapped.fields["body"], body_spec());
    }
    #[test]
    fn incomplete_ambiguous_and_default_contracts_refuse() {
        for change in 0..11 {
            let mut bind = bind();
            match change {
                0 => {
                    bind.field_analysis.pop();
                }
                1 => bind.field_analysis.push(bind.field_analysis[0].clone()),
                2 => bind.field_analysis[1].path = "nested_caption".into(),
                3 => bind.field_analysis[1].path = "unsigned".into(),
                4 => bind.field_analysis[1].path = "payload".into(),
                5 => bind.field_analysis[1].analysis = None,
                6 => bind.analysis = Some(body_spec()),
                7 => bind.field_analysis[1].analysis = Some(AnalysisSpec::default()),
                8 => bind.field_analysis[1].analysis.as_mut().unwrap().tokenizer = 900,
                9 => bind.field_analysis[0]
                    .analysis
                    .as_mut()
                    .unwrap()
                    .char_filters
                    .clear(),
                10 => bind.field_analysis[1].analysis.as_mut().unwrap().stemmer = 1,
                _ => unreachable!(),
            }
            assert!(resolve(&bind).is_err(), "case {change}");
        }
        let mut legacy = bind();
        legacy.field_analysis.clear();
        legacy.analysis = Some(body_spec());
        let resolved = resolve(&legacy).unwrap();
        assert_eq!(resolved.body, legacy.analysis);
        assert!(resolved.digest.is_empty());
        assert!(resolved.fields.is_empty());
    }
    #[test]
    fn contract_decode_checks_identity_shape_and_canonical_encoding() {
        let resolved = resolve(&bind()).unwrap();
        decode_contract(&resolved.digest, &resolved.contract, "body").unwrap();
        assert!(decode_contract(&resolved.digest, &resolved.contract, "another").is_err());
        for change in 0..6 {
            let mut contract =
                crate::pb::MappedAnalysisContract::decode(resolved.contract.as_slice()).unwrap();
            match change {
                0 => contract.fields.reverse(),
                1 => contract.fields[1].name = "body".into(),
                2 => contract.fields[0].analysis = None,
                3 => contract.fields[0].analysis.as_mut().unwrap().tokenizer = 0,
                4 => contract.fields[1].path = "body".into(),
                5 => {
                    contract.fields.clear();
                }
                _ => unreachable!(),
            }
            let bytes = contract.encode_to_vec();
            assert!(
                decode_contract(&contract_digest(&bytes), &bytes, "body").is_err(),
                "case {change}"
            );
        }
        let mut bytes = resolved.contract.clone();
        bytes.extend([0x98, 0x06, 1]);
        assert!(decode_contract(&contract_digest(&bytes), &bytes, "body").is_err());
        assert!(decode_contract(&resolved.digest, &bytes, "body").is_err());
    }

    #[test]
    fn unsupported_native_field_refuses_even_without_documents() {
        let mut bind = bind();
        bind.field_analysis[1].analysis.as_mut().unwrap().tokenizer = 2;
        let resolved = resolve(&bind).unwrap(); // Valid sidecar SIMPLE tokenizer.
        assert!(resolved.validate_native().is_err());
    }
}

fn contract_digest(bytes: &[u8]) -> String {
    let mut hasher = crate::sha256::Sha256::new();
    hasher.update(b"protomolt.search.mapped-analysis.v1\0");
    hasher.update(bytes);
    crate::sha256::to_hex(&hasher.finalize())
}

pub(crate) fn decode_contract(
    digest: &str,
    bytes: &[u8],
    body_path: &str,
) -> Result<crate::pb::MappedAnalysisContract, String> {
    if digest.is_empty() && bytes.is_empty() {
        return Ok(crate::pb::MappedAnalysisContract::default());
    }
    if !valid_digest(digest) || digest != contract_digest(bytes) {
        return Err("invalid mapped analysis digest or contract".into());
    }
    let contract = crate::pb::MappedAnalysisContract::decode(bytes)
        .map_err(|e| format!("invalid mapped analysis contract: {e}"))?;
    if contract.encode_to_vec() != bytes {
        return Err("noncanonical mapped analysis contract".into());
    }
    let mut names = std::collections::BTreeSet::new();
    let mut previous = None;
    let mut body = false;
    for field in &contract.fields {
        if field.path.is_empty()
            || field.name.is_empty()
            || previous.is_some_and(|p| p >= field.path.as_str())
            || !names.insert(&field.name)
        {
            return Err(
                "mapped analysis paths must be sorted and unique, and column names unique".into(),
            );
        }
        previous = Some(field.path.as_str());
        if field.name == "body" {
            body = field.path == body_path;
        } else if field.path == body_path {
            return Err("mapped analysis body path names another column".into());
        }
        validate_spec(
            field
                .analysis
                .as_ref()
                .ok_or("mapped analysis column lacks a spec")?,
        )
        .map_err(|e| e.to_string())?;
    }
    if !body {
        return Err("mapped analysis contract lacks its bound body".into());
    }
    Ok(contract)
}
