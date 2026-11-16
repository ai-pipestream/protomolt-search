//! Structural resolution over the planned fields: the chunk scope, the
//! vector, the document id, and the chunk id.

use super::MappingError;

use super::{refuse, refuse_at};
use crate::pb;

fn paths(fields: &[&pb::MappedField]) -> String {
    fields
        .iter()
        .map(|f| f.path.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// At most one CHUNKS scope per plan.
pub(super) fn resolve_chunks(fields: &[pb::MappedField]) -> Result<Option<String>, MappingError> {
    let chunks: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.role == pb::MappedRole::Chunks as i32)
        .collect();
    match chunks.len() {
        0 => Ok(None),
        1 => Ok(Some(chunks[0].path.clone())),
        n => Err(refuse(format!(
            "the schema hints {n} CHUNKS fields ({}); a document has at most one chunk scope",
            paths(&chunks)
        ))),
    }
}

/// Pick the plan's vector field. An explicit VECTOR hint wins; without
/// one, exactly one repeated float/double field with a vector-shaped
/// name is accepted and its planned kind is rewritten to VECTOR.
/// Anything else is an error naming the fix.
///
/// When the plan has a CHUNKS scope the vector must live inside it
/// (each chunk is a searchable row). When there is no CHUNKS scope the
/// vector must not pass through one.
pub(super) fn resolve_vector(
    fields: &mut [pb::MappedField],
    chunks_path: Option<&str>,
) -> Result<String, MappingError> {
    let explicit: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.kind == pb::MappedKind::Vector as i32)
        .collect();
    let path = match explicit.len() {
        1 => explicit[0].path.clone(),
        0 => {
            let candidates: Vec<usize> = fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    f.repeated
                        && (f.kind == pb::MappedKind::Float as i32
                            || f.kind == pb::MappedKind::Double as i32)
                        && vector_shaped_name(f.path.rsplit('.').next().unwrap_or(&f.path))
                })
                .map(|(i, _)| i)
                .collect();
            match candidates.len() {
                1 => {
                    let index = candidates[0];
                    fields[index].kind = pb::MappedKind::Vector as i32;
                    fields[index].family = pb::ColumnFamily::Vector as i32;
                    fields[index].path.clone()
                }
                0 => {
                    return Err(refuse(
                        "no vector field: hint exactly one repeated float field with \
                         (ai.protomolt.proto.index.hints.v1.index).type = \
                         INDEX_FIELD_TYPE_VECTOR, or name it vector/embedding",
                    ))
                }
                _ => {
                    let named: Vec<&pb::MappedField> =
                        candidates.iter().map(|&i| &fields[i]).collect();
                    return Err(refuse(format!(
                        "several fields look like the vector ({}); hint the intended one with \
                         (ai.protomolt.proto.index.hints.v1.index).type = \
                         INDEX_FIELD_TYPE_VECTOR",
                        paths(&named)
                    )));
                }
            }
        }
        n => {
            return Err(refuse(format!(
                "the schema hints {n} VECTOR fields ({}); an index is built over exactly one",
                paths(&explicit)
            )))
        }
    };
    if let Some(chunks) = chunks_path {
        if !path.starts_with(&format!("{chunks}.")) {
            return Err(refuse_at(
                &path,
                "the vector field must live inside the CHUNKS scope when the schema \
                 declares one; each chunk is a searchable row",
            ));
        }
    }
    Ok(path)
}

/// Pick the plan's document id field. An explicit DOC_ID role wins;
/// without one, a singular top-level field named "id" is accepted and
/// its planned role is rewritten. The document id always lives on the
/// parent, never inside the CHUNKS scope.
pub(super) fn resolve_doc_id(
    fields: &mut [pb::MappedField],
    chunks_path: Option<&str>,
) -> Result<String, MappingError> {
    let explicit: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.role == pb::MappedRole::DocId as i32)
        .collect();
    let path = match explicit.len() {
        1 => explicit[0].path.clone(),
        0 => {
            let fallback = fields.iter().position(|f| {
                f.path == "id"
                    && !f.repeated
                    && (f.kind == pb::MappedKind::Keyword as i32
                        || f.kind == pb::MappedKind::Text as i32
                        || f.kind == pb::MappedKind::Int32 as i32
                        || f.kind == pb::MappedKind::Int64 as i32
                        || f.kind == pb::MappedKind::Uint32 as i32
                        || f.kind == pb::MappedKind::Uint64 as i32)
            });
            match fallback {
                Some(index) => {
                    fields[index].role = pb::MappedRole::DocId as i32;
                    fields[index].path.clone()
                }
                None => {
                    return Err(refuse(
                        "no document id field: hint exactly one integer or string field with \
                         (ai.protomolt.proto.index.hints.v1.index).block_role = \
                         BLOCK_ROLE_DOC_ID, or declare a singular top-level field named \"id\"",
                    ))
                }
            }
        }
        n => {
            return Err(refuse(format!(
                "the schema hints {n} DOC_ID fields ({}); a document has exactly one identity",
                paths(&explicit)
            )))
        }
    };
    if let Some(chunks) = chunks_path {
        if path == chunks || path.starts_with(&format!("{chunks}.")) {
            return Err(refuse_at(
                &path,
                "the document id field cannot live inside the CHUNKS scope",
            ));
        }
    }
    let target = fields
        .iter()
        .find(|f| f.path == path)
        .expect("path came from this plan");
    if target.kind == pb::MappedKind::Text as i32 {
        return Err(refuse_at(
            &path,
            "a TEXT document id would dissolve into postings; hint it KEYWORD or use an integer id",
        ));
    }
    if target.repeated {
        return Err(refuse_at(&path, "the document id field must be singular"));
    }
    Ok(path)
}

/// Optional CHUNK_ID inside the CHUNKS scope.
pub(super) fn resolve_chunk_id(
    fields: &[pb::MappedField],
    chunks_path: Option<&str>,
) -> Result<Option<String>, MappingError> {
    let ids: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.role == pb::MappedRole::ChunkId as i32)
        .collect();
    match (chunks_path, ids.len()) {
        (_, 0) => Ok(None),
        (None, _) => Err(refuse(format!(
            "CHUNK_ID fields ({}) require a CHUNKS scope",
            paths(&ids)
        ))),
        (Some(chunks), 1) => {
            let path = &ids[0].path;
            if !path.starts_with(&format!("{chunks}.")) {
                return Err(refuse_at(
                    path,
                    "CHUNK_ID must live inside the CHUNKS scope",
                ));
            }
            if ids[0].repeated {
                return Err(refuse_at(path, "CHUNK_ID must be singular"));
            }
            Ok(Some(path.clone()))
        }
        (Some(_), n) => Err(refuse(format!(
            "the schema hints {n} CHUNK_ID fields ({}); a chunk has at most one identity",
            paths(&ids)
        ))),
    }
}

fn vector_shaped_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "vector"
        || name == "embedding"
        || name.ends_with("_vector")
        || name.ends_with("_embedding")
}
