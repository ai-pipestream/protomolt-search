//! Assembly of one engine row from projected slots.

use tonic::Status;

use super::extractor::{ExtractedDoc, Extractor, Land, Leaf, Slot};
use super::refuse_at;
use crate::pb;
use crate::sha256;

impl Extractor {
    /// Build one engine row from the parent slots plus, for chunked
    /// plans, one chunk's slots. Parent values denormalize onto every
    /// chunk row — a filter sees parent and chunk fields together with
    /// no query-time join — and the row's lineage carries the REDUCED
    /// parent id as `parent_id`, the key the engine's parent-collapse
    /// scans already group by.
    pub(super) fn assemble(
        &self,
        parent: &[Option<Slot>],
        chunk: Option<&Vec<Option<Slot>>>,
    ) -> Result<ExtractedDoc, Status> {
        let mut request = pb::AddDocumentsRequest::default();
        let mut vector = Vec::new();
        let mut reduced_id = None;
        for (index, leaf) in self.leaves.iter().enumerate() {
            let from_chunk = self
                .chunked
                .as_ref()
                .is_some_and(|shape| shape.in_chunk[index]);
            let raw = if from_chunk {
                chunk.expect("chunk rows pass their slots")[index].as_ref()
            } else {
                parent[index].as_ref()
            };

            let Some(slot) = raw else {
                self.refuse_absent(index, leaf)?;
                continue;
            };
            if index == self.doc_id {
                reduced_id = Some(reduce_id(&leaf.land, slot, &leaf.path)?);
            }
            self.land_slot(index, leaf, slot.clone(), &mut request, &mut vector);
        }

        if vector.is_empty() {
            return Err(refuse_at(
                &self.leaves[self.vector].path,
                "the document has no vector",
            ));
        }
        if self.plan.dim != 0 && vector.len() != self.plan.dim as usize {
            return Err(refuse_at(
                &self.leaves[self.vector].path,
                format!(
                    "the vector has {} floats; the plan declares dim {}",
                    vector.len(),
                    self.plan.dim
                ),
            ));
        }
        if self.chunked.is_some() {
            let parent_key = reduced_id.expect("an absent doc id refused above");
            request.lineage = Some(pb::DocLineage {
                parent_id: parent_key,
                group_id: 0,
                span_start: 0,
                span_end: 0,
            });
        }
        Ok(ExtractedDoc { request, vector })
    }

    /// Refuses the absence of a value every row carries: body, vector,
    /// document id, or a declared chunk id.
    fn refuse_absent(&self, index: usize, leaf: &Leaf) -> Result<(), Status> {
        if index == self.body {
            return Err(refuse_at(&leaf.path, "the document has no body text"));
        }
        if index == self.vector {
            return Err(refuse_at(&leaf.path, "the document has no vector"));
        }
        if index == self.doc_id {
            return Err(refuse_at(
                &leaf.path,
                "the document has no id; identity is required",
            ));
        }
        let declared_chunk_id = self
            .chunked
            .as_ref()
            .is_some_and(|shape| shape.chunk_id == Some(index));
        if declared_chunk_id {
            return Err(refuse_at(
                &leaf.path,
                "the chunk has no id; the plan declares CHUNK_ID, so every chunk \
                     carries one",
            ));
        }

        Ok(())
    }

    /// Lands one projected value on the request under its column name.
    fn land_slot(
        &self,
        index: usize,
        leaf: &Leaf,
        slot: Slot,
        request: &mut pb::AddDocumentsRequest,
        vector: &mut Vec<f32>,
    ) {
        match slot {
            Slot::MapFacet(entries) => {
                request
                    .map_facets
                    .extend(entries.into_iter().map(|(key, value)| pb::MapFacetEntry {
                        field: leaf.name.clone(),
                        key,
                        value,
                    }))
            }
            Slot::MapNumeric(entries) => {
                request
                    .map_numerics
                    .extend(entries.into_iter().map(|(key, value)| pb::MapNumericEntry {
                        field: leaf.name.clone(),
                        key,
                        value,
                    }))
            }
            Slot::MapInteger(entries) => {
                request
                    .map_integers
                    .extend(entries.into_iter().map(|(key, value)| pb::MapIntegerEntry {
                        field: leaf.name.clone(),
                        key,
                        value,
                    }))
            }
            Slot::MapUnsignedInteger(entries) => {
                request
                    .map_unsigned_integers
                    .extend(
                        entries
                            .into_iter()
                            .map(|(key, value)| pb::MapUnsignedIntegerEntry {
                                field: leaf.name.clone(),
                                key,
                                value,
                            }),
                    )
            }
            Slot::Str(value) => self.land_string(index, leaf, value, request),
            Slot::Int(value) => request.integers.push(pb::IntegerValue {
                field: leaf.name.clone(),
                value,
            }),
            Slot::Uint(value) => request.unsigned_integers.push(pb::UnsignedIntegerValue {
                field: leaf.name.clone(),
                value,
            }),
            Slot::Ts { seconds, nanos } => request.timestamps.push(pb::TimestampValue {
                field: leaf.name.clone(),
                value: Some(prost_types::Timestamp { seconds, nanos }),
            }),
            Slot::Num(value) => request.numerics.push(pb::NumericValue {
                field: leaf.name.clone(),
                value,
            }),

            Slot::Floats(v) => *vector = v,
        }
    }
    /// A string lands as the body, a multi-field text column, or a facet.
    fn land_string(
        &self,
        index: usize,
        leaf: &Leaf,
        value: String,
        request: &mut pb::AddDocumentsRequest,
    ) {
        if matches!(leaf.land.value_land(), Land::Text) {
            if index == self.body {
                request.text = value;
            } else {
                request.fields.push(pb::DocumentField {
                    field: leaf.name.clone(),
                    text: value,
                    analysis: None,
                });
            }
        } else {
            request.facets.push(pb::FacetValue {
                field: leaf.name.clone(),
                value,
            });
        }
    }
}

/// The document-id reduction — part of the contract, so any client can
/// compute the same parent key: an integer id is its 64-bit two's
/// complement pattern verbatim; a string id reduces to the first 8
/// bytes of SHA-256 over its UTF-8 bytes, big-endian. Keyed on the
/// DESCRIPTOR type (a KEYWORD hint on an integer field renders as a
/// facet string but still reduces as the integer it is).
fn reduce_id(land: &Land, slot: &Slot, path: &str) -> Result<u64, Status> {
    if let Land::Wrapper(inner) = land {
        return reduce_id(inner, slot, path);
    }
    match (land, slot) {
        (Land::Int, Slot::Int(value)) => Ok(*value as u64),
        (Land::Uint, Slot::Uint(value)) => Ok(*value),
        (Land::FacetInt | Land::FacetUint, Slot::Str(rendered)) => {
            let parsed = if matches!(land, Land::FacetUint) {
                rendered.parse::<u64>()
            } else {
                rendered.parse::<i64>().map(|value| value as u64)
            };
            parsed.map_err(|_| {
                Status::internal(format!(
                    "plan: doc id {path}: non-decimal own rendering {rendered:?}"
                ))
            })
        }
        (_, Slot::Str(value)) => Ok(u64::from_be_bytes(
            sha256::digest(value.as_bytes())[..8]
                .try_into()
                .expect("32 bytes hold 8"),
        )),
        _ => Err(Status::internal(format!(
            "plan: doc id {path} landed a non-identity slot"
        ))),
    }
}
