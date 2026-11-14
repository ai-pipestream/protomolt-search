//! Streaming document analysis, CEL/NLP materialization, and row mutation commits.

mod commit;
mod materialize;
mod mutations;
mod pipeline;

pub(crate) use self::materialize::{
    apply_materialize, cased_field_index, compile_materialize_spec, join_fields, materialize_sha,
};
pub(in crate::node) use self::materialize::{
    geography_wanted, quality_wanted, CompiledMaterialize,
};
pub(crate) use self::pipeline::session_layers;
pub(in crate::node) use self::pipeline::{IngestDoc, IngestSource, MappedSource};
