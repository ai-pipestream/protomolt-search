//! Streaming document analysis, CEL/NLP materialization, and row mutation commits.

mod commit;
mod materialize;
mod mutations;
mod pipeline;
mod stream_keys;

pub(crate) use self::materialize::{
    apply_materialize, cased_field_index, compile_materialize_spec, join_fields, materialize_sha,
};
pub(in crate::node) use self::materialize::{
    geography_wanted, quality_wanted, CompiledMaterialize,
};
pub(crate) use self::pipeline::session_layers;
pub(in crate::node) use self::pipeline::{IngestDoc, IngestSource, MappedSource};
pub(in crate::node) use self::stream_keys::{replay_first_id, StreamKeys};
