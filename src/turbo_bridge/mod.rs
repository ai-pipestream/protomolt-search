//! Unified pure-Rust `turboembed` (`turbo-client` / `turbo-kserve`) integration
//! surface.
//!
//! Encapsulates:
//! - Pre-WAL materialization of one-pass NLP typed columns ([`NlpColumnSpec`],
//!   [`materialize_nlp_columns`]) and single-pass multi-resolution / sentence
//!   vectors ([`MultiResVectorOutput`]).
//! - Quantized `u8`/`u16` SPLADE 128-doc Block-Max postings and L1 rounding
//!   slack pruning ([`splade`]).
//! - One-pass `AnalyzeQuery` structured filter binding, HNSW visit-budget
//!   selection, `MappedSurfaceLexicon` stem-to-surface completion, and
//!   `QuerySpellchecker` contextual spellcheck ([`query_plan`]).
//! - Stage-2 calibrated `RerankAndHighlight` over top-`C` candidates with
//!   `DocLineage.span_start` chunk-to-parent offset composition, plus mTLS,
//!   bearer auth, priority lanes, and deadline propagation ([`rerank`]).
//! - Batch embedding behind one [`BatchEmbedder`] seam for remote endpoints
//!   and in-process static or transformer bundles ([`embed`]).

mod call_policy;
pub mod embed;
mod materialize;
mod multi_res;
mod nlp_columns;
pub mod query_plan;
pub mod rerank;
pub mod splade;
pub mod tensor;

pub use call_policy::{remaining_deadline_duration, KserveCallPolicy, MtlsClientMaterial};
#[cfg(feature = "local-core")]
pub use embed::LocalBatchEmbedder;
pub use embed::{BatchEmbedder, EmbedOptions, EmbedRole, RemoteBatchEmbedder};
pub use materialize::materialize_nlp_columns;
pub use multi_res::{MatryoshkaBatchOutput, MultiResVectorOutput};
pub use nlp_columns::{validate_cel_identifier, NlpColumnSpec, MICROS_PER_SECOND};
