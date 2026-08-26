//! Demo and pipeline support for SPECIFIC corpora — deliberately
//! quarantined from the engine.
//!
//! Nothing under this module is part of the serving path, the wire
//! contract, or the ranking logic, and nothing in the engine references
//! it; the consumers are the `examples/` binaries (the court pipeline,
//! the Lucene-era shakedown corpus). The engine itself is
//! schema-agnostic: bring your own `FileDescriptorSet`
//! (`docs/descriptor-mappings.md`) or drive the generic ingest RPCs
//! directly. If your corpus is not one of these, this module has
//! nothing for you — and that is the point.

pub mod court;
pub mod dataset;
