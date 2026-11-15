//! Cross-platform lexical analysis with OpenNLP-compatible term identity.
//!
//! The crate is intentionally independent of gRPC, Tokio, TurboVec, and the
//! search server. It is the embeddable analysis core for native clients and
//! for the in-process search backend.

mod porter;

use std::collections::{HashMap, HashSet};

use aho_corasick::{AhoCorasick, AhoCorasickBuilder, MatchKind};
use icu_casemap::CaseMapper;
use icu_normalizer::{ComposingNormalizerBorrowed, DecomposingNormalizerBorrowed};
use icu_properties::props::{ExtendedPictographic, GeneralCategory, Script};
use icu_properties::{CodePointMapData, CodePointSetData};
use unicode_segmentation::UnicodeSegmentation;

mod analyze;
mod glossary;
mod normalize;
#[cfg(test)]
mod sentence_tests;
#[cfg(test)]
mod tests;
mod tokenize;
mod types;

pub use self::analyze::*;
pub use self::glossary::*;
pub use self::normalize::*;
use self::tokenize::*;
pub use self::types::*;
