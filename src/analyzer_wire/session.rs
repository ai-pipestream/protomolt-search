//! Session options carried in `InputBinding` on analysis streams.

use tonic::Status;

use super::codec::{put_i32, put_u32, Reader};
#[cfg(feature = "net")]
use super::SESSION_BINDING_ID;
use crate::analyzer::SessionLayers;
use crate::pb::AnalysisSpec;

/// Session-level lexical specification and optional layer flags carried in
/// `InputBinding` on `AnalyzeStream` and `AnalyzeDocumentProgressive`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct WireSessionOptions {
    pub(crate) spec: Option<AnalysisSpec>,
    pub(crate) layers: SessionLayers,
}

impl WireSessionOptions {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(32);
        out.push(1u8);
        let mut layer_bits = 0u8;
        if self.layers.quality {
            layer_bits |= 1 << 0;
        }
        if self.layers.geography {
            layer_bits |= 1 << 1;
        }
        if self.layers.entities {
            layer_bits |= 1 << 2;
        }
        if self.layers.sentences {
            layer_bits |= 1 << 3;
        }
        if self.layers.dual_cased {
            layer_bits |= 1 << 4;
        }
        if self.layers.nlp_columns {
            layer_bits |= 1 << 5;
        }
        out.push(layer_bits);
        match &self.spec {
            None => out.push(0u8),
            Some(s) => {
                out.push(1u8);
                put_i32(&mut out, s.tokenizer);
                put_i32(&mut out, s.stemmer);
                put_i32(&mut out, s.term_vector_mode);
                put_i32(&mut out, s.term_vector_source);
                put_u32(&mut out, s.char_filters.len() as u32);
                for &f in &s.char_filters {
                    put_i32(&mut out, f);
                }
            }
        }
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Status> {
        let mut r = Reader::new(bytes);
        let ver = r.u8()?;
        if ver != 1 {
            return Err(Status::invalid_argument(format!(
                "unsupported wire session options version {ver}"
            )));
        }
        let layer_bits = r.u8()?;
        let layers = SessionLayers {
            quality: (layer_bits & (1 << 0)) != 0,
            geography: (layer_bits & (1 << 1)) != 0,
            entities: (layer_bits & (1 << 2)) != 0,
            sentences: (layer_bits & (1 << 3)) != 0,
            dual_cased: (layer_bits & (1 << 4)) != 0,
            nlp_columns: (layer_bits & (1 << 5)) != 0,
        };
        let has_spec = r.u8()?;
        let spec = if has_spec == 0 {
            None
        } else {
            let tokenizer = r.i32()?;
            let stemmer = r.i32()?;
            let term_vector_mode = r.i32()?;
            let term_vector_source = r.i32()?;
            let n = r.u32()? as usize;
            if n > 1024 {
                return Err(Status::invalid_argument("excessive char_filters length"));
            }
            let mut char_filters = Vec::with_capacity(n);
            for _ in 0..n {
                char_filters.push(r.i32()?);
            }
            Some(AnalysisSpec {
                tokenizer,
                stemmer,
                term_vector_mode,
                term_vector_source,
                char_filters,
            })
        };
        Ok(Self { spec, layers })
    }

    #[cfg(feature = "net")]
    pub(crate) fn to_binding(&self) -> turbo_client::proto::result::InputBinding {
        turbo_client::proto::result::InputBinding {
            source_revision: String::new(),
            parse_artifact_id: SESSION_BINDING_ID.to_string(),
            parse_artifact_digest: self.encode(),
        }
    }

    #[cfg(feature = "net")]
    pub(crate) fn from_binding(
        binding: Option<&turbo_client::proto::result::InputBinding>,
    ) -> Result<Self, Status> {
        let Some(b) = binding else {
            return Ok(Self::default());
        };
        if b.parse_artifact_id != SESSION_BINDING_ID || b.parse_artifact_digest.is_empty() {
            return Ok(Self::default());
        }
        Self::decode(&b.parse_artifact_digest)
    }
}
