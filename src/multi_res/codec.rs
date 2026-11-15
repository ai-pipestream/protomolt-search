//! The bounded protobuf codec for one retained row: admission validation
//! of an owned row, allocation-free inspection of an encoded one, and the
//! materialization between the two.

use std::io;

use prost::encoding::{
    decode_key, decode_varint, encoded_len_varint, skip_field, DecodeContext, WireType,
};
use prost::Message;

use super::{invalid, Limits, RetainedVector, LIMITS};
use crate::sha256::Sha256;
use crate::turbo_bridge::MultiResVectorOutput;

#[derive(Clone, PartialEq, Message)]
pub(super) struct Tier {
    #[prost(uint32, tag = "1")]
    pub(super) dim: u32,
    #[prost(float, repeated, packed = "true", tag = "2")]
    pub(super) values: Vec<f32>,
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct Embedding {
    #[prost(float, repeated, packed = "true", tag = "1")]
    pub(super) full: Vec<f32>,
    #[prost(message, repeated, tag = "2")]
    pub(super) prefixes: Vec<Tier>,
    #[prost(float, optional, tag = "3")]
    pub(super) inv_norm: Option<f32>,
    #[prost(bytes = "vec", optional, tag = "4")]
    pub(super) quantized_codes: Option<Vec<u8>>,
    #[prost(message, repeated, tag = "5")]
    pub(super) sentences: Vec<Tier>,
    #[prost(bytes = "vec", tag = "6")]
    pub(super) sha256: Vec<u8>,
    // Tag 7 is reserved; replay carries materialized tiers, never raw inputs.
}

#[derive(Clone, PartialEq, Message)]
pub(super) struct Row {
    #[prost(uint32, tag = "1")]
    pub(super) slot: u32,
    #[prost(message, optional, tag = "2")]
    pub(super) embedding: Option<Embedding>,
}

pub(super) fn embedding(retained: &RetainedVector) -> Embedding {
    let row = &retained.output;
    Embedding {
        full: row.full.clone(),
        prefixes: row
            .prefixes
            .iter()
            .map(|(dim, values)| Tier {
                dim: *dim,
                values: values.clone(),
            })
            .collect(),
        inv_norm: Some(row.inv_norm),
        quantized_codes: row.quantized_codes.clone(),
        sentences: row
            .sentence_vectors
            .iter()
            .map(|values| Tier {
                dim: values.len() as u32,
                values: values.clone(),
            })
            .collect(),
        sha256: row.sha256.to_vec(),
    }
}

pub(super) fn materialized(row: Embedding) -> io::Result<RetainedVector> {
    if row.full.is_empty()
        || row.full.iter().any(|v| !v.is_finite())
        || row
            .inv_norm
            .is_none_or(|norm| !norm.is_finite() || norm < 0.0)
    {
        return Err(invalid("invalid full-resolution vector or norm"));
    }
    for tier in row.prefixes.iter().chain(&row.sentences) {
        if tier.dim == 0
            || tier.dim as usize != tier.values.len()
            || tier.values.iter().any(|v| !v.is_finite())
        {
            return Err(invalid("invalid multi-resolution vector tier"));
        }
    }
    if row
        .prefixes
        .iter()
        .any(|tier| tier.values.len() > row.full.len())
    {
        return Err(invalid("vector prefix exceeds full dimension"));
    }
    let mut digest = Sha256::new();
    for value in &row.full {
        digest.update(&value.to_le_bytes());
    }
    let sha256 = digest.finalize();
    if row.sha256.as_slice() != sha256 {
        return Err(invalid("multi-resolution vector digest mismatch"));
    }
    Ok(RetainedVector {
        output: MultiResVectorOutput {
            full: row.full,
            prefixes: row
                .prefixes
                .into_iter()
                .map(|tier| (tier.dim, tier.values))
                .collect(),
            inv_norm: row.inv_norm.expect("validated norm presence"),
            quantized_codes: row.quantized_codes,
            sentence_vectors: row.sentences.into_iter().map(|tier| tier.values).collect(),
            sha256,
        },
    })
}

// All tags in this private codec fit in one key byte. Reserve the largest
// slot varint even for WAL payloads, whose slot is carried by their envelope.
pub(super) fn envelope_size(payload: usize) -> io::Result<usize> {
    payload
        .checked_add(1 + 5 + 1 + encoded_len_varint(payload as u64))
        .ok_or_else(|| invalid("multi-resolution vector size overflow"))
}

pub(super) fn bounded_payload(payload: usize, limits: Limits) -> io::Result<()> {
    if envelope_size(payload)? > limits.record {
        return Err(invalid("multi-resolution vector record exceeds size bound"));
    }
    Ok(())
}

fn delimited_size(bytes: usize) -> usize {
    1 + encoded_len_varint(bytes as u64) + bytes
}

struct Budget {
    used: usize,
    limit: usize,
}
impl Budget {
    fn new(limit: usize) -> Self {
        Self {
            used: std::mem::size_of::<Embedding>() + std::mem::size_of::<RetainedVector>(),
            limit,
        }
    }
    fn add(&mut self, bytes: usize) -> io::Result<()> {
        self.used = self
            .used
            .checked_add(bytes)
            .ok_or_else(|| invalid("multi-resolution allocation overflow"))?;
        if self.used > self.limit {
            return Err(invalid(
                "multi-resolution decoded allocation exceeds size bound",
            ));
        }
        Ok(())
    }
    fn vector(&mut self, bytes: usize) -> io::Result<()> {
        self.add(
            bytes
                .checked_mul(4)
                .ok_or_else(|| invalid("multi-resolution allocation overflow"))?,
        )
    }
    fn tier(&mut self, prefix: bool) -> io::Result<()> {
        self.add(
            4 * std::mem::size_of::<Tier>()
                + if prefix {
                    std::mem::size_of::<(u32, Vec<f32>)>()
                } else {
                    std::mem::size_of::<Vec<f32>>()
                },
        )
    }
}

fn finite(values: &[f32]) -> io::Result<()> {
    if values.iter().any(|v| !v.is_finite()) {
        return Err(invalid("nonfinite multi-resolution vector value"));
    }
    Ok(())
}

/// Borrow first: rejection cannot clone an oversized input or encode a row
/// which the sidecar's enclosing Row would subsequently reject.
pub(super) fn validate_retained(row: &RetainedVector, limits: Limits) -> io::Result<usize> {
    let out = &row.output;
    if out.full.is_empty() || !out.inv_norm.is_finite() || out.inv_norm < 0.0 {
        return Err(invalid("invalid full-resolution vector or norm"));
    }
    let mut budget = Budget::new(limits.decoded);
    let mut size = 5 + delimited_size(32); // present inv_norm, full-vector digest
    budget.vector(32)?;
    {
        let bytes = out
            .full
            .len()
            .checked_mul(4)
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
        budget.vector(bytes)?;
        size = size
            .checked_add(delimited_size(bytes))
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
    }
    for (prefix, dim, values) in out
        .prefixes
        .iter()
        .map(|(dim, v)| (true, *dim as usize, v))
        .chain(out.sentence_vectors.iter().map(|v| (false, v.len(), v)))
    {
        if dim == 0
            || dim != values.len()
            || dim > u32::MAX as usize
            || (prefix && dim > out.full.len())
        {
            return Err(invalid("invalid multi-resolution vector tier"));
        }
        budget.tier(prefix)?;
        let bytes = values
            .len()
            .checked_mul(4)
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
        budget.vector(bytes)?;
        let tier_size = 1 + encoded_len_varint(dim as u64) + delimited_size(bytes);
        size = size
            .checked_add(delimited_size(tier_size))
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
    }
    if let Some(codes) = &out.quantized_codes {
        budget.vector(codes.len())?;
        size = size
            .checked_add(delimited_size(codes.len()))
            .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
    }
    bounded_payload(size, limits)?;
    finite(&out.full)?;
    for values in out
        .prefixes
        .iter()
        .map(|(_, v)| v)
        .chain(&out.sentence_vectors)
    {
        finite(values)?;
    }
    let mut digest = Sha256::new();
    for value in &out.full {
        digest.update(&value.to_le_bytes());
    }
    if digest.finalize() != out.sha256 {
        return Err(invalid("multi-resolution vector digest mismatch"));
    }
    Ok(size)
}

pub(super) fn delimited<'a>(wire: WireType, bytes: &mut &'a [u8]) -> io::Result<&'a [u8]> {
    if wire != WireType::LengthDelimited {
        return Err(invalid("invalid multi-resolution protobuf wire type"));
    }
    let len = decode_varint(bytes).map_err(|e| invalid(e.to_string()))?;
    let len = usize::try_from(len).map_err(|_| invalid("multi-resolution vector size overflow"))?;
    if len > bytes.len() {
        return Err(invalid("truncated multi-resolution protobuf field"));
    }
    let (field, rest) = bytes.split_at(len);
    *bytes = rest;
    Ok(field)
}

fn floats(wire: WireType, bytes: &mut &[u8], budget: &mut Budget) -> io::Result<usize> {
    let size = if wire == WireType::ThirtyTwoBit {
        if bytes.len() < 4 {
            return Err(invalid("truncated multi-resolution float"));
        }
        *bytes = &bytes[4..];
        4
    } else {
        delimited(wire, bytes)?.len()
    };
    if !size.is_multiple_of(4) {
        return Err(invalid("invalid packed multi-resolution float length"));
    }
    budget.vector(size)?;
    Ok(size / 4)
}

pub(super) fn skip(wire: WireType, tag: u32, bytes: &mut &[u8]) -> io::Result<()> {
    skip_field(wire, tag, bytes, DecodeContext::default()).map_err(|e| invalid(e.to_string()))
}

fn scan_tier(mut bytes: &[u8], budget: &mut Budget) -> io::Result<usize> {
    let mut dim = 0;
    let mut values = 0usize;
    while !bytes.is_empty() {
        let (tag, wire) = decode_key(&mut bytes).map_err(|e| invalid(e.to_string()))?;
        match tag {
            1 if wire == WireType::Varint => {
                dim = decode_varint(&mut bytes).map_err(|e| invalid(e.to_string()))?;
            }
            2 => {
                values = values
                    .checked_add(floats(wire, &mut bytes, budget)?)
                    .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
            }
            _ => skip(wire, tag, &mut bytes)?,
        }
    }
    if dim == 0 || dim > u32::MAX as u64 || dim != values as u64 {
        return Err(invalid("invalid multi-resolution vector tier"));
    }
    Ok(values)
}

/// Allocation-free inspection precedes prost's owned message construction.
/// It accounts for repeated packed/unpacked fields and message descriptors,
/// including fields that a later scalar occurrence would overwrite.
pub(super) fn scan_embedding(mut bytes: &[u8], limits: Limits) -> io::Result<()> {
    bounded_payload(bytes.len(), limits)?;
    let mut budget = Budget::new(limits.decoded);
    let mut full = 0usize;
    let mut largest_prefix = 0usize;
    let mut norm = false;
    while !bytes.is_empty() {
        let (tag, wire) = decode_key(&mut bytes).map_err(|e| invalid(e.to_string()))?;
        match tag {
            1 => {
                full = full
                    .checked_add(floats(wire, &mut bytes, &mut budget)?)
                    .ok_or_else(|| invalid("multi-resolution vector size overflow"))?;
            }
            2 | 5 => {
                budget.tier(tag == 2)?;
                let count = scan_tier(delimited(wire, &mut bytes)?, &mut budget)?;
                if tag == 2 {
                    largest_prefix = largest_prefix.max(count);
                }
            }
            3 if wire == WireType::ThirtyTwoBit => {
                if bytes.len() < 4 {
                    return Err(invalid("truncated multi-resolution norm"));
                }
                bytes = &bytes[4..];
                norm = true;
            }
            4 | 6 => {
                let field = delimited(wire, &mut bytes)?;
                budget.vector(field.len())?;
                if tag == 6 && field.len() != 32 {
                    return Err(invalid("invalid multi-resolution digest length"));
                }
            }
            _ => skip(wire, tag, &mut bytes)?,
        }
    }
    if full == 0 || largest_prefix > full || !norm {
        return Err(invalid("invalid full-resolution vector or norm"));
    }
    Ok(())
}

/// The same materialized payload travels in the WAL and generation files.
pub(crate) fn encoded_len(row: &RetainedVector) -> io::Result<usize> {
    validate_retained(row, LIMITS)
}

pub(crate) fn encode(row: &RetainedVector) -> io::Result<Vec<u8>> {
    encode_with_limits(row, LIMITS)
}
pub(super) fn encode_with_limits(row: &RetainedVector, limits: Limits) -> io::Result<Vec<u8>> {
    let size = validate_retained(row, limits)?;
    let stored = embedding(row);
    debug_assert_eq!(stored.encoded_len(), size);
    Ok(stored.encode_to_vec())
}

pub(crate) fn decode(bytes: &[u8]) -> io::Result<RetainedVector> {
    scan_embedding(bytes, LIMITS)?;
    materialized(Embedding::decode(bytes).map_err(|e| invalid(e.to_string()))?)
}
