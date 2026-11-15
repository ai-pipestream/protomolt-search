//! Inline column-table metadata records (`StoredBinding` and `StoredDerived`)
//! and their symmetric encode/decode helpers.

use std::io::{self, Write};

use super::super::format::{
    write_u16, write_u32, BINDING_ENTRY_NAME, COLUMN_KIND_ANALYSIS_BINDING, COLUMN_KIND_BINDING,
    COLUMN_KIND_DERIVED, COLUMN_KIND_INDEX_BINDING, COLUMN_KIND_VECTOR_BINDING, DERIVED_ENTRY_NAME,
};

/// The shard-level mapped-plan binding: the identity of the plan this
/// store's mapped columns were written under. Persisted as a kind-6, kind-12, kind-13, or kind-14 entry of the column
/// table; an index only ever pairs with the plan it was written under,
/// and a contradiction at bind time is an index compatibility event.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredBinding {
    /// The plan fingerprint (`MappedPlan.fingerprint`, lowercase hex).
    pub plan_fingerprint: String,
    /// The TEXT field bound as the document body.
    pub body_path: String,
    /// SHA-256 over the canonical encoding of the bind's materialize
    /// spec, empty when the bind carried none. Changing materialization
    /// changes what an index means, so it is
    /// part of the bound identity.
    pub materialize_sha: String,
    /// Complete explicit mapped analysis contract, empty for legacy bindings.
    pub analysis_sha: String,
    /// Canonical protobuf `MappedAnalysisContract`, empty for legacy bindings.
    pub analysis_contract: Vec<u8>,
    /// Canonical `MappedVectorBinding`; empty means the legacy field is unnamed.
    pub vector_binding: Vec<u8>,
    /// Canonical `MappedIndexContract`; empty for inferred-policy bindings.
    pub index_contract: Vec<u8>,
}

/// The index's derived-column declaration as a store persists it:
/// the kind-17 column-table entry. A store
/// pairs with one declaration; where it is attached, a differing
/// fingerprint is an index compatibility event, refused by name.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredDerived {
    /// The declaration fingerprint (`derived::fingerprint_of`, lowercase hex).
    pub fingerprint: String,
    /// The canonical `DerivedColumns` protobuf.
    pub declaration: Vec<u8>,
}

impl StoredDerived {
    /// The stored form of a validated declaration.
    pub fn of(declaration: &crate::derived::Declaration) -> Self {
        StoredDerived {
            fingerprint: declaration.fingerprint().to_string(),
            declaration: declaration.canonical(),
        }
    }

    /// Decode the declaration and check the fingerprint covers it.
    pub fn validate(&self) -> Result<crate::pb::DerivedColumns, String> {
        if self.fingerprint.len() != 64
            || !self
                .fingerprint
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(format!(
                "derived-columns entry: fingerprint {:?} is not lowercase SHA-256 hex",
                self.fingerprint
            ));
        }
        let spec = crate::derived::decode_canonical(&self.declaration)?;
        if spec.columns.is_empty() {
            return Err("derived-columns entry declares no columns".to_string());
        }
        let expected = crate::derived::fingerprint_of(&spec);
        if expected != self.fingerprint {
            return Err(format!(
                "derived-columns entry: fingerprint {:?} does not cover its declaration \
                 (which hashes to {expected:?})",
                self.fingerprint
            ));
        }
        Ok(spec)
    }

    /// The compiled declaration.
    pub fn declaration(&self) -> Result<crate::derived::Declaration, String> {
        crate::derived::Declaration::compile(&self.validate()?)
    }
}

/// Header bytes of the derived-columns entry, 0 without a declaration.
pub(in crate::postings) fn derived_entry_size(derived: Option<&StoredDerived>) -> u64 {
    derived.map_or(0, |d| {
        2 + DERIVED_ENTRY_NAME.len() as u64
            + 1
            + 2
            + d.fingerprint.len() as u64
            + 4
            + d.declaration.len() as u64
    })
}

/// Emit the derived-columns entry (a no-op without a declaration).
pub(in crate::postings) fn write_derived_entry<W: Write>(
    w: &mut W,
    derived: Option<&StoredDerived>,
) -> io::Result<()> {
    let Some(d) = derived else { return Ok(()) };
    d.validate()
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    write_u16(w, DERIVED_ENTRY_NAME.len() as u16)?;
    w.write_all(DERIVED_ENTRY_NAME.as_bytes())?;
    w.write_all(&[COLUMN_KIND_DERIVED])?;
    write_u16(w, d.fingerprint.len() as u16)?;
    w.write_all(d.fingerprint.as_bytes())?;
    write_u32(
        w,
        u32::try_from(d.declaration.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "derived declaration exceeds u32",
            )
        })?,
    )?;
    w.write_all(&d.declaration)
}

/// Decode and validate the inline payload of a `COLUMN_KIND_DERIVED`
/// column-table entry, returning `(stored, bytes_consumed)`.
pub(in crate::postings) fn decode_derived_entry(
    payload: &[u8],
) -> io::Result<(StoredDerived, usize)> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let bytes_at = |cur: usize, len: usize| -> io::Result<&[u8]> {
        let end = cur
            .checked_add(len)
            .ok_or_else(|| invalid("derived-columns record length overflow".into()))?;
        payload
            .get(cur..end)
            .ok_or_else(|| invalid("truncated derived-columns record".into()))
    };
    let fp_len = u16::from_le_bytes(bytes_at(0, 2)?.try_into().expect("2 bytes")) as usize;
    let fingerprint = std::str::from_utf8(bytes_at(2, fp_len)?)
        .map_err(|_| invalid("invalid utf-8 in derived-columns record".into()))?
        .to_string();
    let decl_off = 2 + fp_len;
    let decl_len = u32::from_le_bytes(bytes_at(decl_off, 4)?.try_into().expect("4 bytes")) as usize;
    let declaration = bytes_at(decl_off + 4, decl_len)?.to_vec();
    let stored = StoredDerived {
        fingerprint,
        declaration,
    };
    stored.validate().map_err(invalid)?;
    Ok((stored, decl_off + 4 + decl_len))
}

/// Header bytes of the binding's column-table entry, 0 when unbound.
pub(in crate::postings) fn binding_entry_size(binding: Option<&StoredBinding>) -> u64 {
    binding.map_or(0, |b| {
        2 + BINDING_ENTRY_NAME.len() as u64
            + 1
            + 2
            + b.plan_fingerprint.len() as u64
            + 2
            + b.body_path.len() as u64
            + 2
            + b.materialize_sha.len() as u64
            + if b.analysis_sha.is_empty() && b.vector_binding.is_empty() {
                0
            } else {
                2 + b.analysis_sha.len() as u64 + 4 + b.analysis_contract.len() as u64
            }
            + if b.vector_binding.is_empty() {
                0
            } else {
                4 + b.vector_binding.len() as u64
            }
            + if b.index_contract.is_empty() {
                0
            } else {
                4 + b.index_contract.len() as u64
            }
    })
}

/// Emit the binding's column-table entry (a no-op when unbound).
pub(in crate::postings) fn write_binding_entry<W: Write>(
    w: &mut W,
    binding: Option<&StoredBinding>,
) -> io::Result<()> {
    let Some(b) = binding else { return Ok(()) };
    write_u16(w, BINDING_ENTRY_NAME.len() as u16)?;
    w.write_all(BINDING_ENTRY_NAME.as_bytes())?;
    crate::mapped_analysis::decode_contract(&b.analysis_sha, &b.analysis_contract, &b.body_path)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    crate::mapped_vector::decode(&b.vector_binding, &b.plan_fingerprint)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    crate::index_contract::validate_binding(
        &b.index_contract,
        &b.plan_fingerprint,
        &b.vector_binding,
    )
    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
    w.write_all(&[if !b.index_contract.is_empty() {
        COLUMN_KIND_INDEX_BINDING
    } else if !b.vector_binding.is_empty() {
        COLUMN_KIND_VECTOR_BINDING
    } else if b.analysis_sha.is_empty() {
        COLUMN_KIND_BINDING
    } else {
        COLUMN_KIND_ANALYSIS_BINDING
    }])?;
    for value in [&b.plan_fingerprint, &b.body_path, &b.materialize_sha]
        .into_iter()
        .chain(
            (!b.analysis_sha.is_empty() || !b.vector_binding.is_empty()).then_some(&b.analysis_sha),
        )
    {
        write_u16(
            w,
            u16::try_from(value.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "binding string exceeds u16")
            })?,
        )?;
        w.write_all(value.as_bytes())?;
    }
    if !b.analysis_sha.is_empty() || !b.vector_binding.is_empty() {
        write_u32(
            w,
            u32::try_from(b.analysis_contract.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "analysis contract exceeds u32")
            })?,
        )?;
        w.write_all(&b.analysis_contract)?;
    }
    if !b.vector_binding.is_empty() {
        write_u32(
            w,
            u32::try_from(b.vector_binding.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "vector binding exceeds u32")
            })?,
        )?;
        w.write_all(&b.vector_binding)?;
    }
    if !b.index_contract.is_empty() {
        write_u32(
            w,
            u32::try_from(b.index_contract.len()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "index contract exceeds u32")
            })?,
        )?;
        w.write_all(&b.index_contract)?;
    }
    Ok(())
}

/// Decode and validate the inline payload of a kind-6/12/13/14 binding
/// column-table entry, returning `(binding, bytes_consumed)`.
pub(in crate::postings) fn decode_binding_entry(
    kind: u8,
    payload: &[u8],
) -> io::Result<(StoredBinding, usize)> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let bytes_at = |cur: usize, len: usize| -> io::Result<&[u8]> {
        let end = cur
            .checked_add(len)
            .ok_or_else(|| invalid("binding record length overflow".into()))?;
        payload
            .get(cur..end)
            .ok_or_else(|| invalid("truncated binding record".into()))
    };
    let mut cur = 0usize;
    let string_count = if kind != COLUMN_KIND_BINDING { 4 } else { 3 };
    let mut vals = Vec::with_capacity(string_count);
    for part in 0..string_count {
        let len = u16::from_le_bytes(bytes_at(cur, 2)?.try_into().expect("2 bytes")) as usize;
        let s = std::str::from_utf8(bytes_at(cur + 2, len)?)
            .map_err(|_| invalid("invalid utf-8 in binding record".into()))?;
        if part == 3
            && (kind == COLUMN_KIND_ANALYSIS_BINDING || !s.is_empty())
            && !crate::mapped_analysis::valid_digest(s)
        {
            return Err(invalid("invalid mapped analysis digest".into()));
        }
        vals.push(s.to_string());
        cur += 2 + len;
    }
    let analysis_contract = if kind != COLUMN_KIND_BINDING {
        let len = u32::from_le_bytes(bytes_at(cur, 4)?.try_into().expect("4 bytes")) as usize;
        let bytes = bytes_at(cur + 4, len)?;
        crate::mapped_analysis::decode_contract(&vals[3], bytes, &vals[1]).map_err(invalid)?;
        cur += 4 + len;
        bytes.to_vec()
    } else {
        Vec::new()
    };
    let vector_binding = if matches!(kind, COLUMN_KIND_VECTOR_BINDING | COLUMN_KIND_INDEX_BINDING) {
        let len = u32::from_le_bytes(bytes_at(cur, 4)?.try_into().expect("4 bytes")) as usize;
        let bytes = bytes_at(cur + 4, len)?;
        if crate::mapped_vector::decode(bytes, &vals[0])
            .map_err(|e| invalid(e.to_string()))?
            .is_none()
        {
            return Err(invalid("empty vector binding in vector/index entry".into()));
        }
        cur += 4 + len;
        bytes.to_vec()
    } else {
        Vec::new()
    };
    let index_contract = if kind == COLUMN_KIND_INDEX_BINDING {
        let len = u32::from_le_bytes(bytes_at(cur, 4)?.try_into().expect("4 bytes")) as usize;
        let bytes = bytes_at(cur + 4, len)?;
        if crate::index_contract::validate_binding(bytes, &vals[0], &vector_binding)
            .map_err(|e| invalid(e.to_string()))?
            .is_none()
        {
            return Err(invalid("empty index contract in kind-14 entry".into()));
        }
        cur += 4 + len;
        bytes.to_vec()
    } else {
        Vec::new()
    };
    let mut it = vals.into_iter();
    let binding = StoredBinding {
        plan_fingerprint: it.next().expect("plan_fingerprint"),
        body_path: it.next().expect("body_path"),
        materialize_sha: it.next().expect("materialize_sha"),
        analysis_sha: it.next().unwrap_or_default(),
        analysis_contract,
        vector_binding,
        index_contract,
    };
    Ok((binding, cur))
}
