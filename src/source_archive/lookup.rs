use super::*;
use crate::pb::storage::SourceIdentityLookup;

/// One append-only reverse link per bound row; no allocation per identity.
#[derive(Debug)]
pub(super) struct IdentityLink {
    pub row: u32,
    pub previous: u32,
}

impl SourceArchive {
    /// Visit all stored rows for an exact key, across versions. Returns false if
    /// the visitor stops. Row order within a version is unspecified. This does
    /// not apply tombstones, certify the current version, or authorize access.
    pub fn visit_document_rows(
        &self,
        key: &[u8],
        visitor: &mut dyn FnMut(u32, u64, Option<u32>) -> bool,
    ) -> bool {
        for ((_, version), &identity) in self
            .identity_ids
            .range((key.to_vec(), 0)..=(key.to_vec(), u64::MAX))
        {
            let mut link = self.identity_heads[identity as usize - 1];
            while link != 0 {
                let entry = &self.identity_links[link as usize - 1];
                if !visitor(
                    entry.row,
                    *version,
                    self.rows[entry.row as usize].chunk_ordinal,
                ) {
                    return false;
                }
                link = entry.previous;
            }
        }
        true
    }

    pub(super) fn restore_identity_links(&mut self) {
        self.identity_heads.resize(self.identities.len(), 0);
        for (row, reference) in self.rows.iter().enumerate() {
            if reference.identity != 0 {
                let head = &mut self.identity_heads[reference.identity as usize - 1];
                self.identity_links.push(IdentityLink {
                    row: row as u32,
                    previous: *head,
                });
                *head = self.identity_links.len() as u32;
            }
        }
    }
}

impl SourceArchiveReader {
    /// Same contract as `SourceArchive::visit_document_rows`. The lookup uses
    /// exact key comparisons and visits only this key's bound rows after a
    /// binary search. Legacy identity archives build the lookup once on open.
    pub fn visit_document_rows(
        &self,
        key: &[u8],
        visitor: &mut dyn FnMut(u32, u64, Option<u32>) -> bool,
    ) -> bool {
        let Some(lookup) = &self.index.identity_lookup else {
            return true;
        };
        let first = lookup.identities.partition_point(|&id| {
            self.index.identities[id as usize - 1]
                .document_key
                .as_slice()
                < key
        });
        for position in first..lookup.identities.len() {
            let identity = &self.index.identities[lookup.identities[position] as usize - 1];
            if identity.document_key != key {
                break;
            }
            let rows = &lookup.rows
                [lookup.offsets[position] as usize..lookup.offsets[position + 1] as usize];
            for &row in rows {
                if !visitor(
                    row,
                    identity.version,
                    self.index.rows[row as usize].chunk_ordinal,
                ) {
                    return false;
                }
            }
        }
        true
    }
}

pub(super) fn build_lookup(index: &SourceArchiveIndex) -> SourceIdentityLookup {
    let count = index.identities.len();
    let mut identities: Vec<u32> = (1..=count as u32).collect();
    identities.sort_unstable_by(|&left, &right| {
        let left = &index.identities[left as usize - 1];
        let right = &index.identities[right as usize - 1];
        (&left.document_key, left.version).cmp(&(&right.document_key, right.version))
    });
    let mut positions = vec![0; count];
    for (position, &id) in identities.iter().enumerate() {
        positions[id as usize - 1] = position;
    }
    let mut offsets = vec![0u32; count + 1];
    for row in &index.rows {
        if row.identity != 0 {
            offsets[positions[row.identity as usize - 1] + 1] += 1;
        }
    }
    for position in 0..count {
        offsets[position + 1] += offsets[position];
    }
    let mut rows = vec![0; offsets[count] as usize];
    let mut cursor = offsets[..count].to_vec();
    for (row, reference) in index.rows.iter().enumerate() {
        if reference.identity != 0 {
            let next = &mut cursor[positions[reference.identity as usize - 1]];
            rows[*next as usize] = row as u32;
            *next += 1;
        }
    }
    SourceIdentityLookup {
        identities,
        offsets,
        rows,
    }
}

pub(super) fn validate_lookup(index: &SourceArchiveIndex) -> io::Result<()> {
    let lookup = index
        .identity_lookup
        .as_ref()
        .ok_or_else(|| invalid("source archive lacks identity lookup"))?;
    if lookup.identities.len() != index.identities.len()
        || lookup.offsets.len() != lookup.identities.len() + 1
        || lookup.offsets.first() != Some(&0)
        || lookup.offsets.last().copied().map(u64::from) != Some(lookup.rows.len() as u64)
        || lookup.rows.len() != index.rows.iter().filter(|row| row.identity != 0).count()
    {
        return Err(invalid(
            "identity lookup shape or coverage differs from source rows",
        ));
    }
    let mut previous = None;
    for (position, &id) in lookup.identities.iter().enumerate() {
        let identity = id
            .checked_sub(1)
            .and_then(|id| index.identities.get(id as usize))
            .ok_or_else(|| invalid("identity lookup ordinal is out of range"))?;
        let key = (&identity.document_key, identity.version);
        if previous.is_some_and(|previous| previous >= key) {
            return Err(invalid("identity lookup keys are not strictly ordered"));
        }
        previous = Some(key);
        let start = lookup.offsets[position] as usize;
        let end = lookup.offsets[position + 1] as usize;
        let rows = lookup
            .rows
            .get(start..end)
            .ok_or_else(|| invalid("identity lookup offsets are invalid"))?;
        let mut previous_row = None;
        for &row in rows {
            if previous_row.is_some_and(|previous| previous >= row)
                || index
                    .rows
                    .get(row as usize)
                    .is_none_or(|reference| reference.identity != id)
            {
                return Err(invalid(
                    "identity lookup row is repeated, unordered, or bound to another identity",
                ));
            }
            previous_row = Some(row);
        }
    }
    Ok(())
}
