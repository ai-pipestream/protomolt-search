//! The content digest a source row and a child row are compared by, and
//! the independent column checks.

use crate::pb::AddDocumentsRequest;

use super::{CheckTally, ColumnCheck};

/// A SHA-256 over tagged, length-prefixed fields.
struct Digest(turbo_sha256::Sha256);

impl Digest {
    fn bytes(&mut self, tag: u8, data: &[u8]) {
        self.0.update(&[tag]);
        self.0.update(&(data.len() as u64).to_le_bytes());
        self.0.update(data);
    }

    fn optional(&mut self, some: u8, none: u8, data: Option<&[u8]>) {
        match data {
            Some(data) => self.bytes(some, data),
            None => self.bytes(none, &[]),
        }
    }
}

/// The digest of a document's content: every value list sorted, so the
/// order a store lists columns in does not matter, plus the vector.
pub(super) fn digest(doc: &AddDocumentsRequest, vector: Option<&[f32]>) -> [u8; 32] {
    let mut h = Digest(turbo_sha256::Sha256::new());
    digest_provenance(&mut h, doc);
    digest_scalars(&mut h, doc);
    digest_maps(&mut h, doc);
    h.bytes(40, doc.derived_fingerprint.as_bytes());
    let vector = vector.map(|vector| {
        let mut v = Vec::with_capacity(vector.len() * 4);
        for x in vector {
            v.extend_from_slice(&x.to_bits().to_le_bytes());
        }
        v
    });
    h.optional(50, 51, vector.as_deref());
    h.0.finish().0
}

/// The text, lineage, identity, original source, and chunk ordinal.
fn digest_provenance(h: &mut Digest, doc: &AddDocumentsRequest) {
    h.bytes(1, doc.text.as_bytes());
    let lineage = doc.lineage.as_ref().map(|l| {
        let mut v = Vec::with_capacity(32);
        v.extend_from_slice(&l.parent_id.to_le_bytes());
        v.extend_from_slice(&l.group_id.to_le_bytes());
        v.extend_from_slice(&l.span_start.to_le_bytes());
        v.extend_from_slice(&l.span_end.to_le_bytes());
        v
    });
    h.optional(2, 3, lineage.as_deref());
    match &doc.identity {
        Some(identity) => {
            h.bytes(4, &identity.document_key);
            h.bytes(5, &identity.version.to_le_bytes());
            let ordinal = identity.chunk_ordinal.map(|o| o.to_le_bytes().to_vec());
            h.optional(6, 7, ordinal.as_deref());
        }
        None => h.bytes(8, &[]),
    }
    match &doc.original_source {
        Some(source) => {
            h.bytes(9, &source.descriptor_set);
            h.bytes(10, source.message_type.as_bytes());
            h.bytes(11, &source.payload);
        }
        None => h.bytes(12, &[]),
    }
    let ordinal = doc.source_chunk_ordinal.map(|o| o.to_le_bytes().to_vec());
    h.optional(13, 14, ordinal.as_deref());
}

/// `(field, value)` pairs, sorted by field then value, each filed under
/// its two tags as little-endian bytes.
fn digest_pairs<V: Ord + Copy + ToLe>(h: &mut Digest, tags: [u8; 2], mut pairs: Vec<(&str, V)>) {
    pairs.sort_unstable();
    for (field, value) in pairs {
        h.bytes(tags[0], field.as_bytes());
        h.bytes(tags[1], &value.to_le());
    }
}

/// A value's little-endian bytes, as the digest files it.
trait ToLe {
    fn to_le(self) -> Vec<u8>;
}

impl ToLe for u64 {
    fn to_le(self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

impl ToLe for i64 {
    fn to_le(self) -> Vec<u8> {
        self.to_le_bytes().to_vec()
    }
}

fn digest_scalars(h: &mut Digest, doc: &AddDocumentsRequest) {
    let mut facets: Vec<(&str, &str)> = doc
        .facets
        .iter()
        .map(|v| (v.field.as_str(), v.value.as_str()))
        .collect();
    facets.sort_unstable();
    for (field, value) in facets {
        h.bytes(20, field.as_bytes());
        h.bytes(21, value.as_bytes());
    }
    let numerics = doc.numerics.iter();
    let numerics = numerics.map(|v| (v.field.as_str(), v.value.to_bits()));
    digest_pairs(h, [22, 23], numerics.collect());
    let integers = doc.integers.iter().map(|v| (v.field.as_str(), v.value));
    digest_pairs(h, [24, 25], integers.collect());
    let unsigned = doc.unsigned_integers.iter();
    digest_pairs(
        h,
        [26, 27],
        unsigned.map(|v| (v.field.as_str(), v.value)).collect(),
    );
    let mut geo: Vec<(&str, u64, u64)> = doc
        .geo_points
        .iter()
        .map(|v| (v.field.as_str(), v.lat.to_bits(), v.lon.to_bits()))
        .collect();
    geo.sort_unstable();
    for (field, lat, lon) in geo {
        h.bytes(28, field.as_bytes());
        h.bytes(29, &lat.to_le_bytes());
        h.bytes(30, &lon.to_le_bytes());
    }
}

/// `(field, key, value)` triples, sorted, each filed under its tags.
fn digest_triples<V: Ord + Copy + ToLe>(
    h: &mut Digest,
    tags: [u8; 3],
    mut triples: Vec<(&str, &str, V)>,
) {
    triples.sort_unstable();
    for (field, key, value) in triples {
        h.bytes(tags[0], field.as_bytes());
        h.bytes(tags[1], key.as_bytes());
        h.bytes(tags[2], &value.to_le());
    }
}

fn digest_maps(h: &mut Digest, doc: &AddDocumentsRequest) {
    let mut map_facets: Vec<(&str, &str, &str)> = doc
        .map_facets
        .iter()
        .map(|v| (v.field.as_str(), v.key.as_str(), v.value.as_str()))
        .collect();
    map_facets.sort_unstable();
    for (field, key, value) in map_facets {
        h.bytes(31, field.as_bytes());
        h.bytes(32, key.as_bytes());
        h.bytes(33, value.as_bytes());
    }
    let entries = doc.map_numerics.iter();
    let entries = entries.map(|v| (v.field.as_str(), v.key.as_str(), v.value.to_bits()));
    digest_triples(h, [34, 35, 36], entries.collect());
    let entries = doc.map_integers.iter();
    let entries = entries.map(|v| (v.field.as_str(), v.key.as_str(), v.value));
    digest_triples(h, [60, 61, 62], entries.collect());
    let entries = doc.map_unsigned_integers.iter();
    let entries = entries.map(|v| (v.field.as_str(), v.key.as_str(), v.value));
    digest_triples(h, [63, 64, 65], entries.collect());
}

fn integer_of(doc: &AddDocumentsRequest, name: &str) -> Option<i64> {
    doc.integers
        .iter()
        .find(|v| v.field == name)
        .map(|v| v.value)
}

fn is_leap(year: i64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

/// The UTC civil year of an epoch-micro timestamp, counted year by year
/// from 1970 over whole days: the independent form of `calendar.year`.
pub fn civil_year(micros: i64) -> i64 {
    let mut days = micros.div_euclid(86_400_000_000);
    let mut year = 1970i64;
    if days >= 0 {
        loop {
            let length = if is_leap(year) { 366 } else { 365 };
            if days < length {
                return year;
            }
            days -= length;
            year += 1;
        }
    }
    loop {
        year -= 1;
        days += if is_leap(year) { 366 } else { 365 };
        if days >= 0 {
            return year;
        }
    }
}

/// Count one child row into a check's tally.
pub(super) fn tally(check: &ColumnCheck, doc: &AddDocumentsRequest, tally: &mut CheckTally) {
    let (column, other) = match check {
        ColumnCheck::Equal { column, other } => (integer_of(doc, column), integer_of(doc, other)),
        ColumnCheck::CivilYear { column, timestamp } => {
            (integer_of(doc, column), integer_of(doc, timestamp))
        }
    };
    let holds = |a: i64, b: i64| match check {
        ColumnCheck::Equal { .. } => a == b,
        ColumnCheck::CivilYear { .. } => a == civil_year(b),
    };
    match (column, other) {
        (Some(a), Some(b)) => {
            tally.both += 1;
            if holds(a, b) {
                tally.agree += 1;
            } else {
                tally.disagree += 1;
            }
        }
        (Some(_), None) => tally.only_column += 1,
        (None, Some(_)) => tally.only_other += 1,
        (None, None) => tally.neither += 1,
    }
}
