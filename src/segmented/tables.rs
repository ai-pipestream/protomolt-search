//! Agreement between a sealed segment's schema tables and the tail's.

use super::*;

/// The first table on which a sealed segment and the tail disagree,
/// named with both sides, for a refusal that says what to fix.
fn first_table_difference(other: &crate::postings::Bm25Reader, tail: &Bm25Store) -> Option<String> {
    fn names<'a>(count: usize, name: impl Fn(usize) -> &'a str) -> Vec<String> {
        (0..count).map(|i| name(i).to_string()).collect()
    }
    let fields = |names: Vec<String>| format!("{names:?}");
    let pairs: [(&str, Vec<String>, Vec<String>); 8] = [
        (
            "field",
            names(other.field_count(), |f| other.field_name(f)),
            names(tail.field_count(), |f| tail.field_name(f)),
        ),
        (
            "facet",
            names(other.facet_count(), |i| other.facet_name(i)),
            names(tail.facet_count(), |i| tail.facet_name(i)),
        ),
        (
            "numeric",
            names(other.numeric_count(), |i| other.numeric_name(i)),
            names(tail.numeric_count(), |i| tail.numeric_name(i)),
        ),
        (
            "integer",
            names(other.integer_count(), |i| other.integer_name(i)),
            names(tail.integer_count(), |i| tail.integer_name(i)),
        ),
        (
            "unsigned-integer",
            names(other.unsigned_integer_count(), |i| {
                other.unsigned_integer_name(i)
            }),
            names(tail.unsigned_integer_count(), |i| {
                tail.unsigned_integer_name(i)
            }),
        ),
        (
            "geo",
            names(other.geo_count(), |i| other.geo_name(i)),
            names(tail.geo_count(), |i| tail.geo_name(i)),
        ),
        (
            "map-facet",
            names(other.map_facet_count(), |i| other.map_facet_name(i)),
            names(tail.map_facet_count(), |i| tail.map_facet_name(i)),
        ),
        (
            "map-numeric",
            names(other.map_numeric_count(), |i| other.map_numeric_name(i)),
            names(tail.map_numeric_count(), |i| tail.map_numeric_name(i)),
        ),
    ];
    for (what, segment, shard) in pairs {
        if segment != shard {
            return Some(format!(
                "{what} table: the segment has {}, this shard {}",
                fields(segment),
                fields(shard)
            ));
        }
    }
    None
}

impl SegmentedShard {
    /// Refuse a sealed segment whose tables differ from the tail's, or
    /// whose derived-column declaration is not the tail's: one shard,
    /// one declaration.
    pub(super) fn check_tables(
        &self,
        reader: &crate::postings::Bm25Reader,
        id: &str,
    ) -> Result<(), String> {
        // The declaration first: a differing one explains a differing
        // table, and names the fix.
        match (reader.derived(), self.tail.derived()) {
            (None, None) => {}
            (Some(held), Some(wanted)) if held.fingerprint == wanted.fingerprint => {}
            (Some(held), Some(wanted)) => {
                return Err(format!(
                    "segment {id:?} was written under derived-column declaration {:?}, this \
                     shard declares {:?}; a changed declaration is a rebuild through the \
                     reshard tool",
                    held.fingerprint, wanted.fingerprint
                ));
            }
            (Some(held), None) => {
                return Err(format!(
                    "segment {id:?} was written under derived-column declaration {:?}, which \
                     this shard does not declare (--derived-columns)",
                    held.fingerprint
                ));
            }
            (None, Some(wanted)) => {
                return Err(format!(
                    "segment {id:?} was written without derived columns, this shard declares \
                     {:?}; backfill the rows through the reshard tool (--derive)",
                    wanted.fingerprint
                ));
            }
        }
        if !same_tables!(reader, &self.tail) {
            let difference = first_table_difference(reader, &self.tail)
                .unwrap_or_else(|| "a field flag".to_string());
            return Err(format!(
                "segment {id:?} declares a field or column table this shard does not \
                 ({difference}); the catalog and the node configuration must agree"
            ));
        }
        Ok(())
    }
}
