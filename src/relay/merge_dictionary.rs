//! Exact merges of the dictionaries: prefix expansions and term suggestions.

use tonic::Status;

use super::merge_util::add_count;

/// The children's prefix expansions as one dictionary's: the union in
/// byte order and its exact size when every child stayed within the
/// cap; a child past the cap answers no terms and a count above it, and
/// the relay then answers the largest such count (a lower bound on the
/// subtree's, enough for the root's refusal) and no terms, as a node
/// past the cap does. `known` when any child knows the field.
pub fn merge_prefix_expansions(
    cap: u32,
    shares: &[crate::pb::ExpandTermPrefixResponse],
) -> crate::pb::ExpandTermPrefixResponse {
    let mut union = std::collections::BTreeSet::new();
    let mut known = false;
    let mut past_cap: u64 = 0;
    for share in shares {
        if !share.known {
            continue;
        }
        known = true;
        if share.count > u64::from(cap) {
            past_cap = past_cap.max(share.count);
            continue;
        }
        union.extend(share.terms.iter().cloned());
    }
    if past_cap > 0 {
        return crate::pb::ExpandTermPrefixResponse {
            terms: Vec::new(),
            count: past_cap,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
        };
    }
    let count = union.len() as u64;
    crate::pb::ExpandTermPrefixResponse {
        terms: if count > u64::from(cap) {
            Vec::new()
        } else {
            union.into_iter().collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
    }
}

/// The children's dictionary scans as one: entries unioned in byte order
/// with each term's df summed (checked), the tombstone count summed, the
/// exact union size as the count within the bound; a child past the scan
/// bound makes the relay answer the largest such count and no entries,
/// as a node past the bound does.
pub fn merge_suggest_terms(
    max_scan: u64,
    shares: &[crate::pb::SuggestTermsResponse],
) -> Result<crate::pb::SuggestTermsResponse, Status> {
    let mut union: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    let mut known = false;
    let mut tombstoned: u64 = 0;
    let mut past_bound: u64 = 0;
    for (child, share) in shares.iter().enumerate() {
        add_count(
            &mut tombstoned,
            share.tombstoned_rows,
            child,
            "tombstone count",
        )?;
        if !share.known {
            continue;
        }
        known = true;
        if share.count > max_scan {
            past_bound = past_bound.max(share.count);
            continue;
        }
        for entry in &share.entries {
            let df = union.entry(entry.term.clone()).or_insert(0);
            add_count(df, entry.df, child, "posting frequency")?;
        }
    }
    if past_bound > 0 {
        return Ok(crate::pb::SuggestTermsResponse {
            entries: Vec::new(),
            count: past_bound,
            known,
            visibility_fingerprint: Vec::new(),
            visibility_columns_known: Vec::new(),
            tombstoned_rows: tombstoned,
        });
    }
    let count = union.len() as u64;
    Ok(crate::pb::SuggestTermsResponse {
        entries: if count > max_scan {
            Vec::new()
        } else {
            union
                .into_iter()
                .map(|(term, df)| crate::pb::SuggestTermEntry { term, df })
                .collect()
        },
        count,
        known,
        visibility_fingerprint: Vec::new(),
        visibility_columns_known: Vec::new(),
        tombstoned_rows: tombstoned,
    })
}
