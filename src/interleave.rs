//! Team-draft interleaving: serve two rankings as one list and learn
//! which one the user actually preferred.
//!
//! A/B splitting shows variant A to some users and variant B to others,
//! so every comparison is between different people asking different
//! things, and the population difference has to be averaged out. That is
//! why split tests need so many impressions. Interleaving instead merges
//! both rankings into the list ONE user sees, remembering which variant
//! contributed each result; a selection then credits its contributor, and
//! the comparison is within a single query from a single person. It is
//! the standard online evaluation method precisely because it needs
//! orders of magnitude less traffic to separate two rankers.
//!
//! The coin is seeded, not random. Team draft normally breaks ties with a
//! fair random flip, which injects variance into exactly the comparison
//! the rest of this engine works to keep exact. Seeding it from the query
//! makes the interleaving a deterministic function of (query, ranking A,
//! ranking B): the same query re-run produces the same merged list and
//! the same attribution, so a disagreement between two runs is a real
//! change rather than a coin. Over many DISTINCT queries the seeds still
//! vary, so neither team gets a systematic first-position advantage.
//!
//! Nothing here needs a judgment signal to exist. Building the mechanism
//! is what makes collecting one possible.

/// Which variant contributed a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Team {
    /// The first ranking (conventionally the incumbent).
    A,
    /// The second ranking (conventionally the challenger).
    B,
}

/// One interleaved result list plus its per-position attribution.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Interleaved {
    /// The merged ids, in the order to display them.
    pub ids: Vec<u64>,
    /// `team[i]` contributed `ids[i]`; parallel to `ids`.
    pub team: Vec<Team>,
}

impl Interleaved {
    /// The team that contributed `id`, if it is in the list.
    pub fn team_of(&self, id: u64) -> Option<Team> {
        self.ids.iter().position(|x| *x == id).map(|i| self.team[i])
    }
}

/// Credits from a set of selected results.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Outcome {
    /// Selections credited to ranking A.
    pub a: usize,
    /// Selections credited to ranking B.
    pub b: usize,
    /// Selected ids that were not in the interleaved list at all.
    pub unattributed: usize,
}

impl Outcome {
    /// The preferred team, or `None` for a tie (including no selections).
    ///
    /// A tie is a real and common outcome for a single query; it is the
    /// aggregate over many queries that separates two rankers, so this
    /// deliberately does not invent a winner.
    pub fn winner(&self) -> Option<Team> {
        match self.a.cmp(&self.b) {
            std::cmp::Ordering::Greater => Some(Team::A),
            std::cmp::Ordering::Less => Some(Team::B),
            std::cmp::Ordering::Equal => None,
        }
    }
}

/// SplitMix64: a tiny, well-distributed deterministic bit source. The
/// interleaving must be reproducible across processes and machines, so it
/// cannot use the thread RNG.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Interleave two rankings into one list of at most `k` results.
///
/// Each round the team that has contributed fewer results so far picks
/// its next unused document; when the teams are level the seeded coin
/// decides. A document both rankings contain is contributed once, by
/// whichever team reached it first, and never counted for the other:
/// results the two variants agree on carry no evidence either way, and
/// crediting them would drown the disagreements that actually
/// discriminate.
pub fn team_draft(a: &[u64], b: &[u64], k: usize, seed: u64) -> Interleaved {
    let mut out = Interleaved::default();
    let mut state = seed;
    let (mut ia, mut ib) = (0usize, 0usize);
    let (mut na, mut nb) = (0usize, 0usize);
    let mut taken: std::collections::HashSet<u64> = std::collections::HashSet::new();

    let next_unused = |list: &[u64], i: &mut usize, taken: &std::collections::HashSet<u64>| {
        while *i < list.len() && taken.contains(&list[*i]) {
            *i += 1;
        }
        (*i < list.len()).then(|| list[*i])
    };

    while out.ids.len() < k {
        let a_has = next_unused(a, &mut ia, &taken).is_some();
        let b_has = next_unused(b, &mut ib, &taken).is_some();
        if !a_has && !b_has {
            break;
        }
        // Whoever is behind picks; a seeded coin breaks a level draft.
        let a_picks = match (a_has, b_has) {
            (true, false) => true,
            (false, true) => false,
            _ => match na.cmp(&nb) {
                std::cmp::Ordering::Less => true,
                std::cmp::Ordering::Greater => false,
                std::cmp::Ordering::Equal => splitmix64(&mut state) & 1 == 0,
            },
        };
        let (list, idx, count, team) = if a_picks {
            (a, &mut ia, &mut na, Team::A)
        } else {
            (b, &mut ib, &mut nb, Team::B)
        };
        let Some(id) = next_unused(list, idx, &taken) else {
            break;
        };
        taken.insert(id);
        *idx += 1;
        *count += 1;
        out.ids.push(id);
        out.team.push(team);
    }
    out
}

/// Credit selected results to the teams that contributed them.
///
/// `selected` is whatever counts as a positive signal: clicks, an
/// explicit relevance mark, a document the user went on to cite. The
/// measure does not care which, only that it was chosen from THIS list.
pub fn credit(interleaved: &Interleaved, selected: &[u64]) -> Outcome {
    let mut outcome = Outcome::default();
    for id in selected {
        match interleaved.team_of(*id) {
            Some(Team::A) => outcome.a += 1,
            Some(Team::B) => outcome.b += 1,
            None => outcome.unattributed += 1,
        }
    }
    outcome
}

/// A stable seed for a query string, so an interleaving is reproducible
/// without the caller inventing one. FNV-1a: cheap and stable across
/// processes, which is the whole requirement.
pub fn seed_for(query: &str) -> u64 {
    let mut hash = 0xCBF2_9CE4_8422_2325u64;
    for byte in query.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01B3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: [u64; 5] = [1, 2, 3, 4, 5];
    const B: [u64; 5] = [6, 7, 8, 9, 10];

    #[test]
    fn same_seed_reproduces_the_same_interleaving() {
        let first = team_draft(&A, &B, 6, 42);
        let second = team_draft(&A, &B, 6, 42);
        assert_eq!(
            first, second,
            "interleaving must not depend on a coin we cannot replay"
        );
    }

    #[test]
    fn a_document_appears_once_even_when_both_rankings_have_it() {
        let a = [1u64, 2, 3];
        let b = [3u64, 2, 1];
        let merged = team_draft(&a, &b, 6, 7);
        let mut sorted = merged.ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), merged.ids.len(), "no id may be shown twice");
        assert_eq!(merged.ids.len(), 3, "the union is only three documents");
    }

    #[test]
    fn both_teams_contribute_within_one_result_of_each_other() {
        // The draft alternates, so exposure cannot be lopsided; that is
        // what keeps the comparison fair within a single query.
        for seed in 0..32u64 {
            let merged = team_draft(&A, &B, 6, seed);
            let a_count = merged.team.iter().filter(|t| **t == Team::A).count();
            let b_count = merged.team.len() - a_count;
            assert!(
                a_count.abs_diff(b_count) <= 1,
                "seed {seed}: {a_count} vs {b_count}"
            );
        }
    }

    #[test]
    fn the_coin_does_not_favour_one_team_across_queries() {
        // Determinism per query must not become bias across queries.
        let firsts = (0..200u64)
            .map(|q| team_draft(&A, &B, 4, seed_for(&format!("query {q}"))).team[0])
            .filter(|t| *t == Team::A)
            .count();
        assert!(
            (60..=140).contains(&firsts),
            "team A led {firsts}/200 queries, which is not a fair coin"
        );
    }

    #[test]
    fn credit_follows_the_contributing_team() {
        let merged = team_draft(&A, &B, 6, 1);
        // Credit every result the interleaving drew from B.
        let from_b: Vec<u64> = merged
            .ids
            .iter()
            .zip(&merged.team)
            .filter(|(_, t)| **t == Team::B)
            .map(|(id, _)| *id)
            .collect();
        let outcome = credit(&merged, &from_b);
        assert_eq!(outcome.a, 0);
        assert_eq!(outcome.b, from_b.len());
        assert_eq!(outcome.winner(), Some(Team::B));
    }

    #[test]
    fn selections_outside_the_list_are_reported_not_absorbed() {
        let merged = team_draft(&A, &B, 4, 3);
        let outcome = credit(&merged, &[9999]);
        assert_eq!(outcome.unattributed, 1);
        assert_eq!(
            outcome.winner(),
            None,
            "an unattributable click decides nothing"
        );
    }

    #[test]
    fn no_selections_is_a_tie_not_a_win() {
        let merged = team_draft(&A, &B, 4, 5);
        assert_eq!(credit(&merged, &[]).winner(), None);
    }

    #[test]
    fn one_empty_ranking_yields_the_other_intact() {
        let merged = team_draft(&A, &[], 5, 11);
        assert_eq!(merged.ids, A.to_vec());
        assert!(merged.team.iter().all(|t| *t == Team::A));
    }
}
