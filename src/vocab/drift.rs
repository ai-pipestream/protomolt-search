//! Drift between two windows: novelty, divergence, and embedding
//! vocabulary coverage.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::window::WindowState;
use crate::pb::storage::{VocabChannel, VocabSnapshot};

/// The drift metrics of one channel comparison.
#[derive(Debug, Clone)]
pub struct DriftMetrics {
    /// Estimated distinct-term count of the older window.
    pub from_cardinality: f64,
    /// Estimated distinct-term count of the newer window.
    pub to_cardinality: f64,
    /// Estimated distinct-term count of the union.
    pub union_cardinality: f64,
    /// `(|union| − |from|) / |to|`, in [0, 1].
    pub novelty_rate: f64,
    /// JS divergence (base 2) over the union of the two heavy-hitter
    /// lists, in [0, 1].
    pub jensen_shannon_divergence: f64,
    /// Whether `embedding_oov_share` is meaningful.
    pub embedding_oov_computed: bool,
    /// Share of the newer window's heavy-hitter token mass absent from
    /// the embedding vocabulary.
    pub embedding_oov_share: f64,
}

/// Computes the drift of `to` (the newer window) relative to `from` (the
/// older window). `embedding_vocabulary` is `Some` only for the TOKENS
/// channel when a model vocabulary was readable.
pub fn compute_drift(
    from: &WindowState,
    to: &WindowState,
    embedding_vocabulary: Option<&HashSet<String>>,
) -> DriftMetrics {
    // Union cardinality by inclusion-exclusion over merged registers: the
    // whole point of HyperLogLog mergeability.
    let mut merged = from.cardinality_sketch();
    // Same-precision merge; a mismatch means the snapshots come from
    // different sketch generations and the union falls back to the sum's
    // upper bound rather than a wrong answer.
    let union_cardinality = match merged.merge(&to.cardinality_sketch()) {
        Ok(()) => merged.estimate(),
        Err(_) => from.cardinality_estimate() + to.cardinality_estimate(),
    };
    let from_cardinality = from.cardinality_estimate();
    let to_cardinality = to.cardinality_estimate();
    let novelty_rate = if to_cardinality > 0.0 {
        ((union_cardinality - from_cardinality) / to_cardinality).max(0.0)
    } else {
        0.0
    };
    let divergence = jensen_shannon(from, to);
    let oov_computed = embedding_vocabulary.is_some() && to.occurrences() > 0;
    let oov_share = match embedding_vocabulary.filter(|_| oov_computed) {
        Some(vocabulary) => oov_share(to, vocabulary),
        None => 0.0,
    };
    DriftMetrics {
        from_cardinality,
        to_cardinality,
        union_cardinality,
        novelty_rate,
        jensen_shannon_divergence: divergence,
        embedding_oov_computed: oov_computed,
        embedding_oov_share: oov_share,
    }
}

/// Jensen-Shannon divergence (base 2, so bounded by 1) between the two term
/// distributions over the union of their heavy-hitter lists. Counts for a
/// term missing from one list come from that window's count-min sketch, so
/// the comparison sees the tail too, at sketch accuracy.
fn jensen_shannon(from: &WindowState, to: &WindowState) -> f64 {
    let mut union: Vec<String> = Vec::new();
    let mut seen: HashSet<String> = HashSet::new();
    for entry in from.heavy_hitters().into_iter().chain(to.heavy_hitters()) {
        if seen.insert(entry.term.clone()) {
            union.push(entry.term);
        }
    }
    let mut total_from = 0u64;
    let mut total_to = 0u64;
    let mut counts_from = Vec::with_capacity(union.len());
    let mut counts_to = Vec::with_capacity(union.len());
    for term in &union {
        let cf = from.estimate(term);
        let ct = to.estimate(term);
        total_from += cf;
        total_to += ct;
        counts_from.push(cf);
        counts_to.push(ct);
    }
    if total_from == 0 && total_to == 0 {
        return 0.0;
    }
    if total_from == 0 || total_to == 0 {
        // One side has no mass at all: maximal divergence.
        return 1.0;
    }
    let mut divergence = 0.0f64;
    for j in 0..counts_from.len() {
        let p = counts_from[j] as f64 / total_from as f64;
        let q = counts_to[j] as f64 / total_to as f64;
        let m = (p + q) / 2.0;
        if p > 0.0 {
            divergence += 0.5 * p * (p / m).log2();
        }
        if q > 0.0 {
            divergence += 0.5 * q * (q / m).log2();
        }
    }
    divergence
}

/// The out-of-vocabulary share of the window's token mass, computed over
/// its heavy-hitter list: the tail beyond top-K contributes negligible
/// mass on a realistically skewed corpus, which is the same premise the JS
/// divergence runs on.
fn oov_share(window: &WindowState, vocabulary: &HashSet<String>) -> f64 {
    let mut mass = 0u64;
    let mut oov = 0u64;
    for entry in window.heavy_hitters() {
        mass += entry.count;
        if !vocabulary.contains(&entry.term) {
            oov += entry.count;
        }
    }
    if mass > 0 {
        oov as f64 / mass as f64
    } else {
        0.0
    }
}

/// Loads the surface forms an embedding model covers, read from the model
/// directory rather than from a private API: a `vocab.txt` with one token
/// per line, or a HuggingFace-style `tokenizer.json` whose `model.vocab`
/// maps tokens to ids (BPE-style object) or lists `[token, score]` pairs
/// (Unigram-style array); both yield their tokens.
///
/// Absence is a first-class outcome: no readable vocabulary means the
/// out-of-vocabulary share is not computable, and the drift report says so
/// instead of inventing a number.
pub fn load_embedding_vocabulary(dir: &Path) -> Option<HashSet<String>> {
    let vocab_txt = dir.join("vocab.txt");
    if vocab_txt.is_file() {
        match std::fs::read_to_string(&vocab_txt) {
            Ok(text) => {
                let vocabulary: HashSet<String> = text.lines().map(str::to_string).collect();
                eprintln!(
                    "vocab: loaded embedding vocabulary from {} ({} entries)",
                    vocab_txt.display(),
                    vocabulary.len()
                );
                return Some(vocabulary);
            }
            Err(e) => {
                eprintln!(
                    "vocab: could not read embedding vocabulary from {}: {e}",
                    vocab_txt.display()
                );
                return None;
            }
        }
    }
    let tokenizer_json = dir.join("tokenizer.json");
    if tokenizer_json.is_file() {
        let vocabulary = std::fs::read_to_string(&tokenizer_json)
            .ok()
            .and_then(|text| parse_model_vocab(&text));
        match &vocabulary {
            Some(v) => eprintln!(
                "vocab: loaded embedding vocabulary from {} ({} entries)",
                tokenizer_json.display(),
                v.len()
            ),
            None => eprintln!(
                "vocab: {} carries no readable model.vocab; embedding coverage is not computable",
                tokenizer_json.display()
            ),
        }
        return vocabulary;
    }
    None
}

/// Extracts the tokens of the `"vocab"` member of `"model"` from a
/// tokenizer.json. Two shapes exist in the wild: an object mapping token
/// to id (BPE-style) and an array of `[token, score]` pairs
/// (Unigram-style); both yield their tokens here.
fn parse_model_vocab(text: &str) -> Option<HashSet<String>> {
    let json: serde_json::Value = serde_json::from_str(text).ok()?;
    let vocab = json.get("model")?.get("vocab")?;
    match vocab {
        serde_json::Value::Object(map) => Some(map.keys().cloned().collect()),
        serde_json::Value::Array(pairs) => Some(
            pairs
                .iter()
                .filter_map(|pair| pair.as_array()?.first()?.as_str().map(str::to_string))
                .collect(),
        ),
        _ => None,
    }
}

/// The drift metrics of one channel comparison, tagged with its channel.
#[derive(Debug, Clone)]
pub struct ChannelDrift {
    /// Which channel these metrics describe.
    pub channel: VocabChannel,
    /// The metrics.
    pub metrics: DriftMetrics,
}

/// Restores the per-channel window states of a persisted snapshot.
pub fn states_by_channel(
    snapshot: &VocabSnapshot,
) -> Result<HashMap<VocabChannel, WindowState>, String> {
    let mut states = HashMap::with_capacity(snapshot.channels.len());
    for channel_snapshot in &snapshot.channels {
        let state = WindowState::from_proto(channel_snapshot)?;
        states.insert(state.channel(), state);
    }
    Ok(states)
}

/// Computes the drift of the `to` snapshot relative to the `from`
/// snapshot, per channel — the offline form of
/// [`super::VocabularyListener::drift`]. `embedding_vocabulary` applies to the
/// TOKENS channel only.
pub fn compute_channel_drift(
    from: &VocabSnapshot,
    to: &VocabSnapshot,
    embedding_vocabulary: Option<&HashSet<String>>,
) -> Result<Vec<ChannelDrift>, String> {
    let from = states_by_channel(from)?;
    let to = states_by_channel(to)?;
    let mut out = Vec::with_capacity(2);
    for channel in [VocabChannel::Terms, VocabChannel::Tokens] {
        let from_state = from
            .get(&channel)
            .ok_or_else(|| format!("from snapshot has no {channel:?} channel"))?;
        let to_state = to
            .get(&channel)
            .ok_or_else(|| format!("to snapshot has no {channel:?} channel"))?;
        let vocabulary = if channel == VocabChannel::Tokens {
            embedding_vocabulary
        } else {
            None
        };
        out.push(ChannelDrift {
            channel,
            metrics: compute_drift(from_state, to_state, vocabulary),
        });
    }
    Ok(out)
}
