use std::path::PathBuf;
use std::time::UNIX_EPOCH;

use super::{
    load_embedding_vocabulary, load_snapshot_file, ChannelDrift, DriftMetrics, VocabularyListener,
    WindowState, LIVE,
};
use crate::pb::storage::VocabChannel;

/// Unique temp directory per test (the config tests' `target/tmp`
/// pattern; no tempfile dependency in this crate).
pub(super) fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(format!(
        "vocab_test_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&path).unwrap();
    path
}

pub(super) fn feed_terms(listener: &VocabularyListener, terms: &[(&str, i64)], tokens: &[&str]) {
    listener.feed(terms.iter().copied(), tokens.iter().copied());
}

fn channel_drift(drift: &[ChannelDrift], channel: VocabChannel) -> &DriftMetrics {
    &drift.iter().find(|d| d.channel == channel).unwrap().metrics
}

// ---- Window/listener machinery (mirrors VocabularyListenerTest) ----

#[test]
fn rollover_persists_a_snapshot_and_starts_fresh() {
    let dir = temp_dir("rollover");
    let listener = VocabularyListener::create(&dir, 3, 16).unwrap();
    for _ in 0..3 {
        feed_terms(
            &listener,
            &[("alpha", 2), ("beta", 1)],
            &["Alpha", "beta", "alpha"],
        );
    }

    // The third document rolls the window over: a snapshot file exists
    // and the live window is empty again.
    let snapshots_on_disk = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.starts_with("snapshot-") && name.ends_with(".pb")
        })
        .count();
    assert_eq!(snapshots_on_disk, 1);
    assert_eq!(listener.window_documents(), 0);
    assert_eq!(listener.snapshots().len(), 1);

    // The next document opens the next window.
    feed_terms(&listener, &[("gamma", 1)], &["gamma"]);
    assert_eq!(listener.window_documents(), 1);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn snapshot_round_trip_keeps_identical_estimates() {
    let dir = temp_dir("roundtrip");
    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    for _ in 0..10 {
        feed_terms(
            &listener,
            &[("court", 5), ("ruling", 2), ("appeal", 1)],
            &["Court", "court", "ruling"],
        );
    }
    let live_cardinality = listener
        .live_stats()
        .iter()
        .find(|s| s.channel == VocabChannel::Terms as i32)
        .unwrap()
        .cardinality_estimate;

    let meta = listener.snapshot_now().expect("window was fed");

    // Round-trip through the persisted bytes: same cardinality, same
    // heavy hitters, same count-min point estimates.
    let snapshot = load_snapshot_file(&dir.join(&meta.name)).unwrap();
    let channel = snapshot
        .channels
        .iter()
        .find(|c| c.channel == VocabChannel::Terms as i32)
        .unwrap();
    let restored = WindowState::from_proto(channel).unwrap();
    assert_eq!(restored.cardinality_estimate(), live_cardinality);
    assert_eq!(restored.documents(), 10);
    assert_eq!(restored.occurrences(), 80);
    assert_eq!(restored.estimate("court"), 50);
    assert_eq!(restored.estimate("ruling"), 20);
    assert_eq!(restored.estimate("appeal"), 10);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn explicit_snapshot_on_an_empty_window_persists_nothing() {
    let dir = temp_dir("empty");
    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    assert!(listener.snapshot_now().is_none());
    assert!(listener.snapshots().is_empty());
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn disjoint_windows_have_novelty_one() {
    let dir = temp_dir("disjoint");
    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    for _ in 0..5 {
        feed_terms(&listener, &[("alpha", 2), ("beta", 1)], &["alpha", "beta"]);
    }
    let older = listener.snapshot_now().unwrap().name;
    for _ in 0..5 {
        feed_terms(
            &listener,
            &[("gamma", 2), ("delta", 1)],
            &["gamma", "delta"],
        );
    }

    let drift = listener.drift(&older, LIVE, None).unwrap();
    let terms = channel_drift(&drift, VocabChannel::Terms);
    assert!(terms.novelty_rate > 0.9, "{}", terms.novelty_rate);
    // Disjoint vocabularies are also maximally divergent distributions.
    assert!(terms.jensen_shannon_divergence > 0.9);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn identical_distributions_have_no_drift() {
    let dir = temp_dir("identical");
    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    for _ in 0..5 {
        feed_terms(&listener, &[("alpha", 2), ("beta", 1)], &["alpha", "beta"]);
    }
    let older = listener.snapshot_now().unwrap().name;
    for _ in 0..5 {
        feed_terms(&listener, &[("alpha", 2), ("beta", 1)], &["alpha", "beta"]);
    }

    let drift = listener.drift(&older, LIVE, None).unwrap();
    let terms = channel_drift(&drift, VocabChannel::Terms);
    assert!(terms.novelty_rate < 0.05, "{}", terms.novelty_rate);
    assert!(terms.jensen_shannon_divergence < 0.01);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn shifted_frequencies_have_positive_divergence_but_no_novelty() {
    let dir = temp_dir("shifted");
    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    for _ in 0..5 {
        feed_terms(&listener, &[("alpha", 10), ("beta", 1)], &["alpha", "beta"]);
    }
    let older = listener.snapshot_now().unwrap().name;
    for _ in 0..5 {
        feed_terms(&listener, &[("alpha", 1), ("beta", 10)], &["alpha", "beta"]);
    }

    let drift = listener.drift(&older, LIVE, None).unwrap();
    let terms = channel_drift(&drift, VocabChannel::Terms);
    assert!(terms.novelty_rate < 0.05, "{}", terms.novelty_rate);
    assert!(terms.jensen_shannon_divergence > 0.05);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn embedding_coverage_reads_the_model_vocabulary() {
    let dir = temp_dir("coverage");
    let embeddings = dir.join("embeddings");
    std::fs::create_dir_all(&embeddings).unwrap();
    std::fs::write(embeddings.join("vocab.txt"), "alpha\nbeta\n").unwrap();
    let vocabulary = load_embedding_vocabulary(&embeddings).expect("vocab.txt loads");
    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    for _ in 0..5 {
        // Covered: alpha x2, beta x1 per doc. OOV: gamma x1 per doc.
        feed_terms(
            &listener,
            &[("alpha", 2), ("beta", 1), ("gamma", 1)],
            &["alpha", "alpha", "beta", "gamma"],
        );
    }
    let older = listener.snapshot_now().unwrap().name;
    for _ in 0..5 {
        feed_terms(
            &listener,
            &[("alpha", 2), ("beta", 1), ("gamma", 1)],
            &["alpha", "alpha", "beta", "gamma"],
        );
    }

    let drift = listener.drift(&older, LIVE, Some(&vocabulary)).unwrap();
    let tokens = channel_drift(&drift, VocabChannel::Tokens);
    assert!(tokens.embedding_oov_computed);
    // One of four token occurrences per document is OOV.
    assert!((tokens.embedding_oov_share - 0.25).abs() < 0.01);
    // Coverage is a TOKENS property; TERMS never reports it.
    assert!(!channel_drift(&drift, VocabChannel::Terms).embedding_oov_computed);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn tokenizer_json_vocab_is_read_too() {
    let dir = temp_dir("tokenizer");
    let embeddings = dir.join("embeddings");
    std::fs::create_dir_all(&embeddings).unwrap();
    std::fs::write(
        embeddings.join("tokenizer.json"),
        r#"{"model": {"type": "BPE", "vocab": {"alpha": 0, "beä": 1}}}"#,
    )
    .unwrap();
    let vocabulary = load_embedding_vocabulary(&embeddings).expect("tokenizer.json loads");
    assert!(vocabulary.contains("alpha"));
    assert!(vocabulary.contains("beä"));

    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    feed_terms(&listener, &[("alpha", 1)], &["alpha", "gamma"]);

    let drift = listener.drift(LIVE, LIVE, Some(&vocabulary)).unwrap();
    let tokens = channel_drift(&drift, VocabChannel::Tokens);
    assert!(tokens.embedding_oov_computed);
    assert!((tokens.embedding_oov_share - 0.5).abs() < 0.01);
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn unknown_snapshot_references_are_rejected() {
    let dir = temp_dir("unknown");
    let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    feed_terms(&listener, &[("alpha", 1)], &["alpha"]);
    let err = listener.drift("snapshot-99-1.pb", LIVE, None).unwrap_err();
    assert!(err.contains("unknown snapshot"), "{err}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn restart_resumes_the_snapshot_history() {
    let dir = temp_dir("restart");
    {
        let listener = VocabularyListener::create(&dir, 1_000, 16).unwrap();
        feed_terms(&listener, &[("alpha", 1)], &["alpha"]);
        listener.snapshot_now();
    }

    let resumed = VocabularyListener::create(&dir, 1_000, 16).unwrap();
    assert_eq!(resumed.snapshots().len(), 1);
    assert_eq!(resumed.snapshots()[0].documents, 1);
    // The next snapshot continues the sequence rather than clobbering.
    feed_terms(&resumed, &[("beta", 1)], &["beta"]);
    assert_eq!(resumed.snapshot_now().unwrap().sequence, 1);
    std::fs::remove_dir_all(&dir).ok();
}
