//! Pure-Rust lexical, aligned-view offset, and wire-protocol conformance across
//! `native` (`protomolt-analyzer`), in-process `turbo` (`turbo-client`), and
//! remote `turbo-kserve` (`turbo.result.v1.AnalysisService`) (`#103`).

mod common;

use pipestream_search::analyzer::{
    analyze_document, analyze_document_with_options, body_spec, turbo_handshake, AnalysisBackend,
    RemoteCallOptions, SessionLayers, CHAR_FILTER_ACCENT_FOLD, CHAR_FILTER_DEHYPHENATE,
    CHAR_FILTER_FULL_CASE_FOLD, CHAR_FILTER_HTML_ENTITY, CHAR_FILTER_HTML_STRIP, CHAR_FILTER_NFKC,
    CHAR_FILTER_STRIP_INVISIBLE, CHAR_FILTER_WHITESPACE, NATIVE_ANALYSIS_BACKEND,
    SOURCE_NORMALIZED_STEMS, STEMMER_PORTER, TERM_VECTOR_MODE_FULL, TOKENIZER_UAX29,
    TURBO_ANALYSIS_BACKEND,
};
use pipestream_search::pb::AnalysisSpec;
use turbo_client::{
    FieldAnalysisConfig, LexicalAnalyzer, NormalizerKind, StemmerKind, TermSourceKind,
    TokenizerKind,
};

/// Verifies that no legacy JVM OpenNLP sidecar dependency, environment knob,
/// or container build reference remains in `Cargo.toml`, `src/`, `examples/`,
/// `deploy/court-e2e/`, or `deploy/v7-rebuild/`.
#[test]
fn legacy_jvm_opennlp_sidecar_is_completely_retired() {
    fn collect_files(path: &std::path::Path, out: &mut Vec<std::path::PathBuf>) {
        if path.is_file() {
            out.push(path.to_path_buf());
        } else if path.is_dir() {
            let mut entries: Vec<_> = std::fs::read_dir(path)
                .unwrap_or_else(|e| panic!("read_dir {}: {e}", path.display()))
                .map(|r| r.unwrap().path())
                .collect();
            entries.sort();
            for entry in entries {
                collect_files(&entry, out);
            }
        }
    }

    let manifest_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"));
    let cargo_toml = std::fs::read_to_string(manifest_dir.join("Cargo.toml")).unwrap();
    assert!(
        cargo_toml.contains("turbo-client"),
        "Cargo.toml must depend on turbo-client"
    );

    let mut files = vec![
        manifest_dir.join("Cargo.toml"),
        manifest_dir.join("Dockerfile"),
    ];
    for rel in ["src", "examples", "deploy/court-e2e", "deploy/v7-rebuild"] {
        collect_files(&manifest_dir.join(rel), &mut files);
    }

    const BANNED: &[&str] = &[
        "grpc-opennlp-analysis",
        "opennlp-sidecar",
        "OPENNLP_ANALYSIS_REPO",
        "OPENNLP_EMBEDDINGS_DIR",
        "JAVA_OPTS",
        "gradlew",
    ];
    for file in files {
        let Ok(contents) = std::fs::read_to_string(&file) else {
            continue;
        };
        for needle in BANNED {
            assert!(
                !contents.contains(needle),
                "{} still contains retired JVM sidecar token {needle:?}",
                file.display()
            );
        }
    }
}

/// Verifies `native`, in-process `turbo`, and remote `AnalysisService` produce
/// valid `ViewId::SOURCE` UTF-8 byte spans across multi-byte emoji, invisible
/// controls, and diacritic folding.
#[tokio::test]
async fn native_turbo_and_remote_preserve_original_utf8_source_offsets() {
    let text = "😀\u{200B} Café Rodríguez appealed the inter-\nnational court’s ﬁnance order.";
    let spec = body_spec();

    let native_doc = analyze_document(NATIVE_ANALYSIS_BACKEND, text, Some(&spec))
        .await
        .expect("native analysis succeeds");
    let turbo_doc = analyze_document(TURBO_ANALYSIS_BACKEND, text, Some(&spec))
        .await
        .expect("in-process turbo analysis succeeds");

    let (mock_addr, mock_handle) = common::mock::start_mock_analysis().await;
    let remote_doc = analyze_document(&mock_addr, text, Some(&spec))
        .await
        .expect("remote AnalysisService succeeds");
    mock_handle.abort();

    for (label, doc) in [
        ("native", &native_doc),
        ("turbo", &turbo_doc),
        ("remote", &remote_doc),
    ] {
        let field = &doc.fields[0];
        assert!(!field.terms.is_empty(), "{label} must emit terms");
        field
            .check_positions()
            .unwrap_or_else(|e| panic!("{label} positions invalid: {e}"));
        for (term, tf, occs) in &field.terms {
            assert_eq!(
                *tf as usize,
                occs.len(),
                "{label} term {term:?} tf must match occurrence count"
            );
            for &(start, end) in occs {
                let s = start as usize;
                let e = end as usize;
                assert!(
                    s < e && e <= text.len(),
                    "{label} term {term:?} span ({s}, {e}) out of bounds for len {}",
                    text.len()
                );
                assert!(
                    text.is_char_boundary(s) && text.is_char_boundary(e),
                    "{label} term {term:?} span ({s}, {e}) splits UTF-8 char boundary"
                );
                let surface = &text[s..e];
                assert!(
                    !surface.trim().is_empty(),
                    "{label} term {term:?} mapped to empty source slice"
                );
            }
        }
    }

    // Verify exact Café and Rodríguez source slices under accent + case folding in `turbo`.
    let turbo_field = &turbo_doc.fields[0];
    let rodriguez = turbo_field
        .terms
        .iter()
        .find(|(t, _, _)| t.starts_with("rodriguez"))
        .expect("Rodríguez folded + stemmed");
    let (r_start, r_end) = rodriguez.2[0];
    assert_eq!(&text[r_start as usize..r_end as usize], "Rodríguez");
}

/// Verifies aligned-view UTF-8, UTF-16, and Unicode code-point span projection
/// across NFKC ligature expansion (`ﬁ`), HTML tag/entity stripping, invisible
/// stripping, and line-break dehyphenation (`#103`, `#2337`, `#2340`).
#[tokio::test]
async fn aligned_view_html_nfkc_and_dehyphenation_project_utf8_and_utf16_cleanly() {
    // Surrogate-pair emoji (2 UTF-16 code units, 4 UTF-8 bytes, 1 scalar value)
    // followed by HTML markup, HTML entity, NFKC ligature `ﬁ`, and line-break hyphen.
    let source = "🚀 <b>R&amp;D ﬁnance</b> reviewed inter-\nnational filings.";

    let spec = AnalysisSpec {
        tokenizer: TOKENIZER_UAX29,
        stemmer: STEMMER_PORTER,
        term_vector_mode: TERM_VECTOR_MODE_FULL,
        term_vector_source: SOURCE_NORMALIZED_STEMS,
        char_filters: vec![
            CHAR_FILTER_STRIP_INVISIBLE,
            CHAR_FILTER_HTML_STRIP,
            CHAR_FILTER_HTML_ENTITY,
            CHAR_FILTER_NFKC,
            CHAR_FILTER_ACCENT_FOLD,
            CHAR_FILTER_FULL_CASE_FOLD,
            CHAR_FILTER_DEHYPHENATE,
            CHAR_FILTER_WHITESPACE,
        ],
    };

    let doc = analyze_document_with_options(
        TURBO_ANALYSIS_BACKEND,
        source,
        Some(&spec),
        SessionLayers {
            sentences: true,
            ..Default::default()
        },
        &RemoteCallOptions::default(),
    )
    .await
    .expect("turbo aligned-view analysis");

    let field = &doc.fields[0];
    field.check_positions().expect("valid lattice positions");
    field
        .check_sentences()
        .expect("sentences cover every occurrence");

    // `ﬁnance` (NFKC -> `finance` -> stem `financ`) maps back to exact `ﬁnance` in `source`.
    let financ_idx = field
        .terms
        .iter()
        .position(|(t, _, _)| t == "financ")
        .expect("NFKC ligature stem `financ` present");
    let (f_start, f_end) = field.terms[financ_idx].2[0];
    assert_eq!(&source[f_start as usize..f_end as usize], "ﬁnance");

    // `inter-\nnational` emits BOTH the joined compound stem `intern` (`pos_len = 2`)
    // spanning `"inter-\nnational"` AND the split component arcs (`inter`, `nation`).
    let positions = field.positions.as_ref().expect("positions present");
    let pos_lengths = field
        .pos_lengths
        .as_ref()
        .expect("dehyphenated lattice emits pos_lengths");

    let joined_idx = field
        .terms
        .iter()
        .position(|(t, _, _)| t == "intern")
        .expect("joined stem `intern` present");
    assert_eq!(pos_lengths[joined_idx], vec![2]);
    let (j_start, j_end) = field.terms[joined_idx].2[0];
    assert_eq!(
        &source[j_start as usize..j_end as usize],
        "inter-\nnational"
    );

    let left_idx = field
        .terms
        .iter()
        .position(|(t, _, _)| t == "inter")
        .expect("split left arc `inter`");
    let right_idx = field
        .terms
        .iter()
        .position(|(t, _, _)| t == "nation")
        .expect("split right arc `nation`");
    assert_eq!(positions[left_idx][0] + 1, positions[right_idx][0]);
    assert_eq!(positions[joined_idx][0], positions[left_idx][0]);

    // Direct `LexicalAnalyzer` verification of UTF-16 & code-point coordinates
    // after the 4-byte / 2-UTF16-unit `🚀` emoji!
    let lex = LexicalAnalyzer::new(FieldAnalysisConfig {
        normalizers: vec![
            NormalizerKind::StripInvisible,
            NormalizerKind::HtmlTagStrip,
            NormalizerKind::HtmlEntityDecode,
            NormalizerKind::Nfkc,
            NormalizerKind::AccentFold,
            NormalizerKind::FullCaseFold,
            NormalizerKind::Dehyphenate,
        ],
        tokenizer: TokenizerKind::Uax29,
        stemmer: StemmerKind::Porter,
        term_source: TermSourceKind::NormalizedStems,
        sentence_language: "eng".into(),
        emit_lattice: true,
    });
    let direct = lex.analyze(source).expect("direct LexicalAnalyzer");
    let utf16_units: Vec<u16> = source.encode_utf16().collect();
    let financ_arc = direct
        .arcs
        .iter()
        .find(|a| a.term == "financ")
        .expect("financ arc");
    let (u16_s, u16_e) = financ_arc.spans.utf16;
    let decoded_u16 = String::from_utf16(&utf16_units[u16_s as usize..u16_e as usize]).unwrap();
    assert_eq!(decoded_u16, "ﬁnance");

    let chars: Vec<char> = source.chars().collect();
    let (cp_s, cp_e) = financ_arc.spans.code_point;
    let decoded_cp: String = chars[cp_s as usize..cp_e as usize].iter().collect();
    assert_eq!(decoded_cp, "ﬁnance");

    // Verify backend parser & cryptographic handshake determinism.
    assert_eq!(
        AnalysisBackend::parse("turbo").unwrap(),
        AnalysisBackend::Turbo
    );
    assert_eq!(
        AnalysisBackend::parse("native").unwrap(),
        AnalysisBackend::Native
    );
    let hs1 = turbo_handshake(Some(&spec), None).unwrap();
    let hs2 = turbo_handshake(Some(&spec), None).unwrap();
    assert_eq!(hs1, hs2);
}
