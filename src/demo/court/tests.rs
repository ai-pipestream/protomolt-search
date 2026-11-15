use super::*;

fn spans(v: &[(u32, u32)]) -> Vec<Span> {
    v.iter().map(|&(start, end)| Span { start, end }).collect()
}

#[test]
fn parse_opinion_extracts_fields() {
    let (id, cluster, text) =
        parse_opinion(r#"{"id": "4245481", "cluster_id": "4468228", "plain_text": "hello"}"#)
            .unwrap();
    assert_eq!(id, 4245481);
    assert_eq!(cluster, 4468228);
    assert_eq!(text, "hello");
    assert!(parse_opinion(r#"{"id": "x", "cluster_id": "1", "plain_text": "y"}"#).is_err());
}

#[test]
fn chunks_pack_sentences_up_to_target() {
    // Text: three sentences at [0,10), [10,20), [20,30); tokens 4/4/4.
    let text = "0123456789abcdefghijABCDEFGHIJ";
    let sentences = spans(&[(0, 10), (10, 20), (20, 30)]);
    let tokens = spans(&[
        (0, 1),
        (3, 4),
        (6, 7),
        (8, 9),
        (10, 11),
        (13, 14),
        (16, 17),
        (18, 19),
        (20, 21),
        (23, 24),
        (26, 27),
        (28, 29),
    ]);
    // Target 9 tokens: the first two sentences (4+4=8) pack into one
    // chunk; adding the third (12 total) exceeds, so one boundary
    // lands at 20.
    let chunks = assemble_chunks(text, &sentences, &tokens, 9);
    assert_eq!(chunks, spans(&[(0, 20), (20, 30)]));
    assert!(is_contiguous(text, &chunks));
    // Target 12: everything packs into one chunk.
    let chunks = assemble_chunks(text, &sentences, &tokens, 12);
    assert_eq!(chunks, spans(&[(0, 30)]));
    assert!(is_contiguous(text, &chunks));
}

#[test]
fn oversize_sentence_splits_at_token_boundaries() {
    let text = "0123456789abcdefghij";
    let sentences = spans(&[(0, 20)]);
    let tokens = spans(&[
        (0, 1),
        (2, 3),
        (4, 5),
        (6, 7),
        (8, 9),
        (10, 11),
        (12, 13),
        (14, 15),
    ]);
    // 8 tokens, target 3: splits at 3-token boundaries -> [0,6),[6,12),[12,20).
    let chunks = assemble_chunks(text, &sentences, &tokens, 3);
    assert_eq!(chunks, spans(&[(0, 6), (6, 12), (12, 20)]));
    assert!(is_contiguous(text, &chunks));
    let joined: String = chunks
        .iter()
        .map(|s| slice_chars(text, s.start, s.end))
        .collect();
    assert_eq!(
        joined, text,
        "contiguity: chunks must reproduce the original"
    );
}

#[test]
fn gap_free_invariant_holds_with_inter_sentence_whitespace() {
    // Sentence spans exclude the gaps; chunk boundaries must NOT.
    let text = "aa  bb  cc";
    let sentences = spans(&[(0, 2), (4, 6), (8, 10)]);
    let tokens = spans(&[(0, 2), (4, 6), (8, 10)]);
    let chunks = assemble_chunks(text, &sentences, &tokens, 2);
    assert!(is_contiguous(text, &chunks));
    let joined: String = chunks
        .iter()
        .map(|s| slice_chars(text, s.start, s.end))
        .collect();
    assert_eq!(joined, text);
}

#[test]
fn plan_chunks_packs_and_splits_with_block_attribution() {
    // Three blocks [0,10),[10,20),[20,30), 4 tokens each.
    let text = "0123456789abcdefghijABCDEFGHIJ";
    let sentences = spans(&[(0, 10), (10, 20), (20, 30)]);
    let tokens = spans(&[
        (0, 1),
        (3, 4),
        (6, 7),
        (8, 9),
        (10, 11),
        (13, 14),
        (16, 17),
        (18, 19),
        (20, 21),
        (23, 24),
        (26, 27),
        (28, 29),
    ]);
    // Target 9: first two blocks pack, third is its own chunk.
    let plans = plan_chunks(text, &sentences, &tokens, 9, 100);
    assert_eq!(plans.len(), 2);
    assert_eq!(plans[0].span, Span { start: 0, end: 20 });
    assert_eq!(plans[0].source, ChunkSource::Blocks { first: 0, last: 1 });
    assert_eq!(plans[1].span, Span { start: 20, end: 30 });
    assert_eq!(plans[1].source, ChunkSource::Blocks { first: 2, last: 2 });
    let spans: Vec<Span> = plans.iter().map(|p| p.span).collect();
    assert!(is_contiguous(text, &spans));
}

#[test]
fn plan_chunks_splits_oversized_block_into_solo_pieces() {
    let text = "0123456789abcdefghij";
    let sentences = spans(&[(0, 20)]);
    let tokens = spans(&[
        (0, 1),
        (2, 3),
        (4, 5),
        (6, 7),
        (8, 9),
        (10, 11),
        (12, 13),
        (14, 15),
    ]);
    // 8 tokens, cap 3: pieces [0,6),[6,12),[12,20), all SoloPiece.
    let plans = plan_chunks(text, &sentences, &tokens, 2, 3);
    assert_eq!(plans.len(), 3);
    assert!(plans.iter().all(|p| p.source == ChunkSource::SoloPiece));
    let spans: Vec<Span> = plans.iter().map(|p| p.span).collect();
    assert!(is_contiguous(text, &spans));
    let joined: String = spans
        .iter()
        .map(|s| slice_chars(text, s.start, s.end))
        .collect();
    assert_eq!(joined, text);
}

#[test]
fn pooling_is_token_weighted_mean() {
    let v1 = [1.0f32, 0.0];
    let v2 = [0.0f32, 1.0];
    // 3 tokens at v1, 1 token at v2 -> mean [0.75, 0.25], then
    // L2-normalized (the model's Normalize stage).
    let pooled = pool_block_vectors(&[(&v1, 3), (&v2, 1)], 2);
    let norm = (0.75f64 * 0.75 + 0.25 * 0.25).sqrt();
    assert!((f64::from(pooled[0]) - 0.75 / norm).abs() < 1e-6);
    assert!((f64::from(pooled[1]) - 0.25 / norm).abs() < 1e-6);
    let n: f32 = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!((n - 1.0).abs() < 1e-5);
    // Cosine sanity.
    assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-9);
    assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-9);
}

#[test]
fn chunk_and_embedding_files_round_trip() {
    let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target/tmp")
        .join(format!("tvcourt_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let chunks_path = dir.join("chunks.ndjson");
    {
        let mut writer = ChunkWriter::new(std::fs::File::create(&chunks_path).unwrap(), 0);
        writer
            .write_opinion_chunks(7, 9, 0, "abcdef", &spans(&[(0, 3), (3, 6)]))
            .unwrap();
        writer.finish().unwrap();
    }
    let chunks: Vec<Chunk> = read_chunks(&chunks_path)
        .unwrap()
        .collect::<std::io::Result<_>>()
        .unwrap();
    assert_eq!(chunks.len(), 2);
    assert_eq!(chunks[0].chunk_id, 0);
    assert_eq!(chunks[1].text, "def");
    assert_eq!(chunks[1].opinion_id, 7);
    let (records, max_line) = chunks_resume_state(&chunks_path).unwrap();
    assert_eq!(records, 2);
    assert_eq!(max_line, 1);

    let emb_path = dir.join("emb.bin");
    {
        let mut writer =
            EmbeddingWriter::create(std::fs::File::create(&emb_path).unwrap(), 2).unwrap();
        writer.write(7, 0, &[1.0, 2.0]).unwrap();
        writer.write(7, 1, &[3.0, 4.0]).unwrap();
        writer.finish().unwrap();
    }
    let keys = embedded_keys(&emb_path).unwrap();
    assert!(keys.contains(&(7, 1)));
    let (dim, reader) = EmbeddingReader::open(&emb_path).unwrap();
    assert_eq!(dim, 2);
    let records: Vec<EmbeddingRecord> = reader.collect::<std::io::Result<_>>().unwrap();
    assert_eq!(records[1].vector, vec![3.0, 4.0]);
    let _ = std::fs::remove_dir_all(&dir);
}
