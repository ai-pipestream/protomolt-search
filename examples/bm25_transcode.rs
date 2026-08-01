//! Streaming v3/v4 -> v5 transcode for `.bm25` sidecars
//! (`docs/block-max.md`): same corpus, block-max format, no
//! re-analysis. After the write, the output is reopened (which runs
//! the full structural validation including the skip-run walk) and
//! sampled term streams plus document metadata are compared against
//! the source before declaring success.
//!
//! Usage: bm25_transcode <src.bm25> <dst.bm25> [sample_terms]

use std::path::Path;
use std::time::Instant;

use turbovec_search::postings::{transcode_to_v5, Bm25Index, Bm25Reader};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 3 {
        eprintln!("usage: bm25_transcode <src.bm25> <dst.bm25> [sample_terms]");
        std::process::exit(2);
    }
    let src = Path::new(&args[1]);
    let dst = Path::new(&args[2]);
    let samples: u64 = args
        .get(3)
        .map(|s| s.parse().expect("sample_terms must be a number"))
        .unwrap_or(500);

    let t = Instant::now();
    let stats = transcode_to_v5(src, dst).expect("transcode failed");
    let secs = t.elapsed().as_secs_f64();
    println!(
        "transcoded {} terms / {} postings / {:.2} GB -> {:.2} GB in {:.0}s ({:.0} MB/s)",
        stats.n_terms,
        stats.postings,
        stats.bytes_in as f64 / 1e9,
        stats.bytes_out as f64 / 1e9,
        secs,
        stats.bytes_in as f64 / 1e6 / secs
    );

    let t = Instant::now();
    let a = Bm25Reader::open(src).expect("reopen source");
    let b = Bm25Reader::open(dst).expect("open output (full structural validation)");
    println!("both files validated in {:.0}s", t.elapsed().as_secs_f64());

    assert_eq!(a.next_doc_id(), b.next_doc_id(), "slot counts differ");
    assert_eq!(
        Bm25Index::doc_count(&a),
        Bm25Index::doc_count(&b),
        "doc counts differ"
    );
    assert_eq!(
        a.total_doc_length(),
        b.total_doc_length(),
        "total lengths differ"
    );
    assert_eq!(a.term_count(), b.term_count(), "term counts differ");

    // Sampled term streams: identical (doc, tf, offsets) sequences, and
    // the output must expose the block-max surface.
    let t = Instant::now();
    let n = a.term_count() as u64;
    let step = (n / samples.max(1)).max(1);
    let mut checked = 0u64;
    let mut postings = 0u64;
    let mut i = 0u64;
    while i < n {
        let term = a.term_at(i as u32);
        assert_eq!(term, b.term_at(i as u32), "term order diverged at {i}");
        assert_eq!(a.df(&term), b.df(&term), "df({term})");
        assert!(b.has_impacts(&term), "no impacts on {term}");
        let mut want = Vec::new();
        a.for_each_posting(&term, &mut |d, tf, o| want.push((d, tf, o.to_vec())));
        let mut got = Vec::new();
        b.for_each_posting(&term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
        assert_eq!(got, want, "posting stream differs for {term}");
        postings += want.len() as u64;
        checked += 1;
        i += step;
    }
    println!(
        "verified {checked} sampled terms / {postings} postings in {:.0}s",
        t.elapsed().as_secs_f64()
    );

    // Sampled document plane: lengths, texts, lineages.
    let slots = a.next_doc_id();
    let mut slot = 0u32;
    while slot < slots {
        assert_eq!(a.doc_length(slot), b.doc_length(slot), "doc_length({slot})");
        assert_eq!(
            Bm25Index::text(&a, slot),
            Bm25Index::text(&b, slot),
            "text({slot})"
        );
        assert_eq!(
            Bm25Index::lineage(&a, slot),
            Bm25Index::lineage(&b, slot),
            "lineage({slot})"
        );
        slot = slot.saturating_add((slots / 997).max(1));
    }
    println!("verified document plane samples; transcode GOOD");
}
