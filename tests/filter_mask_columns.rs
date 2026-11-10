//! The filter mask built from the columns (`docs/vector-filters.md`, "The
//! mask from the columns") is the row-by-row answer, on a shard whose rows
//! are spread over sealed segments, a frozen part and the tail, each with
//! its own facet dictionary.
use pipestream_search::boolean_bits::Bits;
use pipestream_search::filter::{ResolvedFilter, ResolvedLeaf, Tri};
use pipestream_search::filter_bits::{members, FacetSets};
use pipestream_search::postings::{AnalyzedDoc, Bm25Store};
use pipestream_search::scorefn::NumericRead;
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::segments::SegmentSource;

fn directory(tag: &str) -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("filter-mask-columns-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The shard's columns read forward, a row at a time: the oracle.
struct Forward<'a>(&'a SegmentedShard);

impl NumericRead for Forward<'_> {
    fn value(&self, _ni: usize, _doc: u32) -> Option<f64> {
        None
    }
    fn map_value(&self, _c: usize, _k: u32, _doc: u32) -> Option<f64> {
        None
    }
    fn map_int_value(&self, _c: usize, _k: u32, _doc: u32) -> Option<i64> {
        None
    }
    fn map_uint_value(&self, _c: usize, _k: u32, _doc: u32) -> Option<u64> {
        None
    }
    fn int_value(&self, ii: usize, doc: u32) -> Option<i64> {
        self.0.integer_value(ii, doc)
    }
    fn uint_value(&self, _ii: usize, _doc: u32) -> Option<u64> {
        None
    }
    fn geo_value(&self, _gi: usize, _doc: u32) -> Option<(f64, f64)> {
        None
    }
    fn facet_ord(&self, fi: usize, doc: u32) -> Option<u32> {
        self.0.facet_ord(fi, doc)
    }
    fn map_facet_value_ord(&self, _c: usize, _k: u32, _doc: u32) -> Option<u32> {
        None
    }
}

struct Rng(u64);
impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// Court of a row: one popular value (a bitmap in the inverted column),
/// many rare ones (lists), a value only late rows carry, and no value for
/// every ninth row.
fn court(row: usize) -> Option<String> {
    match row {
        r if r % 9 == 4 => None,
        r if r % 2 == 0 => Some("scotus".into()),
        r if r > 700 && r % 5 == 0 => Some("late".into()),
        r => Some(format!("ca{}", r % 23)),
    }
}

fn year(row: usize) -> Option<i64> {
    (row % 6 != 1).then_some(1990 + (row % 37) as i64)
}

fn check(shard: &SegmentedShard, stage: &str, rng: &mut Rng) {
    let n = shard.next_doc_id() as usize;
    let forward = Forward(shard);
    let ordinal = |value: &str| shard.facet_value_ord_of(0, value);
    // Every value's rows, and the rows holding any value.
    let mut any = Bits::empty(n);
    shard.facet_present(0, &mut any);
    let holding: Vec<usize> = (0..n).filter(|&row| court(row).is_some()).collect();
    assert_eq!(any.iter().collect::<Vec<_>>(), holding, "{stage}: present");
    for value in ["scotus", "late", "ca0", "ca7", "ca22", "nowhere"] {
        let mut got = Bits::empty(n);
        if let Some(ord) = ordinal(value) {
            shard.facet_rows(0, &|o| o == ord, &mut got);
        }
        let want: Vec<usize> = (0..n)
            .filter(|&row| court(row).as_deref() == Some(value))
            .collect();
        assert_eq!(got.iter().collect::<Vec<_>>(), want, "{stage}: {value}");
    }
    // Random trees over both columns, both verdicts, two domains.
    let values: Vec<u32> = ["scotus", "late", "ca0", "ca3", "ca7", "ca11", "ca22"]
        .iter()
        .filter_map(|value| ordinal(value))
        .collect();
    let leaf = |rng: &mut Rng| {
        ResolvedFilter::Leaf(match rng.below(4) {
            0 | 1 => {
                let mut ords: Vec<u32> = (0..1 + rng.below(3))
                    .map(|_| values[rng.below(values.len() as u64) as usize])
                    .collect();
                ords.sort_unstable();
                ords.dedup();
                ResolvedLeaf::Facet {
                    column: Some(0),
                    ords,
                }
            }
            2 => {
                let lo = 1985 + rng.below(40) as i64;
                ResolvedLeaf::IntRange {
                    column: 0,
                    lo,
                    hi: lo + rng.below(20) as i64,
                }
            }
            _ => ResolvedLeaf::Has {
                facet: Some(0),
                numeric: None,
                integer: None,
                unsigned_integer: None,
                geo: None,
            },
        })
    };
    fn tree(
        rng: &mut Rng,
        depth: u32,
        leaf: &dyn Fn(&mut Rng) -> ResolvedFilter,
    ) -> ResolvedFilter {
        if depth == 0 || rng.below(3) == 0 {
            return leaf(rng);
        }
        match rng.below(3) {
            0 => ResolvedFilter::And(
                (0..1 + rng.below(3))
                    .map(|_| tree(rng, depth - 1, leaf))
                    .collect(),
            ),
            1 => ResolvedFilter::Or(
                (0..1 + rng.below(3))
                    .map(|_| tree(rng, depth - 1, leaf))
                    .collect(),
            ),
            _ => ResolvedFilter::Not(Box::new(tree(rng, depth - 1, leaf))),
        }
    }
    let domains = [
        Bits::full(n),
        Bits::from_bools(&(0..n).map(|row| row % 4 != 2).collect::<Vec<_>>()),
    ];
    for round in 0..60 {
        let filter = tree(rng, 3, &leaf);
        for domain in &domains {
            for want in [true, false] {
                let got = members(&filter, want, domain, &forward, shard);
                let verdict = Tri::from(want);
                let expected: Vec<usize> = domain
                    .iter()
                    .filter(|&row| filter.eval(row as u32, &forward) == verdict)
                    .collect();
                assert_eq!(
                    got.iter().collect::<Vec<_>>(),
                    expected,
                    "{stage} round {round} want {want}: {filter:?}"
                );
            }
        }
    }
}

#[test]
fn the_mask_from_the_columns_is_the_row_by_row_mask_across_every_part() {
    let dir = directory("parts");
    let root = dir.join("catalog");
    let tail = || {
        Bm25Store::new()
            .with_facets(&["court"])
            .with_integers(&["year"])
    };
    let document = || AnalyzedDoc::body(vec![("opinion".into(), 1, vec![(0, 7)])], 1);
    let mut shard = SegmentedShard::open(&root, tail()).unwrap();
    let mut rng = Rng(0x00C0_1DAD_5EED);
    let add_rows = |shard: &mut SegmentedShard, upto: usize| {
        for row in shard.next_doc_id() as usize..upto {
            let local = row as u32 - shard.tail_base();
            shard
                .add_document(row as u32, "opinion".into(), document(), None)
                .unwrap();
            if let Some(value) = court(row) {
                shard.tail_mut().set_facet(0, local, &value);
            }
            if let Some(value) = year(row) {
                shard.tail_mut().set_integer(0, local, value);
            }
        }
        shard.sync_tail();
    };
    // Segments of different sizes, so the same value is a bitmap in one
    // part and a list in another.
    for (batch, rows) in [300usize, 37, 500].into_iter().enumerate() {
        let sealed_upto = shard.next_doc_id() as usize + rows;
        add_rows(&mut shard, sealed_upto);
        check(&shard, &format!("tail before seal {batch}"), &mut rng);
        // A seal takes the whole tail, the rows a previous round left there
        // included.
        let sealing = shard.next_doc_id() - shard.tail_base();
        let frozen = shard.freeze_tail(tail(), sealing).unwrap();
        // Rows arrive in the fresh tail while the frozen part is written.
        add_rows(&mut shard, sealed_upto + 20);
        check(&shard, &format!("frozen {batch}"), &mut rng);
        let (base, count, _) = shard.frozen().unwrap();
        let image = dir.join(format!("{batch}.bm25"));
        let live = dir.join(format!("{batch}.live"));
        frozen.save(&image).unwrap();
        pipestream_search::live_docs::LiveDocs::default()
            .write(&live, count as u64)
            .unwrap();
        let published = shard
            .catalog()
            .append(SegmentSource {
                segment_id: &format!("seg-{batch}"),
                generation: batch as u64 + 1,
                base_label: base as u64,
                backend_kind: "",
                vector_path: None,
                exact_vector_path: None,
                bm25_path: &image,
                live_docs_path: &live,
                partition_column: None,
            })
            .unwrap();
        shard.republish(published).unwrap();
        check(&shard, &format!("sealed {batch}"), &mut rng);
    }
    assert_eq!(shard.sealed_parts(), 3);
    // Reopened, the inverted columns are built again from the files.
    drop(shard);
    let reopened = SegmentedShard::open(&root, tail()).unwrap();
    check(&reopened, "reopened", &mut rng);
}

/// Not a test of behavior: prints what the mask costs each way on a shard
/// of a million rows in eight sealed segments and a tail.
/// `cargo test --release --test filter_mask_columns -- --ignored --nocapture`
#[test]
#[ignore = "a measurement, run by hand"]
fn measure_the_mask_from_the_columns() {
    let dir = directory("measure");
    let root = dir.join("catalog");
    let tail = || {
        Bm25Store::new()
            .with_facets(&["court"])
            .with_integers(&["year"])
    };
    let document = || AnalyzedDoc::body(vec![("opinion".into(), 1, vec![(0, 7)])], 1);
    // 50%, 10%, 1% and 0.1% of the rows, and a long tail of rare values.
    let court = |row: usize| -> String {
        let draw = (row as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> 44;
        match draw % 1000 {
            0..=499 => "half".into(),
            500..=599 => "tenth".into(),
            600..=609 => "hundredth".into(),
            610 => "thousandth".into(),
            other => format!("court-{}", other * 7 + draw % 7),
        }
    };
    let mut shard = SegmentedShard::open(&root, tail()).unwrap();
    let per_segment = 120_000usize;
    for batch in 0..9 {
        let rows = if batch == 8 { 40_000 } else { per_segment };
        for _ in 0..rows {
            let row = shard.next_doc_id() as usize;
            let local = row as u32 - shard.tail_base();
            shard
                .add_document(row as u32, "opinion".into(), document(), None)
                .unwrap();
            shard.tail_mut().set_facet(0, local, &court(row));
            shard
                .tail_mut()
                .set_integer(0, local, 1950 + (row % 75) as i64);
        }
        shard.sync_tail();
        if batch == 8 {
            break;
        }
        let frozen = shard.freeze_tail(tail(), rows as u32).unwrap();
        let (base, count, _) = shard.frozen().unwrap();
        let image = dir.join(format!("{batch}.bm25"));
        let live = dir.join(format!("{batch}.live"));
        frozen.save(&image).unwrap();
        pipestream_search::live_docs::LiveDocs::default()
            .write(&live, count as u64)
            .unwrap();
        let published = shard
            .catalog()
            .append(SegmentSource {
                segment_id: &format!("seg-{batch}"),
                generation: batch as u64 + 1,
                base_label: base as u64,
                backend_kind: "",
                vector_path: None,
                exact_vector_path: None,
                bm25_path: &image,
                live_docs_path: &live,
                partition_column: None,
            })
            .unwrap();
        shard.republish(published).unwrap();
    }
    let n = shard.next_doc_id() as usize;
    let forward = Forward(&shard);
    let all = Bits::full(n);
    let facet = |value: &str| {
        ResolvedFilter::Leaf(ResolvedLeaf::Facet {
            column: Some(0),
            ords: vec![shard.facet_value_ord_of(0, value).unwrap()],
        })
    };
    let years = ResolvedFilter::Leaf(ResolvedLeaf::IntRange {
        column: 0,
        lo: 2000,
        hi: 2010,
    });
    let cases = [
        ("court = half (50%)", facet("half")),
        ("court = tenth (10%)", facet("tenth")),
        ("court = hundredth (1%)", facet("hundredth")),
        ("court = thousandth (0.1%)", facet("thousandth")),
        (
            "court = hundredth AND year in 2000..=2010",
            ResolvedFilter::And(vec![years.clone(), facet("hundredth")]),
        ),
        (
            "NOT court = half",
            ResolvedFilter::Not(Box::new(facet("half"))),
        ),
        ("year in 2000..=2010 (no inverted column)", years.clone()),
    ];
    let started = std::time::Instant::now();
    let mut first = Bits::empty(n);
    shard.facet_present(0, &mut first);
    println!(
        "{n} rows; inverting the column in 8 segments, once: {:.1} ms",
        started.elapsed().as_secs_f64() * 1e3
    );
    for (name, filter) in &cases {
        let time = |run: &dyn Fn() -> u64| {
            let mut best = f64::INFINITY;
            let mut count = 0;
            for _ in 0..5 {
                let started = std::time::Instant::now();
                count = run();
                best = best.min(started.elapsed().as_secs_f64() * 1e3);
            }
            (best, count)
        };
        let (rows_ms, rows_count) = time(&|| {
            (0..n as u32)
                .filter(|&row| filter.eval(row, &forward) == Tri::True)
                .count() as u64
        });
        let (bits_ms, bits_count) = time(&|| members(filter, true, &all, &forward, &shard).count());
        assert_eq!(rows_count, bits_count);
        println!(
            "{name}: {bits_count} rows; row by row {rows_ms:.2} ms, from the columns {bits_ms:.3} ms ({:.0}x)",
            rows_ms / bits_ms
        );
    }
}
