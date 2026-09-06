//! Print a `.bm25` store's field and column tables, in index order: the
//! tables a segment declares and a node compares with its own by position.
//!
//! Usage: `bm25_tables <file.bm25>...`
use pipestream_search::postings::Bm25Reader;
fn main() {
    for arg in std::env::args().skip(1) {
        let r = match Bm25Reader::open(std::path::Path::new(&arg)) {
            Ok(r) => r,
            Err(e) => {
                println!("{arg}: {e}");
                continue;
            }
        };
        println!("{arg}");
        let fields: Vec<String> = (0..r.field_count())
            .map(|f| {
                format!(
                    "{}{}{}",
                    r.field_name(f),
                    if r.field_has_positions(f) { "+pos" } else { "" },
                    if r.field_has_sentences(f) {
                        "+sent"
                    } else {
                        ""
                    }
                )
            })
            .collect();
        println!("  fields   {fields:?}");
        println!(
            "  facets   {:?}",
            (0..r.facet_count())
                .map(|i| r.facet_name(i))
                .collect::<Vec<_>>()
        );
        println!(
            "  integers {:?}",
            (0..r.integer_count())
                .map(|i| r.integer_name(i))
                .collect::<Vec<_>>()
        );
        println!(
            "  unsigned {:?}",
            (0..r.unsigned_integer_count())
                .map(|i| r.unsigned_integer_name(i))
                .collect::<Vec<_>>()
        );
        println!(
            "  numerics {:?}",
            (0..r.numeric_count())
                .map(|i| r.numeric_name(i))
                .collect::<Vec<_>>()
        );
        println!(
            "  geo      {:?}",
            (0..r.geo_count())
                .map(|i| r.geo_name(i))
                .collect::<Vec<_>>()
        );
        println!(
            "  mapfacet {:?}",
            (0..r.map_facet_count())
                .map(|i| r.map_facet_name(i))
                .collect::<Vec<_>>()
        );
        println!(
            "  mapnum   {:?}",
            (0..r.map_numeric_count())
                .map(|i| r.map_numeric_name(i))
                .collect::<Vec<_>>()
        );
    }
}
