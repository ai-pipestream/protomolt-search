//! Deep integrity verification for `.bm25` files: open each one and
//! check every recorded section CRC against the bytes on disk — the
//! whole file, including the big postings and texts blobs that open
//! deliberately skips.
//!
//! Usage: `bm25_verify <file.bm25>...`
//!
//! Prints one line per file. Exit status: 0 when every file verified,
//! 1 when any file failed to open or any CRC mismatched, 2 when no
//! file failed but at least one predates v8 and so has nothing to
//! verify — "unverifiable" must never look like "verified".

use std::path::Path;
use turbovec_search::postings::Bm25Reader;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        eprintln!("usage: bm25_verify <file.bm25>...");
        std::process::exit(1);
    }
    let mut any_fail = false;
    let mut any_unverifiable = false;
    for arg in &args {
        let path = Path::new(arg);
        let started = std::time::Instant::now();
        match Bm25Reader::open(path) {
            Err(e) => {
                println!("FAIL  {arg}: {e}");
                any_fail = true;
            }
            Ok(reader) => match reader.verify_integrity() {
                Ok((sections, bytes)) => {
                    println!(
                        "PASS  {arg}: {sections} sections, {bytes} bytes, {:.1}s",
                        started.elapsed().as_secs_f64()
                    );
                }
                Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
                    println!("NONE  {arg}: {e}");
                    any_unverifiable = true;
                }
                Err(e) => {
                    println!("FAIL  {arg}: {e}");
                    any_fail = true;
                }
            },
        }
    }
    std::process::exit(if any_fail {
        1
    } else if any_unverifiable {
        2
    } else {
        0
    });
}
