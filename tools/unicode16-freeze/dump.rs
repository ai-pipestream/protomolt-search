//! Dump, for every Unicode scalar value, each ICU4X answer that
//! protomolt-analyzer and protomolt-embedder read, one tab-separated row
//! per scalar: code point, general category, script, Extended_Pictographic,
//! canonical combining class, NFD, NFC, NFC of the NFD (which exercises
//! every canonical composition pair) and the full case fold.
//!
//! Built once against ICU4X 2.0.0 (`icu-2.0`) and once against 2.3.0
//! (`icu-2.3`); `generate` compares the two dumps. See `run.sh`.
use icu_casemap::CaseMapper;
use icu_normalizer::{ComposingNormalizerBorrowed, DecomposingNormalizerBorrowed};
use icu_properties::props::{
    CanonicalCombiningClass, ExtendedPictographic, GeneralCategory, Script,
};
use icu_properties::{CodePointMapData, CodePointSetData};
use std::io::Write;

fn hex(s: &str) -> String {
    s.chars()
        .map(|c| format!("{:04X}", c as u32))
        .collect::<Vec<_>>()
        .join(" ")
}

#[allow(deprecated)]
fn script_value(s: Script) -> u16 {
    s.to_icu4c_value()
}

#[allow(deprecated)]
fn ccc_value(c: CanonicalCombiningClass) -> u8 {
    c.to_icu4c_value()
}

fn main() {
    let gc = CodePointMapData::<GeneralCategory>::new();
    let sc = CodePointMapData::<Script>::new();
    let cc = CodePointMapData::<CanonicalCombiningClass>::new();
    let ep = CodePointSetData::new::<ExtendedPictographic>();
    let nfd = DecomposingNormalizerBorrowed::new_nfd();
    let nfc = ComposingNormalizerBorrowed::new_nfc();
    let cm = CaseMapper::new();
    let out = std::io::stdout();
    let mut out = std::io::BufWriter::new(out.lock());
    for cp in 0u32..=0x10FFFF {
        let Some(ch) = char::from_u32(cp) else {
            continue;
        };
        let s = ch.to_string();
        let d = nfd.normalize(&s);
        let c = nfc.normalize(&s);
        let cd = nfc.normalize(&d);
        let f = cm.fold_string(&s);
        writeln!(
            out,
            "{:04X}\t{:?}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            cp,
            gc.get(ch),
            script_value(sc.get(ch)),
            ep.contains(ch) as u8,
            ccc_value(cc.get(ch)),
            hex(&d),
            hex(&c),
            hex(&cd),
            hex(&f)
        )
        .unwrap();
    }
}
