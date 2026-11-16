use turbo_glossary::{Boundaries, Case, Glossary, TextMatcher};

fn entries(values: &[(&str, &str)]) -> Glossary {
    let mut glossary = Glossary::new();
    for (id, term) in values {
        glossary.push(id, term).unwrap();
    }
    glossary
}

fn every_boundary(text: &str) -> Boundaries {
    Boundaries::from_offsets(text.len() as u32, 0..=text.len() as u32)
}

fn fingerprint(matcher: &TextMatcher) -> u64 {
    let debug = format!("{matcher:?}");
    debug
        .rsplit("fingerprint: ")
        .next()
        .unwrap()
        .split(' ')
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

fn main() {
    let baseline = entries(&[("ny", "new york"), ("york", "york")]);
    let matcher = TextMatcher::new(&baseline, Case::Sensitive).unwrap();
    let mut matches = Vec::new();
    matcher.find("new york", &every_boundary("new york"), &mut matches);
    println!(
        "BASELINE {} {} {}",
        matches[0].entry.0, matches[0].span.start, matches[0].span.end
    );

    let case = entries(&[("strasse", "STRASSE")]);
    let matcher = TextMatcher::new(&case, Case::Insensitive).unwrap();
    let mut matches = Vec::new();
    matcher.find("Straße", &every_boundary("Straße"), &mut matches);
    println!("CASE {}", matches.len());

    let tied = entries(&[("zeta", "cat"), ("alpha", "cat")]);
    let matcher = TextMatcher::new(&tied, Case::Sensitive).unwrap();
    let mut matches = Vec::new();
    matcher.find("cat", &every_boundary("cat"), &mut matches);
    println!("TIE {}", matches[0].entry.0);

    let matcher = TextMatcher::new(&tied, Case::Insensitive).unwrap();
    println!("FINGERPRINT {}", fingerprint(&matcher));
    let reversed = entries(&[("alpha", "cat"), ("zeta", "cat")]);
    let matcher = TextMatcher::new(&reversed, Case::Insensitive).unwrap();
    println!("REVERSED {}", fingerprint(&matcher));
}
