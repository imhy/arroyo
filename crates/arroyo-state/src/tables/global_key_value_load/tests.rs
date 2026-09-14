//! The page bound, as a property of the type that builds pages.
//!
//! [`GlobalKeyValuePageBuilder`] is the only constructor of a [`GlobalKeyValuePage`], so
//! what it refuses to build is what no implementation of [`GlobalKeyValueLoad`] can
//! produce. These cases pin each clause of the bound separately — the entry count, the
//! payload, the single-entry exception, and the announcing page — over entry shapes chosen
//! so that exactly one clause is the binding one, which is what makes a failure name the
//! clause that broke.
//!
//! The bound over real checkpointed state, at 1× and 10× the entries, is
//! `crate::provider::tests::global_load`.

use super::{
    GlobalKeyValuePage, GlobalKeyValuePageBuilder, LoadPageLimits, LoadPagePeak,
    MAX_LOAD_PAGE_BYTES, MAX_LOAD_PAGE_ENTRIES,
};

/// A page-building run: every page the builder closed, in order, including the last
/// partial one.
fn pages(limits: LoadPageLimits, entries: Vec<(Vec<u8>, Vec<u8>)>) -> Vec<GlobalKeyValuePage> {
    let mut builder = GlobalKeyValuePageBuilder::new("source", 7, limits);
    let mut closed = Vec::new();
    for (key, value) in entries {
        if let Some(page) = builder.push(key, value) {
            closed.push(page);
        }
    }
    closed.extend(builder.take());
    closed
}

/// `count` entries of `key_len` plus `value_len` bytes each, distinct from one another.
fn entries(count: usize, key_len: usize, value_len: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let tag = u8::try_from(i % 251).expect("under 251");
            (vec![tag; key_len], vec![tag ^ 0xff; value_len])
        })
        .collect()
}

/// The entry count closes a page when entries are small enough that the payload never
/// binds.
///
/// 2050 entries of 2 bytes each is 4100 payload bytes over the whole run, far under the
/// 1 MiB cap, so every page boundary here is the count's doing: two full pages of 1024 and
/// a partial page of 2.
#[test]
fn the_entry_count_closes_a_page_when_the_payload_is_small() {
    let built = pages(LoadPageLimits::DEFAULT, entries(2050, 1, 1));

    assert_eq!(
        built
            .iter()
            .map(GlobalKeyValuePage::len)
            .collect::<Vec<_>>(),
        vec![MAX_LOAD_PAGE_ENTRIES, MAX_LOAD_PAGE_ENTRIES, 2],
    );
    assert_eq!(
        built
            .iter()
            .map(GlobalKeyValuePage::payload_bytes)
            .collect::<Vec<_>>(),
        vec![2 * MAX_LOAD_PAGE_ENTRIES, 2 * MAX_LOAD_PAGE_ENTRIES, 4],
    );
}

/// The payload closes a page when entries are large enough that the count never binds.
///
/// Each entry is 512 KiB of key plus 512 KiB of value, exactly the 1 MiB cap, so a second
/// entry never fits and every page holds one — at a count of 1, three orders of magnitude
/// under the 1024 the count would allow.
#[test]
fn the_payload_closes_a_page_when_entries_are_large() {
    let half = MAX_LOAD_PAGE_BYTES / 2;
    let built = pages(LoadPageLimits::DEFAULT, entries(3, half, half));

    assert_eq!(
        built
            .iter()
            .map(GlobalKeyValuePage::len)
            .collect::<Vec<_>>(),
        vec![1, 1, 1],
    );
    for page in &built {
        assert_eq!(page.payload_bytes(), MAX_LOAD_PAGE_BYTES);
        assert!(LoadPageLimits::DEFAULT.admits(page));
    }
}

/// An entry bigger than the whole payload cap becomes a page of one, and is neither split
/// nor dropped.
///
/// The run is a small entry, then an oversized one, then a small one: the oversized entry
/// closes the page before it and is alone in its own, so the three entries come back in
/// order across three pages with every byte intact.
#[test]
fn an_entry_larger_than_the_payload_bound_is_a_page_of_one() {
    let oversize = MAX_LOAD_PAGE_BYTES + 1;
    let built = pages(
        LoadPageLimits::DEFAULT,
        vec![
            (b"before".to_vec(), b"x".to_vec()),
            (b"huge".to_vec(), vec![b'v'; oversize]),
            (b"after".to_vec(), b"y".to_vec()),
        ],
    );

    assert_eq!(
        built
            .iter()
            .map(GlobalKeyValuePage::len)
            .collect::<Vec<_>>(),
        vec![1, 1, 1],
    );
    assert_eq!(built[1].entries()[0].0, b"huge".to_vec());
    assert_eq!(built[1].entries()[0].1.len(), oversize);
    assert_eq!(built[1].payload_bytes(), oversize + 4);

    // Over the cap, and still within the contract: the single-entry exception is the whole
    // difference between this page and a violation.
    assert!(built[1].payload_bytes() > MAX_LOAD_PAGE_BYTES);
    assert!(LoadPageLimits::DEFAULT.admits(&built[1]));

    let round_trip: Vec<Vec<u8>> = built
        .iter()
        .flat_map(|page| page.entries().iter().map(|(key, _)| key.clone()))
        .collect();
    assert_eq!(
        round_trip,
        vec![b"before".to_vec(), b"huge".to_vec(), b"after".to_vec()],
    );
}

/// The announcing page carries the source's identity and no entries, and is the only page
/// that can be empty.
#[test]
fn a_header_page_announces_a_source_and_is_the_only_empty_page() {
    let mut builder =
        GlobalKeyValuePageBuilder::new("checkpoint-3/table-t-000", 2, LoadPageLimits::DEFAULT);

    let header = builder.header();
    assert_eq!(header.source(), "checkpoint-3/table-t-000");
    assert_eq!(header.state_version(), 2);
    assert_eq!(header.len(), 0);
    assert!(header.is_empty());
    assert_eq!(header.payload_bytes(), 0);
    assert!(LoadPageLimits::DEFAULT.admits(&header));

    // A builder with nothing in it produces no page at all, rather than an empty one.
    assert_eq!(builder.take(), None);

    assert_eq!(builder.push(b"k".to_vec(), b"v".to_vec()), None);
    let page = builder.take().expect("one entry is one page");
    assert_eq!(page.len(), 1);
    assert_eq!(page.source(), "checkpoint-3/table-t-000");
    assert_eq!(page.state_version(), 2);
    assert_eq!(builder.take(), None);
}

/// A page's source and state version are the builder's, on every page it produces.
#[test]
fn every_page_of_a_source_carries_that_source_and_its_state_version() {
    let limits = LoadPageLimits::new(2, MAX_LOAD_PAGE_BYTES).expect("a tighter bound");
    let mut builder = GlobalKeyValuePageBuilder::new("f", 9, limits);

    let mut produced = vec![builder.header()];
    for (key, value) in entries(5, 1, 1) {
        produced.extend(builder.push(key, value));
    }
    produced.extend(builder.take());

    assert_eq!(
        produced
            .iter()
            .map(GlobalKeyValuePage::len)
            .collect::<Vec<_>>(),
        vec![0, 2, 2, 1],
    );
    for page in &produced {
        assert_eq!(page.source(), "f");
        assert_eq!(page.state_version(), 9);
    }
}

/// The seam's own maximum is enforceable because no wider `LoadPageLimits` can be built.
#[test]
fn limits_refuse_zero_and_anything_wider_than_the_seam_default() {
    assert_eq!(LoadPageLimits::new(0, 1), None);
    assert_eq!(LoadPageLimits::new(1, 0), None);
    assert_eq!(
        LoadPageLimits::new(MAX_LOAD_PAGE_ENTRIES + 1, MAX_LOAD_PAGE_BYTES),
        None
    );
    assert_eq!(
        LoadPageLimits::new(MAX_LOAD_PAGE_ENTRIES, MAX_LOAD_PAGE_BYTES + 1),
        None
    );

    let widest = LoadPageLimits::new(MAX_LOAD_PAGE_ENTRIES, MAX_LOAD_PAGE_BYTES)
        .expect("the default itself is constructible");
    assert_eq!(widest, LoadPageLimits::DEFAULT);
    assert_eq!(LoadPageLimits::default(), LoadPageLimits::DEFAULT);
    assert_eq!(LoadPageLimits::DEFAULT.max_entries(), MAX_LOAD_PAGE_ENTRIES);
    assert_eq!(
        LoadPageLimits::DEFAULT.max_payload_bytes(),
        MAX_LOAD_PAGE_BYTES
    );
}

/// `admits` refuses a page wider than the bound in either unit, and excuses only a page of
/// exactly one entry from the payload clause.
///
/// The pages here are built under the default bound and checked against a tighter one,
/// which is how a loader that declared a bound it did not keep would look to the decode
/// above the seam.
#[test]
fn admits_refuses_a_page_wider_than_the_bound_in_either_unit() {
    let declared = LoadPageLimits::new(4, 16).expect("a tighter bound");

    let too_many = pages(LoadPageLimits::DEFAULT, entries(5, 1, 1));
    assert_eq!(too_many.len(), 1, "the default bound holds all five");
    assert_eq!(too_many[0].len(), 5);
    assert!(!declared.admits(&too_many[0]));

    let too_wide = pages(LoadPageLimits::DEFAULT, entries(2, 8, 8));
    assert_eq!(too_wide[0].len(), 2);
    assert_eq!(too_wide[0].payload_bytes(), 32);
    assert!(!declared.admits(&too_wide[0]));

    let single = pages(LoadPageLimits::DEFAULT, entries(1, 8, 24));
    assert_eq!(single[0].len(), 1);
    assert_eq!(single[0].payload_bytes(), 32);
    assert!(
        declared.admits(&single[0]),
        "one entry over the payload cap is the documented exception"
    );
}

/// The high-water mark holds each unit's maximum, which need not come from one page.
#[test]
fn the_peak_tracks_each_unit_independently() {
    let mut peak = LoadPagePeak::ZERO;
    assert_eq!(peak.entries(), 0);
    assert_eq!(peak.payload_bytes(), 0);
    assert_eq!(LoadPagePeak::default(), LoadPagePeak::ZERO);

    let many_small = pages(LoadPageLimits::DEFAULT, entries(4, 1, 1));
    let one_large = pages(LoadPageLimits::DEFAULT, entries(1, 100, 100));
    for page in many_small.iter().chain(one_large.iter()) {
        peak.record(page);
    }

    assert_eq!(peak.entries(), 4, "the widest page held four entries");
    assert_eq!(
        peak.payload_bytes(),
        200,
        "the heaviest page held one 200-byte entry"
    );
}
