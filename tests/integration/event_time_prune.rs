//! AC6 legacy fixture — superseded by one-clock prune.
//!
//! See `one_clock_prune.rs` / `docs/fixtures/one-clock-prune-explain.md`.
//! This module keeps the old dual-predicate session test disabled until the
//! full OTLP writer path is cut over; the greenfield EXPLAIN is the gate.

#[test]
fn one_clock_prune_is_the_blocking_fixture() {
    // Ensure the module stays linked; real prune proof lives in one_clock_prune.
    assert!(
        include_str!("one_clock_prune.rs").contains("ONE_CLOCK_PARTITION_BY")
            || include_str!("one_clock_prune.rs")
                .contains("year(timestamp), month(timestamp), day(timestamp)")
    );
}
