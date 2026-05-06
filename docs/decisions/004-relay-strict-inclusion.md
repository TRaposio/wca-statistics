# 004 — Relay strict inclusion

**Status:** Accepted
**Date:** 2026-05-07

## Context

`relays._compute_relay_base` builds virtual relay leaderboards (Guilford,
Mini Guilford, Lucky, Blind, 3x3 Master) from `ranks_single_nationality`,
pivoting per-event singles into one row per competitor.

The original implementation handled missing events with a "fill with
worst + 1" penalty: a competitor missing one of the required events
still appeared in the table, with their gap filled by the slowest known
time plus one centisecond. The intent was to keep the leaderboard
populated.

This produced two failure modes, surfaced by Phase 7 smoke tests:

1. When *every* competitor is missing the same event (small countries,
   rare events), the per-column `c.max() + 1` is `NaN + 1 = NaN`, the
   fill is a no-op, NaN survives into the `Total` sum, and
   `uw.timeconvert(NaN)` raises `ValueError`.
2. The semantic problem: a "Guilford" leaderboard computed without
   `666` and `777` is not Guilford. Penalty-filling smaller missing
   gaps is also questionable — the resulting "Total" mixes real and
   synthetic times.

## Decision

`_compute_relay_base` now applies **strict inclusion**: a competitor
must have a recorded single in every event in `event_list` to appear.
Competitors missing any required event are dropped. If no competitor
has all events, an empty DataFrame is returned (with a warning logged).

The "fill with worst + 1" block is removed. Per-event times shown in
the output are always real recorded singles; the `Total` is always a
sum of real times.

## Consequences

**Positive.**

- The output is unambiguous: every cell is a real result. No synthetic
  penalty values to explain in documentation or eventual website copy.
- The crash mode is gone. NaN can no longer reach `timeconvert`.
- The semantic identity of each relay is preserved. A 12-event Guilford
  is computed only over competitors who have all 12 events.

**Negative.**

- Leaderboards are smaller, sometimes much smaller. For Italy on real
  data this is a modest reduction; for smaller countries some relays
  may go empty.
- Competitors who are strong in 11 of 12 events but missing one
  (common for less-popular events like `666`, `777`) disappear from
  the leaderboard entirely. Under the previous rule they would have
  appeared with a penalty.
- This is a **behavioral change to existing pipeline output**, not a
  pure bug fix. Comparing pre- and post-change CSVs will show
  different rows, not just different values.

**Revisit if:**

- A "lenient" leaderboard (penalty-filled) is wanted alongside the
  strict one — e.g., for the eventual website, to show "who's close to
  qualifying." Compute both, expose both, let the consumer pick.
- A formal WCA-community convention emerges for handling missing events
  in virtual relays. The current strict rule is a defensible default,
  not a community standard.