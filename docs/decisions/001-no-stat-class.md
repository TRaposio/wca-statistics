# 001 — No `Statistic` base class

**Status:** Accepted
**Date:** 2026-05-06

## Context

Each stat module follows the same shape: a `run(db_tables, config)`
entry point that calls one or more `compute_<stat>` functions, then
hands results to `uw.export_data`. With ~12 modules planned, an OOP
refactor was on the table — a `Statistic` abstract base class with
`compute()` / `export()` methods, subclassed per stat.

The shared structure is real: `db_tables, config, logger → DataFrame`
appears everywhere. The question is whether inheritance is the right
mechanism to express it.

## Decision

We do **not** introduce a `Statistic` base class. Stats stay as plain
functions in plain modules. Shared behavior lives in `utils_wca.py` and
is invoked by free functions.

A value object (e.g. `RankingTable` for SOR / Kinch / future Elo / ATP
outputs) is allowed if it emerges as a real need. Value objects are not
a compute hierarchy and don't conflict with this decision.

## Consequences

**Positive.**

- `utils_wca` already does the job a base class would: it's the shared-
  logic mechanism, accessed without inheritance ceremony or `self.`
  navigation.
- Compute functions are often composed of multiple private helpers
  (Kinch has 5+). Forcing them onto a class adds `self.` everywhere
  without clarity gain.
- Functional style with explicit dependencies is more testable than
  methods reading `self.config`, `self.db_tables`. Phase 7 tests can
  call compute functions directly with synthetic fixtures.
- Stat families (rankings, records, podiums, streaks) don't share enough
  structure to make one base class meaningful. Multiple bases would
  fragment the hierarchy and add navigation overhead.

**Negative.**

- No enforced contract that every module must expose `run(db_tables,
  config)`. Convention-only. Mitigated by the module template in
  `CLAUDE.md` and by Phase 7 smoke tests asserting each module runs.
- No shared lifecycle hooks (e.g. pre-compute validation). If we ever
  need them, we add a helper in `utils_wca` rather than a base class.

**Revisit if:**

- A meaningful subset of stats genuinely shares 3+ steps that can't
  cleanly factor into `utils_wca` helpers.
- We move to a plugin / discovery system where modules need a stable
  registered interface.
