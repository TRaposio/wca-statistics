# 003 — `"world"` as a wildcard for `country` / `nationality` / `championship_type`

**Status:** Proposed (deferred — not yet implemented)
**Date:** 2026-05-06

## Context

`config.country`, `config.nationality`, and `config.championship_type`
currently scope the entire pipeline to a single country. Tom wants a
future mode where setting any of these to `"world"` means "no filtering"
— produce stats across all competitors / competitions / championships.

Use cases:

- World-level versions of regional stats (top-100 holders, podium
  collections, etc.) without rewriting modules.
- Quick toggling between a national report and a global report from the
  same codebase.

This is semantic feature work, not plumbing — it affects how every
module interprets the three keys. Deferred until after the restructure
is complete.

## Decision (proposed)

Add a `world_mode` flag derived in `load_config`:

- Parse `country`, `nationality`, `championship_type` from
  `[global_variables]` as today.
- If any value is `"world"` (case-insensitive), set
  `config.world_mode = True` and leave the corresponding scope key as a
  sentinel (`None` or `"world"`).
- Modules check `config.world_mode` (or the per-key sentinel) at filter
  boundaries and skip the corresponding `.query("country_id == ...")`
  step.

Open design questions to resolve when implementing:

- Per-key world flags vs one global flag? (Tom may want `nationality =
  world` while keeping `country = "Italy"`, or vice versa.)
- How does `process_tables` handle derived tables that currently filter
  to nationality (`results_nationality`, `results_fixed`) under world
  mode — produce the unfiltered version, or skip them?
- Output folder naming (`output/<country_tag>/...`) when `country` is
  `"world"` — `output/world/...`?
- Plot titles and legends generated from `config.country` —
  templating layer, or special-case in each plot helper?

## Consequences (anticipated)

**Positive.**

- Single codebase covers national and global reporting.
- Three keys stay in `[global_variables]` — single-point change.

**Negative.**

- Every filter site in every module becomes a conditional. Risk of
  silently wrong results if a module forgets the world-mode branch.
  Mitigation: a `uw.scope_results(...)` helper that handles the branch
  centrally — modules call the helper, never filter directly.
- Some stats are inherently national (e.g. national championship medal
  table) and don't have a world-mode interpretation. Need an explicit
  list of stats that no-op or raise under world mode.

**Revisit when:** restructure is complete (post-Phase 8) and Tom
prioritizes this over feature work on the 18 stat backlog.
