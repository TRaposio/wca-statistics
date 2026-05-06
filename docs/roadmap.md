# Roadmap

Stat backlog and module plan. Status markers:

- ⬜ todo
- 🟨 in progress
- ✅ done

## Module structure

12 modules total. 8 existing (1 renamed), 4 new.

### Existing

| Module | Purpose |
|---|---|
| `competitions` | Competition-level & scheduling stats |
| `events` | Event participation & combinations |
| `regions` | Country-specific regional stats (Italy-only today) |
| `championships` | National & major championship analysis |
| `relays` | Official relay leaderboards |
| `records` | Records history & record-adjacent stats |
| `results` | Per-event result-distribution stats |
| `rankings` | Ranking systems (SOR, Kinch, future Elo / ATP) — renamed from `sor_kinch` |

### New (not yet implemented)

| Module | Purpose |
|---|---|
| `podiums` | Placement-based stats (medals, podium counts, 4th places) |
| `streaks` | Temporal sequential analysis per competitor |
| `comparisons` | Head-to-head competitor comparisons (radar, others) |
| `solves` | Per-attempt / solve-count analysis (from `attempts` table) |

## Stat → module mapping

Numbers refer to the 18 original stat requests at the bottom of this
doc.

### `competitions` (extend)

- ⬜ **1** — WCA World Covered by country
- ⬜ **11** — most different competition days / days-of-week / dates competed
- ⬜ **13** — most delegates per country, most-delegated competitions
- ⬜ **19** — most attended comps per calendar week / month

### `events` (extend)

- ⬜ **5.partial** — smallest final relative to R1
- ⬜ **5.partial** — lowest time to qualify for a final

### `records` (extend)

- ⬜ **10** — best counting, best-possible after 4, better-than-NR potentials
- ⬜ **12.partial** — most WR / CR by country, most WR / CR by person

### `results` (extend)

- ⬜ **2** — top-100 singles/averages holder evolution over time
- ⬜ **6** — official rolling averages of 5 / 12 / 50 / 100

### `championships` (extend)

- ⬜ **12.partial** — country medal table at World championships

### `rankings`

- ⬜ **4** — Elo- or ATP-style ranking system

### `podiums` (new)

- ⬜ **migration** — `championships.compute_international_podiums` moves here.
  Rewrite inline as part of creating the module, not after.
- ⬜ **7** — best medal collection abroad-only
- ⬜ **8** — countries ranked by national podium places stolen abroad
- ⬜ **9** — best result not on a podium per event
- ⬜ **12.partial** — biggest podiums ever (with valid result, counting ties)
- ⬜ **12.partial** — sweeps (all golds at a competition), podium sweeps
- ⬜ **18** — most 4th places overall and per event

### `streaks` (new)

- ⬜ **5.partial** — streak of comps with ≥1 PR / no PRs / ≥1 NR
- ⬜ **14** — "true king" chain
- ⬜ **15** — longest time to first medal / first record / first gold /
  first sub-10 3x3 single / first sub-10 3x3 average
- ⬜ **16** — longest streak of comps in / outside own country

### `comparisons` (new)

- ⬜ **3** — radar charts (5 axes) + future head-to-head visuals

### `solves` (new)

Everything that lives on the `attempts` table rather than `results`.

- ⬜ **5.partial** — longest multi-blind attempts (penalties over 1hr limit)
- ⬜ **5.partial** — most completed (non-DNF/DNS) solves per event
- ⬜ **5.partial** — most completed solves in a single competition
- ⬜ **5.partial** — person with most solves per competition year
- ⬜ **5.partial** — ranking for most solves in a year
- ⬜ **5.partial** — most time spent solving (sum of attempt times,
  counting 1hr per FMC attempt)
- ⬜ **17** — most DNFs per event (absolute + %, min 50 solves)

## Implementation order (recommended)

Per-module workflow (repeat for each module):

1. **Plan the module.** Before any code, agree on:
   - What `db_tables` keys it consumes (may reveal new tables needed in
     `utils_wca.process_tables`).
   - Shared helpers (new or existing).
   - `run` function shape and export entries.
2. **Implement the simplest stat end-to-end.** CSV + plot if any.
   Validate against external sources (WCA website) where possible.
3. **Implement the rest.** Module patterns now locked.

Don't batch many stats on a fresh module. The first stat always reveals
constraints that invalidate assumptions about the rest.

### Module order

1. ✅ Rename `sor_kinch` → `rankings`
2. ⬜ Extend `competitions` — stats 1, 11, 13, 19 (no new module, easy
   wins). **← next**
3. ⬜ `podiums` (new) — pull `compute_international_podiums` out of
   `championships` first, then 7, 8, 9, 12 (sweeps / biggest podiums), 18.
4. ⬜ `solves` (new) — stat 17 first (simplest), then rest of group 5.
5. ⬜ `streaks` (new) — stat 16 first (cleanest streak), then 5-streaks,
   14, 15.
6. ⬜ Extend `records` — 10, 12 (WR / CR counts).
7. ⬜ Extend `results` — 2, 6.
8. ⬜ Extend `events` / `championships` — leftover 5.partial and
   12.partial.
9. ⬜ `comparisons` (new) — stat 3, last because it's the most
   visualization-heavy.
10. ⬜ Implement Elo / ATP — stat 4 in `rankings`.

Skip rigidly following this if stats Tom cares about more come up first.

## The 18 stat requests (original list)

1. **WCA World Covered by country** — rank countries by number of
   different countries their competitors have competed in; give %.
2. **Top-100 singles/averages holder evolution over time** — per event,
   at fixed time increments, register who holds the most top-100 spots
   and what %. Line charts colored by current top holder.
3. **Radar charts** — compare cubers across 5 axes: avg of last N avgs,
   best result, avg placement of last rounds, stddev of last N, clutch
   factor.
4. **Better WCA Ranking system** — Elo or ATP-style tournament points.
5. **Various solve / streak / time stats** (split across modules):
   - longest multi-blind attempts (penalties over 1hr limit) → `solves`
   - streak of comps with ≥1 PR → `streaks`
   - streak of comps with no PRs → `streaks`
   - streak of comps with ≥1 NR or better → `streaks`
   - most completed (non-DNF/DNS) solves per event → `solves`
   - most completed solves in a single competition → `solves`
   - person with most solves in each competition year → `solves`
   - ranking of most solves in a year → `solves`
   - most time spent solving (1hr per FMC) → `solves`
   - smallest final relative to R1 → `events`
   - lowest time to qualify for an event final → `events`
6. **Rolling averages** of 5 / 12 / 50 / 100 for all official events.
7. **Best medal collection won abroad only** → `podiums`
8. **Countries ranked by national podium places stolen abroad**
   (all-time + current year) → `podiums`
9. **Best result not providing a podium, per event** → `podiums`
10. **Best counting time, best-possible avg after 4, count of
    better-than-current-NR possible avgs**, per event → `records`
11. **Countries with comps on most different days/year, days-of-week,
    competitors by most different dates competed** → `competitions`
12. **Sweep & record stats**:
    - sweeps (all golds at a comp, ranked by # of golds) → `podiums`
    - podium sweeps (same 3 on all podiums) → `podiums`
    - biggest podiums ever (valid results, counting ties) → `podiums`
    - most WRs by country / by person → `records`
    - most CRs by country / by person → `records`
    - country medal table at World championships → `championships`
13. **Most delegates per country, most-delegated competitions** →
    `competitions`
14. **True king** — first person to win a comp is king; next king is
    first person to win a comp the current king competed in. → `streaks`
15. **Longest time to first medal / first record / first gold /
    first sub-10 single / first sub-10 average** → `streaks`
16. **Longest streak of comps in / outside own country** → `streaks`
17. **Most DNFs per event** (absolute + %, min 50 solves) → `solves`
18. **Most 4th places overall and per event** → `podiums`
19. **Most attended comps per calendar week / month** → `competitions`
