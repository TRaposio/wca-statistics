# Domain Glossary

Reference for WCA-specific terminology, sentinel values, and codebase
conventions used throughout the pipeline. **This is the highest-leverage
doc for agent work** — when in doubt about WCA semantics, the answer is
here or it should be added here.

## Times and units

- **All times in centiseconds.** A result of `1234` means 12.34 seconds.
  Display formatting (e.g. `1:23.45`) is handled by `uw.timeconvert` and
  `uw.format_result`.
- **FMC averages stored ×100.** Fewest Moves Challenge (`333fm`) averages
  are stored as integers ×100 (e.g. `2433` → 24.33 average move count).
  Single attempts are integers (e.g. `24`). `uw.format_result` uses a
  heuristic (`value > 200`) to disambiguate.
- **Multi-blind results are encoded.** `333mbf` results pack
  attempted/solved/wrong/time into a single integer. Decoded by
  `uw.multisolved`, `uw.multiwrong`, `uw.multiattempted`, `uw.multitime`,
  with the human-readable string built by `uw.multiresult`
  (e.g. `"47/50 37:15"`).

## Sentinel values

WCA result columns (`best`, `average`, attempt values) use sentinels
instead of nulls:

| Value | Meaning | Constant in `WCA_CONSTANTS` |
|---|---|---|
| `-1` | DNF (Did Not Finish) | `'dnf'` |
| `-2` | DNS (Did Not Start) | `'dns'` |
| `0`  | No attempt / not submitted | `'no_attempt'` |

The tuple `WCA_CONSTANTS['invalid_results']` = `(0, -1, -2)` covers all
three.

**`0` and `-2` are always invalid times.** `-1` (DNF) is
**context-dependent**:

- For "best/fastest/average" stats → DNF is invalid, drop it.
- For solve counts, attempt counts, DNF rates, completion ratios → DNF
  is meaningful and must be preserved.
- For averages: a valid average (format `'a'`) can include up to one DNF
  among the 5 attempts; an average is itself DNF only if 2+ attempts are
  DNF. So the row's `average` column already encodes the rule —
  filtering on `average > 0` is correct for "valid averages only", but
  the underlying attempts may still contain DNFs you care about.

Decide per stat whether DNF should be excluded or counted before writing
the filter. Always ask for validation.

Two patterns for handling sentinels:

- **`uw.drop_invalid_results(df, cols)`** — masks `(0, -1, -2)` as NaN
  and drops rows entirely invalid on the listed columns. Use for
  aggregations where DNF is *not* meaningful (means, mins, distributions
  of times).
- **`.query("best > 0")`** — idiomatic, drops everything `≤ 0`. Same
  effect as above for "give me valid times only".

When DNF *is* meaningful (solve counts, DNF rates, completion ratios),
**don't use either pattern blindly.** Filter only `0` and `-2` (e.g.
`.query("value not in (0, -2)")`) and treat `-1` as a category. Same for
counting averages where DNF makes the average itself DNF — the
`average` column already encodes the rule, but if you're working off
`attempts` you decide.

Default to the helper; override when the stat says otherwise.

## Rounds

The `rounds` table (loaded from `WCA_export_round_types.tsv`, renamed to
`round_type_id` during `process_tables`) encodes round types via
single-character IDs.

| ID | Meaning |
|---|---|
| `'f'` | Final |
| `'c'` | Combined Final |
| `'1'` | First round |
| `'2'` | Second round |
| `'3'` | Semi-final |

`WCA_CONSTANTS['final_rounds']` = `('c', 'f')` — use this when filtering
to "the round that produced the official result for the competition".

The `rank` column on `rounds` indicates ordering of rounds within a
competition (lower = earlier round). Use `(date, rank)` ascending to
identify the *first* round of a competitor's first competition.

## Formats

The `formats` table maps `format_id` to the scoring rule for a round.

| ID | Format | Notes |
|---|---|---|
| `'1'` | Best of 1 | One solve |
| `'2'` | Best of 2 | Best of two solves |
| `'3'` | Best of 3 | Best of three solves |
| `'a'` | Average of 5 (trimmed mean) | Best and worst dropped, mean of middle 3. |
| `'m'` | Mean of 3 | Straight arithmetic mean |

## Persons and `sub_id`

The `persons` table stores **nationality history** per competitor:

- For each `wca_id`, `sub_id == 1` is the row holding the
  competitor's *current* attributes (name, nationality).
- `sub_id > 1` rows are historical snapshots of past nationalities or
  legal name changes.

`uw.get_current_persons(db_tables, columns=...)` returns one row per
competitor at `sub_id == 1`. Use this whenever you want "the current set
of competitors" — ignoring history avoids double-counting.

The derived table `results_fixed` (built by `process_tables`) overwrites
`person_country_id` on every result with the competitor's current
nationality, so a competitor who switched nationalities counts retroactively
under their current one. Use `results_fixed` for "best-ever per current
nationality"; use `results_nationality` for "results actually competed
under nationality X at the time".

## Countries and continents

- `countries.tsv` columns: `id` (e.g. `"Italy"`), `name`,
  `continent_id` (`"_Europe"`, `"_Asia"`, `"_Africa"`, `"_North America"`, `"_South America"`,
  `"_Oceania"` and `"_Multiple Continents"`),`iso2` (e.g. `"IT"`),
- **Multi-venue codes.** Some competitions span multiple countries
  (e.g. World Championships, continental championships) and use placeholder
  country IDs prefixed with `X`: `XA`, `XE`, `XF`, `XM`, `XN`, `XO`, `XS`,
  `XW`. These should be excluded from "real country" counts. The list
  lives in `config.multivenue`; the derived `config.real_countries`
  excludes them.

## Championships

The `championships` table maps competitions to championship designations:

- `championship_type` = `"world"`, `"continental"`, or a country ISO code
  (e.g. `"IT"`, `"US"`) for national championships.
- `config.championship_type` (e.g. `"IT"`) is used to identify the
  national-championship subset for the configured country.
- `config.nats` (built by `process_tables`) is the list of
  `competition_id`s for those national championships.


## Events

Event IDs are short codes like `333`, `222`, `333fm`, `333oh`, `333mbf`.

- **`config.current_events`** lists *officially active* events. Events
  retired from active competition (e.g. `333mbo` — old multi-blind
  format, `333ft` — feet) are excluded.
- Some retired events still appear in historical `results` rows; filter
  on `config.current_events` when you only want stats for currently
  active events.
- The `events` table itself includes a `rank` column for canonical
  ordering — use it when displaying events in a stable order.

### Kinch event partitioning

The Kinch ranking system partitions events by which result type counts
toward the score. Defined in `WCA_CONSTANTS`:

| Group | Events | Score basis |
|---|---|---|
| `kinch_average_events` | 222, 333, 444, 555, 666, 777, 333oh, 333ft, minx, pyram, sq1, clock, skewb | Best official average |
| `kinch_single_only_events` | 444bf, 555bf | Best official single |
| `kinch_best_of_both_events` | 333bf, 333fm | Max of normalized single and average |
| `kinch_mbld_event` | 333mbf | Special encoded scoring |

## Records

WCA tracks four record scopes:

| Code | Scope |
|---|---|
| `WR` | World Record |
| `CR` | Continental Record (e.g. `ER` = European, `NAR` = North American) |
| `NR` | National Record |
| `PR` | Personal Record (codebase-internal — not on the WCA website) |

`config.continental_record_name` is set in `process_tables` to the
continent code matching `config.country` (e.g. `"ER"` for Italy).

**Records are explicitly marked in the database.** The `results` table
has two columns — `regional_single_record` and `regional_average_record`
— each containing the record type (`WR`, `CR` code like `ER`, `NR`, or
empty) at the time the result was set. No need to recompute records by
sorting; query these columns directly.

## Output structure

Each module's `run` calls `uw.export_data` which writes to:

```
output/<country_tag>/<module_name>/
├── <entry_name>_<timestamp>.csv     # one CSV per result entry
└── figures/
    └── <fig_name>_<timestamp>.png   # if any
```

CSVs use `;` as separator, UTF-8 encoding (chosen because `,` is a
common decimal separator in Italian locales and conflicts with CSV
parsing in Excel).

## `WCA_CONSTANTS` vs `config.ini`

Rule of thumb:

- **`config.ini`** → user choices. Country, nationality, current events,
  plot styling, tunable thresholds (e.g.
  `country_kinch_min_competitors`).
- **`WCA_CONSTANTS`** → WCA domain facts and stat definitions. Sentinels,
  final round codes, Kinch event partitioning, official event lists.

Test: *"if a user changes this, is the output still the same stat?"* If
no → constant.
