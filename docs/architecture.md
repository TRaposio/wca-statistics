# Architecture

## What this is

A Python pipeline that ingests the WCA results database export
(TSV files) into pandas DataFrames and produces per-module statistics as
CSVs and figures. No database engine — everything runs in memory.

Future scope: a static/interactive website driven by the exported CSVs,
and a RAG chatbot on the WCA regulations.

## Pipeline flow

`src/wca_stats/main.py` orchestrates a single linear pipeline:

```
setup
├── load_config(config.ini)         # parse + attach common attrs
├── set_plot_style(config)          # apply matplotlib rcParams
└── update_data(config)             # download WCA export if stale

load + preprocess
├── read_table(...) for each TSV    # → raw db_tables
├── process_tables(db_tables, ...)  # → derived db_tables (see below)
├── export_db_schema(db_tables)     # dump current schema to file
└── read_aux_file("regions", ...)   # city → region mapping

run modules (each consumes db_tables + config, writes CSV/figures)
├── competitions
├── events
├── regions
├── championships
├── relays
├── records
├── rankings
└── results
```

Each module exposes `run(db_tables, config)` and writes its outputs via
`uw.export_data(...)` to:

```
output/<country_tag>/<module_name>/
├── <entry_name>_<timestamp>.csv     # one CSV per result entry
└── figures/
    └── <fig_name>_<timestamp>.png   # if any
```

The `<country_tag>` is derived from `config.country` (lowercased,
spaces → underscores).

## Running and debugging

Two equivalent ways to invoke the pipeline:

### Terminal

```bash
python -m wca_stats.main
```

Run from the repo root. The `-m` flag puts the package root on
`sys.path`, so `from wca_stats import ...` resolves correctly.

### VS Code (debugger)

The plain ▶️ Run button on `src/wca_stats/main.py` does **not** work —
it invokes `python src/wca_stats/main.py`, which fails on
`from wca_stats import ...` because the package isn't on `sys.path`.

Use a launch configuration instead. Create `.vscode/launch.json`
(local-only, since `.vscode/` is gitignored):

```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Run wca_stats pipeline",
      "type": "debugpy",
      "request": "launch",
      "module": "wca_stats.main",
      "cwd": "${workspaceFolder}",
      "console": "integratedTerminal",
      "justMyCode": false
    }
  ]
}
```

`"module": "wca_stats.main"` is the IDE equivalent of `python -m
wca_stats.main`. F5 launches it; breakpoints work normally.
`justMyCode: false` lets you step into pandas / matplotlib if needed.

## Repository layout

```
wca-stats/
├── config.ini                       # runtime config
├── pyproject.toml                   # package identity + deps
├── environment.yml                  # conda recipe
├── CLAUDE.md                        # agent guide
├── data/                            # gitignored — WCA exports + aux
├── output/                          # gitignored — generated CSVs/figures
├── logs/                            # gitignored — per-run logs
├── docs/
│   ├── architecture.md              # this file
│   ├── domain-glossary.md
│   ├── roadmap.md
│   ├── db_schema.md                 # hand-curated schema doc
│   └── decisions/                   # ADRs
├── src/
│   └── wca_stats/
│       ├── __init__.py
│       ├── main.py
│       ├── utils_wca.py             # shared utilities + constants
│       └── modules/
│           ├── championships.py
│           ├── competitions.py
│           ├── events.py
│           ├── rankings.py
│           ├── records.py
│           ├── regions.py
│           ├── relays.py
│           └── results.py
├── sql/                             # ad-hoc SQL probes (reference only)
└── tests/                           # pytest scaffold
```

## `db_tables` keys

`db_tables` is the single dict that flows through the entire pipeline.
Every module reads from it; `process_tables` populates the derived keys.

### Raw (loaded directly from WCA TSV export)

| Key | Source file |
|---|---|
| `results` | `WCA_export_results.tsv` |
| `attempts` | `WCA_export_result_attempts.tsv` |
| `persons` | `WCA_export_persons.tsv` |
| `competitions` | `WCA_export_competitions.tsv` |
| `events` | `WCA_export_events.tsv` |
| `formats` | `WCA_export_formats.tsv` |
| `ranks_single` | `WCA_export_ranks_single.tsv` |
| `ranks_average` | `WCA_export_ranks_average.tsv` |
| `countries` | `WCA_export_countries.tsv` |
| `continents` | `WCA_export_continents.tsv` |
| `championships` | `WCA_export_championships.tsv` |
| `rounds` | `WCA_export_round_types.tsv` |
| `scrambles` | `WCA_export_scrambles.tsv` |

### Derived (built by `utils_wca.process_tables`)

| Key | Description |
|---|---|
| `results_nationality` | Results for competitors of `config.nationality`, joined with competition info + round rank. |
| `results_nationality_detailed` | Same as above, joined with `attempts` (per-attempt grain). |
| `results_country` | Results for competitions held in `config.country` (any competitor nationality), joined with competition info + round rank. |
| `results_country_detailed` | Same as above, joined with `attempts`. |
| `results_fixed` | `results_nationality` but `person_country_id` is overwritten with each competitor's *current* nationality (`sub_id == 1`). Use this whenever you want "all results currently attributable to nationality X regardless of historical nationality". |
| `multi_results` | `333mbf` attempts decoded into structured columns: `attempted`, `solved`, `wrong`, `points`, `time`, plus display strings. |
| `ranks_single_nationality` / `ranks_average_nationality` | Ranks restricted to `config.nationality`, joined with name and country_id. |

### Cached (built inside a module's `run`, reused by other stats in that module)

| Key | Built by |
|---|---|
| `newcomers` | `competitions` — per-year newcomer counts. |
| `nats_champions` | `championships` — full national-championship winners table. |
| `kinch_event_scores`, `kinch_event_scores_national`, `kinch_country_event_scores` | `rankings` — per-event Kinch scores cached for plot reuse. |

### Auxiliary (loaded from CSV files in `data/regions/`)

| Key | File | Notes |
|---|---|---|
| `regions` | `city_to_region_map_ita.csv` | Italy-only today. |

## Config attributes

`load_config` parses `config.ini` and attaches commonly-used values as
attributes on the returned `ConfigParser` object. This avoids repeated
`config["section"]["key"]` boilerplate across modules.

### Set by `load_config` (available immediately)

| Attribute | Type | Source | Notes |
|---|---|---|---|
| `config.country` | str | `[global_variables].country` | Must match `name` in `countries.tsv`. Scopes country-hosted competitions. |
| `config.nationality` | str | `[global_variables].nationality` | Must match `name` in `countries.tsv`. Scopes competitor nationality. |
| `config.championship_type` | str | `[global_variables].championship_type` | E.g. `"IT"`, `"US"`. Used to identify national championships. |
| `config.current_events` | list[str] | `[global_variables].current_events` | Officially active events; excludes retired events like `333mbo`. |
| `config.multivenue` | list[str] | `[global_variables].multivenue` | Multi-venue placeholder country codes (XA/XE/...) to exclude from "real" country counts. |

### Set by `process_tables` (require `db_tables` loaded)

| Attribute | Description |
|---|---|
| `config.continent_id` | E.g. `"_Europe"`. Derived from `config.country` via `countries`. |
| `config.continental_record_name` | E.g. `"ER"`, `"AsR"`. Derived from `continents`. |
| `config.nats` | `competition_id`s of national championships in `config.country`. |
| `config.countries`, `config.real_countries` | Country lists; `real_countries` excludes `config.multivenue`. |

### Other config sections

`config["paths"]`, `config["url"]`, `config["tables"]`, `config["aux_files"]`,
`config["plot"]`, `config["rankings"]`, `config["records"]`, `config["results"]`,
`config["output"]` are read via the standard ConfigParser interface (no
attribute attachment).

`[paths]` entries are resolved against the repo root in `load_config`,
so the pipeline works regardless of CWD.

## Conventions

- **Module shape.** Each module exposes `run(db_tables, config)` with no
  return value; outputs go through `uw.export_data`. Internal helpers are
  prefixed with `_` and split between `# COMPUTATIONS` and `# PLOTS`
  sections.
- **Shared logic.** Goes in `utils_wca.py`. If logic is reused across two
  or more modules, move it. The codebase deliberately avoids a `Statistic`
  base class — see `decisions/001-no-stat-class.md`.
- **WCA constants.** `utils_wca.WCA_CONSTANTS` holds domain facts
  (sentinels, final round codes, Kinch event partitions). User-tunable
  values live in `config.ini`.
- **Sentinel handling.** Always account for DNF (`-1`) and DNS (`-2`) when
  filtering result columns. Use `uw.drop_invalid_results(df, cols)` for
  aggregations or `.query("best > 0")` for simple filters.

## Logging

`uw.setup_logger` initializes a root logger writing to console + a daily
log file under `logs/<YYYY-MM-DD>/`. Modules retrieve a child logger via
`logging.getLogger(__name__)` inside their `run` function.

A `CRITICAL` log level triggers `sys.exit(1)` via `ExitOnCriticalHandler`
— used for unrecoverable config or data errors.
