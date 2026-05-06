# CLAUDE.md

Agent guide for the `wca-stats` repo. Read this first every session.
Depth lives in `docs/`; this file is the index + the rules.

## What this is

Python pipeline that ingests the WCA results database export
(TSV → pandas DataFrames) and produces per-module statistics as CSVs and
figures. No DB engine. Single-author project.

## Stack

Python 3.10+. pandas, numpy, matplotlib, geopandas, shapely, configparser.
Conda env `wca_env` (Python 3.11.13) with `pip install -e ".[dev]"` of
this package. See `pyproject.toml` and `environment.yml`.

## Run

From repo root:

```bash
python -m wca_stats.main
```

VS Code launch config in `docs/architecture.md`. Plain ▶️ on `main.py`
does not work — the package must be on `sys.path`.

## Repo layout

See `docs/architecture.md` for the full tree and pipeline flow.
Source lives in `src/wca_stats/`; modules in `src/wca_stats/modules/`;
shared utilities in `src/wca_stats/utils_wca.py`.

## Where to look first

| If you need... | Read |
|---|---|
| Pipeline flow, `db_tables` keys, config attributes | `docs/architecture.md` |
| Domain rules (sentinels, formats, rounds, sub_id, Kinch) | `docs/domain-glossary.md` |
| Why a structural choice was made | `docs/decisions/` |
| What's planned next | `docs/roadmap.md` |
| Current session state | `SESSION_HANDOFF.md` (lives outside repo, pasted in) |

## Conventions (binding)

- **Module shape.** Each module in `src/wca_stats/modules/` exposes
  `run(db_tables, config)`. No return value. Outputs go through
  `uw.export_data(...)`. Internal helpers are `_`-prefixed.
- **Shared logic → `utils_wca.py`.** If logic appears in two modules,
  centralize it. Don't duplicate.
- **Domain facts → `WCA_CONSTANTS`.** User-tunable values → `config.ini`.
  Test: "if a user changes this, is the output still the same stat?"
  No → constant.
- **`WCA_CONSTANTS` entries are tuples** (immutable). Coerce to list at
  the consuming module's boundary if needed.
- **Don't promote derived values into `WCA_CONSTANTS`.** Compute them
  module-locally from the constants. Single source of truth.
- **Sentinel handling.** DNF (-1), DNS (-2), no-attempt (0) are never
  valid times. Filter with `uw.drop_invalid_results(df, cols)` for
  aggregations or `.query("best > 0")` for simple filters.
- **Module-related tunables** go in the `[<module_name>]` section of
  `config.ini`. Not in `utils_wca`, not scattered in module bodies.

## Module template

Skeleton for a new stat module under `src/wca_stats/modules/`:

```python
import pandas as pd
import logging
import configparser

from wca_stats import utils_wca as uw


###################################################################
######################### COMPUTATIONS ############################
###################################################################


def compute_<stat_name>(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
) -> pd.DataFrame:
    """<One-line description.>"""
    try:
        logger.info(f"Computing <stat_name> for {config.nationality}...")

        final_rounds = uw.WCA_CONSTANTS['final_rounds']  # if needed
        results = db_tables["<appropriate_table>"]
        # ... computation ...

        logger.info(f"Computed <stat_name>: {len(df)} rows.")
        return df

    except Exception as e:
        logger.error(f"Error computing <stat_name>: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
########################### PLOTS #################################
###################################################################

# (if any)


###################################################################
############################### RUN ###############################
###################################################################


def run(db_tables, config):
    logger = logging.getLogger(__name__)
    logger.info("Producing stats for <ModuleName> module")

    results = {
        "<Entry_Name>": compute_<stat_name>(
            db_tables=db_tables, config=config, logger=logger
        ),
        # ...
    }

    figures = {
        # if any
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(
        results,
        figures=figures,
        section_name=section_name,
        config=config,
        logger=logger,
    )
```

Module name flows automatically into the output folder via
`__name__.split(".")[-1]` — don't hardcode it.

## Workflow: adding a new stat (binding)

Follow this sequence. Do not skip steps.

1. **Read first.** `docs/roadmap.md` for the stat description.
   `docs/domain-glossary.md` for any terms in it. The relevant module's
   current code if extending an existing module.

2. **Ask before coding.** Required questions:
   - Is there old code for this stat? (Semantic reference only — not
     a structural template.)
   - Domain subtleties: multivenue inclusion, historical vs current
     nationality, DNF/DNS handling, average format if applicable.
   - Output shape: columns, sort order, tie-breaking.
   - Plot? If yes, what kind.
   Do not proceed until Tom confirms.

3. **Check `utils_wca` before writing helpers.** If the operation
   already exists or should exist there, use / add it there. Do not
   duplicate logic across modules.

4. **Implement.** Match module conventions (template in this file).
   For a new module, do the simplest stat end-to-end first; lock the
   shape before adding more stats.

5. **Flag refactoring opportunities.** If you spot duplicated logic
   that belongs in `utils_wca`, or a tunable that should move to
   `config.ini`, raise it before commit.

6. **Update docs.** After Tom confirms the stat works:
   - Mark the stat ✅ in `docs/roadmap.md`.
   - If a structural decision was made (new shared helper, new derived
     `db_tables` key, deviation from convention), draft an ADR in
     `docs/decisions/`.
   - Do **not** auto-update `architecture.md` or
     `domain-glossary.md` — those are hand-curated. Suggest the diff,
     let Tom apply.

## Don'ts

- **No `Statistic` base class / OOP hierarchy for stats.** Decided
  against — see `docs/decisions/001-no-stat-class.md`. Don't relitigate
  without explicit user request.
- **Don't reach for Claude Code conventions (`.claude/skills/` etc.).**
  This project is run from Claude.ai chat, not Claude Code.
- **Don't batch many stats in one go on a fresh module.** First stat
  always reveals constraints that invalidate assumptions about the rest.
- **Don't guess at WCA domain rules.** Check `docs/domain-glossary.md`
  or ask. Averages, rounds, records have precise definitions.

## User preferences (Tom)

- Direct, concise, peer-level. No filler.
- Point out mistakes explicitly — Tom wants to learn, not just get fixes.
- Suggest better approaches when they exist, flagged clearly.
- Assume Python + SQL fundamentals. Skip basic explanations.
