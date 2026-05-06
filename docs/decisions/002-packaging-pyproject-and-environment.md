# 002 — Packaging via `pyproject.toml` + `environment.yml`

**Status:** Accepted
**Date:** 2026-05-06

## Context

Phase 2 of the restructure moved the codebase to a `src/wca_stats/`
layout. That requires a packaging file so imports (`from wca_stats import
...`) resolve cleanly via an editable install. Two further questions:

1. Which packaging file format? (`pyproject.toml`, `setup.py`,
   `setup.cfg`, plain `requirements.txt`)
2. Do we also keep an environment recipe? Some deps (geopandas, shapely)
   install much more reliably via conda-forge than pip.

## Decision

Use **`pyproject.toml`** as the source of truth for package identity and
dependencies. Install with `pip install -e ".[dev]"`.

Also keep **`environment.yml`** as a conda recipe (channel:
`conda-forge`) for reproducible setup, primarily for the geo stack.

The two files are not redundant:

- `pyproject.toml` defines what `wca-stats` *is* as a package — name,
  version, deps, entry points, dev extras. Used by `pip` and any modern
  Python tool.
- `environment.yml` defines a reproducible *environment* — Python
  version, conda-only binaries (geopandas / shapely), then a `pip:`
  block that installs `-e .` to pick up `pyproject.toml`'s deps.

Tom's daily env is the existing conda env `wca_env` (Python 3.11.13)
with `wca-stats` installed editably. `environment.yml` exists for
collaborators and reproducibility, not as Tom's day-to-day env.

## Consequences

**Positive.**

- Modern, standard layout. `pyproject.toml` is the PEP 517/621 standard;
  any future tooling (ruff, build, hatch) reads it natively.
- Editable install means changes in `src/wca_stats/` are picked up
  without reinstall.
- conda-forge handles geopandas / shapely binaries cleanly — avoids the
  classic pip-install-on-Windows pain.
- Dev deps (pytest) are isolated under `[project.optional-dependencies]
  .dev`, opt-in via `pip install -e ".[dev]"`.

**Negative.**

- Two files to keep in sync when adding a dep. Mitigation: deps are
  declared canonically in `pyproject.toml`; `environment.yml` only lists
  the conda-installed binaries explicitly and defers everything else
  to its `pip:` block (`pip install -e .`).
- Newcomers unfamiliar with the split may add deps to the wrong file.
  Documented in README setup instructions.

**Revisit if:**

- The geo stack ships clean wheels everywhere and conda stops being
  necessary — at that point drop `environment.yml`.
- We adopt a different env manager (uv, pixi) that subsumes both roles.
