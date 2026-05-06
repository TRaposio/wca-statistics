"""
Parametrized smoke test for every stat module.

What it checks: running module.run(db_tables, config) produces no
ERROR-or-higher log records.

What it does NOT check: correctness of the stats. Add targeted tests with
hand-crafted data when verifying a specific stat.
"""
from __future__ import annotations

import logging

import pytest

from wca_stats.modules import (
    championships, competitions, events, rankings,
    records, regions, relays, results,
)


# Modules that depend on a real shapefile in [paths]/shapefile_dir.
# Skipped in tests until a shapefile fixture is bundled (see SESSION_HANDOFF).
_NEEDS_SHAPEFILE = {"regions", "championships"}


def _module_id(m):
    return m.__name__.rsplit(".", 1)[-1]


def _module_param(m):
    name = _module_id(m)
    if name in _NEEDS_SHAPEFILE:
        return pytest.param(m, marks=pytest.mark.skip(reason="needs shapefile fixture"), id=name)
    return pytest.param(m, id=name)


MODULES = [
    competitions,
    events,
    regions,
    championships,
    relays,
    records,
    rankings,
    results,
]


@pytest.mark.parametrize("module", [_module_param(m) for m in MODULES])
def test_module_runs_without_errors(module, db_tables, config, caplog):
    caplog.set_level(logging.WARNING)

    module.run(db_tables, config)

    bad = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert not bad, (
        f"{module.__name__} produced {len(bad)} ERROR/CRITICAL log record(s):\n"
        + "\n".join(f"  [{r.levelname}] {r.name}: {r.getMessage()}" for r in bad)
    )