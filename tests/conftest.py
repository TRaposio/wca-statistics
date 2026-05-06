"""
Shared fixtures for the wca-stats test suite.

Provides:
    config     — a configparser loaded from the real config.ini, with paths
                 redirected to a per-test tmp_path so module exports don't
                 clobber real output/.
    db_tables  — synthetic raw tables passed through process_tables, plus an
                 in-memory `regions` aux table (skipping read_aux_file's CSV
                 read). Function-scoped because process_tables mutates inputs.

Logging note: tests use plain logging.getLogger(), not uw.setup_logger,
because the production setup_logger installs ExitOnCriticalHandler, which
calls sys.exit(1) on CRITICAL — that would kill pytest with no traceback.
caplog captures records before they reach any handler, so assertions still
work.
"""
from __future__ import annotations

import configparser
import logging
from pathlib import Path

import pandas as pd
import pytest

from wca_stats import utils_wca as uw


REPO_ROOT = Path(__file__).resolve().parents[1]


# --------------------------------------------------------------------------- #
# Config                                                                      #
# --------------------------------------------------------------------------- #

@pytest.fixture
def config(tmp_path: Path) -> configparser.ConfigParser:
    """
    Load real config.ini, redirect [paths] entries to a tmp dir so test runs
    can't write to the real output/ or logs/.
    """
    cfg = configparser.ConfigParser()
    cfg.read(REPO_ROOT / "config.ini")

    for key in cfg["paths"]:
        cfg["paths"][key] = str(tmp_path / key)
        Path(cfg["paths"][key]).mkdir(parents=True, exist_ok=True)

    cfg.current_events = [
        x.strip() for x in cfg["global_variables"]["current_events"].split(",")
    ]
    cfg.multivenue = [
        x.strip() for x in cfg["global_variables"]["multivenue"].split(",")
    ]
    cfg.country = cfg["global_variables"]["country"]
    cfg.nationality = cfg["global_variables"]["nationality"]
    cfg.championship_type = cfg["global_variables"]["championship_type"]

    return cfg


# --------------------------------------------------------------------------- #
# Synthetic raw tables                                                        #
# --------------------------------------------------------------------------- #

def _build_raw_tables() -> dict[str, pd.DataFrame]:
    """13 raw WCA tables, sized small but covering the cases tests need."""

    persons = pd.DataFrame([
        {"name": "Anna Rossi",    "gender": "f", "wca_id": "2020ROSS01", "sub_id": 1, "country_id": "Italy"},
        {"name": "Marco Bianchi", "gender": "m", "wca_id": "2019BIAN01", "sub_id": 1, "country_id": "Italy"},
        {"name": "Luca Verdi",    "gender": "m", "wca_id": "2018VERD01", "sub_id": 1, "country_id": "Italy"},
        {"name": "Giulia Neri",   "gender": "f", "wca_id": "2017NERI01", "sub_id": 1, "country_id": "United States"},
        {"name": "Giulia Neri",   "gender": "f", "wca_id": "2017NERI01", "sub_id": 2, "country_id": "Italy"},
        {"name": "John Doe",      "gender": "m", "wca_id": "2015DOEJ01", "sub_id": 1, "country_id": "United States"},
    ])

    competitions = pd.DataFrame([
        {"id": "ItalianChamp2024", "name": "Italian Championship 2024", "country_id": "Italy",
         "city_name": "Milan, Lombardy", "venue": "V", "venue_address": "A", "venue_details": "",
         "cell_name": "Italian Championship 2024", "information": "", "external_website": "",
         "event_specs": "333 222 333mbf", "delegates": "", "organizers": "",
         "year": 2024, "month": 6, "day": 1, "end_year": 2024, "end_month": 6, "end_day": 2,
         "latitude_microdegrees": 45_400_000, "longitude_microdegrees": 9_100_000, "cancelled": 0},
        {"id": "RomeOpen2024", "name": "Rome Open 2024", "country_id": "Italy",
         "city_name": "Rome, Lazio", "venue": "V", "venue_address": "A", "venue_details": "",
         "cell_name": "Rome Open 2024", "information": "", "external_website": "",
         "event_specs": "333 222", "delegates": "", "organizers": "",
         "year": 2023, "month": 9, "day": 10, "end_year": 2023, "end_month": 9, "end_day": 10,
         "latitude_microdegrees": 41_900_000, "longitude_microdegrees": 12_500_000, "cancelled": 0},
        {"id": "USNats2024", "name": "US Nationals 2024", "country_id": "United States",
         "city_name": "Boston, MA", "venue": "V", "venue_address": "A", "venue_details": "",
         "cell_name": "US Nationals 2024", "information": "", "external_website": "",
         "event_specs": "333", "delegates": "", "organizers": "",
         "year": 2024, "month": 7, "day": 15, "end_year": 2024, "end_month": 7, "end_day": 16,
         "latitude_microdegrees": 42_300_000, "longitude_microdegrees": -71_000_000, "cancelled": 0},
    ])

    events = pd.DataFrame([
        {"id": "333",    "format": "time",  "name": "3x3x3 Cube",      "rank": 10},
        {"id": "222",    "format": "time",  "name": "2x2x2 Cube",      "rank": 20},
        {"id": "333mbf", "format": "multi", "name": "3x3 Multi-Blind", "rank": 30},
    ])

    formats = pd.DataFrame([
        {"id": "a", "expected_solve_count": 5, "name": "Average of 5", "sort_by": "average", "sort_by_second": "best", "trim_fastest_n": 1, "trim_slowest_n": 1},
        {"id": "m", "expected_solve_count": 3, "name": "Mean of 3",    "sort_by": "average", "sort_by_second": "best", "trim_fastest_n": 0, "trim_slowest_n": 0},
        {"id": "3", "expected_solve_count": 3, "name": "Best of 3",    "sort_by": "best",    "sort_by_second": "average", "trim_fastest_n": 0, "trim_slowest_n": 0},
    ])

    countries = pd.DataFrame([
        {"id": "Italy",         "iso2": "IT", "name": "Italy",         "continent_id": "_Europe"},
        {"id": "United States", "iso2": "US", "name": "United States", "continent_id": "_North America"},
    ])
    continents = pd.DataFrame([
        {"id": "_Europe",        "name": "Europe",        "record_name": "ER"},
        {"id": "_North America", "name": "North America", "record_name": "NAR"},
    ])

    championships = pd.DataFrame([
        {"id": 1, "competition_id": "ItalianChamp2024", "championship_type": "IT"},
        {"id": 2, "competition_id": "USNats2024",       "championship_type": "US"},
    ])

    rounds = pd.DataFrame([
        {"id": "1", "final": 0, "name": "First round",   "rank": 1, "cell_name": "First round"},
        {"id": "2", "final": 0, "name": "Second round",  "rank": 2, "cell_name": "Second round"},
        {"id": "f", "final": 1, "name": "Final",         "rank": 3, "cell_name": "Final"},
        {"id": "c", "final": 1, "name": "Combined Final","rank": 4, "cell_name": "Combined Final"},
    ])

    # results + attempts: 13 rows, mix of valid/DNF/DNS, finals + first rounds
    results_rows = []
    rid = 1
    def add(person, comp, event, rt, fmt, best, avg, pos, country, vals):
        nonlocal rid
        results_rows.append({
            "id": rid, "pos": pos, "best": best, "average": avg,
            "competition_id": comp, "round_type_id": rt, "event_id": event,
            "person_name": person["name"], "person_id": person["wca_id"],
            "format_id": fmt, "regional_single_record": "", "regional_average_record": "",
            "person_country_id": country, "_attempts": vals,
        })
        rid += 1

    add(persons.iloc[0], "ItalianChamp2024", "333", "f", "a", 800,  900, 1, "Italy", [850, 900, 950, 1000, 800])
    add(persons.iloc[1], "ItalianChamp2024", "333", "f", "a", 850,  920, 2, "Italy", [870, 920, 970, 850, 1100])
    add(persons.iloc[2], "ItalianChamp2024", "333", "f", "a", 900,  950, 3, "Italy", [950, 900, 1000, 950, -1])
    add(persons.iloc[0], "ItalianChamp2024", "222", "f", "a", 200, 250, 1, "Italy", [250, 200, 300, 240, 260])
    add(persons.iloc[1], "ItalianChamp2024", "222", "f", "a", 220, 270, 2, "Italy", [270, 220, 320, 270, 280])
    add(persons.iloc[0], "ItalianChamp2024", "333mbf", "f", "3", 970360002, 0, 1, "Italy", [970360002, -1, -2])
    add(persons.iloc[1], "ItalianChamp2024", "333mbf", "f", "3", 960420003, 0, 2, "Italy", [960420003, 970480003, -1])
    add(persons.iloc[0], "RomeOpen2024", "333", "1", "a", 820,  920, 1, "Italy", [820, 920, 970, 1000, 850])
    add(persons.iloc[2], "RomeOpen2024", "333", "1", "a", 880, -1,   3, "Italy", [880, -1, -1, 950, -2])
    add(persons.iloc[0], "RomeOpen2024", "333", "f", "a", 810,  910, 1, "Italy", [810, 910, 960, 990, 850])
    add(persons.iloc[2], "RomeOpen2024", "222", "f", "a", 230, 280, 1, "Italy", [230, 280, 330, 270, 290])
    add(persons.iloc[3], "USNats2024",   "333", "f", "a", 750,  830, 1, "United States", [750, 830, 880, 800, 900])
    add(persons.iloc[5], "USNats2024",   "333", "f", "a", 780,  860, 2, "United States", [780, 860, 910, 830, 940])

    attempts_rows = []
    for r in results_rows:
        for i, v in enumerate(r.pop("_attempts"), start=1):
            attempts_rows.append({"value": v, "attempt_number": i, "result_id": r["id"]})

    results = pd.DataFrame(results_rows)
    attempts = pd.DataFrame(attempts_rows)

    ranks_single = pd.DataFrame([
        {"person_id": "2020ROSS01", "event_id": "333",    "best": 800,       "world_rank": 100, "continent_rank": 20, "country_rank": 1},
        {"person_id": "2019BIAN01", "event_id": "333",    "best": 850,       "world_rank": 200, "continent_rank": 40, "country_rank": 2},
        {"person_id": "2018VERD01", "event_id": "333",    "best": 880,       "world_rank": 300, "continent_rank": 60, "country_rank": 3},
        {"person_id": "2017NERI01", "event_id": "333",    "best": 750,       "world_rank":  50, "continent_rank": 10, "country_rank": 1},
        {"person_id": "2015DOEJ01", "event_id": "333",    "best": 780,       "world_rank":  80, "continent_rank": 15, "country_rank": 2},
        {"person_id": "2020ROSS01", "event_id": "222",    "best": 200,       "world_rank": 150, "continent_rank": 30, "country_rank": 1},
        {"person_id": "2019BIAN01", "event_id": "222",    "best": 220,       "world_rank": 250, "continent_rank": 50, "country_rank": 2},
        {"person_id": "2018VERD01", "event_id": "222",    "best": 230,       "world_rank": 260, "continent_rank": 55, "country_rank": 3},
        {"person_id": "2020ROSS01", "event_id": "333mbf", "best": 970360002, "world_rank":  20, "continent_rank":  5, "country_rank": 1},
        {"person_id": "2019BIAN01", "event_id": "333mbf", "best": 960420003, "world_rank":  40, "continent_rank": 10, "country_rank": 2},
    ])

    ranks_average = pd.DataFrame([
        {"person_id": "2020ROSS01", "event_id": "333", "best": 900, "world_rank": 110, "continent_rank": 22, "country_rank": 1},
        {"person_id": "2019BIAN01", "event_id": "333", "best": 920, "world_rank": 220, "continent_rank": 44, "country_rank": 2},
        {"person_id": "2018VERD01", "event_id": "333", "best": 950, "world_rank": 330, "continent_rank": 66, "country_rank": 3},
        {"person_id": "2020ROSS01", "event_id": "222", "best": 250, "world_rank": 160, "continent_rank": 32, "country_rank": 1},
        {"person_id": "2019BIAN01", "event_id": "222", "best": 270, "world_rank": 270, "continent_rank": 54, "country_rank": 2},
    ])

    scrambles = pd.DataFrame(columns=[
        "scramble", "id", "competition_id", "event_id", "group_id",
        "is_extra", "round_type_id", "scramble_num"
    ])

    return {
        "results": results, "attempts": attempts, "persons": persons,
        "competitions": competitions, "events": events, "formats": formats,
        "ranks_single": ranks_single, "ranks_average": ranks_average,
        "countries": countries, "continents": continents,
        "championships": championships, "rounds": rounds, "scrambles": scrambles,
    }


# --------------------------------------------------------------------------- #
# db_tables                                                                   #
# --------------------------------------------------------------------------- #

@pytest.fixture
def db_tables(config) -> dict[str, pd.DataFrame]:
    """
    Build raw tables, run process_tables, inject regions aux table.
    Function-scoped: process_tables mutates inputs in place.
    """
    raw = _build_raw_tables()
    logger = logging.getLogger("wca_stats.test")
    tables = uw.process_tables(raw, config, logger)

    tables["regions"] = pd.DataFrame([
        {"city_name": "Milan, Lombardy", "region_name": "Lombardia"},
        {"city_name": "Rome, Lazio",     "region_name": "Lazio"},
    ])

    return tables