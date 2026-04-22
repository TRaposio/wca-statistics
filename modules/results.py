import pandas as pd
import numpy as np
import logging
import configparser
import matplotlib.pyplot as plt
import utils_wca as uw


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_FINAL_ROUND_TYPES = ("c", "f")
_INVALID_VALUES = (0, -1, -2)
_TOP_N = 100
_MBLD_EVENT = "333mbf"
_FMC_EVENT = "333fm"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _get_event_list(config: configparser.ConfigParser, logger: logging.Logger) -> list[str]:
    """
    Read the per-event list driving stats 4-8 from [results] -> event_list.
    Falls back to ['333'] with a warning if the section is missing.
    """
    try:
        raw = config["results"]["event_list"]
    except KeyError:
        logger.warning("Missing [results]->event_list in config.ini; defaulting to ['333'].")
        return ["333"]
    return [e.strip() for e in raw.split(",") if e.strip()]


def _get_medal_table_year(config: configparser.ConfigParser, logger: logging.Logger) -> int | None:
    """
    Read the year for the per-year medal table from [results] -> medal_table_year.
    Returns None if missing or empty (i.e. no per-year table will be produced).
    """
    try:
        raw = config["results"].get("medal_table_year", "").strip()
    except KeyError:
        return None
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning(f"Invalid [results]->medal_table_year='{raw}'; ignoring.")
        return None


###################################################################
###################### COUNTRY-WIDE STATS #########################
###################################################################


def compute_medal_table(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    year: int | None = None,
) -> pd.DataFrame:
    """
    Stat 1 — Medal table for finals (round_type 'c' or 'f') of competitors of
    the configured nationality, across ALL events.

    Parameters
    ----------
    year : int | None
        If provided, restrict the computation to competitions held in that year.
        If None, computes the all-time medal table.
    """
    try:
        scope = f"year={year}" if year is not None else "all-time"
        logger.info(f"Computing medal table for {config.nationality} ({scope})")

        df = (
            db_tables["results_nationality"]
            .query("round_type_id in @_FINAL_ROUND_TYPES and pos in [1, 2, 3] and best > 0")
            .copy()
        )

        if year is not None:
            df = df.query("year == @year")

        if df.empty:
            logger.warning(f"No medal-eligible finals found ({scope}).")
            return pd.DataFrame(columns=["WCAID", "Name", "Gold", "Silver", "Bronze", "Total"])

        # One row per (person, medal_type), then pivot
        df["medal"] = df["pos"].map({1: "Gold", 2: "Silver", 3: "Bronze"})
        counts = (
            df.groupby(["person_id", "medal"], observed=True)
            .size()
            .unstack(fill_value=0)
            .reindex(columns=["Gold", "Silver", "Bronze"], fill_value=0)
            .reset_index()
        )
        counts["Total"] = counts[["Gold", "Silver", "Bronze"]].sum(axis=1)

        out = (
            counts.merge(uw.get_current_persons(db_tables), left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            [["WCAID", "Name", "Gold", "Silver", "Bronze", "Total"]]
            .sort_values(["Gold", "Silver", "Bronze", "Total"], ascending=[False, False, False, False])
            .reset_index(drop=True)
        )
        out.index += 1
        return out

    except Exception as e:
        logger.error(f"Error computing medal table: {e}", exc_info=True)
        return pd.DataFrame()


def compute_current_world_rankings(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Stat 2 — Best current world rankings (single + average) for the nationality,
    restricted to current_events. Sorted by world_rank ascending.
    """
    try:
        logger.info(f"Computing best current world rankings for {config.nationality}")

        cols = ["person_id", "name", "event_id", "best", "world_rank"]
        single = (
            db_tables["ranks_single_nationality"]
            .query("event_id in @config.current_events")[cols]
            .assign(type="single")
        )
        avg = (
            db_tables["ranks_average_nationality"]
            .query("event_id in @config.current_events")[cols]
            .assign(type="average")
        )

        out = pd.concat([single, avg], ignore_index=True)
        out["Result"] = out.apply(lambda r: uw.format_result(r["best"], r["event_id"], logger), axis=1)
        out = (
            out.rename(columns={
                "person_id": "WCAID", "name": "Name", "event_id": "Event",
                "type": "Type", "world_rank": "World Rank",
            })
            [["World Rank", "WCAID", "Name", "Event", "Type", "Result"]]
            .sort_values(["World Rank", "Name"], ascending=[True, True])
            .reset_index(drop=True)
        )
        out.index += 1
        return out

    except Exception as e:
        logger.error(f"Error computing world rankings: {e}", exc_info=True)
        return pd.DataFrame()


def compute_most_top10_rankings(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
) -> pd.DataFrame:
    """
    Stat 3 — Number of top-10 national rankings (single + average combined) per
    competitor, across current_events.
    """
    try:
        logger.info(f"Computing most top-10 national rankings for {config.nationality}")

        single = (
            db_tables["ranks_single_nationality"]
            .query("event_id in @config.current_events and 0 < country_rank <= 10")
            .groupby("person_id").size()
        )
        avg = (
            db_tables["ranks_average_nationality"]
            .query("event_id in @config.current_events and 0 < country_rank <= 10")
            .groupby("person_id").size()
        )

        total = single.add(avg, fill_value=0).astype(int)

        out = (
            total.reset_index(name="Top10 Rankings")
            .merge(uw.get_current_persons(db_tables), left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            [["WCAID", "Name", "Top10 Rankings"]]
            .sort_values("Top10 Rankings", ascending=False)
            .reset_index(drop=True)
        )
        out.index += 1
        return out

    except Exception as e:
        logger.error(f"Error computing top-10 rankings: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
###################### PER-EVENT STATS ############################
###################################################################


def compute_top100_singles(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event_id: str,
) -> pd.DataFrame:
    """
    Stat 4 — Take the 100 fastest singles ever (any attempt, any round) by
    competitors of the configured nationality for `event_id`, then count
    appearances per person.
    """
    try:
        logger.info(f"Computing top-{_TOP_N} single appearances for {event_id}")

        df = (
            db_tables["results_nationality_detailed"]
            .query("event_id == @event_id and value > 0")
            [["person_id", "value"]]
            .nsmallest(_TOP_N, "value")
        )

        if df.empty:
            logger.warning(f"No valid singles for event {event_id}.")
            return pd.DataFrame(columns=["WCAID", "Name", f"Top{_TOP_N} singles"])

        counts = (
            df.groupby("person_id").size()
            .reset_index(name=f"Top{_TOP_N} singles")
            .merge(uw.get_current_persons(db_tables), left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            [["WCAID", "Name", f"Top{_TOP_N} singles"]]
            .sort_values(f"Top{_TOP_N} singles", ascending=False)
            .reset_index(drop=True)
        )
        counts.index += 1
        return counts

    except Exception as e:
        logger.error(f"Error computing top-{_TOP_N} singles for {event_id}: {e}", exc_info=True)
        return pd.DataFrame()


def compute_top100_averages(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event_id: str,
) -> pd.DataFrame:
    """
    Stat 5 — Take the 100 fastest averages ever (any round) by competitors of the
    configured nationality for `event_id`, then count appearances per person.
    """
    try:
        logger.info(f"Computing top-{_TOP_N} average appearances for {event_id}")

        df = (
            db_tables["results_nationality"]
            .query("event_id == @event_id and average > 0")
            [["person_id", "average"]]
            .nsmallest(_TOP_N, "average")
        )

        if df.empty:
            logger.warning(f"No valid averages for event {event_id}.")
            return pd.DataFrame(columns=["WCAID", "Name", f"Top{_TOP_N} averages"])

        counts = (
            df.groupby("person_id").size()
            .reset_index(name=f"Top{_TOP_N} averages")
            .merge(uw.get_current_persons(db_tables), left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            [["WCAID", "Name", f"Top{_TOP_N} averages"]]
            .sort_values(f"Top{_TOP_N} averages", ascending=False)
            .reset_index(drop=True)
        )
        counts.index += 1
        return counts

    except Exception as e:
        logger.error(f"Error computing top-{_TOP_N} averages for {event_id}: {e}", exc_info=True)
        return pd.DataFrame()


def compute_best_podiums(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event_id: str,
) -> pd.DataFrame:
    """
    Stat 6 — Best all-time podiums hosted in the configured country for `event_id`,
    ranked by the sum of the three podium averages (lower = better).
    Uses results_country (i.e. competitions held in the country, regardless of
    competitor nationality).
    """
    try:
        logger.info(f"Computing best podiums for {event_id}")

        base = (
            db_tables["results_country"]
            .query(
                "event_id == @event_id and round_type_id in @_FINAL_ROUND_TYPES "
                "and pos in [1, 2, 3] and average > 0"
            )
            [["competition_id", "pos", "person_name", "average"]]
            .copy()
        )

        if base.empty:
            logger.warning(f"No valid podiums for event {event_id}.")
            return pd.DataFrame()

        # Pivot: one row per competition with First/Second/Third columns
        wide = (
            base.pivot_table(
                index="competition_id",
                columns="pos",
                values=["person_name", "average"],
                aggfunc="first",
            )
        )
        # Keep only competitions with all three positions populated
        wide = wide.dropna()
        if wide.empty:
            logger.warning(f"No complete podiums (1st/2nd/3rd) for event {event_id}.")
            return pd.DataFrame()

        out = pd.DataFrame({
            "competition_id": wide.index,
            "First":   wide[("person_name", 1)].values,
            "raw1":    wide[("average",     1)].values,
            "Second":  wide[("person_name", 2)].values,
            "raw2":    wide[("average",     2)].values,
            "Third":   wide[("person_name", 3)].values,
            "raw3":    wide[("average",     3)].values,
        })
        out["raw_sum"] = out["raw1"] + out["raw2"] + out["raw3"]
        out = out.sort_values("raw_sum", ascending=True).reset_index(drop=True)

        # Format for display (event-aware)
        out["Average1"]   = out["raw1"].apply(lambda v: uw.format_result(v, event_id, logger))
        out["Average2"]   = out["raw2"].apply(lambda v: uw.format_result(v, event_id, logger))
        out["Average3"]   = out["raw3"].apply(lambda v: uw.format_result(v, event_id, logger))
        out["Podium Sum"] = out["raw_sum"].apply(lambda v: uw.format_result(v, event_id, logger))

        out = out[[
            "competition_id", "Podium Sum",
            "First", "Average1", "Second", "Average2", "Third", "Average3"
        ]].rename(columns={"competition_id": "Competition ID"})
        out.index += 1
        return out

    except Exception as e:
        logger.error(f"Error computing best podiums for {event_id}: {e}", exc_info=True)
        return pd.DataFrame()


def compute_best_first_average(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event_id: str,
) -> pd.DataFrame:
    """
    Stat 7 — Best "first average" ever for `event_id`: for each competitor whose
    CURRENT nationality matches the configured one, take their truly first
    valid official average (regardless of nationality at the time), then rank
    by speed.

    Uses `results_fixed`, where person_country_id is overwritten with each
    competitor's current (sub_id=1) nationality. This ensures that someone who
    e.g. used to compete as American but is now Italian still has their early
    American results considered.

    Within a single competition, multiple rounds may exist; the chronologically
    earliest round is the one with the LOWEST `rank` value (round rank
    increases with later rounds: 1st round < 2nd round < semifinal < final).
    """
    try:
        logger.info(f"Computing best first averages for {event_id}")

        df = (
            db_tables["results_fixed"]
            .query("event_id == @event_id and average > 0")
            [["person_id", "average", "competition_id", "date", "rank"]]
            .sort_values(["date", "rank"], ascending=[True, True])
        )

        if df.empty:
            logger.warning(f"No valid averages for event {event_id}.")
            return pd.DataFrame()

        first = (
            df.groupby("person_id", as_index=False).first()
            .merge(uw.get_current_persons(db_tables), left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
        )
        first["First Average"] = first["average"].apply(lambda v: uw.format_result(v, event_id, logger))

        out = (
            first.sort_values("average", ascending=True)
            .rename(columns={
                "person_id": "WCAID", "name": "Name",
                "competition_id": "Competition ID", "date": "Date",
            })
            [["WCAID", "Name", "First Average", "Competition ID", "Date"]]
            .reset_index(drop=True)
        )
        out.index += 1
        return out

    except Exception as e:
        logger.error(f"Error computing best first averages for {event_id}: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
############################# PLOTS ###############################
###################################################################


def plot_male_vs_female_distribution(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event_id: str,
    upper_percentile: float = 99.0,
) -> plt.Figure | None:
    """
    Stat 8 — Histogram of male vs female averages distribution for `event_id`,
    restricted to the configured nationality.

    The x-axis is clipped to the `upper_percentile`-th percentile of the
    combined distribution (default: 99th) to avoid a few outlier slow times
    squashing the meaningful left part of the distribution. All data is still
    counted in the bins; only the visible range changes.
    """
    try:
        logger.info(f"Plotting M vs F averages distribution for {event_id}")

        persons = db_tables["persons"].query("sub_id == 1")
        female_ids = set(persons.query("gender == 'f' and country_id == @config.nationality")["wca_id"])

        df = (
            db_tables["results_nationality"]
            .query("event_id == @event_id and average > 0")
            [["person_id", "average"]]
            .copy()
        )
        if df.empty:
            logger.warning(f"No averages to plot for event {event_id}.")
            return None

        if event_id == _MBLD_EVENT:
            logger.warning(f"Event {event_id} has no average; skipping M/F histogram.")
            return None

        # Both FMC and timed events: divide by 100 to get natural unit (moves or seconds).
        df["display"] = df["average"] / 100
        unit = "moves" if event_id == _FMC_EVENT else "s"
        xlabel = f"{event_id} average ({unit})"

        male = df.loc[~df["person_id"].isin(female_ids), "display"]
        female = df.loc[df["person_id"].isin(female_ids), "display"]

        # --- Determine x-axis upper bound from combined distribution ---
        x_min = max(0, df["display"].min() * 0.95)
        x_max = np.percentile(df["display"], upper_percentile)
        # Pad slightly so the rightmost bar isn't flush against the edge
        x_max = x_max * 1.02

        # --- Shared bin edges across both series for a fair comparison ---
        bin_edges = np.linspace(x_min, x_max, 80)

        fig, ax = plt.subplots()
        ax.hist(male,   bins=bin_edges, density=True, color="tab:blue", alpha=0.7, label=f"M (n={len(male):,})")
        ax.hist(female, bins=bin_edges, density=True, color="tab:red",  alpha=0.7, label=f"F (n={len(female):,})", zorder=3)

        ax.set_xlim(x_min, x_max)
        ax.set_xlabel(xlabel)
        ax.set_ylabel("Density")
        ax.set_title(
            f"{config.nationality} — {event_id}: Male vs Female averages distribution"
        )
        ax.legend()

        fig.tight_layout()
        plt.close(fig)
        return fig

    except Exception as e:
        logger.error(f"Error plotting M vs F distribution for {event_id}: {e}", exc_info=True)
        return None


###################################################################
############################### RUN ###############################
###################################################################


def run(db_tables, config):

    logger = logging.getLogger(__name__)
    logger.info("Producing stats for Results module")

    event_list = _get_event_list(config, logger)
    medal_year = _get_medal_table_year(config, logger)
    logger.info(f"Per-event stats will be computed for: {event_list}")

    # --- Country-wide tables ---
    results = {
        "Medal Table": compute_medal_table(db_tables=db_tables, config=config, logger=logger),
        "Current World Rankings": compute_current_world_rankings(db_tables=db_tables, config=config, logger=logger),
        "Most Top10 Rankings": compute_most_top10_rankings(db_tables=db_tables, config=config, logger=logger),
    }

    if medal_year is not None:
        results[f"Medal Table {medal_year}"] = compute_medal_table(
            db_tables=db_tables, config=config, logger=logger, year=medal_year,
        )

    # --- Per-event tables (stats 4, 5, 6, 7) ---
    for event in event_list:
        results[f"Top100 Singles {event}"]  = compute_top100_singles(db_tables, config, logger, event)
        results[f"Top100 Averages {event}"] = compute_top100_averages(db_tables, config, logger, event)
        results[f"Best Podiums {event}"]    = compute_best_podiums(db_tables, config, logger, event)
        results[f"Best First Average {event}"] = compute_best_first_average(db_tables, config, logger, event)

    # --- Per-event plots (stat 8) ---
    figures = {
        f"M_vs_F_{event}": plot_male_vs_female_distribution(db_tables, config, logger, event)
        for event in event_list
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)
