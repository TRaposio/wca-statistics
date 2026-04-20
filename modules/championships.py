import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw
import matplotlib.pyplot as plt
from pathlib import Path
from shapely.geometry import Point
import geopandas as gpd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Round types representing a final / combined-final.
_FINAL_ROUND_TYPES = ("c", "f")

# Major international championship types in the WCA championships table.
_WORLD_CHAMPIONSHIP_TYPE = "world"


###################################################################
##################### NATIONAL CHAMPIONSHIPS ######################
###################################################################


def compute_national_championship_winners(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event: str | None = None
) -> pd.DataFrame:
    """
    Compute the list of winners at national championships, ordered by year.
    If `event` is provided, restrict the result to that event id.
    Stores the full (unfiltered) winners table in db_tables["nats_champions"].
    """
    try:
        nationality = config.nationality
        logger.info(f"Computing {event or 'all-event'} winners at {config.country} championships...")

        results_country = db_tables["results_country"].copy()

        # --- Filter for national finals/combined rounds ---
        champs = results_country.query(
            "competition_id in @config.nats and round_type_id in @_FINAL_ROUND_TYPES "
            "and best > 0 and person_country_id == @nationality"
        )

        if champs.empty:
            logger.warning(f"No national championship results found for {config.country}.")
            return pd.DataFrame(columns=["Year", "Competition ID", "Event", "WCAID", "Winner"])

        # --- Find the minimum (best) position for each competition + event ---
        min_pos_per_comp = (
            champs.groupby(["competition_id", "event_id"], observed=True)["pos"]
            .min()
            .reset_index()
        )

        # --- Keep all competitors who share that best position (handles ties) ---
        champs = champs.merge(
            min_pos_per_comp,
            on=["competition_id", "event_id", "pos"],
            how="inner"
        )

        champs = (
            champs[["year", "competition_id", "event_id", "person_id", "person_name"]]
            .rename(columns={
                "year": "Year",
                "competition_id": "Competition ID",
                "person_id": "WCAID",
                "person_name": "Winner",
                "event_id": "Event",
            })
            .sort_values(by=["Year", "Event"])
            .reset_index(drop=True)
        )
        champs.index += 1

        db_tables["nats_champions"] = champs

        if event:
            champs = champs[champs["Event"] == event].copy()

        logger.info(f"Found {len(champs)} national championship winners ({event or 'all events'}).")
        return champs

    except Exception as e:
        logger.error(f"Error computing national championship winners: {e}", exc_info=True)
        return pd.DataFrame()


def compute_national_championship_medal_table(
    db_tables: dict,
    config,
    logger,
    event: str | None = None
) -> pd.DataFrame:
    """
    Compute the national championship medal table.
    If `event` is provided, restrict to that event only.
    """
    try:
        logger.info(f"Computing national championship medal table{' for ' + event if event else ''}")

        results = db_tables["results_nationality"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        solve = results.query(
            "round_type_id in @_FINAL_ROUND_TYPES and competition_id in @config.nats and best > 0"
        ).copy()

        if event:
            solve = solve.query("event_id == @event").copy()

        if solve.empty:
            logger.warning(f"No championship results found{' for event ' + event if event else ''}.")
            return pd.DataFrame(columns=["WCAID", "Name", "gold", "silver", "bronze", "podiums"])

        # --- Compute ranking positions per competition/event ---
        solve["nr_rank"] = solve.groupby(["competition_id", "event_id"])["pos"].rank(method="min")

        # --- Count medals and podiums ---
        solve["gold"] = solve["nr_rank"].eq(1).groupby(solve["person_id"]).transform("sum")
        solve["silver"] = solve["nr_rank"].eq(2).groupby(solve["person_id"]).transform("sum")
        solve["bronze"] = solve["nr_rank"].eq(3).groupby(solve["person_id"]).transform("sum")
        solve["podiums"] = solve["nr_rank"].le(3).groupby(solve["person_id"]).transform("sum")

        # --- Aggregate medal counts ---
        medal_table = (
            solve.groupby("person_id")[["gold", "silver", "bronze", "podiums"]]
            .max()
            .reset_index()
            .merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            .sort_values(by=["gold", "silver", "bronze"], ascending=[False, False, False])
            .reset_index(drop=True)
        )
        medal_table.index += 1

        logger.info(
            f"Medal table computed ({len(medal_table)} competitors)"
            f"{' for event ' + event if event else ''}"
        )
        return medal_table[["WCAID", "Name", "gold", "silver", "bronze", "podiums"]]

    except Exception as e:
        logger.error(
            f"Error while computing medal table{' for event ' + event if event else ''}: {e}",
            exc_info=True,
        )
        return pd.DataFrame()


def compute_championship_streaks(
    db_tables: dict,
    config,
    logger,
    event: str | None = None
) -> pd.DataFrame:
    """
    Compute the longest streak of national championship titles per event.
    """
    try:
        logger.info(f"Computing championship win streaks{' for ' + event if event else ''}")

        results = db_tables["results_nationality"]

        champs = results.query(
            "competition_id in @config.nats and round_type_id in @_FINAL_ROUND_TYPES "
            "and best > 0 and person_country_id == @config.nationality"
        ).copy()

        if event:
            champs = champs.query("event_id == @event")

        if champs.empty:
            logger.warning(f"No championship results found{' for event ' + event if event else ''}.")
            return pd.DataFrame(columns=["event_id", "person_id", "person_name", "Consecutive Wins"])

        # --- Keep only winners (position 1) per competition+event ---
        champs = (
            champs.sort_values(by="pos")
            .groupby(["competition_id", "event_id"])
            .nth(0)
            .reset_index(drop=False)[["event_id", "person_id", "person_name", "year"]]
            .sort_values(by=["event_id", "year"])
            .reset_index(drop=True)
        )

        # --- Identify consecutive streaks per event ---
        champs["streak_id"] = (champs["person_id"] != champs["person_id"].shift(1)).cumsum()

        max_streaks = (
            champs.groupby(["event_id", "person_id", "person_name", "streak_id"])
            .size()
            .reset_index(name="count")
            .groupby(["event_id", "person_id", "person_name"])["count"]
            .max()
            .reset_index()
            .rename(columns={"count": "Consecutive Wins"})
            .sort_values(by=["Consecutive Wins", "event_id"], ascending=[False, True])
            .reset_index(drop=True)
        )
        max_streaks.index += 1

        logger.info(
            f"Championship streaks computed ({len(max_streaks)} competitors)"
            f"{' for event ' + event if event else ''}"
        )
        return max_streaks[["event_id", "person_id", "person_name", "Consecutive Wins"]]

    except Exception as e:
        logger.error(
            f"Error while computing championship streaks"
            f"{' for event ' + event if event else ''}: {e}",
            exc_info=True,
        )
        return pd.DataFrame()


def compute_hall_of_fame(
    db_tables: dict,
    config,
    logger,
    event: str = "333"
) -> pd.DataFrame:
    """
    Hall of fame for a specific event: list of national champions ordered
    by total wins, with the years of victory.
    """
    try:
        logger.info(f"Computing championship hall of fame for event {event}")

        results = db_tables["results_nationality"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        champs = results.query(
            "competition_id in @config.nats and round_type_id in @_FINAL_ROUND_TYPES "
            "and best > 0 and event_id == @event"
        ).copy()

        if champs.empty:
            logger.warning(f"No championship winners found for event {event}.")
            return pd.DataFrame(columns=["WCAID", "Name", "wins", "years"])

        # --- Keep only winners (position 1) ---
        champs = (
            champs.sort_values(by="pos")
            .groupby(["competition_id", "event_id"])
            .nth(0)
            .reset_index(drop=False)[["event_id", "person_id", "person_name", "year"]]
            .sort_values(by=["event_id", "year"])
            .reset_index(drop=True)
        )

        # --- Count wins per competitor ---
        w = (
            champs.groupby("person_id")["person_name"]
            .count()
            .reset_index(name="wins")
            .merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            .sort_values(by="wins", ascending=False)
            .reset_index(drop=True)
        )

        # --- Collect years of wins per competitor ---
        w["years"] = [
            ", ".join(map(str, champs.query("person_id == @pid")["year"].sort_values().unique()))
            for pid in w["WCAID"]
        ]
        w.index += 1

        logger.info(f"Hall of fame computed ({len(w)} winners) for event {event}")
        return w[["WCAID", "Name", "wins", "years"]]

    except Exception as e:
        logger.error(f"Error while computing hall of fame for event {event}: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
################ INTERNATIONAL CHAMPIONSHIPS ######################
###################################################################


def compute_international_podiums(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Chronologically ordered podiums (1st-3rd place) obtained by competitors
    of the configured nationality at World and Continental Championships.

    For continental championships, the podium considers only competitors
    whose nationality belongs to the configured continent.
    """
    cont_id = config.continent_id   # e.g. "_Europe", "_Asia"

    try:
        logger.info(
            f"Computing international championship podiums (World + {cont_id})"
        )

        results = db_tables["results"]
        championships = db_tables["championships"]
        countries = db_tables["countries"]

        # --- Identify World + Continental championships ---
        target_types = [_WORLD_CHAMPIONSHIP_TYPE, cont_id]
        intl_champs = championships.query(
            "championship_type in @target_types"
        )[["competition_id", "championship_type"]]

        if intl_champs.empty:
            logger.warning(
                f"No World or Continental ({cont_id}) Championships found in championship table."
            )
            return pd.DataFrame(columns=[
                "year", "championship_type", "competition_id",
                "event_id", "pos", "person_id", "person_name",
            ])

        # --- Precompute continent-eligible country IDs ---
        continent_country_ids = countries.query("continent_id == @cont_id")["id"].unique()
        target_country = config.nationality

        podiums_list = []

        for ctype in target_types:
            comp_ids = intl_champs.query("championship_type == @ctype")["competition_id"].unique()

            subset = results.query(
                "competition_id in @comp_ids and round_type_id in @_FINAL_ROUND_TYPES and best > 0"
            ).copy()

            if subset.empty:
                continue

            if ctype == _WORLD_CHAMPIONSHIP_TYPE:
                # World podium: top 3 overall, then keep our nationality
                subset = subset.query("person_country_id == @target_country and pos <= 3")
            else:
                # Continental podium: rank within the continent, then keep our nationality
                subset = subset.query("person_country_id in @continent_country_ids").copy()
                subset["cont_rank"] = subset.groupby(["competition_id", "event_id"])["pos"].rank(method="min")
                subset = subset.query("cont_rank <= 3 and person_country_id == @target_country").copy()
                subset["pos"] = subset["cont_rank"]

            if not subset.empty:
                subset.loc[:, "championship_type"] = ctype
                podiums_list.append(subset)

        if not podiums_list:
            logger.warning(f"No international podiums found (World + {cont_id}).")
            return pd.DataFrame(columns=[
                "year", "championship_type", "competition_id",
                "event_id", "pos", "person_id", "person_name",
            ])

        podiums = pd.concat(podiums_list, ignore_index=True)

        # --- Extract year from competition_id (last 4 characters) ---
        podiums["year"] = podiums["competition_id"].str[-4:]

        podiums = (
            podiums[[
                "year", "championship_type", "competition_id",
                "event_id", "pos", "person_id", "person_name",
            ]]
            .sort_values(by=["year", "championship_type", "event_id", "pos"])
            .reset_index(drop=True)
        )
        podiums.index += 1

        logger.info(
            f"Computed {len(podiums)} international championship podiums (World + {cont_id})."
        )
        return podiums

    except Exception as e:
        logger.error(
            f"Error while computing international championship podiums: {e}",
            exc_info=True,
        )
        return pd.DataFrame()


def compute_major_final_appearances(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Number of final appearances (sum across all events) at major championships
    (World + Continental) for competitors of the configured nationality.
    """
    cont_id = config.continent_id

    try:
        logger.info(
            f"Computing major championship final appearances (World + {cont_id})"
        )

        results = db_tables["results_nationality"]
        championships = db_tables["championships"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        target_types = [_WORLD_CHAMPIONSHIP_TYPE, cont_id]
        intl_champs = championships.query(
            "championship_type in @target_types"
        )["competition_id"].unique()

        subset = results.query(
            "competition_id in @intl_champs and round_type_id in @_FINAL_ROUND_TYPES and best > 0"
        ).copy()

        if subset.empty:
            logger.warning("No major championship final results found.")
            return pd.DataFrame(columns=["WCAID", "Name", "final_appearances"])

        counts = (
            subset.groupby("person_id")["event_id"]
            .count()
            .reset_index(name="final_appearances")
        )

        df = (
            counts.merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            .sort_values(by="final_appearances", ascending=False)
            .reset_index(drop=True)
        )
        df.index += 1

        logger.info(f"Computed {len(df)} competitors with major championship final appearances")
        return df[["WCAID", "Name", "final_appearances"]]

    except Exception as e:
        logger.error(
            f"Error while computing major championship final appearances: {e}",
            exc_info=True,
        )
        return pd.DataFrame()


###################################################################
################# NATIONAL PARTICIPATION STATS ####################
###################################################################


def compute_national_final_appearances(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Number of national championship final appearances (across all events)
    for competitors of the configured nationality.
    """
    try:
        logger.info("Computing national championship final appearances")

        results = db_tables["results_nationality"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        subset = results.query(
            "competition_id in @config.nats and round_type_id in @_FINAL_ROUND_TYPES"
        ).copy()

        if subset.empty:
            logger.warning("No national championship final results found.")
            return pd.DataFrame(columns=["WCAID", "Name", "final_appearances"])

        counts = (
            subset.groupby("person_id")["event_id"]
            .count()
            .reset_index(name="final_appearances")
        )

        df = (
            counts.merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            .sort_values(by="final_appearances", ascending=False)
            .reset_index(drop=True)
        )
        df.index += 1

        logger.info(f"Computed {len(df)} competitors with national final appearances")
        return df[["WCAID", "Name", "final_appearances"]]

    except Exception as e:
        logger.error(
            f"Error while computing national championship final appearances: {e}",
            exc_info=True,
        )
        return pd.DataFrame()


def compute_national_championships_competed(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Number of distinct national championships competed in by competitors
    of the configured nationality.
    """
    try:
        logger.info("Computing number of national championships competed in")

        results = db_tables["results_nationality"]

        subset = results.query("competition_id in @config.nats and best > 0").copy()

        if subset.empty:
            logger.warning("No national championship participation data found.")
            return pd.DataFrame(columns=["WCAID", "Name", "championships_competed"])

        counts = (
            subset.groupby("person_id")["competition_id"]
            .nunique()
            .reset_index(name="championships_competed")
        )

        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        df = (
            counts.merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            .sort_values(by="championships_competed", ascending=False)
            .reset_index(drop=True)
        )
        df.index += 1

        logger.info(f"Computed {len(df)} competitors with national championship participations")
        return df[["WCAID", "Name", "championships_competed"]]

    except Exception as e:
        logger.error(
            f"Error while computing number of national championships competed in: {e}",
            exc_info=True,
        )
        return pd.DataFrame()


def compute_title_retention_rate(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    For each event, compute the national title retention rate:
        retained / (retained + failed),
    where 'failed' includes cases where the previous year's champion did not repeat
    (even if they didn’t compete). Missing years (e.g. 2020) are skipped entirely
    so consecutive existing years are compared (e.g. 2019 -> 2021).
    """
    try:
        logger.info("Computing national title retention rate by event")

        results = db_tables["results_nationality"]

        subset = results.query(
            "competition_id in @config.nats and round_type_id in @_FINAL_ROUND_TYPES and best > 0"
        ).copy()

        if subset.empty:
            logger.warning("No national finals found for retention computation.")
            return pd.DataFrame(columns=["event_id", "retention_rate", "retained", "failed", "total_transitions"])

        # Best-ranked national champion per EVENT-YEAR
        winners = (
            subset.sort_values(["year", "event_id", "pos"])
                  .drop_duplicates(subset=["year", "event_id"], keep="first")
                  .loc[:, ["year", "event_id", "person_id"]]
                  .sort_values(["event_id", "year"])
                  .reset_index(drop=True)
        )

        # Vectorized consecutive-year comparison per event
        winners["prev_person"] = winners.groupby("event_id")["person_id"].shift(1)
        winners["prev_year"]   = winners.groupby("event_id")["year"].shift(1)

        transitions = winners[winners["prev_person"].notna()].copy()
        transitions["retained"] = (transitions["person_id"] == transitions["prev_person"])

        summary = (
            transitions.groupby("event_id", as_index=False)
            .agg(
                retained=("retained", "sum"),
                total_transitions=("retained", "count"),
            )
        )
        summary["failed"] = summary["total_transitions"] - summary["retained"]
        summary["retention_rate"] = (summary["retained"] / summary["total_transitions"]).round(3)

        summary = summary.sort_values(
            ["retention_rate", "total_transitions", "event_id"],
            ascending=[False, False, True],
        ).reset_index(drop=True)
        summary.index += 1

        logger.info(f"Computed retention rates for {summary['event_id'].nunique()} events")
        return summary[["event_id", "retention_rate", "retained", "failed", "total_transitions"]]

    except Exception as e:
        logger.error(f"Error while computing title retention rate: {e}", exc_info=True)
        return pd.DataFrame()


def compute_sweeps(
    db_tables: dict,
    config,
    logger,
    req_events
) -> pd.DataFrame:
    """
    Chronological list of sweeps where the same competitor of the configured
    nationality is the best-ranked national in ALL the required events at the
    SAME national championship competition.
    """
    try:
        logger.info(f"Computing sweep for events: {' + '.join(req_events)}")

        results = db_tables["results_nationality"]

        subset = results.query(
            "competition_id in @config.nats and round_type_id in @_FINAL_ROUND_TYPES "
            "and best > 0 and event_id in @req_events"
        ).copy()

        if subset.empty:
            logger.warning("No results found for sweep computation.")
            return pd.DataFrame(columns=["year", "competition_id", "person_id", "person_name"])

        # Best-ranked competitor per competition & event
        best_nat = (
            subset.sort_values(["competition_id", "event_id", "pos"])
            .drop_duplicates(subset=["competition_id", "event_id"], keep="first")
        )

        # Count how many required events each person owns per competition
        agg = (
            best_nat.groupby(["competition_id", "year", "person_id", "person_name"])["event_id"]
            .nunique()
            .reset_index(name="events_won")
        )

        n_events = len(req_events)

        sweeps = agg.query("events_won == @n_events").copy()
        sweeps = sweeps.sort_values(["year", "competition_id"]).reset_index(drop=True)
        sweeps.index += 1

        logger.info(f"Found {len(sweeps)} sweeps")
        return sweeps[["year", "competition_id", "person_id", "person_name"]]

    except Exception as e:
        logger.error(f"Error while computing sweeps: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
########################### PLOTS #################################
###################################################################


def plot_competition_locations(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    championship: bool
) -> plt.Figure | None:
    """
    Plot the geographic location of competitions on a country map.

    Currently only supports Italy because the only available shapefile is
    the Istat Italian regions one. For other countries this is a no-op
    with a non-critical warning, until a per-country shapefile system is
    introduced.
    """
    try:
        if config.country.lower() != "italy":
            logger.warning(
                f"Competition location plotting is only supported for Italy "
                f"(config.country = '{config.country}'); skipping."
            )
            return None

        logger.info("Creating competition location map...")

        comps = db_tables["competitions"].copy()

        # --- Filter valid competitions ---
        base_query = "city_name != 'Multiple cities' & country_id == @config.country"
        if championship:
            df = comps.query(f"{base_query} & competition_id in @config.nats")
        else:
            df = comps.query(base_query)
        df = df.drop_duplicates(subset=["competition_id"]).copy()

        # --- Convert microdegrees to decimal ---
        df["latitude"] = df["latitude_microdegrees"] / 1_000_000
        df["longitude"] = df["longitude_microdegrees"] / 1_000_000
        df = df.dropna(subset=["latitude", "longitude"])

        logger.info(f"{len(df):,} competitions with valid coordinates found.")

        # --- Create points GeoDataFrame ---
        geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

        # --- Load country shapefile ---
        shp_dir = Path(config["paths"]["shapefile_dir"])
        shp_file = next(shp_dir.glob("*.shp"), None)
        if not shp_file:
            logger.error(f"No .shp file found in {shp_dir}; skipping plot.")
            return None

        country_map = gpd.read_file(shp_file)
        gdf = gdf.to_crs(country_map.crs)

        # --- Plot ---
        fig, ax = plt.subplots(figsize=(12, 12))
        country_map.plot(ax=ax, color="#f9f9f9", edgecolor="grey", linewidth=0.8)
        gdf.plot(ax=ax, color="red", edgecolor="black", markersize=30, zorder=3)

        title_suffix = " (Championships)" if championship else ""
        ax.set_title(
            f"Location of {config.country} Competitions{title_suffix}",
            fontsize=14, fontweight="bold",
        )
        ax.axis("off")

        fig.tight_layout()
        plt.close(fig)

        logger.info("Competition location map created successfully.")
        return fig

    except Exception as e:
        logger.error(f"Error creating competition location map: {e}", exc_info=True)
        return None


###################################################################
############################### RUN ###############################
###################################################################


def run(db_tables, config):

    logger = logging.getLogger(__name__)
    logger.info("Producing stats for Championships module")

    # --- Tables ---
    results = {
        "Winners": compute_national_championship_winners(
            db_tables=db_tables, config=config, logger=logger, event=None,
        ),
        "Hall of Fame": compute_hall_of_fame(
            db_tables=db_tables, config=config, logger=logger, event="333",
        ),
        "Medal Table": compute_national_championship_medal_table(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Medal Table (FMC)": compute_national_championship_medal_table(
            db_tables=db_tables, config=config, logger=logger, event="333fm",
        ),
        "Streaks": compute_championship_streaks(
            db_tables=db_tables, config=config, logger=logger, event=None,
        ),
        "Major Championship Podiums": compute_international_podiums(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Nats Appearances": compute_national_championships_competed(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Nats Final Appearances": compute_national_final_appearances(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Major Final Appearances": compute_major_final_appearances(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Title Retention": compute_title_retention_rate(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Big Cube Sweeps": compute_sweeps(
            db_tables=db_tables, config=config, logger=logger,
            req_events=["555", "666", "777"],
        ),
        "Blind Sweeps": compute_sweeps(
            db_tables=db_tables, config=config, logger=logger,
            req_events=["333bf", "444bf", "555bf", "333mbf"],
        ),
    }

    # --- Figures ---
    figures = {
        "Locations": plot_competition_locations(
            db_tables=db_tables, config=config, logger=logger, championship=True,
        ),
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)
