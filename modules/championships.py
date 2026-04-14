import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path
from shapely.geometry import Point, Polygon
import geopandas as gpd


def compute_italian_championship_winners_event(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event: str | None = None
) -> pd.DataFrame:
    
    """
    Compute the list of Italian 3x3 winners at the national championships, ordered by year.
    """

    try:
        nationality = config.nationality
        logger.info(f"Computing {event} winners at {config.country} championships...")

        # --- Load pre-filtered data ---
        results_country = db_tables["results_country"].copy()

        # --- Filter results for national finals/combined rounds ---
        champs = (
                results_country
                .query(
                    "competition_id in @config.nats and round_type_id in ['f', 'c'] "
                    "and best > 0 and person_country_id == @nationality"
                )
            )

        # --- Find the minimum Italian position for each competition ---
        min_pos_per_comp = (
            champs.groupby(["competition_id", "event_id"], observed=True)["pos"].min().reset_index()
        )

        # --- Keep all Italians who share that best position ---
        champs = champs.merge(
            min_pos_per_comp,
            on=["competition_id", "event_id", "pos"],
            how="inner"
        )

        # --- Select and rename columns ---
        champs = champs[["year", "competition_id", "event_id", "person_id", "person_name"]]
        champs = champs.rename(columns={
            "year": "Year",
            "competition_id": "Competition ID",
            "person_id": "WCAID",
            "person_name": "Winner",
            "event_id": "Event"
        })

        champs = champs.sort_values(by=["Year", "Event"]).reset_index(drop=True)
        champs.index += 1

        db_tables["nats_champions"] = champs

        if event:
            champs = champs[champs["Event"] == event].copy()

        logger.info(f"Found {len(champs)} Italian {event} championship winners.")
        return champs

    except Exception as e:
        logger.critical(f"Error computing Italian Championship winners: {e}", exc_info=True)


def compute_national_championship_medal_table(
    db_tables: dict,
    config,
    logger,
    event: str | None = None
) -> pd.DataFrame:
    
    """
    Compute the national championship medal table.
    If `event` is provided, filter results to that specific event only.
    """

    try:
        logger.info(f"Computing national championship medal table{' for ' + event if event else ''}")

        # --- Load required tables ---
        results = db_tables["results_nationality"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        # --- Base result filtering ---
        solve = results.query(
            "person_country_id == @config.nationality and round_type_id in ('c','f') and competition_id in @config.nats and best > 0"
        ).copy()

        # Optional event filter
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
        logger.critical(f"Error while computing medal table{' for event ' + event if event else ''}: {e}")


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

        # --- Load tables ---
        results = db_tables["results_nationality"]
        competitions = db_tables["competitions"]

        # --- Filter to relevant results ---
        champs = results.query(
            "competition_id in @config.nats and round_type_id in ('c','f') "
            "and best > 0 and person_country_id == @config.nationality"
        ).copy()

        # Optional event filter
        if event:
            champs = champs.query("event_id == @event")

        if champs.empty:
            logger.warning(f"No championship results found{' for event ' + event if event else ''}.")
            return pd.DataFrame(columns=["event_id", "person_id", "person_name", "Consecutive Wins"])

        # --- Keep only winners (position 1) ---
        champs = (
            champs
            .sort_values(by="pos")
            .groupby(["competition_id", "event_id"])
            .nth(0)
            .reset_index(drop=False)[["event_id", "person_id", "person_name", "year"]]
            .sort_values(by=["event_id", "year"])
            .reset_index(drop=True)
        )

        # --- Identify consecutive streaks per event ---
        champs["streak_id"] = (champs["person_id"] != champs["person_id"].shift(1)).cumsum()

        max_streaks = (
            champs
            .groupby(["event_id", "person_id", "person_name", "streak_id"])
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
        logger.critical(f"Error while computing championship streaks{' for event ' + event if event else ''}: {e}")


def compute_albo_doro(
    db_tables: dict,
    config,
    logger,
    event: str = "333"
) -> pd.DataFrame:
    
    """
    Compute the 'albo d'oro' — list of national championship winners for a specific event.
    Default event is 3x3 ('333').
    """

    try:
        logger.info(f"Computing championship winners (albo d'oro) for event {event}")

        # --- Load table ---
        results = db_tables["results_nationality"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        # --- Filter for national championships and winners ---
        champs = results.query(
            "competition_id in @config.nats and round_type_id in ('c','f') "
            "and best > 0 and event_id == @event"
        ).copy()

        if champs.empty:
            logger.warning(f"No championship winners found for event {event}.")
            return pd.DataFrame(columns=["WCAID", "Name", "wins", "years"])

        # --- Keep only winners (position 1) ---
        champs = (
            champs
            .sort_values(by="pos")
            .groupby(["competition_id", "event_id"])
            .nth(0)
            .reset_index(drop=False)[["event_id", "person_id", "person_name", "year"]]
            .sort_values(by=["event_id", "year"])
            .reset_index(drop=True)
        )


        # --- Count number of wins per competitor ---
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

        logger.info(f"Albo d'oro computed ({len(w)} winners) for event {event}")

        return w[["WCAID", "Name", "wins", "years"]]

    except Exception as e:
        logger.critical(f"Error while computing championship winners (albo d'oro) for event {event}: {e}")


def compute_international_podiums(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute chronologically ordered podiums (1st–3rd place) obtained by Italians
    at World and European Championships.

    For '_Europe' championships, podiums include only competitors whose
    nationality belongs to countries with continent_id == '_Europe'.
    """

    try:
        logger.info("Computing international championship podiums (World + _Europe)")

        # --- Load tables ---
        results = db_tables["results"]
        championships = db_tables["championships"]
        countries = db_tables["countries"]

        # --- Identify World + European Championships ---
        intl_champs = championships.query("championship_type in ['world', '_Europe']")[["competition_id", "championship_type"]]

        if intl_champs.empty:
            logger.warning("No World or European Championships found in championship table.")
            return pd.DataFrame(
                columns=[
                    "year", "championship_type", "competition_id",
                    "event_id", "pos", "person_id", "person_name"
                ]
            )

        # --- Precompute European country IDs ---
        european_country_ids = countries.query("continent_id == '_Europe'")["id"].unique()
        target_country = config.nationality

        podiums_list = []

        # --- Process each championship type ---
        for ctype in ["world", "_Europe"]:
            comp_ids = intl_champs.query("championship_type == @ctype")["competition_id"].unique()

            # Filter to finals with valid results
            subset = results.query(
                "competition_id in @comp_ids and round_type_id in ('c','f') and best > 0"
            ).copy()

            if subset.empty:
                continue

            if ctype == "world":
                # World podium: top 3 overall Italians
                subset = subset.query("person_country_id == @target_country and pos <= 3")

            else:  # "_Europe"
                # Keep only European competitors
                subset = subset.query("person_country_id in @european_country_ids").copy()

                # Rank within Europeans per competition/event
                subset["euro_rank"] = subset.groupby(["competition_id", "event_id"])["pos"].rank(method="min")

                # Keep podium (top 3 Europeans)
                subset = subset.query("euro_rank <= 3 and person_country_id == @target_country").copy()

                subset["pos"] = subset["euro_rank"]

            if not subset.empty:
                subset.loc[:, "championship_type"] = ctype
                podiums_list.append(subset)

        # --- Combine all podiums ---
        if not podiums_list:
            logger.warning("No international podiums found (World + _Europe).")
            return pd.DataFrame(
                columns=[
                    "year", "championship_type", "competition_id",
                    "event_id", "pos", "person_id", "person_name"
                ]
            )

        podiums = pd.concat(podiums_list, ignore_index=True)

        # --- Extract year from competition_id (last 4 characters) ---
        podiums["year"] = podiums["competition_id"].str[-4:]

        # --- Clean up and order ---
        podiums = (
            podiums[[
                "year", "championship_type", "competition_id",
                "event_id", "pos", "person_id", "person_name"
            ]]
            .sort_values(by=["year", "championship_type", "event_id", "pos"])
            .reset_index(drop=True)
        )

        podiums.index += 1

        logger.info(f"Computed {len(podiums)} international championship podiums (World + _Europe).")

        return podiums

    except Exception as e:
        logger.critical(f"Error while computing international championship podiums: {e}")


def compute_national_final_appearances(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute the number of national championship final appearances (sum of all events)
    for competitors of the configured nationality.
    """

    try:
        logger.info("Computing national championship final appearances")

        results = db_tables["results_nationality"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        # Finals only, valid results, national championships only
        subset = results.query(
            "competition_id in @config.nats and round_type_id in ('c','f')"
        ).copy()

        if subset.empty:
            logger.warning("No national championship final results found.")
            return pd.DataFrame(columns=["WCAID", "Name", "final_appearances"])

        # Count distinct (competition_id, event_id) appearances per person
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
        logger.critical(f"Error while computing national championship final appearances: {e}")


def compute_national_championships_competed(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute the number of distinct national championships competed in
    by competitors of the configured nationality.
    """

    try:
        logger.info("Computing number of national championships competed in")

        results = db_tables["results_nationality"]

        subset = results.query("competition_id in @config.nats and best > 0").copy()

        if subset.empty:
            logger.warning("No national championship participation data found.")
            return pd.DataFrame(columns=["WCAID", "Name", "championships_competed"])

        # Count distinct national championships
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
        logger.critical(f"Error while computing number of national championships competed in: {e}")


def compute_major_final_appearances(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute the number of final appearances (sum of all events)
    for competitors of the configured nationality at major championships
    (World and European).
    """

    try:
        logger.info("Computing major championship final appearances (World + _Europe)")

        results = db_tables["results_nationality"]
        championships = db_tables["championships"]
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        intl_champs = championships.query("championship_type in ['world', '_Europe']")["competition_id"].unique()

        subset = results.query(
            "competition_id in @intl_champs and round_type_id in ('c','f') and best > 0"
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
        logger.critical(f"Error while computing major championship final appearances: {e}")


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

        # National finals with valid results
        subset = results.query(
            "competition_id in @config.nats and round_type_id in ('c','f') and best > 0"
        ).copy()

        if subset.empty:
            logger.warning("No national finals found for retention computation.")
            return pd.DataFrame(columns=["event_id", "retention_rate", "retained", "failed", "total_transitions"])

        # Best-ranked Italian champion per EVENT-YEAR
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

        # Any row with a previous champion defines a transition (we SKIP missing years automatically)
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

        summary = summary.sort_values(["retention_rate", "total_transitions", "event_id"], ascending=[False, False, True]).reset_index(drop=True)
        summary.index += 1

        logger.info(f"Computed retention rates for {summary['event_id'].nunique()} events")
        return summary[["event_id", "retention_rate", "retained", "failed", "total_transitions"]]

    except Exception as e:
        logger.critical(f"Error while computing title retention rate: {e}")


def compute_sweeps(
    db_tables: dict,
    config,
    logger,
    req_events
) -> pd.DataFrame:
    """
    Chronological list of sweeps where the same Italian is the best-ranked Italian
    in 555, 666, and 777 finals at the SAME national championship competition.
    """
    try:
        logger.info(f"Computing Sweep for events: {' + '.join(req_events)}")

        results = db_tables["results_nationality"]

        subset = results.query(
            "competition_id in @config.nats and round_type_id in ('c','f') and best > 0 and event_id in @req_events"
        ).copy()

        if subset.empty:
            logger.warning("No results found for sweep computation.")
            return pd.DataFrame(columns=["year", "competition_id", "person_id", "person_name"])

        # Best-ranked Italian per competition & event
        best_it = (
            subset.sort_values(["competition_id", "event_id", "pos"])
                  .drop_duplicates(subset=["competition_id", "event_id"], keep="first")
        )

        # Count how many required events each person owns per competition
        agg = (
            best_it.groupby(["competition_id", "year", "person_id", "person_name"])["event_id"]
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
        logger.critical(f"Error while computing Big Cubes sweeps: {e}")



def plot_competition_locations(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    championship: bool
) -> plt.Figure:
    """
    Plot the geographic location of all Italian competitions.

    Reads coordinates from the competitions table, filters only valid ones,
    and overlays them on the Italian region shapefile.
    """

    try:
        if config.country.lower() != "italy":
            logger.warning("Competition location plotting is only supported for Italy.")
            return None

        logger.info("Creating competition location map...")

        comps = db_tables["competitions"].copy()

        # --- Filter valid competitions ---
        if championship:
            df = comps.query("city_name != 'Multiple cities' & country_id == 'Italy' & competition_id in @config.nats").drop_duplicates(subset=["competition_id"])
        else:
            df = comps.query("city_name != 'Multiple cities' & country_id == 'Italy'").drop_duplicates(subset=["competition_id"])

        # --- Convert microdegrees to decimal ---
        df["latitude"] = df["latitude_microdegrees"] / 1000000
        df["longitude"] = df["longitude_microdegrees"] / 1000000

        # --- Drop missing coordinates ---
        df = df.dropna(subset=["latitude", "longitude"])

        logger.info(f"{len(df):,} competitions with valid coordinates found.")

        # --- Create points GeoDataFrame ---
        geometry = [Point(xy) for xy in zip(df["longitude"], df["latitude"])]
        gdf = gpd.GeoDataFrame(df, geometry=geometry, crs="EPSG:4326")

        # --- Load Italian shapefile ---
        shp_dir = Path(config["paths"]["shapefile_dir"])
        shp_file = next(shp_dir.glob("*.shp"), None)
        if not shp_file:
            raise FileNotFoundError(f"No .shp file found in {shp_dir}")

        italy = gpd.read_file(shp_file)
        italy_crs = italy.crs

        # --- Convert points CRS to match map ---
        gdf = gdf.to_crs(italy_crs)

        # --- Plot ---
        fig, ax = plt.subplots(figsize=(12, 12))
        italy.plot(ax=ax, color="#f9f9f9", edgecolor="grey", linewidth=0.8)
        gdf.plot(ax=ax, color="red", edgecolor="black", markersize=30, zorder=3)

        # --- Styling ---
        ax.set_title("Location of Italian Competitions", fontsize=14, fontweight="bold")
        ax.axis("off")

        # --- Layout and return ---
        fig.tight_layout()
        plt.close(fig)

        logger.info("Competition location map created successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating competition location map: {e}", exc_info=True)




###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Championships module")

    results = {
        "Vincitori": compute_italian_championship_winners_event(db_tables=db_tables, config=config, logger=logger, event=None),
        "Albo d'oro": compute_albo_doro(db_tables=db_tables, config=config, logger=logger, event='333'),
        "Medagliere": compute_national_championship_medal_table(db_tables=db_tables, config=config, logger=logger),
        "Medagliere FMC": compute_national_championship_medal_table(db_tables=db_tables, config=config, logger=logger, event='333fm'),
        "Streaks": compute_championship_streaks(db_tables=db_tables, config=config, logger=logger, event=None),
        "Major Championshp Podiums": compute_international_podiums(db_tables=db_tables, config=config, logger=logger),
        "Nats Appearances": compute_national_championships_competed(db_tables=db_tables, config=config, logger=logger),
        "Nats Final Appearances": compute_national_final_appearances(db_tables=db_tables, config=config, logger=logger),
        "Major Final Appearances": compute_major_final_appearances(db_tables=db_tables, config=config, logger=logger),
        "Title Retention": compute_title_retention_rate(db_tables=db_tables, config=config, logger=logger),
        "Big Cube Sweeps": compute_sweeps(db_tables=db_tables, config=config, logger=logger, req_events=["555", "666", "777"]),
        "Blind Sweeps": compute_sweeps(db_tables=db_tables, config=config, logger=logger, req_events=["333bf", "444bf", "555bf", "333mbf"])
    }

    figures = {
        "Locations": plot_competition_locations(db_tables=db_tables, config=config, logger=logger, championship=True)
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)