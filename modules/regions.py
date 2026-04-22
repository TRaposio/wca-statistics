"""
Regional statistics for WCA competitions.

This module produces region-level stats (competitions per region, regional
win times) and geoplots (regional distribution, competition locations on a
country map). It currently only supports Italy because the only available
shapefile is the Istat regions one and the only city->region mapping file
is Italian.

===========================================================================
HOW TO ADD SUPPORT FOR A NEW COUNTRY  (e.g. Switzerland, cantons)
===========================================================================

Three files on disk + three edits in this file. No other module needs to
change.

FILES ON DISK
    1. Shapefile with country's administrative divisions, in a new folder
       under ./data/  (e.g. ./data/swiss_cantons/*.shp). The shapefile must
       have one polygon per region/canton and a numeric code column used to
       join with REGIONS_DICT (for Italy this is "COD_REG", Istat's region
       code; for Switzerland it would typically be "KANTONSNUM" or similar).

    2. City -> region mapping CSV, semicolon-separated, in ./data/regions/
       Columns: city_name, region_name.
       Example filename: city_to_region_map_che.csv

    3. Point config.ini to the new shapefile:
            [paths]
            shapefile_dir = ./data/swiss_cantons
       and the new mapping file:
            [aux_files]
            regions = city_to_region_map_che.csv

EDITS IN THIS FILE  (search for "# COUNTRY-SPECIFIC:" to find them all)
    A. Change _SUPPORTED_COUNTRY to the new country (lowercase, exact
       string from countries.tsv), or turn it into a set of supported
       countries if you want to keep Italy working too.

    B. Replace REGIONS_DICT with the new country's region -> code dict.
       The code values MUST match the numeric values in the shapefile's
       join column (see step 1 above).

    C. If the shapefile's join column is not "COD_REG", replace the
       three "COD_REG" string literals inside
       plot_competition_distribution_by_region with the new column name.

CONFIG EDITS
    D. config.ini: change [global_variables] country, nationality, and
       championship_type to the new country's WCA codes. Update the
       [paths] shapefile_dir and [aux_files] regions as described above.

TEST
    Run the pipeline. You should see no warnings from this module and
    two new plots: a regional distribution map and a competition location
    map, both styled on the new country's shapefile.

===========================================================================
"""

import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw
import matplotlib.pyplot as plt
import geopandas as gpd
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path
from datetime import datetime
from shapely.geometry import Point


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# COUNTRY-SPECIFIC (A): the only country this module currently supports.
# Compared case-insensitively against config.country.
_SUPPORTED_COUNTRY = "italy"

# COUNTRY-SPECIFIC (B): region_name -> numeric code used to join the
# country's administrative-regions shapefile. Italian values are Istat
# region codes. Replace wholesale when adding another country.
REGIONS_DICT = {
    'Piemonte': 1,
    "Valle d'Aosta": 2,
    'Lombardia': 3,
    'Trentino-Alto Adige': 4,
    'Veneto': 5,
    'Friuli-Venezia Giulia': 6,
    'Liguria': 7,
    'Emilia-Romagna': 8,
    'Toscana': 9,
    'Umbria': 10,
    'Marche': 11,
    'Lazio': 12,
    'Abruzzo': 13,
    'Molise': 14,
    'Campania': 15,
    'Puglia': 16,
    'Basilicata': 17,
    'Calabria': 18,
    'Sicilia': 19,
    'Sardegna': 20,
}


def _country_supported(config, logger, what: str) -> bool:
    """
    Single source of truth for the 'is this country supported?' guard.
    Logs a non-critical warning and returns False for non-supported countries.
    """
    if config.country.lower() != _SUPPORTED_COUNTRY:
        logger.warning(
            f"{what} requires config.country = '{_SUPPORTED_COUNTRY.title()}' "
            f"(currently '{config.country}'); skipping."
        )
        return False
    return True


###################################################################
######################### COMPUTATIONS ############################
###################################################################


def compute_most_regions(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Compute the number of unique national regions each competitor has competed in.

    Expects `db_tables["regions"]` to contain a city_name -> region_name mapping
    (loaded via uw.read_aux_file).
    """
    try:
        logger.info("Computing number of unique regions visited per competitor...")

        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates().copy()
        regions_df = db_tables["regions"]

        # --- Merge competition region data ---
        df = results.merge(regions_df, how="inner", on="city_name")

        # --- Compute number of distinct regions per person ---
        region_counts = (
            df.groupby("person_id", observed=True)["region_name"]
            .nunique()
            .rename("Number of Regions")
            .reset_index()
        )

        summary = (
            region_counts.merge(
                persons,
                how="left",
                left_on="person_id",
                right_on="wca_id",
            )
            .drop(columns=["wca_id"])
            .rename(columns={"person_id": "WCAID", "name": "Name"})
            .sort_values(by="Number of Regions", ascending=False)
            .reset_index(drop=True)
        )
        summary.index += 1

        logger.info(f"Computed regions visited for {len(summary)} competitors.")
        return summary[["WCAID", "Name", "Number of Regions"]]

    except Exception as e:
        logger.error(f"Error computing regions visited: {e}", exc_info=True)
        return pd.DataFrame()


def compute_average_win_times_by_region(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    n_years: int = 2
) -> pd.DataFrame:
    """
    Compute average winning times per event for each national region,
    considering only the past N years of competitions (default 2).
    """
    try:
        logger.info(f"Computing average regional winning times (past {n_years} years)...")

        results = db_tables["results_country"].copy()
        region_map = db_tables["regions"].copy()

        # --- Filter for last N years ---
        final_rounds = uw.WCA_CONSTANTS['final_rounds']
        latest_date = results["date"].max()
        cutoff = latest_date - pd.Timedelta(days=n_years * 365)
        df = results.query(
            "date >= @cutoff & pos == 1 & round_type_id in @final_rounds & best > 0"
        ).copy()
        logger.info(f"Keeping results from {cutoff.date()} to {latest_date.date()}.")

        if df.empty:
            logger.warning("No qualifying results in the time window.")
            return pd.DataFrame()

        # --- Merge with region mapping ---
        df = df.merge(region_map[["city_name", "region_name"]], on="city_name", how="left")

        # --- Special handling for 333fm based on format_id ---
        df["event_id"] = np.where(
            (df["event_id"] == "333fm") & (df["format_id"].isin(["1", "2"])),
            "333fm_1",
            np.where(
                (df["event_id"] == "333fm") & (df["format_id"] == "m"),
                "333fm_m",
                df["event_id"],
            ),
        )

        # --- Special handling for MBLD: net solved (solved - wrong) ---
        df["best"] = np.where(
            df["event_id"] == "333mbf",
            df["best"].apply(uw.multisolved) - df["best"].apply(uw.multiwrong),
            df["best"],
        )

        # --- Pick the right metric per event family ---
        df["metric"] = np.where(
            df["event_id"].isin(["333bf", "444bf", "555bf", "333mbf", "333fm_1"]),
            df["best"],
            df["average"],
        )

        df = df[df["metric"] > 0]

        if df.empty:
            logger.warning("No valid metric values after filtering.")
            return pd.DataFrame()

        # --- Compute average per region and event ---
        region_event_avg = (
            df.groupby(["region_name", "event_id"], observed=True)["metric"]
            .mean()
            .reset_index()
        )

        # --- Post-process metrics by event type ---
        def convert_metric(row):
            event = row["event_id"]
            val = row["metric"]
            if pd.isna(val) or val <= 0:
                return np.nan
            if event == "333fm_m":
                return str(val / 100)
            elif event not in ['333', '333fm_1', '333mbf']:
                return uw.timeconvert(val)
            else:
                # Keep 333 numeric for sorting; converted to time below.
                return val

        region_event_avg["metric"] = region_event_avg.apply(convert_metric, axis=1)

        # --- Pivot to wide format ---
        pivot_df = region_event_avg.pivot(
            index="region_name", columns="event_id", values="metric"
        ).reset_index()

        # --- Sort by 3x3 speed (numeric), then format 3x3 as time ---
        if "333" in pivot_df.columns:
            pivot_df = pivot_df.sort_values(by="333", ascending=True)
            pivot_df["333"] = pivot_df["333"].apply(uw.timeconvert)

        logger.info(f"Computed average winning times for {len(pivot_df)} regions.")
        return pivot_df

    except Exception as e:
        logger.error(f"Error computing regional winning times: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
########################### PLOTS #################################
###################################################################


def plot_competition_distribution_by_region(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure | None:
    """
    Plot the distribution of WCA competitions across national regions on a
    region shapefile, with per-region counts annotated.

    Currently Italy-only (Istat regions shapefile).
    """
    if not _country_supported(config, logger, "Region-level distribution plot"):
        return None

    try:
        logger.info("Creating regional competition distribution map...")

        comps = db_tables["competitions"].query(
            "country_id == @config.country & cancelled == 0"
        ).copy()
        city_to_region = db_tables["regions"].copy()

        comps["date"] = pd.to_datetime(
            dict(year=comps["year"], month=comps["month"], day=comps["day"]),
            errors="coerce",
        )

        # Filter only past or current competitions
        today = pd.Timestamp(datetime.now().date())
        comps = comps[comps["date"].notna() & (comps["date"] <= today)].copy()

        if comps.empty:
            logger.warning("No past competitions to map; skipping.")
            return None

        # --- Merge competitions with city -> region mapping ---
        df = comps.merge(
            city_to_region[["city_name", "region_name"]],
            on="city_name",
            how="left",
            validate="m:1",
        )

        # --- Count competitions per region ---
        comp_per_region = (
            df.groupby("region_name", observed=True)["competition_id"]
            .nunique()
            .reset_index()
            .rename(columns={"competition_id": "Competitions"})
        )

        # --- Merge ISTAT codes ---
        # COUNTRY-SPECIFIC (C): "COD_REG" is the join column in the Istat
        # shapefile. For other countries, replace this column name here
        # and in the two usages below (3 occurrences total).
        df_regions = (
            pd.DataFrame.from_dict(REGIONS_DICT, orient="index")
            .reset_index()
            .rename(columns={"index": "region_name", 0: "COD_REG"})
        )
        region_counts = df_regions.merge(comp_per_region, on="region_name", how="left").fillna(0)

        # --- Load shapefile ---
        shp_dir = Path(config["paths"]["shapefile_dir"])
        shp_file = next(shp_dir.glob("*.shp"), None)
        if not shp_file:
            logger.error(f"No .shp file found in {shp_dir}; skipping plot.")
            return None

        country_gdf = gpd.read_file(shp_file)
        country_gdf["COD_REG"] = country_gdf["COD_REG"].astype(int)

        merged = country_gdf.merge(region_counts, on="COD_REG", how="left").fillna(0)

        # --- Color map ---
        cmap = LinearSegmentedColormap.from_list(
            "",
            ["#faf9fd", "#b6a6e8", "#9b84de", "#8165d7", "#7354d3"],
        )

        # --- Plot ---
        fig, ax = plt.subplots(figsize=(12, 12))
        fig.subplots_adjust(top=0.95)
        fig.suptitle(
            "Distribution of WCA Competitions by Region",
            fontsize=15,
            fontweight="bold",
        )

        merged.plot(
            column="Competitions",
            cmap=cmap,
            linewidth=0.8,
            ax=ax,
            edgecolor="black",
            legend=False,
            zorder=2,
        )
        merged.boundary.plot(ax=ax, color="black", linewidth=0.8, zorder=3)

        # --- Add region counts as annotations ---
        merged["coords"] = merged["geometry"].apply(lambda x: x.representative_point().coords[:][0])
        for _, row in merged.iterrows():
            if row["Competitions"] > 0:
                ax.annotate(
                    text=str(int(row["Competitions"])),
                    xy=row["coords"],
                    ha="center",
                    fontsize=9,
                    color="#2f2f2f",
                    zorder=4,
                )

        ax.axis("off")
        fig.tight_layout()
        plt.close(fig)

        logger.info("Successfully created regional competition map.")
        return fig

    except Exception as e:
        logger.error(f"Error creating regional competition map: {e}", exc_info=True)
        return None


def plot_competition_locations(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    championship: bool
) -> plt.Figure | None:
    """
    Plot the geographic location of all national competitions on a country
    map shapefile.

    Currently Italy-only (Istat shapefile).

    NOTE: same logic also exists in championships.plot_competition_locations.
    The two should eventually be consolidated into utils_wca.
    """
    if not _country_supported(config, logger, "Competition location plot"):
        return None

    try:
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
            fontsize=15, fontweight="bold",
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

    if not _country_supported(config, logger, "Regions module"):
        return

    logger.info("Producing stats for Regions module")

    # --- Tables ---
    results = {
        "Most Regions": compute_most_regions(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Average Winning Time": compute_average_win_times_by_region(
            db_tables=db_tables, config=config, logger=logger, n_years=3,
        ),
    }

    # --- Figures ---
    figures = {
        "Competition Distribution": plot_competition_distribution_by_region(
            db_tables=db_tables, config=config, logger=logger,
        ),
        "Competition Location": plot_competition_locations(
            db_tables=db_tables, config=config, logger=logger, championship=False,
        ),
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)
