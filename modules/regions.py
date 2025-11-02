import pandas as pd
import numpy as np
import utils_wca as uw
import matplotlib.pyplot as plt
import configparser
import logging
import geopandas as gpd
from matplotlib.colors import LinearSegmentedColormap
from pathlib import Path
from datetime import datetime


# Istat codes relative to shapefiles
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
    'Sardegna': 20
}


def compute_most_regions(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Compute the number of unique Italian regions each competitor has competed in.

    This version expects a preloaded DataFrame `regions_df`
    containing competitionId → regionId mappings.

    """

    try:

        logger.info("Computing number of unique Italian regions visited per competitor...")

        # --- Load datasets ---
        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"][["id", "name"]].drop_duplicates().copy()
        regions_df = db_tables["regions"]

        # --- Merge competition region data ---
        df = results.merge(regions_df, how="inner", on="cityName")

        # --- Compute number of distinct regions per person ---
        region_counts = (
            df
            .groupby("personId", observed=True)["regionName"]
            .nunique()
            .rename("Number of Regions")
            .reset_index()
        )

        # --- Merge with person names ---
        summary = (
            region_counts
            .merge(
                persons,
                how="left",
                left_on="personId",
                right_on="id"
            )
            .drop(columns=["id"])
            .rename(columns={"personId": "WCAID", "name": "Name"})
            .sort_values(by="Number of Regions", ascending=False)
            .reset_index(drop=True)
        )

        summary.index += 1

        logger.info(f"Computed regions visited for {len(summary)} competitors.")

        return summary[["WCAID", "Name", "Number of Regions"]]

    except Exception as e:
        logger.critical(f"Error computing regions visited: {e}", exc_info=True)


def plot_italy_competition_distribution(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure:
    
    """
    Plot the distribution of WCA competitions across Italian regions.
    """

    try:
        if config.country.lower() != "italy":
            logger.warning("Region-level plotting is only available for Italy.")
            return None

        logger.info("Creating Italian competition distribution map...")

        comps = db_tables["competitions"].query("countryId == 'Italy' & cancelled == 0").copy()
        city_to_region = db_tables["regions"].copy()

        comps["date"] = pd.to_datetime(
            dict(year=comps["year"], month=comps["month"], day=comps["day"]),
            errors="coerce"
        )

        # Filter only past or current competitions
        today = pd.Timestamp(datetime.now().date())
        comps = comps[comps["date"].notna() & (comps["date"] <= today)].copy()

        # --- Merge competitions with city→region mapping ---
        df = (
            comps
            .merge(
                city_to_region[["cityName", "regionName"]],
                on="cityName",
                how="left",
                validate="m:1"
            )
        )

        # --- Count competitions per region ---
        comp_per_region = (
            df
            .groupby("regionName", observed=True)["id"]
            .nunique()
            .reset_index()
            .rename(columns={"id": "Competitions"})
        )

        # --- Merge ISTAT codes ---
        df_regions = (
            pd.DataFrame.from_dict(REGIONS_DICT, orient="index")
            .reset_index()
            .rename(columns={"index": "regionName", 0: "COD_REG"})
        )
        region_counts = df_regions.merge(comp_per_region, on="regionName", how="left").fillna(0)

        # --- Load shapefile ---
        shp_dir = Path(config["paths"]["shapefile_dir"])
        shp_file = next(shp_dir.glob("*.shp"), None)
        if not shp_file:
            logger.critical(f"No .shp file found in {shp_dir}")

        italy_gdf = gpd.read_file(shp_file)
        italy_gdf["COD_REG"] = italy_gdf["COD_REG"].astype(int)

        # --- Merge counts onto shapefile ---
        merged = italy_gdf.merge(region_counts, on="COD_REG", how="left").fillna(0)

        # --- Color map ---
        cmap = LinearSegmentedColormap.from_list(
            "",
            ["#faf9fd", "#b6a6e8", "#9b84de", "#8165d7", "#7354d3"]
        )

        # --- Plot ---
        fig, ax = plt.subplots(figsize=(12, 12))
        fig.subplots_adjust(top=0.95)
        fig.suptitle(
            "Distribution of WCA Competitions by Region",
            fontsize=15,
            fontweight="bold"
        )

        merged.plot(
            column="Competitions",
            cmap=cmap,
            linewidth=0.8,
            ax=ax,
            edgecolor="black",
            legend=False,
            zorder=2
        )

        merged.boundary.plot(ax=ax, color="black", linewidth=0.8, zorder=3)

        # --- Add region labels and counts ---
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

        logger.info("Successfully created Italian competition map.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating Italian competition map: {e}", exc_info=True)


###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    if config.country.lower() != "italy":
            logger.info("Regions module requires config.country = 'Italy'. Other countries are currently not supported.")

    else:
        logger.info("Producing stats for Events module")

        results = {
            "Most Regions": compute_most_regions(db_tables=db_tables, config=config, logger=logger),
        }

        figures = {
            "Competition Distribution": plot_italy_competition_distribution(db_tables=db_tables, config=config, logger=logger),
        }

        section_name = __name__.split(".")[-1]
        uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)