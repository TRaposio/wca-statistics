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
from shapely.geometry import Point, Polygon


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


def compute_average_win_times_by_region(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    n_years: int = 2
) -> pd.DataFrame:
    """
    Compute average winning times per event for each Italian region
    considering only the past N years of competitions (default 2).

    TODO: multi is broken, the function is not efficient. Probabily best to calculate multi points
    """

    try:
        logger.info("Computing average regional winning times (past 2 years)...")

        results = db_tables["results_country"].copy()
        competitions = db_tables["competitions"].copy()
        region_map = db_tables["regions"].copy()

        # --- Filter for last two years ---
        latest_date = results["date"].max()
        cutoff = latest_date - pd.Timedelta(days=n_years * 365)
        df = results.query("date >= @cutoff & pos == 1 & roundTypeId in ['c','f'] & best > 0").copy()
        logger.info(f"Keeping results from {cutoff.date()} to {latest_date.date()}.")

        # --- Merge with region mapping ---
        df = df.merge(region_map[["cityName", "regionName"]], on="cityName", how="left")

        # --- Handle metric selection ---
        # Special handling for 333fm based on formatId
        df["eventId"] = np.where(
            (df["eventId"] == "333fm") & (df["formatId"] == "1"),
            "333fm_1",
            np.where(
                (df["eventId"] == "333fm") & (df["formatId"] == "m"),
                "333fm_m",
                df["eventId"]
            )
        )

        # Special handling for multi: calculate points
        df["best"] = np.where(
            df["eventId"] == "333mbf",
            df["best"].apply(uw.multisolved) - df["best"].apply(uw.multiwrong),
            df["best"]
        )

        # --- Filter valid results ---
        df["metric"] = np.where(
            df["eventId"].isin(["333bf", "444bf", "555bf", "333mbf", "333fm_1"]),
            df["best"],
            df["average"]
        )

        # Keep only valid (positive) results
        df = df[df["metric"] > 0]

        # --- Compute average per region and event ---
        region_event_avg = (
            df.groupby(["regionName", "eventId"], observed=True)["metric"]
            .mean()
            .reset_index()
        )

         # --- Post-process metrics by event type ---
        def convert_metric(row):
            event = row["eventId"]
            val = row["metric"]

            if pd.isna(val) or val <= 0:
                return np.nan

            # Fewest Moves → convert to moves (divide by 100)
            if event == "333fm_m":
                return str(val / 100)

            # All others → convert centiseconds to seconds/minutes
            elif event not in ['333', '333fm_1', '333mbf']:
                return uw.timeconvert(val)
            
            # 333 later because I need to sort
            else:
                return val

        region_event_avg["metric"] = region_event_avg.apply(convert_metric, axis = 1) ### loops,

        # --- Pivot to wide format ---
        pivot_df = region_event_avg.pivot(
            index="regionName", columns="eventId", values="metric"
        ).reset_index()

        # --- Sort by 3x3 speed ---
        if "333" in pivot_df.columns:
            pivot_df = pivot_df.sort_values(by="333", ascending=True)
            pivot_df["333"] = pivot_df["333"].apply(uw.timeconvert)

        logger.info(f"Computed average winning times for {len(pivot_df)} regions.")

        return pivot_df

    except Exception as e:
        logger.critical(f"Error computing regional winning times: {e}", exc_info=True)


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


def plot_competition_locations(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
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
        df = comps.query("cityName != 'Multiple cities' & countryId == 'Italy'").drop_duplicates(subset=["id"])

        # --- Convert microdegrees to decimal ---
        df["latitude"] = df["latitude"] / 1000000
        df["longitude"] = df["longitude"] / 1000000

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

    if config.country.lower() != "italy":
            logger.info("Regions module requires config.country = 'Italy'. Other countries are currently not supported.")

    else:
        logger.info("Producing stats for Events module")

        results = {
            "Most Regions": compute_most_regions(db_tables=db_tables, config=config, logger=logger),
            "Average Winning Time": compute_average_win_times_by_region(db_tables=db_tables, config=config, logger=logger, n_years=3),
        }

        figures = {
            "Competition Distribution": plot_italy_competition_distribution(db_tables=db_tables, config=config, logger=logger),
            "Competition Location": plot_competition_locations(db_tables=db_tables, config=config, logger=logger),
        }

        section_name = __name__.split(".")[-1]
        uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)