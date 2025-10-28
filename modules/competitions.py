from pathlib import Path
import pandas as pd
import utils_wca as uw
import matplotlib.pyplot as plt
from datetime import datetime
import configparser
import logging


def compute_most_competitions(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger) -> pd.DataFrame:
    """
    Rank competitors by the number of attended competitions.
    """

    try:
        logger.info("Computing most competitions per competitor...")

        results = db_tables["results_nationality"]
        persons = db_tables["persons"]

        # Count unique competitions per competitor
        df_counts = (
            results
            .groupby("personId")["competitionId"]
            .nunique()
            .reset_index()
            .rename(columns={"personId": "WCAID", "competitionId": "Number of Competitions"})
        )

        # Merge with persons table to get competitor names
        df_final = (
            df_counts
            .merge(persons[["id", "name"]], how="left", left_on="WCAID", right_on="id")
            .drop(columns="id")
            .rename(columns={"name": "Name"})
            .sort_values(by="Number of Competitions", ascending=False)
            .reset_index(drop=True)
        )

        df_final.index += 1

        logger.info(f"Computed most competitions: {len(df_final)} competitors")

        return df_final[["WCAID", "Name", "Number of Competitions"]]

    except Exception as e:
        logger.critical(f"Error computing return rate by country: {e}", exc_info=True)


def compute_most_countries(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger) -> pd.DataFrame:
    """
    Rank competitors by the number of countries competed in.
    """

    try:
        logger.info("Computing most countries per competitor...")

        results = db_tables["results_nationality"]
        persons = db_tables["persons"]

        # Count unique countries per competitor
        df_counts = (
            results
            .query("countryId not in @config.multivenue")
            .groupby("personId")["countryId"]
            .nunique()
            .reset_index()
            .rename(columns={"personId": "WCAID", "countryId": "Number of Countries"})
        )

        # Merge with persons table to get competitor names
        df_final = (
            df_counts
            .merge(persons[["id", "name"]], how="left", left_on="WCAID", right_on="id")
            .drop(columns="id")
            .rename(columns={"name": "Name"})
            .sort_values(by="Number of Countries", ascending=False)
            .reset_index(drop=True)
        )

        df_final.index += 1

        logger.info(f"Computed most countries per competitor.")

        return df_final[["WCAID", "Name", "Number of Countries"]]

    except Exception as e:
        logger.critical(f"Error computing return rate by country: {e}", exc_info=True)


def compute_most_competitors(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger) -> pd.DataFrame:
    """
    Rank competitions by the number of competitors.
    """

    try:
        logger.info("Computing most competitors per competition...")

        results = db_tables["results_country"]
        persons = db_tables["persons"]

        # Count unique countries per competitor
        df = (
            results
            .groupby("competitionId")["personId"]
            .nunique()
            .reset_index()
            .rename(columns={"competitionId": "Competition ID", "personId": "Number of Competitors"})
            .sort_values(by="Number of Competitors", ascending=False)
            .reset_index(drop=True)
        )

        df.index += 1

        logger.info(f"Computed most competitors per competition.")

        return df[["Competition ID", "Number of Competitors"]]

    except Exception as e:
        logger.critical(f"Error computing return rate by country: {e}", exc_info=True)


def compute_return_rate(db_tables: dict,config: configparser.ConfigParser, logger: logging.Logger, min_competitors: int = 25) -> pd.DataFrame:
    """
    Computes competitor return rate by country: the percentage of competitors who attended more than 1 competition.
    """

    try:
        df = db_tables["results"]

        logger.info("Computing competitor return rate by country...")

        # competitions per competitor -> group by country and agg number of competitors and number of returners -> compute return rate -> filter for threshold
        retrate = (
            df
            .groupby(["personCountryId", "personId"])["competitionId"]
            .nunique()
            .reset_index(name="num_comps")
            .groupby("personCountryId")
            .agg(
                Competitors=("num_comps", "size"),
                Returners=("num_comps", lambda x: (x >= 2).sum())
            )
            .reset_index()
            .assign(
                return_rate=lambda x: (100 * x["Returners"] / x["Competitors"]).round(2)
            )
            .query("Competitors >= @min_competitors")
            .rename(columns={"personCountryId": "Country", "return_rate": "Return Rate"})
            .sort_values("Return Rate", ascending=False)
            .reset_index(drop=True)
        )

        retrate.index += 1

        logger.info(
            f"Return rate computed for {len(retrate)} countries (at least {min_competitors} competitors)."
        )

        return retrate

    except Exception as e:
        logger.critical(f"Error computing return rate by country: {e}", exc_info=True)


def run(db_tables, config, logger):

    plt.rcParams["figure.figsize"] = config.figure_size
    plt.rcParams["figure.dpi"] = config.dpi

    logger.info("Producing stats for Competitions module")

    results = {
        "Most Competitions": compute_most_competitions(db_tables=db_tables, config=config, logger=logger),
        "Most Countries": compute_most_countries(db_tables=db_tables, config=config, logger=logger),
        "Most Competitors": compute_most_competitors(db_tables=db_tables, config=config, logger=logger),
        "Return Rate": compute_return_rate(db_tables=db_tables, config=config, logger=logger, min_competitors=25),
    }

    # figures = {
    #     "Competitions": "a"
    # }

    # # Example figure
    # fig, ax = plt.subplots(figsize=(8,6))
    # results["Most Competitions"].plot(kind="bar", x="Competition", y="Count", ax=ax)
    # ax.set_title("Top 10 Competitions")
    # plt.tight_layout()
    # figures["top10_competitions"] = fig

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=None, section_name=section_name, config=config, logger=logger) #figures
