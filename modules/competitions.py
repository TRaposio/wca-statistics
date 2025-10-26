from pathlib import Path
import pandas as pd
import utils_wca as uw
import matplotlib.pyplot as plt
from datetime import datetime

import pandas as pd

def most_competitions(db_tables: dict, config, logger=None) -> pd.DataFrame:
    """
    Rank competitors by the number of attended competitions.
    """

    if logger:
        logger.info("Computing most competitions per competitor...")

    # Retrieve necessary tables
    try:
        results = db_tables["results"]
        persons = db_tables["persons"]
    except KeyError as e:
        raise KeyError(f"Missing table in db_tables: {e}")

    country_filter = config.country

    # Count unique competitions per competitor
    df_counts = (
        results.query("personCountryId == @country_filter")
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

    if logger:
        logger.info(f"Computed most competitions: {len(df_final)} competitors")

    return df_final[["WCAID", "Name", "Number of Competitions"]]



def run(db_tables, config, logger):

    plt.rcParams["figure.figsize"] = config.figure_size
    plt.rcParams["figure.dpi"] = config.dpi

    logger.info("Producing stats for Competitions module")

    results = {
        "Most Competitions": most_competitions(db_tables=db_tables, config=config, logger=logger),
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
