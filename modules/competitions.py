from pathlib import Path
import pandas as pd
import utils_wca as uw
import matplotlib.pyplot as plt
from datetime import datetime
import configparser
import logging


def compute_most_competitions(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger
) -> pd.DataFrame:
    
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


def compute_most_countries(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger
) -> pd.DataFrame:
    
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


def compute_most_competitors(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger
) -> pd.DataFrame:
    
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


def compute_return_rate(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger, 
    min_competitors: int = 25
) -> pd.DataFrame:
    
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


def compute_community_recency(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger,
    min_competitors: int = 25, 
    threshold: str = '2022-01-01'
) -> pd.DataFrame:
    
    """
    Computes percentage of a country's competitors that competed post Covid-19.
    """

    try:
        df = db_tables["results"]

        logger.info("Computing percentage of each country's competitors that has competed after Covid-19...")

        # competitions per competitor -> group by country and agg number of competitors and number of returners -> compute return rate -> filter for threshold

        threshold_year = pd.to_datetime(threshold).year
        df["competitionYear"] = (
            df["competitionId"]
            .str.extract(r"(\d{4})$")[0]
            .astype(float)
        )
        df["post_covid"] = df["competitionYear"] >= threshold_year

        post_covid = (
            df
            .groupby(["personCountryId", "personId"])["post_covid"]
            .any()
            .reset_index()
            .groupby("personCountryId")
            .agg(
                Competitors=("post_covid", "size"),
                post_covid_competitors=("post_covid", "sum")
            )
            .reset_index()
            .assign(
                rate=lambda x: (100 * x["post_covid_competitors"] / x["Competitors"]).round(2)
            )
            .query("Competitors >= @min_competitors")
            .rename(columns={"personCountryId": "Country", "post_covid_competitors": "Competed After Covid", "rate": "Competed After Covid (%)"})
            .sort_values("Competed After Covid (%)", ascending=False)
            .reset_index(drop=True)
        )

        post_covid.index += 1

        logger.info(
            f"Competitors after covid percentage computed for {len(post_covid)} countries (at least {min_competitors} competitors)."
        )

        return post_covid

    except Exception as e:
        logger.critical(f"Error computing return rate by country: {e}", exc_info=True)


def compute_newcomer_statistics(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    Newcomers per year
    """

    try:
        nationality = config.nationality
        df = db_tables["results_nationality"].copy()
        persons = db_tables["persons"][["id", "gender"]].copy()

        logger.info(f"Computing yearly newcomer statistics for nationality={nationality} (with gender breakdown)")

        # --- Merge gender onto results ---
        df = (
            df.
            merge(
                persons[["id", "gender"]], 
                how="left", 
                left_on="personId", 
                right_on="id"
            )
            .drop(columns="id")
        )

        # --- Extract newcomer's registration year (first 4 chars of personId) ---
        df["newcomer_year"] = df["personId"].str[:4].astype(int)


        # --- Newcomers per year ---
        newcomers_per_year = (
            df.loc[df["newcomer_year"] == df["year"]]
            .groupby("year")["personId"]
            .nunique()
            .rename("Newcomers")
            .reset_index()
        )

        # --- Competitions and competitors per year ---
        totals = (
            df.groupby("year", observed=True)
            .agg(
                Competitions=("competitionId", "nunique"),
                Competitors=("personId", "nunique"),
            )
            .reset_index()
            .merge(newcomers_per_year, on="year", how="left")
            .fillna(0)
        )

        # --- Gender breakdowns ---
        gender_stats = (
            df.groupby(["year", "gender"], observed=True)["personId"]
            .nunique()
            .unstack(fill_value=0)
            .rename_axis(None, axis=1)
            .reset_index()
        )

        newcomers_by_gender = (
            df.loc[df["is_newcomer"]]
            .groupby(["year", "gender"], observed=True)["personId"]
            .nunique()
            .unstack(fill_value=0)
            .rename_axis(None, axis=1)
            .reset_index()
        )

        # --- Merge all ---
        summary = (
            totals.merge(newcomers_per_year, on="year", how="left")
            .merge(gender_stats, on="year", how="left", suffixes=("", "_Competitors"))
            .merge(
                newcomers_by_gender,
                on="year",
                how="left",
                suffixes=("_Competitors", "_Newcomers"),
            )
            .fillna(0)
        )

        # --- Rename gender columns (M, F, O) ---
        summary.rename(
            columns={
                "m_Competitors": "Competitors_M",
                "f_Competitors": "Competitors_F",
                "o_Competitors": "Competitors_O",
                "m_Newcomers": "Newcomers_M",
                "f_Newcomers": "Newcomers_F",
                "o_Newcomers": "Newcomers_O",
            },
            inplace=True,
        )

        # --- Compute ratios ---
        summary["Newcomer_Ratio"] = summary["Newcomers"] / summary["Competitors"]
        summary["Newcomer_Ratio_M"] = summary["Newcomers_M"] / summary["Competitors_M"].replace(0, pd.NA)
        summary["Newcomer_Ratio_F"] = summary["Newcomers_F"] / summary["Competitors_F"].replace(0, pd.NA)
        summary["Newcomer_Ratio_O"] = summary["Newcomers_O"] / summary["Competitors_O"].replace(0, pd.NA)

        # --- Final formatting ---
        summary = summary.fillna(0)
        summary = summary.sort_values("year").reset_index(drop=True)
        summary.index += 1

        # --- Store result ---
        db_tables["newcomers"] = summary
        logger.info("Added 'newcomers' DataFrame to db_tables successfully.")

        return db_tables

    except Exception as e:
        logger.critical(f"Failed to compute newcomer statistics: {e}", exc_info=True)
        raise



##################################################################
##################################################################
##################################################################



def run(db_tables, config, logger):

    plt.rcParams["figure.figsize"] = config.figure_size
    plt.rcParams["figure.dpi"] = config.dpi

    logger.info("Producing stats for Competitions module")

    results = {
        "Most Competitions": compute_most_competitions(db_tables=db_tables, config=config, logger=logger),
        "Most Countries": compute_most_countries(db_tables=db_tables, config=config, logger=logger),
        "Most Competitors": compute_most_competitors(db_tables=db_tables, config=config, logger=logger),
        "Return Rate": compute_return_rate(db_tables=db_tables, config=config, logger=logger, min_competitors=25),
        "Community Recency": compute_community_recency(db_tables=db_tables, config=config, logger=logger, min_competitors=25, threshold='2022-01-01'),
        "Newcomers": compute_newcomer_statistics(db_tables=db_tables, config=config, logger=logger), #salva la tabella in db_tables e la passa ai plot
        "Female Competitors": compute_female_presence_statistics(db_tables=db_tables, config=config, logger=logger)
    }

    figures = {
        "Competition Distribution": plot_competition_distribution(db_tables=db_tables, config=config, logger=logger),
        "Competitor Distribution": plot_competitor_distribution(db_tables=db_tables, config=config, logger=logger),
        "Newcomer Ratio": plot_newcomers_ratio(db_tables=db_tables, config=config, logger=logger),
        "Female Presence at Competitions": plot_female_distribution(db_tables=db_tables, config=config, logger=logger)

    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger) #figures

    

    # # Example figure
    # fig, ax = plt.subplots(figsize=(8,6))
    # results["Most Competitions"].plot(kind="bar", x="Competition", y="Count", ax=ax)
    # ax.set_title("Top 10 Competitions")
    # plt.tight_layout()
    # figures["top10_competitions"] = fig