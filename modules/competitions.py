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

        logger.info(f"Computed most competitions: {len(df_final)} competitors.")

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
    Newcomers and competitors per year
    """

    try:
        nationality = config.nationality
        df_n = db_tables["results_nationality"].copy() #df_nationality
        df_c = db_tables["results_country"].copy() #df_country
        persons = db_tables["persons"][["id", "gender"]]

        logger.info(f"Computing yearly newcomer statistics for nationality={nationality} (with gender breakdown)")

        # --- Merge gender onto results ---
        df_n = (
            df_n.
            merge(
                persons[["id", "gender"]], 
                how="left", 
                left_on="personId", 
                right_on="id"
            )
            .drop(columns="id")
        )

        df_c = (
            df_c.
            merge(
                persons[["id", "gender"]], 
                how="left", 
                left_on="personId", 
                right_on="id"
            )
            .drop(columns="id")
        )

        # --- Extract newcomer's registration year (first 4 chars of personId) ---
        df_n["newcomer_year"] = df_n["personId"].str[:4].astype(int)


        # --- Newcomers per year ---
        newcomers_by_gender = (
            df_n.loc[df_n["newcomer_year"] == df_n["year"]]
            .groupby(["year", "gender"], observed=True)["personId"]
            .nunique()
            .unstack(fill_value=0)
            .rename_axis(None, axis=1)
            .reset_index()
            .rename(columns = {"f": "Newcomer F", "m": "Newcomer M", "o": "Newcomer O"})
        )

        # --- Competitions and competitors per year --
        competitors_by_gender = (
            df_n
            .groupby(["year", "gender"], observed=True)["personId"]
            .nunique()
            .unstack(fill_value=0)
            .rename_axis(None, axis=1)
            .reset_index()
            .rename(columns = {"f": "Competitors F", "m": "Competitors M", "o": "Competitors O"})
        )

        # --- Italian Competitions ---
        country_competitions = (
            df_c.groupby("year", observed=True)["competitionId"]
            .nunique()
            .reset_index()
            .rename(columns = {"competitionId": "Number of Competitions"})
        )

        # --- Merge all ---
        summary = (
            country_competitions
            .merge(competitors_by_gender, on="year", how="outer")
            .merge(newcomers_by_gender, on="year", how="outer")
            .fillna(0)
            .astype(int)
        )

        # --- Compute totals ---
        summary["Competitors"] = summary["Competitors F"] + summary["Competitors M"] + summary["Competitors O"]
        summary["Newcomer"] = summary["Newcomer F"] + summary["Newcomer M"] + summary["Newcomer O"]

        # --- Compute ratios ---
        summary["Newcomer Ratio"] = summary["Newcomer"] / summary["Competitors"]
        summary["Newcomer Ratio M"] = summary["Newcomer M"] / summary["Competitors M"]
        summary["Newcomer Ratio F"] = summary["Newcomer F"] / summary["Competitors F"]
        summary["Newcomer Ratio O"] = summary["Newcomer O"] / summary["Competitors O"]

        summary = summary.sort_values("year").reset_index(drop=True)
        summary.index += 1

        db_tables["newcomers"] = summary
        logger.info("Computed Newcomer counts.")
        logger.info("Added 'newcomers' DataFrame to db_tables successfully.")

        # --- reorder ---
        summary = summary[[
            'year',
            'Number of Competitions',
            'Competitors',
            'Competitors F',
            'Competitors M',
            'Competitors O',
            'Newcomer',
            'Newcomer F',
            'Newcomer M',
            'Newcomer O',
            'Newcomer Ratio',
            'Newcomer Ratio M',
            'Newcomer Ratio F',
            'Newcomer Ratio O'
        ]]

        return summary

    except Exception as e:
        logger.critical(f"Failed to compute newcomer statistics: {e}", exc_info=True)


def plot_competition_distribution(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure:
    
    """
    Plots competitions trend for a specific country.
    """

    try:
        logger.info(f"Creating figure: Competition Distribution for country {config.country}")

        # --- Load data ---
        if "newcomers" not in db_tables:
            logger.critical("'newcomers' table not found in db_tables.")
        
        df = db_tables["newcomers"].query("year > 2000")

        # --- Create figure and axis ---
        fig, ax = plt.subplots()

        # --- Plot line ---
        ax.plot(
            df["year"],
            df["Number of Competitions"],
            color="tab:blue",
            marker="o",
            linewidth=2,
            zorder=2,
        )

        # --- Labels & title ---
        ax.set_title(f"Competitions per Year - {config.country}", fontweight="bold")
        ax.set_xlabel("Year")
        ax.set_ylabel("Number of Competitions")

        # --- Grid (follows global style) ---
        ax.grid(True, which="major", axis="y", zorder=1)

        # --- X ticks ---
        years = range(df["year"].min(), df["year"].max() + 1)
        step = max(len(years) // 10, 1)
        ax.set_xticks(years[::step])
        ax.set_xticklabels(years[::step], rotation=45, ha="center", fontsize=9)

        # --- Apply tight layout and close (no popup) ---
        fig.tight_layout()
        plt.close(fig)

        logger.info("Figure 'Competition Distribution' created successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating Competition Distribution plot: {e}", exc_info=True)




###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Competitions module")

    results = {
        "Most Competitions": compute_most_competitions(db_tables=db_tables, config=config, logger=logger),
        "Most Countries": compute_most_countries(db_tables=db_tables, config=config, logger=logger),
        "Most Competitors": compute_most_competitors(db_tables=db_tables, config=config, logger=logger),
        "Return Rate": compute_return_rate(db_tables=db_tables, config=config, logger=logger, min_competitors=25),
        "Community Recency": compute_community_recency(db_tables=db_tables, config=config, logger=logger, min_competitors=25, threshold='2022-01-01'),
        "Newcomers": compute_newcomer_statistics(db_tables=db_tables, config=config, logger=logger),
    }

    figures = {
        "Competition Distribution": plot_competition_distribution(db_tables=db_tables, config=config, logger=logger),
        # "Competitor Distribution": plot_competitor_distribution(db_tables=db_tables, config=config, logger=logger),
        # "Newcomer Ratio": plot_newcomers_ratio(db_tables=db_tables, config=config, logger=logger),
        # "Female Presence at Competitions": plot_female_distribution(db_tables=db_tables, config=config, logger=logger)

    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)