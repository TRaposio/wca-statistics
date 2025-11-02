import pandas as pd
import utils_wca as uw
import matplotlib.pyplot as plt
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

        results = db_tables["results_fixed"]
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

        results = db_tables["results_fixed"]
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

        # Count unique competitors per competition
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
        ax.plot(df["year"], df["Number of Competitions"], color="tab:blue", marker="o", linewidth=2, zorder=2)

        # --- Labels & title ---
        ax.set_title(f"Competitions per Year - {config.country}", fontweight="bold")
        ax.set_xlabel("Year", fontweight="bold")
        ax.set_ylabel("Number of Competitions", fontweight="bold")

        # --- Grid (follows global style) ---
        ax.grid(True, which="major", axis="y", zorder=1)

        # --- X ticks ---
        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        ax.set_xticklabels(years, rotation=45, ha="center")

        # --- Footnote for total competitions ---
        tot = df["Number of Competitions"].sum().astype(int)
        if tot:
            note = f"{tot} total competitions were held the country."
            fig.text(
                0.5, 0.005,  # centered at bottom
                note,
                ha="center",
                fontsize=9,
                color="dimgray",
                style="italic"
            )

        # --- Apply tight layout and close (no popup) ---
        fig.tight_layout()
        plt.close(fig)

        logger.info("Figure 'Competition Distribution' created successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating Competition Distribution plot: {e}", exc_info=True)


def plot_unique_competitor_distribution(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure:
    
    """
    Plots competitor and newcomer distribution for a specific nationality.
    """

    try:
        logger.info(f"Creating figure: Competition Distribution for country {config.nationality}")

        # --- Load data ---
        if "newcomers" not in db_tables:
            logger.critical("'newcomers' table not found in db_tables.")
        
        df = db_tables["newcomers"].query("year > 2000")

        # --- Create figure and axis ---
        fig, ax = plt.subplots()

        # --- Plot line ---
        ax.bar(df["year"], df["Competitors"], color="#eee600", zorder=2, label="Competitors")
        ax.bar(df["year"], df["Newcomer"], color="#4179e1", zorder=3, label="Newcomers")

        # --- Labels & title ---
        ax.set_title(f"Number of Unique Competitors - {config.country}", fontweight="bold")
        ax.set_xlabel("Year", fontweight="bold")
        ax.set_ylabel("Unique Persons", fontweight="bold")

        # --- Grid (follows global style) ---
        ax.grid(True, which="major", axis="y", zorder=1)

        # --- X ticks ---
        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="center")

        # --- Footnote for total competitions ---
        tot = db_tables["newcomers"]["Newcomer"].sum().astype(int)
        if tot:
            note = f"The WCA has registered {tot} competitors from country {config.country}."
            fig.text(
                0.5, 0.005,  # centered at bottom
                note,
                ha="center",
                fontsize=9,
                color="dimgray",
                style="italic"
            )

        # --- Apply tight layout and close (no popup) ---
        fig.tight_layout()
        plt.close(fig)

        logger.info("Figure 'Competitor Distribution' created successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating Competition Distribution plot: {e}", exc_info=True)


def plot_newcomers_ratio(
    db_tables: dict,
    config,
    logger
) -> plt.Figure:
    """
    Plots newcomer ratios (overall and by gender) per year.
    """
    try:
        logger.info(f"Creating figure: Newcomer Ratios for {config.nationality}")

        # --- Load data ---
        if "newcomers" not in db_tables:
            logger.critical("'newcomers' table not found in db_tables.")
            return None

        df = db_tables["newcomers"].query("year > 2000")

        # --- Create figure ---
        fig, ax = plt.subplots()

        # --- Grid ---
        ax.grid(which="major", axis="y", zorder=1)

        # --- Plot lines ---
        ax.plot(df["year"], df["Newcomer Ratio"], color="#6d757b", lw=5, alpha=0.3, label="Total", zorder=3)
        ax.plot(df["year"], df["Newcomer Ratio M"], color="#2c19d8", lw=2.5, label="Male", linestyle="-", marker=".", zorder=4)
        ax.plot(df["year"], df["Newcomer Ratio F"], color="#fd21bb", lw=2.5, label="Female", linestyle="-", marker=".", zorder=4)
        # ax.plot(df["year"], df["Newcomer Ratio O"], color="#8c564b", lw=2, label="Other", linestyle=":", zorder=4)

        # --- Titles and labels ---
        ax.set_title(f"Newcomer Ratios by Gender - {config.country}", fontweight="bold")
        ax.set_xlabel("Year", fontweight="bold")
        ax.set_ylabel("Newcomers / Competitors", fontweight="bold")

        # --- X ticks ---
        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="center")

        # --- Y axis ---
        ax.set_ylim(bottom=0)

        # --- Legend ---
        ax.legend(loc="best")

        # --- Final layout ---
        fig.tight_layout()
        plt.close(fig)

        logger.info("Figure 'Newcomer Ratio' created successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating Newcomer Ratio plot: {e}", exc_info=True)
        return None


def plot_gender_distribution_vert(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure:
    """
    Plot gender-based competitor and newcomer trends per year.
    Creates three vertically stacked subplots: Male, Female, and Other.
    """

    try:
        logger.info("Creating gender-based competitor distribution plot...")

        # --- Load data ---
        if "newcomers" not in db_tables:
            logger.critical("'newcomers' table not found in db_tables.")
            return None

        df = db_tables["newcomers"].query("year > 2000")

        # --- Create figure and axes ---
        fig, (ax_m, ax_f) = plt.subplots(2, sharex=True, figsize=(10, 12))
        fig.suptitle(f"Number of Unique Competitors by Gender - {config.nationality}", fontweight="bold")

        # --- Common grid style ---
        def style_ax(ax):
            ax.grid(which="major", axis="y", zorder=1)
            ax.set_axisbelow(True)
            ax.tick_params(axis="y", labelsize=9)

        # --- Male panel ---
        style_ax(ax_m)
        ax_m.bar(df["year"], df["Competitors M"], color="#4169e1", zorder=2, label="Male Competitors")
        ax_m.bar(df["year"], df["Newcomer M"], color="#73c2fb", zorder=3, label="Male Newcomers")
        ax_m.legend()
        ax_m.set_ylabel("Competitors", fontweight="bold")

        # --- Female panel ---
        style_ax(ax_f)
        ax_f.bar(df["year"], df["Competitors F"], color="#e0115f", zorder=2, label="Female Competitors")
        ax_f.bar(df["year"], df["Newcomer F"], color="#ffb6c1", zorder=3, label="Female Newcomers")
        ax_f.legend()
        ax_f.set_ylabel("Competitors", fontweight="bold")

        # --- X-axis formatting ---
        years = range(df["year"].min(), df["year"].max() + 1)
        ax_m.set_xticks(years)
        ax_f.set_xticks(years)
        plt.setp(ax_f.get_xticklabels(), rotation=45, ha="center", fontsize = 10)


        # --- Footnote for "Other" competitors ---
        nonzero_years = df.loc[df["Newcomer O"] > 0, "year"].tolist()
        if nonzero_years:
            years_text = ", ".join(map(str, nonzero_years))
            total_competitors = df['Newcomer O'].sum().astype(int)
            note = f"{total_competitors} total competitors who identify as 'other' competed in: {years_text}."
        else:
            note = "No competitors who identify as 'other' have been recorded yet."

        fig.text(
            0.5, 0.005,  # centered at bottom
            note,
            ha="center",
            fontsize=9,
            color="dimgray",
            style="italic"
        )

        # --- Layout and return ---
        fig.tight_layout()
        fig.subplots_adjust(top=0.93)
        plt.close(fig)

        logger.info("Gender distribution plot created successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating gender distribution plot: {e}", exc_info=True)
        return None


def plot_gender_distribution_area(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure:
    """
    Stacked area chart showing percentage distribution of genders across years.
    """

    try:
        logger.info("Creating stacked area chart for gender share over time...")

        if "newcomers" not in db_tables:
            logger.critical("'newcomers' table not found in db_tables.")
            return None

        df = db_tables["newcomers"].query("year > 2000").copy()

        # --- Compute percentage per gender ---
        df["Male %"] = df["Competitors M"] / df["Competitors"] * 100
        df["Female %"] = df["Competitors F"] / df["Competitors"] * 100
        df["Other %"] = df["Competitors O"] / df["Competitors"] * 100

        # --- Create the stacked area plot ---
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.stackplot(
            df["year"],
            df["Male %"],
            df["Female %"],
            df["Other %"],
            colors=["#4169e1", "#e0115f", "#9370db"],
            labels=["Male", "Female", "Other"],
            alpha=0.9
        )

        ax.set_title(f"Gender Distribution Among Competitors - {config.nationality}", fontweight="bold")
        ax.set_ylabel("Percentage of Competitors", fontweight="bold")
        ax.set_ylim(0, 100)
        ax.grid(visible=False)

        # --- Format ticks ---
        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="center")

        # --- Move legend below the chart ---
        ax.legend(
            loc="upper center",
            bbox_to_anchor=(0.5, -0.12),
            ncol=3, 
            frameon=False,
            fontsize=10
        )

        fig.tight_layout()
        fig.subplots_adjust(bottom=0.18)  # extra space for legend
        plt.close(fig)

        logger.info("Stacked area gender distribution plot created successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error creating stacked area gender distribution plot: {e}", exc_info=True)


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
        "Competitor Distribution": plot_unique_competitor_distribution(db_tables=db_tables, config=config, logger=logger),
        "Newcomer Ratio": plot_newcomers_ratio(db_tables=db_tables, config=config, logger=logger),
        "Competitor Distribution by gender": plot_gender_distribution_vert(db_tables=db_tables, config=config, logger=logger),
        "Gender distribution": plot_gender_distribution_area(db_tables=db_tables, config=config, logger=logger),
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)