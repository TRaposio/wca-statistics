import pandas as pd
import logging
import configparser
import utils_wca as uw
import matplotlib.pyplot as plt


###################################################################
######################### COMPUTATIONS ############################
###################################################################


def compute_most_competitions(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """Rank competitors by the number of attended competitions."""
    try:
        logger.info("Computing most competitions per competitor...")

        results = db_tables["results_fixed"]
        persons = db_tables["persons"].query("sub_id == 1").copy()

        df_counts = (
            results.groupby("person_id")["competition_id"]
            .nunique()
            .reset_index()
            .rename(columns={"person_id": "WCAID", "competition_id": "Number of Competitions"})
        )

        df_final = (
            df_counts.merge(persons[["wca_id", "name"]], how="left", left_on="WCAID", right_on="wca_id")
            .drop(columns="wca_id")
            .rename(columns={"name": "Name"})
            .sort_values(by="Number of Competitions", ascending=False)
            .reset_index(drop=True)
        )
        df_final.index += 1

        logger.info(f"Computed most competitions: {len(df_final)} competitors.")
        return df_final[["WCAID", "Name", "Number of Competitions"]]

    except Exception as e:
        logger.error(f"Error computing most competitions: {e}", exc_info=True)
        return pd.DataFrame()


def compute_most_countries(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """Rank competitors by the number of countries competed in."""
    try:
        logger.info("Computing most countries per competitor...")

        results = db_tables["results_fixed"]
        persons = db_tables["persons"].query("sub_id == 1").copy()

        df_counts = (
            results.query("country_id not in @config.multivenue")
            .groupby("person_id")["country_id"]
            .nunique()
            .reset_index()
            .rename(columns={"person_id": "WCAID", "country_id": "Number of Countries"})
        )

        df_final = (
            df_counts.merge(persons[["wca_id", "name"]], how="left", left_on="WCAID", right_on="wca_id")
            .drop(columns="wca_id")
            .rename(columns={"name": "Name"})
            .sort_values(by="Number of Countries", ascending=False)
            .reset_index(drop=True)
        )
        df_final.index += 1

        logger.info(f"Computed most countries per competitor ({len(df_final)} competitors).")
        return df_final[["WCAID", "Name", "Number of Countries"]]

    except Exception as e:
        logger.error(f"Error computing most countries per competitor: {e}", exc_info=True)
        return pd.DataFrame()


def compute_most_competitors(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """Rank competitions by the number of competitors."""
    try:
        logger.info("Computing most competitors per competition...")

        results = db_tables["results_country"]

        df = (
            results.groupby("competition_id")["person_id"]
            .nunique()
            .reset_index()
            .rename(columns={"competition_id": "Competition ID", "person_id": "Number of Competitors"})
            .sort_values(by="Number of Competitors", ascending=False)
            .reset_index(drop=True)
        )
        df.index += 1

        logger.info(f"Computed most competitors per competition ({len(df)} competitions).")
        return df[["Competition ID", "Number of Competitors"]]

    except Exception as e:
        logger.error(f"Error computing most competitors per competition: {e}", exc_info=True)
        return pd.DataFrame()


def compute_return_rate(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    min_competitors: int = 25
) -> pd.DataFrame:
    """
    Compute competitor return rate by country: the percentage of competitors
    who attended more than one competition.
    """
    try:
        logger.info("Computing competitor return rate by country...")

        df = db_tables["results"]

        retrate = (
            df.groupby(["person_country_id", "person_id"])["competition_id"]
            .nunique()
            .reset_index(name="num_comps")
            .groupby("person_country_id")
            .agg(
                Competitors=("num_comps", "size"),
                Returners=("num_comps", lambda x: (x >= 2).sum()),
            )
            .reset_index()
            .assign(
                return_rate=lambda x: (100 * x["Returners"] / x["Competitors"]).round(2)
            )
            .query("Competitors >= @min_competitors")
            .rename(columns={"person_country_id": "Country", "return_rate": "Return Rate"})
            .sort_values("Return Rate", ascending=False)
            .reset_index(drop=True)
        )
        retrate.index += 1

        logger.info(
            f"Return rate computed for {len(retrate)} countries "
            f"(at least {min_competitors} competitors)."
        )
        return retrate

    except Exception as e:
        logger.error(f"Error computing return rate by country: {e}", exc_info=True)
        return pd.DataFrame()


def compute_community_recency(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    min_competitors: int = 25,
    threshold: str = '2022-01-01'
) -> pd.DataFrame:
    """
    Compute the percentage of each country's competitors that has competed
    after a given date threshold (defaults to post-COVID era).
    """
    try:
        logger.info(
            f"Computing percentage of each country's competitors that has competed after {threshold}..."
        )

        df = db_tables["results"].copy()

        threshold_year = pd.to_datetime(threshold).year
        df["competition_year"] = (
            df["competition_id"]
            .str.extract(r"(\d{4})$")[0]
            .astype(float)
        )
        df["post_covid"] = df["competition_year"] >= threshold_year

        post_covid = (
            df.groupby(["person_country_id", "person_id"])["post_covid"]
            .any()
            .reset_index()
            .groupby("person_country_id")
            .agg(
                Competitors=("post_covid", "size"),
                post_covid_competitors=("post_covid", "sum"),
            )
            .reset_index()
            .assign(
                rate=lambda x: (100 * x["post_covid_competitors"] / x["Competitors"]).round(2)
            )
            .query("Competitors >= @min_competitors")
            .rename(columns={
                "person_country_id": "Country",
                "post_covid_competitors": "Competed After Covid",
                "rate": "Competed After Covid (%)",
            })
            .sort_values("Competed After Covid (%)", ascending=False)
            .reset_index(drop=True)
        )
        post_covid.index += 1

        logger.info(
            f"Competitors-after-threshold percentage computed for {len(post_covid)} countries "
            f"(at least {min_competitors} competitors)."
        )
        return post_covid

    except Exception as e:
        logger.error(f"Error computing community recency: {e}", exc_info=True)
        return pd.DataFrame()


def compute_newcomer_statistics(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """Newcomers and competitors per year, broken down by gender."""
    try:
        nationality = config.nationality
        logger.info(
            f"Computing yearly newcomer statistics for nationality={nationality} (with gender breakdown)"
        )

        df_n = db_tables["results_nationality"].copy()
        df_c = db_tables["results_country"].copy()
        persons = db_tables["persons"][["wca_id", "gender"]]

        # --- Merge gender onto results ---
        df_n = (
            df_n.merge(persons, how="left", left_on="person_id", right_on="wca_id")
            .drop(columns="wca_id")
        )
        df_c = (
            df_c.merge(persons, how="left", left_on="person_id", right_on="wca_id")
            .drop(columns="wca_id")
        )

        # --- Extract newcomer's registration year (first 4 chars of person_id) ---
        df_n["newcomer_year"] = df_n["person_id"].str[:4].astype(int)

        # --- Newcomers per year ---
        newcomers_by_gender = (
            df_n.loc[df_n["newcomer_year"] == df_n["year"]]
            .groupby(["year", "gender"], observed=True)["person_id"]
            .nunique()
            .unstack(fill_value=0)
            .rename_axis(None, axis=1)
            .reset_index()
            .rename(columns={"f": "Newcomer F", "m": "Newcomer M", "o": "Newcomer O"})
        )

        # --- Competitors per year ---
        competitors_by_gender = (
            df_n.groupby(["year", "gender"], observed=True)["person_id"]
            .nunique()
            .unstack(fill_value=0)
            .rename_axis(None, axis=1)
            .reset_index()
            .rename(columns={"f": "Competitors F", "m": "Competitors M", "o": "Competitors O"})
        )

        # --- Competitions hosted in the configured country, per year ---
        country_competitions = (
            df_c.groupby("year", observed=True)["competition_id"]
            .nunique()
            .reset_index()
            .rename(columns={"competition_id": "Number of Competitions"})
        )

        # --- Merge all ---
        summary = (
            country_competitions
            .merge(competitors_by_gender, on="year", how="outer")
            .merge(newcomers_by_gender, on="year", how="outer")
            .fillna(0)
            .astype(int)
        )

        # --- Totals and ratios ---
        summary["Competitors"] = summary["Competitors F"] + summary["Competitors M"] + summary["Competitors O"]
        summary["Newcomer"] = summary["Newcomer F"] + summary["Newcomer M"] + summary["Newcomer O"]
        summary["Newcomer Ratio"] = summary["Newcomer"] / summary["Competitors"]
        summary["Newcomer Ratio M"] = summary["Newcomer M"] / summary["Competitors M"]
        summary["Newcomer Ratio F"] = summary["Newcomer F"] / summary["Competitors F"]
        summary["Newcomer Ratio O"] = summary["Newcomer O"] / summary["Competitors O"]

        summary = summary.sort_values("year").reset_index(drop=True)
        summary.index += 1

        db_tables["newcomers"] = summary
        logger.info("Computed Newcomer counts; cached 'newcomers' DataFrame in db_tables.")

        # --- Reorder columns for the exported view ---
        summary = summary[[
            'year',
            'Number of Competitions',
            'Competitors', 'Competitors F', 'Competitors M', 'Competitors O',
            'Newcomer', 'Newcomer F', 'Newcomer M', 'Newcomer O',
            'Newcomer Ratio', 'Newcomer Ratio M', 'Newcomer Ratio F', 'Newcomer Ratio O',
        ]]

        return summary

    except Exception as e:
        logger.error(f"Failed to compute newcomer statistics: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
########################### PLOTS #################################
###################################################################


def plot_competition_distribution(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure | None:
    """Plot competitions per year for the configured country."""
    try:
        logger.info(f"Creating figure: Competition Distribution for country {config.country}")

        if "newcomers" not in db_tables:
            logger.warning("'newcomers' table not found in db_tables; skipping plot.")
            return None

        df = db_tables["newcomers"].query("year > 2000")

        fig, ax = plt.subplots()
        ax.plot(df["year"], df["Number of Competitions"], color="tab:blue", marker="o", linewidth=2, zorder=2)

        ax.legend(loc="upper left")

        ax.set_title(f"Competitions per Year - {config.country}", fontweight="bold")
        ax.set_xlabel("Year", fontweight="bold")
        ax.set_ylabel("Number of Competitions", fontweight="bold")
        ax.grid(True, which="major", axis="y", zorder=1)

        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        ax.set_xticklabels(years, rotation=45, ha="center")

        # --- Footnote for total competitions ---
        tot = df["Number of Competitions"].sum().astype(int)
        if tot:
            note = f"{tot} total competitions were held in {config.country}."
            fig.text(0.5, 0.005, note, ha="center", fontsize=9, color="dimgray", style="italic")

        fig.tight_layout()
        plt.close(fig)

        logger.info("Figure 'Competition Distribution' created successfully.")
        return fig

    except Exception as e:
        logger.error(f"Error creating Competition Distribution plot: {e}", exc_info=True)
        return None


def plot_unique_competitor_distribution(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure | None:
    """Plot competitor and newcomer distribution per year for the configured nationality."""
    try:
        logger.info(f"Creating figure: Competitor Distribution for nationality {config.nationality}")

        if "newcomers" not in db_tables:
            logger.warning("'newcomers' table not found in db_tables; skipping plot.")
            return None

        df = db_tables["newcomers"].query("year > 2000")

        fig, ax = plt.subplots()
        ax.bar(df["year"], df["Competitors"], color="#eee600", zorder=2, label="Competitors")
        ax.bar(df["year"], df["Newcomer"], color="#4179e1", zorder=3, label="Newcomers")

        ax.set_title(f"Number of Unique Competitors - {config.country}", fontweight="bold")
        ax.set_xlabel("Year", fontweight="bold")
        ax.set_ylabel("Unique Persons", fontweight="bold")
        ax.grid(True, which="major", axis="y", zorder=1)
        ax.legend(loc="best")

        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="center")

        # --- Footnote for total competitors ---
        tot = db_tables["newcomers"]["Newcomer"].sum().astype(int)
        if tot:
            note = f"The WCA has registered {tot} competitors from {config.country}."
            fig.text(0.5, 0.005, note, ha="center", fontsize=9, color="dimgray", style="italic")

        fig.tight_layout()
        plt.close(fig)

        logger.info("Figure 'Competitor Distribution' created successfully.")
        return fig

    except Exception as e:
        logger.error(f"Error creating Competitor Distribution plot: {e}", exc_info=True)
        return None


def plot_newcomers_ratio(
    db_tables: dict,
    config,
    logger
) -> plt.Figure | None:
    """Plot newcomer ratios (overall and by gender) per year."""
    try:
        logger.info(f"Creating figure: Newcomer Ratios for {config.nationality}")

        if "newcomers" not in db_tables:
            logger.warning("'newcomers' table not found in db_tables; skipping plot.")
            return None

        df = db_tables["newcomers"].query("year > 2000")

        fig, ax = plt.subplots()
        ax.grid(which="major", axis="y", zorder=1)

        ax.plot(df["year"], df["Newcomer Ratio"], color="#6d757b", lw=5, alpha=0.3, label="Total", zorder=3)
        ax.plot(df["year"], df["Newcomer Ratio M"], color="#2c19d8", lw=2.5, label="Male", linestyle="-", marker=".", zorder=4)
        ax.plot(df["year"], df["Newcomer Ratio F"], color="#fd21bb", lw=2.5, label="Female", linestyle="-", marker=".", zorder=4)

        ax.set_title(f"Newcomer Ratios by Gender - {config.country}", fontweight="bold")
        ax.set_xlabel("Year", fontweight="bold")
        ax.set_ylabel("Newcomers / Competitors", fontweight="bold")

        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="center")

        ax.set_ylim(bottom=0)
        ax.legend(loc="best")

        fig.tight_layout()
        plt.close(fig)

        logger.info("Figure 'Newcomer Ratio' created successfully.")
        return fig

    except Exception as e:
        logger.error(f"Error creating Newcomer Ratio plot: {e}", exc_info=True)
        return None


def plot_gender_distribution_vert(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure | None:
    """
    Plot gender-based competitor and newcomer trends per year.
    Creates two vertically stacked subplots (Male, Female), with a footnote
    on competitors identifying as 'other'.
    """
    try:
        logger.info("Creating gender-based competitor distribution plot...")

        if "newcomers" not in db_tables:
            logger.warning("'newcomers' table not found in db_tables; skipping plot.")
            return None

        df = db_tables["newcomers"].query("year > 2000")

        fig, (ax_m, ax_f) = plt.subplots(2, sharex=True, figsize=(10, 12))
        fig.suptitle(f"Number of Unique Competitors by Gender - {config.nationality}", fontweight="bold")

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
        plt.setp(ax_f.get_xticklabels(), rotation=45, ha="center", fontsize=10)

        # --- Footnote for 'other' competitors ---
        nonzero_years = df.loc[df["Newcomer O"] > 0, "year"].tolist()
        if nonzero_years:
            years_text = ", ".join(map(str, nonzero_years))
            total_other = int(df['Newcomer O'].sum())
            note = f"{total_other} total competitors who identify as 'other' competed in: {years_text}."
        else:
            note = "No competitors who identify as 'other' have been recorded yet."

        fig.text(0.5, 0.005, note, ha="center", fontsize=9, color="dimgray", style="italic")

        fig.tight_layout()
        fig.subplots_adjust(top=0.93)
        plt.close(fig)

        logger.info("Gender distribution plot created successfully.")
        return fig

    except Exception as e:
        logger.error(f"Error creating gender distribution plot: {e}", exc_info=True)
        return None


def plot_gender_distribution_area(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> plt.Figure | None:
    """Stacked area chart showing percentage distribution of genders across years."""
    try:
        logger.info("Creating stacked area chart for gender share over time...")

        if "newcomers" not in db_tables:
            logger.warning("'newcomers' table not found in db_tables; skipping plot.")
            return None

        df = db_tables["newcomers"].query("year > 2000").copy()

        # --- Compute percentage per gender ---
        df["Male %"] = df["Competitors M"] / df["Competitors"] * 100
        df["Female %"] = df["Competitors F"] / df["Competitors"] * 100
        df["Other %"] = df["Competitors O"] / df["Competitors"] * 100

        # --- Stacked area plot ---
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.stackplot(
            df["year"],
            df["Male %"],
            df["Female %"],
            df["Other %"],
            colors=["#4169e1", "#e0115f", "#9370db"],
            labels=["Male", "Female", "Other"],
            alpha=0.9,
        )

        ax.set_title(f"Gender Distribution Among Competitors - {config.nationality}", fontweight="bold")
        ax.set_ylabel("Percentage of Competitors", fontweight="bold")
        ax.set_ylim(0, 100)
        ax.grid(visible=False)

        years = range(df["year"].min(), df["year"].max() + 1)
        ax.set_xticks(years)
        plt.setp(ax.get_xticklabels(), rotation=45, ha="center")

        ax.legend(
            loc="upper center",
            bbox_to_anchor=(0.5, -0.12),
            ncol=3,
            frameon=False,
            fontsize=10,
        )

        fig.tight_layout()
        fig.subplots_adjust(bottom=0.18)
        plt.close(fig)

        logger.info("Stacked area gender distribution plot created successfully.")
        return fig

    except Exception as e:
        logger.error(f"Error creating stacked area gender distribution plot: {e}", exc_info=True)
        return None


###################################################################
############################### RUN ###############################
###################################################################


def run(db_tables, config):

    logger = logging.getLogger(__name__)
    logger.info("Producing stats for Competitions module")

    # --- Tables ---
    results = {
        "Most Competitions": compute_most_competitions(db_tables=db_tables, config=config, logger=logger),
        "Most Countries": compute_most_countries(db_tables=db_tables, config=config, logger=logger),
        "Most Competitors": compute_most_competitors(db_tables=db_tables, config=config, logger=logger),
        "Return Rate": compute_return_rate(
            db_tables=db_tables, config=config, logger=logger, min_competitors=25,
        ),
        "Community Recency": compute_community_recency(
            db_tables=db_tables, config=config, logger=logger,
            min_competitors=25, threshold='2022-01-01',
        ),
        "Newcomers": compute_newcomer_statistics(db_tables=db_tables, config=config, logger=logger),
    }

    # --- Figures ---
    figures = {
        "Competition Distribution": plot_competition_distribution(db_tables=db_tables, config=config, logger=logger),
        "Competitor Distribution": plot_unique_competitor_distribution(db_tables=db_tables, config=config, logger=logger),
        "Newcomer Ratio": plot_newcomers_ratio(db_tables=db_tables, config=config, logger=logger),
        "Competitor Distribution by Gender": plot_gender_distribution_vert(db_tables=db_tables, config=config, logger=logger),
        "Gender Distribution": plot_gender_distribution_area(db_tables=db_tables, config=config, logger=logger),
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)
