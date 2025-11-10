import pandas as pd
import numpy as np
import utils_wca as uw
import configparser
import logging


def compute_most_events_won(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    Competitors ranked by most events won
    """

    try:
        logger.info(f"Computing most events won for competitors from {config.nationality}...")

        # --- Load data ---
        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"][["id", "name"]].drop_duplicates()

        # Replace invalid results and drop NaN
        golds = (
            results
            .query("roundTypeId in ['f', 'c'] & pos == 1")      # winners in finals
            .replace([0, -1, -2], np.nan)
            .dropna(subset=["best"])        # must win with a result
            .groupby("personId")["eventId"]
            .nunique()
            .rename("Different Events Won")
            .reset_index()
        )

        total_events = (
            results.groupby("personId", observed=True)["eventId"]
            .nunique()
            .rename("Events Competed In")
            .reset_index()
        )

        # --- Merge and sort ---
        df = (
            persons
            .merge(golds, how="inner", left_on="id", right_on="personId")
            .merge(total_events, how="left", left_on="id", right_on="personId")
            .drop(columns=["personId_x", "personId_y"], errors="ignore")
            .rename(columns={"id": "WCAID", "name": "Name"})
            .sort_values(by=["Different Events Won", "Events Competed In"], ascending=[False, True])
            .reset_index(drop=True)
        )

        df.index += 1
        logger.info(f"Computed different events won for {len(df)} competitors from {config.nationality}.")

        return df

    except Exception as e:
        logger.critical(f"Error computing most events won: {e}", exc_info=True)


def compute_most_events_podiumed(
    db_tables: dict, 
    config: configparser.ConfigParser, 
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    Competitors ranked by most events podiumed
    """

    try:
        logger.info(f"Computing most events podiumed for competitors from {config.nationality}...")

        # --- Load data ---
        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"][["id", "name"]].drop_duplicates()

        # Replace invalid results and drop NaN
        podiums = (
            results
            .query("roundTypeId in ['f', 'c'] & pos <= 3")      # podium finishers in finals
            .replace([0, -1, -2], np.nan)
            .dropna(subset=["best"])        # must win with a result
            .groupby("personId")["eventId"]
            .nunique()
            .rename("Different Events Podiumed")
            .reset_index()
        )

        total_events = (
            results.groupby("personId", observed=True)["eventId"]
            .nunique()
            .rename("Events Competed In")
            .reset_index()
        )

        # --- Merge and sort ---
        df = (
            persons
            .merge(podiums, how="inner", left_on="id", right_on="personId")
            .merge(total_events, how="left", left_on="id", right_on="personId")
            .drop(columns=["personId_x", "personId_y"], errors="ignore")
            .rename(columns={"id": "WCAID", "name": "Name"})
            .sort_values(by=["Different Events Podiumed", "Events Competed In"], ascending=[False, True])
            .reset_index(drop=True)
        )

        df.index += 1
        logger.info(f"Computed different events podiumed for {len(df)} competitors from {config.nationality}.")

        return df

    except Exception as e:
        logger.critical(f"Error computing most events won: {e}", exc_info=True)


def compute_event_participation_percentage(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    Compute, for each event, the percentage of competitors from the selected nationality
    who have achieved at least one valid result. Can be interpreted as a popularity score
    """

    try:
        logger.info(f"Computing event participation percentage for {config.nationality} competitors...")

        # --- Load pre-filtered data ---
        results = db_tables["results_nationality"].copy()
        persons = db_tables["persons"]

        # --- Total number of competitors for this nationality ---
        total_competitors = persons.query("countryId == @config.nationality")["id"].nunique()

        if total_competitors == 0:
            logger.warning(f"No competitors found for nationality '{config.nationality}'. Returning empty DataFrame.")
            return pd.DataFrame(columns=["Event", "Percentage of Competitors"])

        # --- Unique competitors per event ---
        competitors_per_event = (
            results.groupby("eventId", observed=True)["personId"]
            .nunique()
            .reset_index()
            .rename(columns = {"eventId": "Event", "personId": "Competitors"})
            .assign(
                pct = lambda x: (x["Competitors"] / total_competitors * 100).round(2)
            )
            .rename(columns = {"pct": "Percentage of Total Competitors"})
            .sort_values(by="Percentage of Total Competitors", ascending=False)
            .reset_index(drop=True)
        )

        competitors_per_event.index += 1

        logger.info(f"Computed event participation percentages for {len(competitors_per_event)} events (n={total_competitors} competitors).")
        return competitors_per_event

    except Exception as e:
        logger.critical(f"Error computing event participation percentage: {e}", exc_info=True)


def compute_most_common_event_combinations(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    Compute the most common event combinations among competitions
    hosted in the selected country.
    """

    try:
        country = config.country
        logger.info(f"Computing most common event combinations for competitions in {country}...")

        # --- Load competition data ---
        competitions = db_tables["competitions"].query("countryId == @country").copy()

        if competitions.empty:
            logger.warning(f"No competitions found for country '{country}'. Returning empty DataFrame.")
            return pd.DataFrame(columns=["Event Combination", "Competitions"])

        # --- Group by eventSpecs ---
        event_comb = (
            competitions[["id", "eventSpecs"]]
            .dropna(subset=["eventSpecs"])
            .groupby("eventSpecs", observed=True)["id"]
            .nunique()
            .rename("Competitions")
            .reset_index()
            .sort_values(by=["Competitions", "eventSpecs"], ascending=[False, True])
            .reset_index(drop=True)
        )

        event_comb.index += 1

        logger.info(f"Computed most common event combinations across {len(event_comb)} unique event sets.")
        return event_comb.rename(columns={"eventSpecs": "Event Combination"})

    except Exception as e:
        logger.critical(f"Error computing most common event combinations: {e}", exc_info=True)


def compute_average_events_per_competition(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    Compute, for each competition in the selected country, 
    the total number of events held and the average number of events 
    that competitors participated in.
    """

    try:
        country = config.country
        logger.info(f"Computing average events per competitor for competitions hosted in {country}...")

        # --- Load data ---
        results = db_tables["results_country"].copy()

        # --- Compute number of unique events per competition ---
        events_per_competition = (
            results.groupby("competitionId", observed=True)["eventId"]
            .nunique()
            .rename("Events")
        )

        # --- Compute average events per competitor ---
        avg_events_per_competition = (
            results.groupby(["competitionId", "personId"], observed=True)["eventId"]
            .nunique()
            .groupby("competitionId")
            .mean()
            .rename("Avg Events per Competitor")
        )

        # --- Combine results ---
        avgevents = (
            pd.concat([events_per_competition, avg_events_per_competition], axis=1)
            .reset_index()
            .sort_values(by="Avg Events per Competitor", ascending=False)
            .reset_index(drop=True)
        )

        avgevents.index += 1
        avgevents.rename(columns={"competitionId": "Competition"}, inplace=True)

        db_tables["avgevents"] = avgevents

        logger.info(f"Computed average events per competitor for {len(avgevents)} competitions in {country}.")
        return avgevents

    except Exception as e:
        logger.critical(f"Error computing average events per competition: {e}", exc_info=True)


def compute_most_participated_competition(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    Ranks competitions by average events per competitor / total events: the most participated competitions.
    """

    try:
        country = config.country
        logger.info(f"Computing most competed competitions in {country}...")

        # --- Load data ---
        results = db_tables["avgevents"].copy()

        results['Ratio'] = results['Avg Events per Competitor'] / results['Events']
        results = (
            results[results["Events"] > 1]
            .sort_values(by = 'Ratio', ascending = False)
            .reset_index(drop = True)
        )

        results.index += 1

        logger.info(f"Computed most participated competitions for {len(results)} competitions in {country}.")
        return results

    except Exception as e:
        logger.critical(f"Error computing average events per competition: {e}", exc_info=True)


def compute_bronze_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    List of competitors with a bronze membership: an official single in all current WCA Events.
    """

    try:
        nationality = config.nationality
        logger.info(f"Computing Bronze Membership for competitors from {nationality}...")

        # --- Load data ---
        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"].copy()

        events = config.current_events
        num_events_needed = len(events)

        # --- Filter valid results ---
        first_result_date = (
            results
            .query("best > 0 and eventId in @events")
            .sort_values("date")
            .groupby(["personId", "eventId"], observed=True, as_index=False)
            .first()  # earliest single for each event
        )

        # --- Count unique events completed per person ---
        events_per_person = (
            first_result_date
            .groupby("personId", observed=True)["eventId"]
            .nunique()
            .rename("num_events")
            .reset_index()
        )

        # --- Identify bronze members ---
        bronze_ids = (
            events_per_person.query("num_events == @num_events_needed")["personId"].tolist()
        )

        if not bronze_ids:
            logger.info("No bronze members found.")
            return pd.DataFrame(columns=["WCAID", "Name", "Last Event", "Completion Date"])

        # --- Find when each bronze member completed their last required event ---
        last_event_date = (
            first_result_date[first_result_date["personId"].isin(bronze_ids)]
            .sort_values("date", ascending=False)
            .groupby("personId", observed=True, as_index=False)
            .first()
            [["personId", "eventId", "date"]]
            .rename(columns={"eventId": "Last Event", "date": "Completion Date"})
        )

        # --- Merge with names ---
        bronze = (
            persons[["id", "name"]]
            .drop_duplicates()
            .merge(last_event_date, left_on="id", right_on="personId", how="inner")
            .drop(columns="personId")
            .rename(columns={"id": "WCAID", "name": "Name"})
            .sort_values("Completion Date", ascending=True)
            .reset_index(drop=True)
        )

        bronze.index += 1

        logger.info(f"Identified {len(bronze)} bronze members from {nationality}.")
        return bronze

    except Exception as e:
        logger.critical(f"Error computing competitors that achieved Bronze Membership: {e}", exc_info=True)


def compute_silver_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    
    """
    List of competitors with a silver membership: an official average in all current WCA Events.
    """

    try:
        nationality = config.nationality
        logger.info(f"Computing Silver Membership for competitors from {nationality}...")

        # --- Load data ---
        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"].copy()

        events = [e for e in config.current_events if e != '333mbf']
        num_events_needed = len(events)

        # --- Filter valid results ---
        first_result_date = (
            results
            .query("average > 0 and eventId in @events")
            .sort_values("date")
            .groupby(["personId", "eventId"], observed=True, as_index=False)
            .first()  # earliest average for each event
        )

        # --- Count unique events completed per person ---
        events_per_person = (
            first_result_date
            .groupby("personId", observed=True)["eventId"]
            .nunique()
            .rename("num_events")
            .reset_index()
        )

        # --- Identify silver members ---
        silver_ids = (
            events_per_person.query("num_events == @num_events_needed")["personId"].tolist()
        )

        if not silver_ids:
            logger.info("No silver members found.")
            return pd.DataFrame(columns=["WCAID", "Name", "Last Event", "Completion Date"])

        # --- Find when each bronze member completed their last required event ---
        last_event_date = (
            first_result_date[first_result_date["personId"].isin(silver_ids)]
            .sort_values("date", ascending=False)
            .groupby("personId", observed=True, as_index=False)
            .first()
            [["personId", "eventId", "date"]]
            .rename(columns={"eventId": "Last Event", "date": "Completion Date"})
        )

        # --- Merge with names ---
        silver = (
            persons[["id", "name"]]
            .drop_duplicates()
            .merge(last_event_date, left_on="id", right_on="personId", how="inner")
            .drop(columns="personId")
            .rename(columns={"id": "WCAID", "name": "Name"})
            .sort_values("Completion Date", ascending=True)
            .reset_index(drop=True)
        )

        silver.index += 1

        db_tables["silver"] = silver

        logger.info(f"Identified {len(silver)} silver members from {nationality}.")
        return silver

    except Exception as e:
        logger.critical(f"Error computing competitors that achieved Silver Membership: {e}", exc_info=True)


def compute_gold_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Compute Gold Membership:
    Competitors who are Silver Members and have achieved either
    a World Championship podium, a Continental Record, or a World Record.
    """

    try:
        nationality = config.nationality
        logger.info(f"Computing Gold Membership for competitors from {nationality} ...")

        results = db_tables["results_fixed"]
        persons = db_tables["persons"]
        silver = db_tables.get("silver", pd.DataFrame())
        championships = db_tables["championships"]

        # --- Sanity check ---
        if silver.empty:
            logger.warning("No silver members found — gold membership cannot be computed.")
            return pd.DataFrame(columns=["WCAID", "Name", "Gold Condition"])

        # --- Build lookup sets ---
        silver_ids = set(silver["WCAID"])

        # --- World Championship podiums ---
        wc_ids = set(
            championships.loc[
                championships["championship_type"].str.endswith("world"), "competition_id"
            ]
        )
        wc_podium = results.query("competitionId in @wc_ids and pos <= 3")["personId"].unique()
        wc_podium_set = set(wc_podium)

        # --- Continental / World records ---
        record_cols = ["regionalSingleRecord", "regionalAverageRecord"]
        record_df = results[record_cols + ["personId"]].copy()

        has_wr = record_df[
            record_df[record_cols].apply(lambda x: x.str.contains("WR", na=False)).any(axis=1)
        ]["personId"].unique()

        has_cr = record_df[
            record_df[record_cols].apply(
                lambda x: x.notna() & ~x.str.contains("WR|NR", na=False)
            ).any(axis=1)
        ]["personId"].unique()

        wr_set = set(has_wr)
        cr_set = set(has_cr)

        # --- Combine flags ---
        gold_ids = [
            pid for pid in silver_ids
            if pid in wc_podium_set or pid in wr_set or pid in cr_set
        ]

        if not gold_ids:
            logger.info("No gold members found.")
            return pd.DataFrame(columns=["WCAID", "Name", "Gold Condition"])

        # --- Build final DataFrame ---
        def gold_condition(pid):
            conds = []
            if pid in wc_podium_set: conds.append("World Championship Podium")
            if pid in wr_set: conds.append("World Record")
            if pid in cr_set: conds.append("Continental Record")
            return ", ".join(conds)

        gold = (
            persons[["id", "name"]]
            .drop_duplicates()
            .query("id in @gold_ids")
            .rename(columns={"id": "WCAID", "name": "Name"})
        )
        gold["Gold Condition"] = gold["WCAID"].apply(gold_condition)

        gold.index = range(1, len(gold) + 1)

        logger.info(f"Identified {len(gold)} Gold Members.")
        return gold

    except Exception as e:
        logger.critical(f"Error computing gold membership: {e}", exc_info=True)


def compute_platinum_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Compute Platinum Membership:
    Competitors who are Silver Members and have achieved
    a World Championship podium, a Continental Record and a World Record.
    """

    try:
        nationality = config.nationality
        logger.info(f"Computing Platinum Membership for competitors from {nationality} ...")

        results = db_tables["results_fixed"]
        persons = db_tables["persons"]
        silver = db_tables.get("silver", pd.DataFrame())
        championships = db_tables["championships"]

        # --- Sanity check ---
        if silver.empty:
            logger.warning("No silver members found — platinum membership cannot be computed.")
            return pd.DataFrame(columns=["WCAID", "Name"])

        # --- Build lookup sets ---
        silver_ids = set(silver["WCAID"])

        # --- World Championship podiums ---
        wc_ids = set(
            championships.loc[
                championships["championship_type"].str.endswith("world"), "competition_id"
            ]
        )
        wc_podium = results.query("competitionId in @wc_ids and pos <= 3")["personId"].unique()
        wc_podium_set = set(wc_podium)

        # --- Continental / World records ---
        record_cols = ["regionalSingleRecord", "regionalAverageRecord"]
        record_df = results[record_cols + ["personId"]].copy()

        has_wr = record_df[
            record_df[record_cols].apply(lambda x: x.str.contains("WR", na=False)).any(axis=1)
        ]["personId"].unique()

        has_cr = record_df[
            record_df[record_cols].apply(
                lambda x: x.notna() & ~x.str.contains("WR|NR", na=False)
            ).any(axis=1)
        ]["personId"].unique()

        wr_set = set(has_wr)
        cr_set = set(has_cr)

        # --- Combine flags ---
        plat_ids = [
            pid for pid in silver_ids
            if pid in wc_podium_set and pid in wr_set and pid in cr_set
        ]

        if not plat_ids:
            logger.info("No platinum members found.")
            return pd.DataFrame(columns=["WCAID", "Name"])

        platinum = (
            persons[["id", "name"]]
            .drop_duplicates()
            .query("id in @plat_ids")
            .rename(columns={"id": "WCAID", "name": "Name"})
            .reset_index(drop=True)
        )

        platinum.index += 1

        logger.info(f"Identified {len(platinum)} Platinum Members.")
        return platinum

    except Exception as e:
        logger.critical(f"Error computing platinum membership: {e}", exc_info=True)



###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Events module")

    results = {
        "Most Events Won": compute_most_events_won(db_tables=db_tables, config=config, logger=logger),
        "Most Events Podiumed": compute_most_events_podiumed(db_tables=db_tables, config=config, logger=logger),
        "Event Popularity": compute_event_participation_percentage(db_tables=db_tables, config=config, logger=logger),
        "Event Combinations": compute_most_common_event_combinations(db_tables=db_tables, config=config, logger=logger),
        "Avg Events per Competition": compute_average_events_per_competition(db_tables=db_tables, config=config, logger=logger),
        "Most Participated Competitions": compute_most_participated_competition(db_tables=db_tables, config=config, logger=logger),
        "Bronze Membership": compute_bronze_membership(db_tables=db_tables, config=config, logger=logger),
        "Silver Membership": compute_silver_membership(db_tables=db_tables, config=config, logger=logger),
        "Gold Membership": compute_gold_membership(db_tables=db_tables, config=config, logger=logger),
        "Platinum Membership": compute_platinum_membership(db_tables=db_tables, config=config, logger=logger),
    }

    figures = {}

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)