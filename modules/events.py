import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# MBLD event id — excluded from Silver Membership since it has no 'average'.
_MBLD_EVENT = "333mbf"


###################################################################
######################### COMPUTATIONS ############################
###################################################################


def compute_most_events_won(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """Competitors ranked by number of distinct events won in a final."""
    try:
        logger.info(f"Computing most events won for competitors from {config.nationality}...")

        final_rounds = uw.WCA_CONSTANTS['final_rounds']
        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        golds = (
            uw.drop_invalid_results(
                results.query("round_type_id in @final_rounds & pos == 1"),
                "best",
            )
            .groupby("person_id")["event_id"]
            .nunique()
            .rename("Different Events Won")
            .reset_index()
        )

        total_events = (
            results.groupby("person_id", observed=True)["event_id"]
            .nunique()
            .rename("Events Competed In")
            .reset_index()
        )

        df = (
            persons.merge(golds, how="inner", left_on="wca_id", right_on="person_id")
            .merge(total_events, how="left", left_on="wca_id", right_on="person_id")
            .drop(columns=["person_id_x", "person_id_y"], errors="ignore")
            .rename(columns={"wca_id": "WCAID", "name": "Name"})
            .sort_values(by=["Different Events Won", "Events Competed In"], ascending=[False, True])
            .reset_index(drop=True)
        )
        df.index += 1

        logger.info(f"Computed different events won for {len(df)} competitors from {config.nationality}.")
        return df

    except Exception as e:
        logger.error(f"Error computing most events won: {e}", exc_info=True)
        return pd.DataFrame()


def compute_most_events_podiumed(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """Competitors ranked by number of distinct events podiumed in a final."""
    try:
        logger.info(f"Computing most events podiumed for competitors from {config.nationality}...")

        final_rounds = uw.WCA_CONSTANTS['final_rounds']
        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        podiums = (
            uw.drop_invalid_results(
                results.query("round_type_id in @final_rounds & pos <= 3"),
                "best",
            )
            .groupby("person_id")["event_id"]
            .nunique()
            .rename("Different Events Podiumed")
            .reset_index()
        )

        total_events = (
            results.groupby("person_id", observed=True)["event_id"]
            .nunique()
            .rename("Events Competed In")
            .reset_index()
        )

        df = (
            persons.merge(podiums, how="inner", left_on="wca_id", right_on="person_id")
            .merge(total_events, how="left", left_on="wca_id", right_on="person_id")
            .drop(columns=["person_id_x", "person_id_y"], errors="ignore")
            .rename(columns={"wca_id": "WCAID", "name": "Name"})
            .sort_values(by=["Different Events Podiumed", "Events Competed In"], ascending=[False, True])
            .reset_index(drop=True)
        )
        df.index += 1

        logger.info(f"Computed different events podiumed for {len(df)} competitors from {config.nationality}.")
        return df

    except Exception as e:
        logger.error(f"Error computing most events podiumed: {e}", exc_info=True)
        return pd.DataFrame()


def compute_event_participation_percentage(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    For each event, the percentage of competitors from the selected nationality
    who have achieved at least one valid result (popularity proxy).
    """
    try:
        logger.info(f"Computing event participation percentage for {config.nationality} competitors...")

        results = db_tables["results_nationality"].copy()
        persons = db_tables["persons"]

        total_competitors = persons.query("country_id == @config.nationality")["wca_id"].nunique()

        if total_competitors == 0:
            logger.warning(
                f"No competitors found for nationality '{config.nationality}'. Returning empty DataFrame."
            )
            return pd.DataFrame(columns=["Event", "Percentage of Total Competitors"])

        competitors_per_event = (
            results.groupby("event_id", observed=True)["person_id"]
            .nunique()
            .reset_index()
            .rename(columns={"event_id": "Event", "person_id": "Competitors"})
            .assign(pct=lambda x: (x["Competitors"] / total_competitors * 100).round(2))
            .rename(columns={"pct": "Percentage of Total Competitors"})
            .sort_values(by="Percentage of Total Competitors", ascending=False)
            .reset_index(drop=True)
        )
        competitors_per_event.index += 1

        logger.info(
            f"Computed event participation percentages for {len(competitors_per_event)} events "
            f"(n={total_competitors} competitors)."
        )
        return competitors_per_event

    except Exception as e:
        logger.error(f"Error computing event participation percentage: {e}", exc_info=True)
        return pd.DataFrame()


def compute_most_common_event_combinations(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Most common event combinations among competitions hosted in the
    configured country.
    """
    try:
        country = config.country
        logger.info(f"Computing most common event combinations for competitions in {country}...")

        competitions = db_tables["competitions"].query("country_id == @country").copy()

        if competitions.empty:
            logger.warning(f"No competitions found for country '{country}'. Returning empty DataFrame.")
            return pd.DataFrame(columns=["event_specs", "Competitions"])

        event_comb = (
            competitions[["competition_id", "event_specs"]]
            .dropna(subset=["event_specs"])
            .groupby("event_specs", observed=True)["competition_id"]
            .nunique()
            .rename("Competitions")
            .reset_index()
            .sort_values(by=["Competitions", "event_specs"], ascending=[False, True])
            .reset_index(drop=True)
        )
        event_comb.index += 1

        logger.info(f"Computed most common event combinations across {len(event_comb)} unique event sets.")
        # NOTE: column is 'event_specs' (snake_case). Previous code tried to rename
        # from 'eventSpecs' -> 'Event Combination', which was a silent no-op.
        return event_comb.rename(columns={"event_specs": "Event Combination"})

    except Exception as e:
        logger.error(f"Error computing most common event combinations: {e}", exc_info=True)
        return pd.DataFrame()


def compute_average_events_per_competition(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    For each competition in the configured country, total events held and
    the average number of events competitors participated in.

    Side effect: caches the result in db_tables["avgevents"] for use by
    compute_most_participated_competition.
    """
    try:
        country = config.country
        logger.info(f"Computing average events per competitor for competitions hosted in {country}...")

        results = db_tables["results_country"].copy()

        events_per_competition = (
            results.groupby("competition_id", observed=True)["event_id"]
            .nunique()
            .rename("Events")
        )

        avg_events_per_competition = (
            results.groupby(["competition_id", "person_id"], observed=True)["event_id"]
            .nunique()
            .groupby("competition_id")
            .mean()
            .rename("Avg Events per Competitor")
        )

        avgevents = (
            pd.concat([events_per_competition, avg_events_per_competition], axis=1)
            .reset_index()
            .sort_values(by="Avg Events per Competitor", ascending=False)
            .reset_index(drop=True)
        )
        avgevents.index += 1
        avgevents = avgevents.rename(columns={"competition_id": "Competition"})

        db_tables["avgevents"] = avgevents

        logger.info(f"Computed average events per competitor for {len(avgevents)} competitions in {country}.")
        return avgevents

    except Exception as e:
        logger.error(f"Error computing average events per competition: {e}", exc_info=True)
        return pd.DataFrame()


def compute_most_participated_competition(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Rank competitions by avg-events-per-competitor / total-events ratio.
    Requires compute_average_events_per_competition to have run first
    (reads db_tables["avgevents"]).
    """
    try:
        country = config.country
        logger.info(f"Computing most competed competitions in {country}...")

        avgevents = db_tables.get("avgevents", pd.DataFrame())
        if avgevents.empty:
            logger.warning(
                "'avgevents' table not available "
                "(did compute_average_events_per_competition run?). Skipping."
            )
            return pd.DataFrame()

        results = avgevents.copy()
        results['Ratio'] = results['Avg Events per Competitor'] / results['Events']
        results = (
            results[results["Events"] > 1]
            .sort_values(by='Ratio', ascending=False)
            .reset_index(drop=True)
        )
        results.index += 1

        logger.info(f"Computed most participated competitions for {len(results)} competitions in {country}.")
        return results

    except Exception as e:
        logger.error(f"Error computing most participated competition: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
######################## MEMBERSHIP TIERS #########################
###################################################################


def compute_bronze_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Bronze Membership: competitors with an official SINGLE in all current WCA events.
    """
    try:
        nationality = config.nationality
        logger.info(f"Computing Bronze Membership for competitors from {nationality}...")

        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"].copy()

        events = config.current_events
        num_events_needed = len(events)

        # --- Earliest valid single for each (person, event) ---
        first_result_date = (
            results.query("best > 0 and event_id in @events")
            .sort_values("date")
            .groupby(["person_id", "event_id"], observed=True, as_index=False)
            .first()
        )

        # --- Count unique events completed per person ---
        events_per_person = (
            first_result_date.groupby("person_id", observed=True)["event_id"]
            .nunique()
            .rename("num_events")
            .reset_index()
        )

        bronze_ids = events_per_person.query("num_events == @num_events_needed")["person_id"].tolist()

        if not bronze_ids:
            logger.info("No bronze members found.")
            return pd.DataFrame(columns=["WCAID", "Name", "Last Event", "Completion Date"])

        # --- Find when each bronze member completed their last required event ---
        last_event_date = (
            first_result_date[first_result_date["person_id"].isin(bronze_ids)]
            .sort_values("date", ascending=False)
            .groupby("person_id", observed=True, as_index=False)
            .first()
            [["person_id", "event_id", "date"]]
            .rename(columns={"event_id": "Last Event", "date": "Completion Date"})
        )

        bronze = (
            persons[["wca_id", "name"]]
            .drop_duplicates()
            .merge(last_event_date, left_on="wca_id", right_on="person_id", how="inner")
            .drop(columns="person_id")
            .rename(columns={"wca_id": "WCAID", "name": "Name"})
            .sort_values("Completion Date", ascending=True)
            .reset_index(drop=True)
        )
        bronze.index += 1

        logger.info(f"Identified {len(bronze)} bronze members from {nationality}.")
        return bronze

    except Exception as e:
        logger.error(f"Error computing Bronze Membership: {e}", exc_info=True)
        return pd.DataFrame()


def compute_silver_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Silver Membership: competitors with an official AVERAGE in all current
    WCA events (excluding MBLD, which has no 'average' format).

    Side effect: caches the result in db_tables["silver"] for use by
    compute_gold_membership / compute_platinum_membership.
    """
    try:
        nationality = config.nationality
        logger.info(f"Computing Silver Membership for competitors from {nationality}...")

        results = db_tables["results_fixed"].copy()
        persons = db_tables["persons"].copy()

        events = [e for e in config.current_events if e != _MBLD_EVENT]
        num_events_needed = len(events)

        # --- Earliest valid average for each (person, event) ---
        first_result_date = (
            results.query("average > 0 and event_id in @events")
            .sort_values("date")
            .groupby(["person_id", "event_id"], observed=True, as_index=False)
            .first()
        )

        events_per_person = (
            first_result_date.groupby("person_id", observed=True)["event_id"]
            .nunique()
            .rename("num_events")
            .reset_index()
        )

        silver_ids = events_per_person.query("num_events == @num_events_needed")["person_id"].tolist()

        if not silver_ids:
            logger.info("No silver members found.")
            db_tables["silver"] = pd.DataFrame()
            return pd.DataFrame(columns=["WCAID", "Name", "Last Event", "Completion Date"])

        last_event_date = (
            first_result_date[first_result_date["person_id"].isin(silver_ids)]
            .sort_values("date", ascending=False)
            .groupby("person_id", observed=True, as_index=False)
            .first()
            [["person_id", "event_id", "date"]]
            .rename(columns={"event_id": "Last Event", "date": "Completion Date"})
        )

        silver = (
            persons[["wca_id", "name"]]
            .drop_duplicates()
            .merge(last_event_date, left_on="wca_id", right_on="person_id", how="inner")
            .drop(columns="person_id")
            .rename(columns={"wca_id": "WCAID", "name": "Name"})
            .sort_values("Completion Date", ascending=True)
            .reset_index(drop=True)
        )
        silver.index += 1

        db_tables["silver"] = silver

        logger.info(f"Identified {len(silver)} silver members from {nationality}.")
        return silver

    except Exception as e:
        logger.error(f"Error computing Silver Membership: {e}", exc_info=True)
        return pd.DataFrame()


def _get_record_holder_sets(results: pd.DataFrame) -> tuple[set, set]:
    """
    Helper for Gold/Platinum: compute two sets of person_ids —
      - has_wr: anyone with a 'WR' tag (single or average)
      - has_cr: anyone with a continental-record tag (anything that is
        neither 'WR' nor 'NR', matched implicitly so this works for any
        continent: ER, AsR, AfR, NAR, SAR, OcR).
    """
    record_cols = ["regional_single_record", "regional_average_record"]
    record_df = results[record_cols + ["person_id"]].copy()

    has_wr = record_df[
        record_df[record_cols].apply(lambda x: x.str.contains("WR", na=False)).any(axis=1)
    ]["person_id"].unique()

    has_cr = record_df[
        record_df[record_cols].apply(
            lambda x: x.notna() & ~x.str.contains("WR|NR", na=False)
        ).any(axis=1)
    ]["person_id"].unique()

    return set(has_wr), set(has_cr)


def _get_world_championship_podium_set(db_tables: dict) -> set:
    """Helper for Gold/Platinum: set of person_ids with a World Championship podium."""
    results = db_tables["results_fixed"]
    championships = db_tables["championships"]

    wc_ids = set(
        championships.loc[
            championships["championship_type"].str.endswith("world"), "competition_id"
        ]
    )
    return set(results.query("competition_id in @wc_ids and pos <= 3")["person_id"].unique())


def compute_gold_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Gold Membership: Silver Members with at least ONE of:
        - World Championship podium
        - Continental Record
        - World Record
    Requires compute_silver_membership to have run first.
    """
    try:
        nationality = config.nationality
        logger.info(f"Computing Gold Membership for competitors from {nationality}...")

        persons = db_tables["persons"]
        results = db_tables["results_fixed"]
        silver = db_tables.get("silver", pd.DataFrame())

        if silver.empty:
            logger.warning("No silver members found — gold membership cannot be computed.")
            return pd.DataFrame(columns=["WCAID", "Name", "Gold Condition"])

        silver_ids = set(silver["WCAID"])
        wc_podium_set = _get_world_championship_podium_set(db_tables)
        wr_set, cr_set = _get_record_holder_sets(results)

        gold_ids = [
            pid for pid in silver_ids
            if pid in wc_podium_set or pid in wr_set or pid in cr_set
        ]

        if not gold_ids:
            logger.info("No gold members found.")
            return pd.DataFrame(columns=["WCAID", "Name", "Gold Condition"])

        def gold_condition(pid):
            conds = []
            if pid in wc_podium_set:
                conds.append("World Championship Podium")
            if pid in wr_set:
                conds.append("World Record")
            if pid in cr_set:
                conds.append("Continental Record")
            return ", ".join(conds)

        gold = (
            persons[["wca_id", "name"]]
            .drop_duplicates()
            .query("wca_id in @gold_ids")
            .rename(columns={"wca_id": "WCAID", "name": "Name"})
        )
        gold["Gold Condition"] = gold["WCAID"].apply(gold_condition)
        gold.index = range(1, len(gold) + 1)

        logger.info(f"Identified {len(gold)} Gold Members.")
        return gold

    except Exception as e:
        logger.error(f"Error computing Gold Membership: {e}", exc_info=True)
        return pd.DataFrame()


def compute_platinum_membership(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger
) -> pd.DataFrame:
    """
    Platinum Membership: Silver Members with ALL THREE of:
        - World Championship podium
        - Continental Record
        - World Record
    Requires compute_silver_membership to have run first.
    """
    try:
        nationality = config.nationality
        logger.info(f"Computing Platinum Membership for competitors from {nationality}...")

        persons = db_tables["persons"]
        results = db_tables["results_fixed"]
        silver = db_tables.get("silver", pd.DataFrame())

        if silver.empty:
            logger.warning("No silver members found — platinum membership cannot be computed.")
            return pd.DataFrame(columns=["WCAID", "Name"])

        silver_ids = set(silver["WCAID"])
        wc_podium_set = _get_world_championship_podium_set(db_tables)
        wr_set, cr_set = _get_record_holder_sets(results)

        plat_ids = [
            pid for pid in silver_ids
            if pid in wc_podium_set and pid in wr_set and pid in cr_set
        ]

        if not plat_ids:
            logger.info("No platinum members found.")
            return pd.DataFrame(columns=["WCAID", "Name"])

        platinum = (
            persons[["wca_id", "name"]]
            .drop_duplicates()
            .query("wca_id in @plat_ids")
            .rename(columns={"wca_id": "WCAID", "name": "Name"})
            .reset_index(drop=True)
        )
        platinum.index += 1

        logger.info(f"Identified {len(platinum)} Platinum Members.")
        return platinum

    except Exception as e:
        logger.error(f"Error computing Platinum Membership: {e}", exc_info=True)
        return pd.DataFrame()


###################################################################
############################### RUN ###############################
###################################################################


def run(db_tables, config):

    logger = logging.getLogger(__name__)
    logger.info("Producing stats for Events module")

    # --- Tables ---
    results = {
        "Most Events Won": compute_most_events_won(db_tables=db_tables, config=config, logger=logger),
        "Most Events Podiumed": compute_most_events_podiumed(db_tables=db_tables, config=config, logger=logger),
        "Event Popularity": compute_event_participation_percentage(db_tables=db_tables, config=config, logger=logger),
        "Event Combinations": compute_most_common_event_combinations(db_tables=db_tables, config=config, logger=logger),
        "Avg Events per Competition": compute_average_events_per_competition(db_tables=db_tables, config=config, logger=logger),
        "Most Participated Competitions": compute_most_participated_competition(db_tables=db_tables, config=config, logger=logger),
        # Silver must be computed before Gold/Platinum (they read db_tables["silver"])
        "Bronze Membership": compute_bronze_membership(db_tables=db_tables, config=config, logger=logger),
        "Silver Membership": compute_silver_membership(db_tables=db_tables, config=config, logger=logger),
        "Gold Membership": compute_gold_membership(db_tables=db_tables, config=config, logger=logger),
        "Platinum Membership": compute_platinum_membership(db_tables=db_tables, config=config, logger=logger),
    }

    figures = {}

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)
