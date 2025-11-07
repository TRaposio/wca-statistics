import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw


def _compute_relay_base(
    db_tables: dict,
    config: configparser.ConfigParser,
    logger: logging.Logger,
    event_list: list[str],
    relay_name: str
) -> pd.DataFrame:
    """
    Compute a relay-style leaderboard using pre-filtered national ranks
    (ranks_single_nationality).

    Adds per-event times, total summed score, and consistency metrics
    (Best Rank, Median Rank).

    """
    try:
        logger.info(f"Computing {relay_name} relay for {config.nationality} using official ranks...")

        # --- Load filtered ranks ---
        ranks = db_tables["ranks_single_nationality"].copy()
        persons = db_tables["persons"][["id", "name"]].drop_duplicates()

        # --- Filter only target events ---
        df = ranks.query("eventId in @event_list")

        # --- Pivot to get one row per competitor ---
        pivot_best = (
            df.pivot_table(index="personId", columns="eventId", values="best")
            .reindex(columns=event_list)
        )
        pivot_rank = (
            df.pivot_table(index="personId", columns="eventId", values="countryRank")
            .reindex(columns=event_list)
        )

        # --- Fill missing times with worst + 1 ---
        pivot_best = pivot_best.apply(lambda c: c.fillna(c.max() + 1))
        pivot_rank = pivot_rank.fillna(pivot_rank.max().max() + 1)

        # --- Add consistency metrics ---
        consistency = pd.DataFrame({
            "Best Rank": pivot_rank.min(axis=1),
            "Median Rank": pivot_rank.median(axis=1)
        })

        # --- Total score (sum of times) ---
        pivot_best["Total"] = pivot_best.sum(axis=1)

        # --- Merge numeric ranks into results and get personName ---
        df_out = (
            persons
            .merge(pd.concat([pivot_best, consistency], axis=1), how='inner', left_on='id', right_on='personId')
        )

        # --- Sort by Total time then Median Rank ---
        df_out = df_out.sort_values(["Total", "Median Rank"], ascending=[True, True])

        # --- Convert all numeric times to readable WCA format ---
        for e in event_list + ["Total"]:
            df_out[e] = df_out[e].apply(uw.timeconvert)

        df_out.reset_index(inplace=True)
        df_out.index += 1

        logger.info(f"{relay_name} computed successfully ({len(df_out):,} competitors, {len(event_list)} events).")
        return df_out

    except Exception as e:
        logger.critical(f"Error computing {relay_name} relay: {e}", exc_info=True)
        return pd.DataFrame()



def compute_official_guilford(db_tables, config, logger) -> pd.DataFrame:
    eventi = [
        "222", "333", "444", "555", "666", "777",
        "clock", "minx", "pyram", "skewb", "333oh", "sq1"
    ]
    return _compute_relay_base(db_tables, config, logger, eventi, "Official Guilford")


def compute_official_mini_guilford(db_tables, config, logger) -> pd.DataFrame:
    eventi = [
        "222", "333", "444", "555",
        "clock", "minx", "pyram", "skewb", "333oh", "sq1"
    ]
    return _compute_relay_base(db_tables, config, logger, eventi, "Official Mini Guilford")


def compute_official_lucky_relay(db_tables, config, logger) -> pd.DataFrame:
    eventi = ["222", "pyram", "skewb"]
    return _compute_relay_base(db_tables, config, logger, eventi, "Official Lucky")


def compute_official_blind_relay(db_tables, config, logger) -> pd.DataFrame:
    eventi = ["333bf", "444bf", "555bf"]
    return _compute_relay_base(db_tables, config, logger, eventi, "Official Blindfolded Relay")


def compute_3x3_master_relay(db_tables, config, logger) -> pd.DataFrame:
    eventi = ["333", "333oh", "333bf"]
    return _compute_relay_base(db_tables, config, logger, eventi, "Official 3x3 Master Relay")




###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Relays module")

    results = {
        "Official Guilford": compute_official_guilford(db_tables=db_tables, config=config, logger=logger),
        "Official Mini Guilford": compute_official_mini_guilford(db_tables=db_tables, config=config, logger=logger),
        "Official Blinds": compute_official_blind_relay(db_tables=db_tables, config=config, logger=logger),
        "Official 3x3 Master": compute_3x3_master_relay(db_tables=db_tables, config=config, logger=logger),
        "Official Lucky": compute_official_lucky_relay(db_tables=db_tables, config=config, logger=logger),
    }

    figures = {}

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)