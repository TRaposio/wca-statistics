import pandas as pd
import logging
import configparser
import utils_wca as uw


# ---------------------------------------------------------------------------
# Constants — event lists for each official relay format.
# ---------------------------------------------------------------------------

_GUILFORD_EVENTS = [
    "222", "333", "444", "555", "666", "777",
    "clock", "minx", "pyram", "skewb", "333oh", "sq1",
]

_MINI_GUILFORD_EVENTS = [
    "222", "333", "444", "555",
    "clock", "minx", "pyram", "skewb", "333oh", "sq1",
]

_LUCKY_RELAY_EVENTS = ["222", "pyram", "skewb"]
_BLIND_RELAY_EVENTS = ["333bf", "444bf", "555bf"]
_3X3_MASTER_RELAY_EVENTS = ["333", "333oh", "333bf"]


###################################################################
######################### COMPUTATIONS ############################
###################################################################


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

        ranks = db_tables["ranks_single_nationality"].copy()
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        df = ranks.query("event_id in @event_list")

        if df.empty:
            logger.warning(f"No ranks found for any of the required events ({event_list}).")
            return pd.DataFrame()

        # --- Pivot to get one row per competitor ---
        pivot_best = (
            df.pivot_table(index="person_id", columns="event_id", values="best")
            .reindex(columns=event_list)
        )
        pivot_rank = (
            df.pivot_table(index="person_id", columns="event_id", values="country_rank")
            .reindex(columns=event_list)
        )

        # --- Fill missing times/ranks with worst + 1 (WCA convention) ---
        pivot_best = pivot_best.apply(lambda c: c.fillna(c.max() + 1))
        pivot_rank = pivot_rank.fillna(pivot_rank.max().max() + 1)

        # --- Consistency metrics ---
        consistency = pd.DataFrame({
            "Best Rank": pivot_rank.min(axis=1),
            "Median Rank": pivot_rank.median(axis=1),
        })

        # --- Total score (sum of times) ---
        pivot_best["Total"] = pivot_best.sum(axis=1)

        # --- Merge with person names ---
        df_out = persons.merge(
            pd.concat([pivot_best, consistency], axis=1),
            how="inner",
            left_on="wca_id",
            right_on="person_id",
        )

        # --- Sort by Total time, then Median Rank ---
        df_out = df_out.sort_values(["Total", "Median Rank"], ascending=[True, True])

        # --- Convert all numeric times to readable WCA format ---
        for e in event_list + ["Total"]:
            df_out[e] = df_out[e].apply(uw.timeconvert)

        df_out = df_out.reset_index(drop=True)
        df_out.index += 1

        logger.info(
            f"{relay_name} computed successfully "
            f"({len(df_out):,} competitors, {len(event_list)} events)."
        )
        return df_out

    except Exception as e:
        logger.error(f"Error computing {relay_name} relay: {e}", exc_info=True)
        return pd.DataFrame()


def compute_official_guilford(db_tables, config, logger) -> pd.DataFrame:
    return _compute_relay_base(db_tables, config, logger, _GUILFORD_EVENTS, "Official Guilford")


def compute_official_mini_guilford(db_tables, config, logger) -> pd.DataFrame:
    return _compute_relay_base(db_tables, config, logger, _MINI_GUILFORD_EVENTS, "Official Mini Guilford")


def compute_official_lucky_relay(db_tables, config, logger) -> pd.DataFrame:
    return _compute_relay_base(db_tables, config, logger, _LUCKY_RELAY_EVENTS, "Official Lucky")


def compute_official_blind_relay(db_tables, config, logger) -> pd.DataFrame:
    return _compute_relay_base(db_tables, config, logger, _BLIND_RELAY_EVENTS, "Official Blindfolded Relay")


def compute_3x3_master_relay(db_tables, config, logger) -> pd.DataFrame:
    return _compute_relay_base(db_tables, config, logger, _3X3_MASTER_RELAY_EVENTS, "Official 3x3 Master Relay")


###################################################################
############################### RUN ###############################
###################################################################


def run(db_tables, config):

    logger = logging.getLogger(__name__)
    logger.info("Producing stats for Relays module")

    # --- Tables ---
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
