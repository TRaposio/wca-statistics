import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw


def compute_sor_single(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute Sum of Ranks (SOR) across all events for single results.
    Each competitor's rank in each event is summed to give a total score.
    Lower = better.
    """

    try:
        logger.info(f"Computing Sum of Ranks (Single) for {config.nationality}")

        events = config.current_events
        ranks_single = db_tables["ranks_single_nationality"].query("eventId in @events").copy()
        persons = db_tables["persons"][["id", "name"]].drop_duplicates()

        # --- Pivot event ranks by competitor ---
        pivot = (
            ranks_single.pivot_table(
                index="personId",
                columns="eventId",
                values="countryRank"
            )
            .reindex(columns=events)  # ensure consistent event order
        )

        # --- Replace NaN with "max rank + 1" per column (bottom rank for missing events) ---
        pivot = pivot.apply(lambda col: col.fillna(col.max() + 1), axis=0)

        # --- Compute SOR ---
        pivot["SOR single"] = pivot.sum(axis=1)

        # --- Merge competitor names ---
        sor = (
            pivot.reset_index()
            .merge(persons, left_on="personId", right_on="id", how="left")
            .drop(columns="id")
            .rename(columns={"name": "Name"})
            .sort_values("SOR single")
            .reset_index(drop=True)
        )

        sor.index += 1

        logger.info(f"Computed Sum of Ranks (Single) for {len(sor)} competitors.")
        return sor

    except Exception as e:
        logger.critical(f"Error while computing Sum of Ranks (Single): {e}")



###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Relays module")

    results = {
        "SOR Single": compute_sor_single(db_tables=db_tables, config=config, logger=logger),
        "SOR Average": compute_sor_average(db_tables=db_tables, config=config, logger=logger),
    }

    figures = {}

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)