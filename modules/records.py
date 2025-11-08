import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw


def compute_national_records_single(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute current national single records for all events of the configured nationality.
    Stores the resulting DataFrame in db_tables["national_records_single"].
    """
    try:
        logger.info("Computing national single records")

        ranks_s = db_tables["ranks_single_nationality"]
        results = db_tables["results_nationality"].query("eventId in @config.current_events")

        nrs = (
            ranks_s.query("countryRank == 1 & eventId in @config.current_events")
            [["personId", "name", "eventId", "best"]]
            .merge(
                results[['personId','eventId','best','competitionId','date']], 
                    on=['personId','eventId','best'],
                    how="left"
            )
            .rename(columns={"best": "result"})
        )

        if nrs.empty:
            logger.warning("No national single records found.")
            return pd.DataFrame(columns=["personId", "name", "eventId", "type", "result", "competitionId", "date"])

        nrs["type"] = "single"

        # Format results for readability
        nrs["result"] = np.where(
            nrs["eventId"] == "333mbf",
            nrs["result"].apply(uw.multiresult),
            np.where(
                nrs["eventId"] == '333fm',
                nrs["result"],
                nrs["result"].apply(uw.timeconvert)
            )
        )
        nrs.index += 1

        db_tables["national_records_single"] = nrs
        logger.info(f"Computed {len(nrs)} national single records for {config.nationality}")
        return nrs[["personId", "name", "eventId", "type", "result", "competitionId", "date"]]

    except Exception as e:
        logger.critical(f"Error while computing national single records: {e}")



###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Relays module")

    results = {
        "NR Singles": compute_national_records_single(db_tables=db_tables, config=config, logger=logger),
        # "NR Averages": compute_national_records_average(db_tables=db_tables, config=config, logger=logger),

    }

    figures = {}

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)