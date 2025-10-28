# main.py
import utils_wca as uw
from modules import competitions#, events, results, records, italian_championship, relays, sor_kinch

def main():
    logger = uw.setup_logger(__name__)
    logger.info("Starting WCA pipeline...")

    config = uw.load_config(logger=logger, config_path="config.ini")

    uw.update_data(config, logger=logger)

    # --- Load all common tables once ---
    tables_to_load = [
        "results", 
        "competitions", 
        "persons", 
        "countries", 
        "rounds", 
        "ranks_single", 
        "ranks_average",
        "championships"
    ]
    
    db_tables = {name: uw.read_table(name, config, logger) for name in tables_to_load}

    db_tables = uw.process_tables(db_tables, config, logger)

    # --- Run modules, passing the preloaded tables ---
    competitions.run(db_tables, config, logger)
    # events.run(data, config, logger)
    # sor_kinch.run(data, config, logger)
    # results.run(data, config, logger)
    # records.run(data, config, logger)
    # italian_championship.run(data, config, logger)
    # relays.run(data, config, logger)

    logger.info("Pipeline finished successfully")

if __name__ == "__main__":
    main()
