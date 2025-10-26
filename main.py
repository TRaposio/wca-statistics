# main.py
from utils_wca import load_config, setup_logger, update_data, read_table
from modules import competitions#, events, results, records, italian_championship, relays, sor_kinch

def main():
    logger = setup_logger(__name__)
    logger.info("Starting WCA pipeline...")

    config = load_config(logger=logger, config_path="config.ini")

    update_data(config, logger=logger)

    # --- Load all common tables once ---
    tables_to_load = ["results", "competitions", "persons", "countries"]
    data = {name: read_table(name, config, logger) for name in tables_to_load}

    # --- Run modules, passing the preloaded tables ---
    competitions.run(data, config, logger)
    # events.run(data, config, logger)
    # sor_kinch.run(data, config, logger)
    # results.run(data, config, logger)
    # records.run(data, config, logger)
    # italian_championship.run(data, config, logger)
    # relays.run(data, config, logger)

    logger.info("Pipeline finished successfully")

if __name__ == "__main__":
    main()
