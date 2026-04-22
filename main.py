# main.py
import time
from contextlib import contextmanager

import utils_wca as uw
from modules import (
    competitions,
    events,
    regions,
    relays,
    championships,
    records,
    sor_kinch,
    results,
)


@contextmanager
def timed(logger, label: str):
    """Context manager that logs how long the wrapped block took."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        logger.info(f"[timing] {label} completed in {elapsed:.1f}s")


def main():

    logger = uw.setup_logger(__name__)
    logger.info("Starting WCA pipeline...")

    pipeline_start = time.perf_counter()

    # --- Setup ---
    with timed(logger, "setup"):
        config = uw.load_config(logger=logger, config_path="config.ini")
        uw.set_plot_style(config=config, logger=logger)
        uw.update_data(config=config, logger=logger)

    # --- Load and preprocess tables ---
    with timed(logger, "load + preprocess tables"):
        tables_to_load = [
            "results",
            "attempts",
            "persons",
            "competitions",
            "events",
            "formats",
            "ranks_single",
            "ranks_average",
            "countries",
            "continents",
            "championships",
            "rounds",
            "scrambles",
        ]

        db_tables = {name: uw.read_table(name, config, logger) for name in tables_to_load}
        db_tables = uw.process_tables(db_tables, config, logger)
        uw.export_db_schema(db_tables, config, logger)

        # --- check if mappers must be updated ---
        uw.read_aux_file("regions", db_tables, config, logger)
        missing = uw.check_missing_regions(db_tables, config, logger)

        if missing:
            logger.warning(
                f"Found {len(missing)} unmapped competitions. "
                f"These won't be counted in regional stats unless mapped."
            )

    # --- Run modules, passing the preloaded tables ---
    # Comment/uncomment to choose which modules to run.
    with timed(logger, "competitions"):
        competitions.run(db_tables, config)
    with timed(logger, "events"):
        events.run(db_tables, config)
    with timed(logger, "regions"):
        regions.run(db_tables, config)
    with timed(logger, "championships"):
        championships.run(db_tables, config)
    with timed(logger, "relays"):
        relays.run(db_tables, config)
    with timed(logger, "records"):
        records.run(db_tables, config)
    with timed(logger, "sor_kinch"):
        sor_kinch.run(db_tables, config)
    with timed(logger, "results"):
        results.run(db_tables, config)

    total_elapsed = time.perf_counter() - pipeline_start
    logger.info(f"Pipeline finished successfully in {total_elapsed:.1f}s total")


if __name__ == "__main__":
    main()
