import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw
import matplotlib.pyplot as plt
from datetime import datetime


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
        results = db_tables["results_nationality"].query("event_id in @config.current_events")

        nrs = (
            ranks_s.query("country_rank == 1 & event_id in @config.current_events")
            [["person_id", "name", "event_id", "best"]]
            .merge(
                results[['person_id','event_id','best','competition_id','date']], 
                    on=['person_id','event_id','best'],
                    how="left"
            )
            .rename(columns={"best": "result"})
        )

        if nrs.empty:
            logger.warning("No national single records found.")
            return pd.DataFrame(columns=["person_id", "name", "event_id", "type", "result", "competition_id", "date"])

        nrs["type"] = "single"

        # Format results for readability
        nrs["formatted_result"] = np.where(
            nrs["event_id"] == "333mbf",
            nrs["result"].apply(uw.multiresult),
            np.where(
                nrs["event_id"] == '333fm',
                nrs["result"].astype(str),
                nrs["result"].apply(uw.timeconvert)
            )
        )
        nrs.index += 1
        db_tables["national_records_single"] = nrs
        
        logger.info(f"Computed {len(nrs)} national single records for {config.nationality}")
        return nrs[["person_id", "name", "event_id", "type", "formatted_result", "competition_id", "date"]]

    except Exception as e:
        logger.critical(f"Error while computing national single records: {e}")


def compute_national_records_average(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute current national average records for all events of the configured nationality.
    Stores the resulting DataFrame in db_tables["national_records_average"].
    """
    try:
        logger.info("Computing national single records")

        ranks_a = db_tables["ranks_average_nationality"]
        results = db_tables["results_nationality"].query("event_id in @config.current_events")

        nra = (
            ranks_a.query("country_rank == 1 & event_id in @config.current_events")
            [["person_id", "name", "event_id", "best"]]
            .merge(
                results[['person_id','event_id','average','competition_id','date']], 
                    left_on=['person_id','event_id','best'],
                    right_on=['person_id','event_id','average'],
                    how="left"
            )
            .drop(columns="best")
            .rename(columns={"average": "result"})
        )

        if nra.empty:
            logger.warning("No national average records found.")
            return pd.DataFrame(columns=["person_id", "name", "event_id", "type", "result", "competition_id", "date"])

        nra["type"] = "average"

        # Format results for readability
        nra["formatted_result"] = np.where(
            nra["event_id"] == '333fm',
            (nra["result"]/100).astype(str),
            nra["result"].apply(uw.timeconvert)
        )

        nra.index += 1
        db_tables["national_records_average"] = nra

        logger.info(f"Computed {len(nra)} national single records for {config.nationality}")
        return nra[["person_id", "name", "event_id", "type", "formatted_result", "competition_id", "date"]]

    except Exception as e:
        logger.critical(f"Error while computing national average records: {e}")


def compute_oldest_standing_records(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Combine national single and average record tables, compute how long each has stood,
    and return them sorted by oldest standing.
    """
    try:
        logger.info("Computing oldest standing national records")

        nrs = db_tables.get("national_records_single", pd.DataFrame())
        nra = db_tables.get("national_records_average", pd.DataFrame())

        if nrs.empty and nra.empty:
            logger.warning("No record data available to compute oldest standing records.")
            return pd.DataFrame(columns=["person_id", "name", "event_id", "type", "formatted_result", "date", "days"])

        combined = pd.concat([nrs, nra], ignore_index=True)

        combined["days"] = (pd.Timestamp.today() - combined["date"]).dt.days
        combined = combined.sort_values(by="days", ascending=False).reset_index(drop=True)
        combined.index += 1

        logger.info(f"Computed {len(combined)} oldest standing records")

        return combined[[
            "person_id", "name", "event_id", "type", "formatted_result", "date", "days"
        ]]

    except Exception as e:
        logger.critical(f"Error while computing oldest standing records: {e}")


def compute_country_world_continental_records(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute all World (WR) and European (ER) records, single and average,
    in chronological order, for configured country
    """

    try:
        logger.info(f"Computing World and European records for country {config.nationality}")

        results = db_tables["results_nationality"]

        subset = results.query("event_id in @config.current_events and best > 0")

        if subset.empty:
            logger.warning("No World or European records found.")
            db_tables["world_continental_records"] = pd.DataFrame()
            return pd.DataFrame()

        # --- Single records ---
        single_records = subset.query("regional_single_record in ['WR', 'ER']").copy()
        single_records = single_records[[
            "person_id", "person_name", "event_id", "competition_id",
            "competition_name", "date", "best", "regional_single_record"
        ]]
        single_records["type"] = "single"
        single_records = single_records.rename(columns={
            "best": "result",
            "regional_single_record": "record_type"
        })

        # --- Average records ---
        average_records = subset.query("regional_average_record in ['WR', 'ER']").copy()
        average_records = average_records[[
            "person_id", "person_name", "event_id", "competition_id",
            "competition_name", "date", "average", "regional_average_record"
        ]]
        average_records["type"] = "average"
        average_records = average_records.rename(columns={
            "average": "result",
            "regional_average_record": "record_type"
        })

        # --- Combine and sort chronologically ---
        records = (
            pd.concat([single_records, average_records], ignore_index=True)
            .sort_values(by="date", ascending=True)
            .reset_index(drop=True)
        )

        # --- Format results for readability ---
        records["formatted_result"] = np.where(
            records["event_id"] == "333mbf",
            records["result"].apply(uw.multiresult),
            np.where(
                records["event_id"] == '333fm',
                records["result"].astype(str),
                records["result"].apply(uw.timeconvert)
            )
        )

        # --- Store in db_tables ---
        db_tables["world_continental_records_nationality"] = records
        logger.info(f"Computed {len(records)} world/continental records")

        return records[[
            "person_id", "person_name", "event_id", "type",
            "formatted_result", "record_type",
            "competition_id", "competition_name", "date"
        ]]

    except Exception as e:
        logger.critical(f"Error while computing world/continental records: {e}")


def compute_event_record_history(
    db_tables: dict,
    config,
    logger,
    event_id: str
) -> dict:
    """
    Compute the chronological history of National (NR) and World (WR)
    records for a given event, both single and average.
    """

    try:
        logger.info(f"Computing national/world record history for event {event_id}")

        results_nationality = db_tables["results_nationality"].query("event_id == @event_id").copy()
        results = db_tables["results"].query("event_id == @event_id").copy()
        competitions = db_tables["competitions"][["competition_id", "date"]]
        nationality = config.nationality

        # --- Filter to target event only ---
        if results_nationality.empty:
            logger.critical(f"No results found for event {event_id}")

        if results.empty:
            logger.critical(f"No results found for event {event_id}")

        # --- National record SINGLE history ---
        nrs = results_nationality.query(
            "regional_single_record in ['NR', 'ER', 'WR']"
        )[["person_id", "person_name", "competition_id", "best", "date"]].copy()
        nrs = nrs.rename(columns={
            "person_id": "WCAID",
            "person_name": "Name",
            "best": "NR single"
        }).sort_values(by=["date", "NR single"], ascending=[True, False])

        # --- National record AVERAGE history ---
        nra = results_nationality.query(
            "regional_average_record in ['NR', 'ER', 'WR']"
        )[["person_id", "person_name", "competition_id", "average", "date"]].copy()
        nra = nra.rename(columns={
            "person_id": "WCAID",
            "person_name": "Name",
            "average": "NR average"
        }).sort_values(by=["date", "NR average"], ascending=[True, False])

        # --- World record SINGLE history ---
        wrs = results.query(
            "regional_single_record == 'WR' and event_id == @event_id"
        ).copy()

        wrs = (
            wrs
            .rename(columns={
                "person_id": "WCAID",
                "person_name": "Name",
                "best": "WR single"
            })
            .merge(competitions, on="competition_id")
            [["WCAID", "Name", "competition_id", "WR single", "date"]]
            .sort_values(by=["date", "WR single"], ascending=[True, False])
        )

        # --- World record AVERAGE history ---
        wra = results.query(
            "regional_average_record == 'WR' and event_id == @event_id"
        ).copy()

        wra = (
            wra.rename(columns={
                "person_id": "WCAID",
                "person_name": "Name",
                "average": "WR average"
            })
            .merge(competitions, on="competition_id")
            [["WCAID", "Name", "competition_id", "WR average", "date"]]
            .sort_values(by=["date", "WR average"], ascending=[True, False])
        )

        # --- Extend all to "today" so step chart continues ---
        today = pd.to_datetime(datetime.now().date()) + pd.Timedelta(weeks=8)

        def extend_latest(df):
            if df.empty:
                return df
            last = df.iloc[-1:].copy()
            last["date"] = today
            return pd.concat([df, last], ignore_index=True)

        nrs, nra, wrs, wra = map(extend_latest, [nrs, nra, wrs, wra])

        # --- Store in db_tables for reuse ---
        db_tables[f"record_history_{event_id}"] = {
            "nrs": nrs,
            "nra": nra,
            "wrs": wrs,
            "wra": wra
        }

        out = pd.concat([nrs, nra]).sort_values(by="date")

        logger.info(f"Record history computed for event {event_id}")
        return out[["WCAID", "Name", "competition_id", "date", "NR single", "NR average"]].iloc[:-2, :]

    except Exception as e:
        logger.critical(f"Error while computing record history for {event_id}: {e}")


def plot_world_continental_records(
    db_tables: dict,
    config,
    logger
) -> plt.Figure | None:
    """
    Plot a stacked bar chart of World (WR) and European (ER) records by year.
    Includes empty ticks for years with no records.
    """

    try:
        logger.info(f"Plotting World and Continental record timeline for {config.nationality}")

        records = db_tables.get("world_continental_records_nationality", pd.DataFrame())
        if records.empty:
            logger.critical("No data available for record plot.")

        records["year"] = pd.to_datetime(records["date"]).dt.year

        # --- Build full year range (from first record to current year) ---
        min_year = int(records["year"].min())
        max_year = pd.Timestamp.today().year
        all_years = np.arange(min_year, max_year + 1)

        # --- Count records by year and record type ---
        summary = (
            records.groupby(["year", "record_type"])
            .size()
            .unstack(fill_value=0)
            .reindex(columns=["WR", "ER"], fill_value=0)
            .reindex(all_years, fill_value=0)  # ensure full year coverage
        )

        # --- Plot ---
        fig, ax = plt.subplots(figsize=(12, 6))
        summary.plot(
            kind="bar",
            stacked=True,
            color={"WR": "red", "ER": "blue"},
            ax=ax
        )

        # --- Formatting ---
        ax.set_title(f"{config.nationality} World & Continental Records Over Time", fontsize=14)
        ax.set_xlabel("Year")
        ax.set_ylabel("Number of Records")
        ax.legend(title="Record Type")
        ax.grid(axis="y", linestyle=":", alpha=0.6)

        # --- X-ticks: show every year, even if empty ---
        ax.set_xticks(np.arange(len(summary.index)))
        ax.set_xticklabels(summary.index.astype(int), rotation=45, ha="center")

        plt.tight_layout()
        logger.info("Generated World/Continental record chart successfully")
        return fig

    except Exception as e:
        logger.critical(f"Error while plotting World/Continental records: {e}")


def plot_event_record_history(
    db_tables: dict,
    config,
    logger,
    event_id: str
) -> plt.Figure | None:
    """
    Plot the evolution of national and world records for a given event.
    Shows NR single, NR average, WR single, WR average over time.
    """

    try:
        logger.info(f"Plotting record history for event {event_id}")

        record_data = db_tables.get(f"record_history_{event_id}", None)
        if not record_data:
            logger.warning(f"No precomputed record data found for {event_id}")
            return None

        nrs, nra, wrs, wra = (
            record_data["nrs"],
            record_data["nra"],
            record_data["wrs"],
            record_data["wra"],
        )

        if all(df.empty for df in [nrs, nra, wrs, wra]):
            logger.warning(f"No valid data to plot for event {event_id}")
            return None
        
        if event_id == "333fm":
            nra["NR average"] = nra["NR average"] / 100
            wra["WR average"] = wra["WR average"] / 100
        
        elif event_id == '333mbf':
            nrs["solved"] = nrs["NR single"].apply(uw.multisolved)
            nrs["wrong"] = nrs["NR single"].apply(uw.multiwrong)
            nrs["solved"] = nrs["solved"] - nrs["wrong"]

            wrs["solved"] = wrs["WR single"].apply(uw.multisolved)
            wrs["wrong"] = wrs["WR single"].apply(uw.multiwrong)
            wrs["solved"] = wrs["solved"] - wrs["wrong"]
        
        else:
            nrs["NR single"] = nrs["NR single"] / 100
            wrs["WR single"] = wrs["WR single"] / 100
            nra["NR average"] = nra["NR average"] / 100
            wra["WR average"] = wra["WR average"] / 100




        fig, ax = plt.subplots(figsize=(14, 8))

        # Step plots
        if not nrs.empty:
            ax.step(nrs["date"], nrs["NR single"], where="post", label="NR single", color="tab:blue")
        if not nra.empty:
            ax.step(nra["date"], nra["NR average"], where="post", label="NR average", color="tab:orange")
        if not wrs.empty:
            ax.step(wrs["date"], wrs["WR single"], where="post", label="WR single", color="tab:green")
        if not wra.empty:
            ax.step(wra["date"], wra["WR average"], where="post", label="WR average", color="tab:red")

        # Axis formatting
        today = pd.Timestamp.today() + pd.Timedelta(weeks=8)
        ax.set_xlim(pd.to_datetime("2003-01-01"), today)
        ax.set_ylim(bottom=0)
        ax.grid(axis="y", linestyle=":", alpha=0.7)

        # Labels and title
        ax.set_title(f"{config.nationality} National and World Record Progression — {event_id}", fontsize=15)
        ax.set_xlabel("Date")
        ax.set_ylabel(f"{ 'Points' if event_id == '333mbf' else 'Moves' if event_id == '333fm' else 'Time (seconds)'}")
        ax.legend(fontsize=12)

        plt.tight_layout()
        logger.info(f"Record evolution plot generated for {event_id}")
        return fig

    except Exception as e:
        logger.critical(f"Error while plotting record history for {event_id}: {e}")


def plot_national_record_month_distribution(
    db_tables: dict,
    config,
    logger
) -> plt.Figure | None:
    """
    Plot the distribution of national records over the months of a year,
    overlaid with the average number of competitions per month per year.
    """

    try:
        logger.info(f"Plotting monthly distribution of national records for {config.nationality}")

        results = db_tables["results_nationality"]
        countries = db_tables["countries"].query("continent_id == '_Europe'")["id"].unique()
        competitions = db_tables["competitions"].query("country_id in @countries")

        if results.empty or competitions.empty:
            logger.warning("Missing results or competitions data for month distribution plot.")
            return None

        # --- Extract month of each record (single or average) ---
        records = results.query(
            "regional_single_record in ['NR','ER','WR'] or regional_average_record in ['NR','ER','WR']"
        ).copy()

        if records.empty:
            logger.warning("No national record data available for plotting.")
            return None

        records["month"] = pd.to_datetime(records["date"]).dt.month
        nr_counts = (
            records.groupby("month")
            .size()
            .reindex(range(1, 13), fill_value=0)
        )

        # --- Average number of competitions per month ---
        competitions["month"] = pd.to_datetime(competitions["date"]).dt.month
        competitions["year"] = pd.to_datetime(competitions["date"]).dt.year

        comps_per_month_year = (
            competitions.groupby(["year", "month"]).size().reset_index(name="count")
        )

        comps_avg = (
            comps_per_month_year.groupby("month")["count"].mean()
            .reindex(range(1, 13), fill_value=0)
        )

        # --- Plot ---
        fig, ax1 = plt.subplots(figsize=(12, 6))

        # Bar chart for records
        months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                  "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
        ax1.bar(range(1, 13), nr_counts, color="tab:blue", alpha=0.7, label="National Records")
        ax1.set_xticks(range(1, 13))
        ax1.set_xticklabels(months)
        ax1.set_xlabel("Month")
        ax1.set_ylabel("Number of National Records", color="tab:blue")
        ax1.tick_params(axis="y", labelcolor="tab:blue")
        ax1.grid(axis="y", linestyle=":", alpha=0.6)

        # Line plot for average competitions
        ax2 = ax1.twinx()
        ax2.plot(range(1, 13), comps_avg, color="tab:red", marker="o", label="Avg Competitions per Month")
        ax2.set_ylabel("Average Competitions per Month", color="tab:red")
        ax2.tick_params(axis="y", labelcolor="tab:red")

        # Combined legend
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="upper left")

        ax1.set_title(f"Monthly Distribution of {config.nationality} National Records", fontsize=14)
        plt.tight_layout()

        logger.info("Generated monthly national record distribution plot successfully.")
        return fig

    except Exception as e:
        logger.critical(f"Error while plotting monthly national record distribution: {e}")
        return None


###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Relays module")

    results = {
        "NR Singles": compute_national_records_single(db_tables=db_tables, config=config, logger=logger),
        "NR Averages": compute_national_records_average(db_tables=db_tables, config=config, logger=logger),
        "Oldest Standing Records": compute_oldest_standing_records(db_tables=db_tables, config=config, logger=logger),
        "WR+CR": compute_country_world_continental_records(db_tables=db_tables, config=config, logger=logger),
        "333 RecordHistory": compute_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="333"),
        "333fm RecordHistory": compute_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="333fm"),
        "222 RecordHistory": compute_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="222"),
        "444 RecordHistory": compute_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="444"),
        "555 RecordHistory": compute_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="555"),
        "clock RecordHistory": compute_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="clock"),

    }

    figures = {
        "WR+CR History": plot_world_continental_records(db_tables=db_tables, config=config, logger=logger),
        "333 Record History": plot_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="333"),
        "333fm Record History": plot_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="333fm"),
        "222 Record History": plot_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="222"),
        "444 Record History": plot_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="444"),
        "555 Record History": plot_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="555"),
        "clock Record Nistory": plot_event_record_history(db_tables=db_tables, config=config, logger=logger, event_id="clock"),
        "NR per month": plot_national_record_month_distribution(db_tables=db_tables, config=config, logger=logger)
    }

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)