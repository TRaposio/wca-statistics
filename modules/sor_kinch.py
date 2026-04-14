import pandas as pd
import numpy as np
import logging
import configparser
import utils_wca as uw



# Events that use Average, Single, or best-of-both
_KINCH_AVERAGE_EVENTS = [
    "333", "444", "555", "222", "333oh", "333ft", "minx",
    "pyram", "sq1", "clock", "skewb", "666", "777"
]
_KINCH_SINGLE_EVENTS = ["444bf", "555bf", "333mbf"]
_KINCH_BEST_OF_BOTH_EVENTS = ["333bf", "333fm"]  # take best ratio of avg vs single

_ALL_KINCH_EVENTS = _KINCH_AVERAGE_EVENTS + _KINCH_SINGLE_EVENTS + _KINCH_BEST_OF_BOTH_EVENTS
_N_KINCH_EVENTS = 18  # denominator is always 18



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
        ranks_single = db_tables["ranks_single_nationality"].query("event_id in @events").copy()
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        # --- Pivot event ranks by competitor ---
        pivot = (
            ranks_single.pivot_table(
                index="person_id",
                columns="event_id",
                values="country_rank"
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
            .merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"name": "Name"})
            .sort_values("SOR single")
            .reset_index(drop=True)
        )

        sor.index += 1

        logger.info(f"Computed Sum of Ranks (Single) for {len(sor)} competitors.")
        return sor

    except Exception as e:
        logger.critical(f"Error while computing Sum of Ranks (Single): {e}")
        

def compute_sor_average(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute Sum of Ranks (SOR) across all events for average results.
    Each competitor's rank in each event is summed to give a total score.
    Lower = better.
    """

    try:
        logger.info(f"Computing Sum of Ranks (Average) for {config.nationality}")

        events = config.current_events
        ranks_average = db_tables["ranks_average_nationality"].query("event_id in @events").copy()
        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        # --- Pivot event ranks by competitor ---
        pivot = (
            ranks_average.pivot_table(
                index="person_id",
                columns="event_id",
                values="country_rank"
            )
            .reindex(columns=events)  # ensure consistent event order
        )

        # --- Replace NaN with "max rank + 1" per column (bottom rank for missing events) ---
        pivot = pivot.apply(lambda col: col.fillna(col.max() + 1), axis=0)

        # --- Compute SOR ---
        pivot["SOR average"] = pivot.sum(axis=1)

        # --- Merge competitor names ---
        sor = (
            pivot.reset_index()
            .merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"name": "Name"})
            .sort_values("SOR average")
            .reset_index(drop=True)
        )

        sor.index += 1

        logger.info(f"Computed Sum of Ranks (Average) for {len(sor)} competitors.")
        return sor

    except Exception as e:
        logger.critical(f"Error while computing Sum of Ranks (Single): {e}")



def compute_kinch_score(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute KinchRank score for each competitor of the configured nationality.
    For each event, score = (WorldRecord / PersonalBest) * 100.
    The final score is the average across all 18 events.
    Missing events score 0. MultiBlind uses a points+time formula.
    """

    try:
        logger.info(f"Computing Kinch scores for {config.nationality}")

        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        ranks_single = db_tables["ranks_single_nationality"].copy()
        ranks_average = db_tables["ranks_average_nationality"].copy()

        multi_res = db_tables["multi_results"][
            db_tables["multi_results"]["person_country_id"] == config.country
        ].copy()


        # World records (country_id agnostic — use global ranks_single/ranks_average)
        wr_single = db_tables["ranks_single"].copy()
        wr_average = db_tables["ranks_average"].copy()

        scores = _compute_kinch_scores_from_ranks(
            ranks_single=ranks_single,
            ranks_average=ranks_average,
            wr_single=wr_single,
            wr_average=wr_average,
            persons=persons,
            multi_results=multi_res,
            logger=logger
        )

        logger.info(f"Computed Kinch scores for {len(scores)} competitors.")
        return scores

    except Exception as e:
        logger.critical(f"Error while computing Kinch scores: {e}")


def compute_national_level_kinch_score(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute national-level KinchRank score for each competitor.
    For each event, score = (NationalRecord / PersonalBest) * 100.
    National records are derived as the best result per event among
    all nationality-filtered competitors.
    """

    try:
        logger.info(f"Computing National Kinch scores for {config.nationality}")

        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()

        ranks_single = db_tables["ranks_single_nationality"].copy()
        ranks_average = db_tables["ranks_average_nationality"].copy()

        multi_res = db_tables["multi_results"][
            db_tables["multi_results"]["person_country_id"] == config.country
        ].copy()


        # National records = best result per event within nationality
        nr_single = (
            ranks_single.groupby("event_id", as_index=False)["best"]
            .min()
            .rename(columns={"best": "world_best"})
        )
        nr_average = (
            ranks_average.groupby("event_id", as_index=False)["best"]
            .min()
            .rename(columns={"best": "world_best"})
        )

        scores = _compute_kinch_scores_from_ranks(
            ranks_single=ranks_single,
            ranks_average=ranks_average,
            wr_single=nr_single,
            wr_average=nr_average,
            persons=persons,
            multi_results=multi_res,
            logger=logger
        )

        logger.info(f"Computed National Kinch scores for {len(scores)} competitors.")
        return scores

    except Exception as e:
        logger.critical(f"Error while computing National Kinch scores: {e}")


def compute_country_kinch_score(
    db_tables: dict,
    config,
    logger
) -> pd.DataFrame:
    """
    Compute KinchRank score per country.
    Each country's best result per event is scored against the World Record.
    The final score is the average across all 18 events.
    """

    try:
        logger.info("Computing Country Kinch scores")

        ranks_single = db_tables["ranks_single"].copy()
        ranks_average = db_tables["ranks_average"].copy()

        multi_res = db_tables["multi_results"]

        wr_single = (
            ranks_single.groupby("event_id", as_index=False)["best"]
            .min()
            .rename(columns={"best": "world_best"})
        )
        wr_average = (
            ranks_average.groupby("event_id", as_index=False)["best"]
            .min()
            .rename(columns={"best": "world_best"})
        )

        # Best result per country per event
        country_single = (
            ranks_single.groupby(["country_id", "event_id"], as_index=False)["best"]
            .min()
        )
        country_average = (
            ranks_average.groupby(["country_id", "event_id"], as_index=False)["best"]
            .min()
        )

        scores = _compute_kinch_scores_from_ranks(
            ranks_single=country_single,
            ranks_average=country_average,
            wr_single=wr_single,
            wr_average=wr_average,
            persons=None,  # country-level: no person merging
            multi_results=multi_res,
            id_col="country_id",
            name_col="country_id",
            logger=logger
        )

        scores = scores.rename(columns={"country_id": "Country"})

        logger.info(f"Computed Country Kinch scores for {len(scores)} countries.")
        return scores

    except Exception as e:
        logger.critical(f"Error while computing Country Kinch scores: {e}")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _mbf_kinch_series(multi_results: pd.DataFrame, all_entities, id_col: str) -> pd.Series:
    """
    Compute per-entity MBLD kinch value using the pre-computed multi_results table.
    Kinch value = points + proportion_of_hour_left.
    Takes the best (highest) value per entity.
    """
    df = multi_results[multi_results["event_id"] == "333mbf"].copy()
    df = df[df["best"] > 0]  # filter DNF/DNS

    if df.empty:
        return pd.Series(0.0, index=all_entities)

    df["kinch_val"] = df["points"] + (3600 - df["time"] / 100) / 3600
    # time in multi_results is in centiseconds (per multitime()), convert to seconds

    best_per_entity = (
        df.groupby(id_col)["kinch_val"]
        .max()
        .reindex(all_entities)
    )
    return best_per_entity


def _compute_kinch_scores_from_ranks(
    ranks_single: pd.DataFrame,
    ranks_average: pd.DataFrame,
    wr_single,
    wr_average,
    persons,
    multi_results: pd.DataFrame,
    id_col: str = "person_id",
    name_col: str = "name",
    logger=None
) -> pd.DataFrame:
    """
    Core Kinch computation shared by all three public functions.
    Builds per-entity (person or country) scores for all 18 events,
    then averages them.
    """

    # Normalise world-best lookups to dicts for fast access
    if isinstance(wr_single, pd.DataFrame) and "world_best" in wr_single.columns:
        wr_single_map = wr_single.set_index("event_id")["world_best"].to_dict()
    else:
        wr_single_map = (
            wr_single.groupby("event_id")["best"].min().to_dict()
        )

    if isinstance(wr_average, pd.DataFrame) and "world_best" in wr_average.columns:
        wr_average_map = wr_average.set_index("event_id")["world_best"].to_dict()
    else:
        wr_average_map = (
            wr_average.groupby("event_id")["best"].min().to_dict()
        )

    # --- Build per-entity best results ---
    single_pivot = (
        ranks_single[ranks_single["event_id"].isin(_ALL_KINCH_EVENTS)]
        .groupby([id_col, "event_id"])["best"]
        .min()
        .unstack(fill_value=None)
    )
    average_pivot = (
        ranks_average[ranks_average["event_id"].isin(_ALL_KINCH_EVENTS)]
        .groupby([id_col, "event_id"])["best"]
        .min()
        .unstack(fill_value=None)
    )

    all_entities = single_pivot.index.union(average_pivot.index)
    single_pivot = single_pivot.reindex(all_entities)
    average_pivot = average_pivot.reindex(all_entities)

    event_scores = {}

    # Average events
    for ev in _KINCH_AVERAGE_EVENTS:
        wr = wr_average_map.get(ev)
        if wr is None or wr <= 0:
            event_scores[ev] = pd.Series(0.0, index=all_entities)
            continue
        col = average_pivot.get(ev, pd.Series(dtype=float))
        event_scores[ev] = (wr / col.reindex(all_entities)).clip(upper=1.0) * 100
        event_scores[ev] = event_scores[ev].fillna(0.0)

    # Single-only events (excluding MBLD)
    for ev in ["444bf", "555bf"]:
        wr = wr_single_map.get(ev)
        if wr is None or wr <= 0:
            event_scores[ev] = pd.Series(0.0, index=all_entities)
            continue
        col = single_pivot.get(ev, pd.Series(dtype=float))
        event_scores[ev] = (wr / col.reindex(all_entities)).clip(upper=1.0) * 100
        event_scores[ev] = event_scores[ev].fillna(0.0)

    # MultiBLD — use pre-computed multi_results table
    ev = "333mbf"
    mbf_kinch = _mbf_kinch_series(multi_results, all_entities, id_col)
    wr_mbf_kinch = mbf_kinch.max()  # world/national/country best is just the max

    if wr_mbf_kinch > 0:
        event_scores[ev] = (mbf_kinch / wr_mbf_kinch).clip(upper=1.0) * 100
        event_scores[ev] = event_scores[ev].fillna(0.0)
    else:
        event_scores[ev] = pd.Series(0.0, index=all_entities)

    # Best-of-both events (333bf, 333fm)
    for ev in _KINCH_BEST_OF_BOTH_EVENTS:
        wr_avg = wr_average_map.get(ev)
        wr_sngl = wr_single_map.get(ev)

        avg_col = average_pivot.get(ev, pd.Series(dtype=float)).reindex(all_entities)
        sngl_col = single_pivot.get(ev, pd.Series(dtype=float)).reindex(all_entities)

        score_avg = pd.Series(0.0, index=all_entities)
        score_sngl = pd.Series(0.0, index=all_entities)

        if wr_avg and wr_avg > 0:
            score_avg = (wr_avg / avg_col).clip(upper=1.0) * 100
            score_avg = score_avg.fillna(0.0)

        if wr_sngl and wr_sngl > 0:
            score_sngl = (wr_sngl / sngl_col).clip(upper=1.0) * 100
            score_sngl = score_sngl.fillna(0.0)

        event_scores[ev] = pd.concat([score_avg, score_sngl], axis=1).max(axis=1)

    # --- Assemble final scores ---
    scores_df = pd.DataFrame(event_scores, index=all_entities)

    # Sum divided by fixed denominator of 18
    scores_df["Kinch"] = scores_df.sum(axis=1) / _N_KINCH_EVENTS

    # --- Merge names ---
    result = scores_df.reset_index().rename(columns={id_col: id_col})

    if persons is not None:
        result = (
            result
            .merge(persons, left_on=id_col, right_on="wca_id", how="left")
            .drop(columns=["wca_id"])
            .rename(columns={"name": "Name"})
        )
        result = result.sort_values("Kinch", ascending=False).reset_index(drop=True)
        result.index += 1
        cols_front = ["Name", id_col, "Kinch"]
    else:
        result = result.sort_values("Kinch", ascending=False).reset_index(drop=True)
        result.index += 1
        cols_front = [id_col, "Kinch"]
 
    # Round all numeric event and total columns to 2 decimal places
    numeric_cols = event_cols + ["Kinch"]
    result[numeric_cols] = result[numeric_cols].round(2)
    
    # Reorder: identity columns first, then per-event, then total
    event_cols = [e for e in _ALL_KINCH_EVENTS if e in result.columns]
    result = result[cols_front + event_cols]

    return result


###################################################################
############################### RUN ###############################
###################################################################



def run(db_tables, config):

    logger = logging.getLogger(__name__)

    logger.info("Producing stats for Relays module")

    results = {
        "SOR Single": compute_sor_single(db_tables=db_tables, config=config, logger=logger),
        "SOR Average": compute_sor_average(db_tables=db_tables, config=config, logger=logger),
        "Kinch": compute_kinch_score(db_tables=db_tables, config=config, logger=logger),
        "Kinch (National)": compute_national_level_kinch_score(db_tables=db_tables, config=config, logger=logger),
        "Country Kinch": compute_country_kinch_score(db_tables=db_tables, config=config, logger=logger),
    }

    figures = {}

    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)