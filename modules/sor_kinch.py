import pandas as pd
import numpy as np
import logging
import configparser
import matplotlib.pyplot as plt
import utils_wca as uw


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Events that use Average, Single, or best-of-both for Kinch
_KINCH_AVERAGE_EVENTS = [
    "333", "444", "555", "222", "333oh", "333ft", "minx",
    "pyram", "sq1", "clock", "skewb", "666", "777"
]
_KINCH_SINGLE_ONLY_EVENTS = ["444bf", "555bf"]         # singles only (excl. mbld)
_KINCH_BEST_OF_BOTH_EVENTS = ["333bf", "333fm"]        # best ratio of avg vs single
_KINCH_MBLD_EVENT = "333mbf"

_ALL_KINCH_EVENTS = (
    _KINCH_AVERAGE_EVENTS
    + _KINCH_SINGLE_ONLY_EVENTS
    + _KINCH_BEST_OF_BOTH_EVENTS
    + [_KINCH_MBLD_EVENT]
)
_N_KINCH_EVENTS = len(_ALL_KINCH_EVENTS)   # 18


###################################################################
########################## SUM OF RANKS ###########################
###################################################################


def compute_sor_single(db_tables: dict, config, logger) -> pd.DataFrame:
    """
    Sum of Ranks across all events for single results (lower = better).
    """
    return _compute_sor(
        ranks=db_tables["ranks_single_nationality"],
        persons=db_tables["persons"],
        events=config.current_events,
        score_col="SOR single",
        label=f"Single ({config.nationality})",
        logger=logger,
    )


def compute_sor_average(db_tables: dict, config, logger) -> pd.DataFrame:
    """
    Sum of Ranks across all events for average results (lower = better).
    """
    return _compute_sor(
        ranks=db_tables["ranks_average_nationality"],
        persons=db_tables["persons"],
        events=config.current_events,
        score_col="SOR average",
        label=f"Average ({config.nationality})",
        logger=logger,
    )


def _compute_sor(
    ranks: pd.DataFrame,
    persons: pd.DataFrame,
    events: list,
    score_col: str,
    label: str,
    logger,
) -> pd.DataFrame:
    """
    Shared SOR computation.
    Missing events get max_rank_in_event + 1 (worst possible rank).
    """
    try:
        logger.info(f"Computing Sum of Ranks - {label}")

        ranks = ranks[ranks["event_id"].isin(events)]
        persons = persons[["wca_id", "name"]].drop_duplicates()

        pivot = (
            ranks.pivot_table(
                index="person_id",
                columns="event_id",
                values="country_rank",
                aggfunc="min",
            )
            .reindex(columns=events)
        )

        # Vectorized fill: each missing event -> max_rank_in_column + 1
        fill_values = pivot.max(axis=0) + 1
        pivot = pivot.fillna(fill_values)

        pivot[score_col] = pivot.sum(axis=1)

        sor = (
            pivot.reset_index()
            .merge(persons, left_on="person_id", right_on="wca_id", how="left")
            .drop(columns="wca_id")
            .rename(columns={"name": "Name"})
            .sort_values(score_col)
            .reset_index(drop=True)
        )
        sor.index += 1

        logger.info(f"Computed SOR for {len(sor)} competitors - {label}")
        return sor

    except Exception as e:
        logger.critical(f"Error while computing Sum of Ranks ({label}): {e}", exc_info=True)


###################################################################
########################## KINCH RANKS ############################
###################################################################


def compute_kinch_score(db_tables: dict, config, logger) -> pd.DataFrame:
    """
    KinchRank score for each competitor of the configured nationality,
    benchmarked against World Records.
    """
    return _compute_kinch_person(
        db_tables=db_tables,
        config=config,
        logger=logger,
        national_level=False,
        event_scores_key="kinch_event_scores",
    )


def compute_national_level_kinch_score(db_tables: dict, config, logger) -> pd.DataFrame:
    """
    KinchRank score for each competitor, benchmarked against National Records.
    """
    return _compute_kinch_person(
        db_tables=db_tables,
        config=config,
        logger=logger,
        national_level=True,
        event_scores_key="kinch_event_scores_national",
    )


def compute_country_kinch_score(db_tables: dict, config, logger) -> pd.DataFrame:
    """
    KinchRank score per country. Each country's best result per event is
    scored against the World Record.
    """
    try:
        logger.info("Computing Country Kinch scores")

        ranks_single = db_tables["ranks_single"]
        ranks_average = db_tables["ranks_average"]
        multi_results = db_tables["multi_results"]

        # World records
        wr_single = _best_per_event(ranks_single)
        wr_average = _best_per_event(ranks_average)

        # Best result per country per event
        country_single = (
            ranks_single.groupby(["country_id", "event_id"], as_index=False)["best"].min()
        )
        country_average = (
            ranks_average.groupby(["country_id", "event_id"], as_index=False)["best"].min()
        )

        # Country-level MBLD source: rename person_country_id -> country_id
        country_multi = multi_results.copy()
        country_multi["country_id"] = country_multi["person_country_id"]

        event_scores = _compute_kinch_event_scores(
            ranks_single=country_single,
            ranks_average=country_average,
            wr_single=wr_single,
            wr_average=wr_average,
            multi_results=country_multi,
            id_col="country_id",
        )

        # Cache event-level scores for potential downstream use
        db_tables["kinch_country_event_scores"] = event_scores

        result = _finalize_kinch_ranking(
            event_scores=event_scores,
            persons=None,
            id_col="country_id",
        )
        result = result.rename(columns={"country_id": "Country"})

        logger.info(f"Computed Country Kinch scores for {len(result)} countries.")
        return result

    except Exception as e:
        logger.critical(f"Error while computing Country Kinch scores: {e}", exc_info=True)


def _compute_kinch_person(
    db_tables: dict,
    config,
    logger,
    national_level: bool,
    event_scores_key: str,
) -> pd.DataFrame:
    """
    Shared logic for person-level Kinch (world or national benchmark).
    """
    kind = "National" if national_level else "World"

    try:
        logger.info(f"Computing {kind}-level Kinch scores for {config.nationality}")

        persons = db_tables["persons"][["wca_id", "name"]].drop_duplicates()
        ranks_single = db_tables["ranks_single_nationality"]
        ranks_average = db_tables["ranks_average_nationality"]

        # Benchmark records: national pool (if national_level) or global
        if national_level:
            wr_single = _best_per_event(ranks_single)
            wr_average = _best_per_event(ranks_average)
            multi_results = db_tables["multi_results"][db_tables["multi_results"]["person_country_id"] == config.country]
        else:
            wr_single = _best_per_event(db_tables["ranks_single"])
            wr_average = _best_per_event(db_tables["ranks_average"])
            multi_results = db_tables["multi_results"]

        event_scores = _compute_kinch_event_scores(
            ranks_single=ranks_single,
            ranks_average=ranks_average,
            wr_single=wr_single,
            wr_average=wr_average,
            multi_results=multi_results,
            id_col="person_id",
        )

        # Cache event-level scores for the plots (reused downstream)
        db_tables[event_scores_key] = event_scores

        result = _finalize_kinch_ranking(
            event_scores=event_scores,
            persons=persons,
            id_col="person_id",
        )

        logger.info(f"Computed {kind}-level Kinch scores for {len(result)} competitors.")
        return result

    except Exception as e:
        logger.critical(f"Error computing {kind}-level Kinch scores: {e}", exc_info=True)


# ---------------------------------------------------------------------------
# Internal helpers (Kinch)
# ---------------------------------------------------------------------------


def _best_per_event(ranks: pd.DataFrame) -> pd.DataFrame:
    """Return best (min) result per event, columns [event_id, wr]."""
    return (
        ranks.groupby("event_id", as_index=False)["best"]
        .min()
        .rename(columns={"best": "wr"})
    )


def _compute_kinch_event_scores(
    ranks_single: pd.DataFrame,
    ranks_average: pd.DataFrame,
    wr_single: pd.DataFrame,
    wr_average: pd.DataFrame,
    multi_results: pd.DataFrame,
    id_col: str,
) -> pd.DataFrame:
    """
    Compute per-entity per-event Kinch scores. Returns a long DataFrame
    with columns [id_col, event_id, score] where score is in [0, 100].

    Only (entity, event) pairs that actually have a result are returned.
    The "score 0 for missing events" logic is handled by _finalize_kinch_ranking.
    """
    avg_events_set = set(_KINCH_AVERAGE_EVENTS) | set(_KINCH_BEST_OF_BOTH_EVENTS)
    sngl_events_set = set(_KINCH_SINGLE_ONLY_EVENTS) | set(_KINCH_BEST_OF_BOTH_EVENTS)

    avg_long = (
        ranks_average[ranks_average["event_id"].isin(avg_events_set)]
        .groupby([id_col, "event_id"], as_index=False)["best"].min()
        .assign(mode="average")
    )
    sngl_long = (
        ranks_single[ranks_single["event_id"].isin(sngl_events_set)]
        .groupby([id_col, "event_id"], as_index=False)["best"].min()
        .assign(mode="single")
    )
    long = pd.concat([avg_long, sngl_long], ignore_index=True)

    # Merge benchmark records
    wr_df = pd.concat([
        wr_average.assign(mode="average"),
        wr_single.assign(mode="single"),
    ], ignore_index=True)
    long = long.merge(wr_df, on=["event_id", "mode"], how="left")

    # Score = (benchmark / best) * 100, capped at 100
    long["score"] = (long["wr"] / long["best"]).clip(upper=1.0) * 100

    # For best-of-both events, keep the better score per entity
    scores = long.groupby([id_col, "event_id"], as_index=False)["score"].max()

    # MBLD from multi_results
    mbf_scores = _compute_mbld_kinch_scores(multi_results, id_col)
    if not mbf_scores.empty:
        scores = pd.concat([scores, mbf_scores], ignore_index=True)

    return scores


def _compute_mbld_kinch_scores(multi_results: pd.DataFrame, id_col: str) -> pd.DataFrame:
    """
    MultiBLD Kinch scores. The Kinch value for a MBLD result is:
        points + (time_limit - time_seconds) / time_limit
    where time_limit = min(attempted, 6) * 10 minutes.
    The benchmark is the max Kinch value observed in the given pool.
    """
    mbf = multi_results[
        (multi_results["event_id"] == _KINCH_MBLD_EVENT)
        & (multi_results["value"] > 0)
    ]
    if mbf.empty:
        return pd.DataFrame(columns=[id_col, "event_id", "score"])

    time_limit = mbf["attempted"].clip(upper=6) * 10 * 60  # seconds
    kinch_val = mbf["points"] + (time_limit - mbf["time"] / 100) / time_limit

    best_mbf = (
        pd.DataFrame({id_col: mbf[id_col].values, "kinch_val": kinch_val.values})
        .groupby(id_col, as_index=False)["kinch_val"].max()
    )
    benchmark = best_mbf["kinch_val"].max()
    if benchmark <= 0:
        return pd.DataFrame(columns=[id_col, "event_id", "score"])

    best_mbf["score"] = (best_mbf["kinch_val"] / benchmark).clip(upper=1.0) * 100
    best_mbf["event_id"] = _KINCH_MBLD_EVENT

    return best_mbf[[id_col, "event_id", "score"]]


def _finalize_kinch_ranking(
    event_scores: pd.DataFrame,
    persons: pd.DataFrame,
    id_col: str,
) -> pd.DataFrame:
    """
    Turn a long (entity, event, score) frame into the final wide ranking
    with a total "Kinch" column, optional name merge, sorting and rounding.
    """
    pivot = (
        event_scores
        .pivot_table(index=id_col, columns="event_id", values="score", aggfunc="max")
        .reindex(columns=_ALL_KINCH_EVENTS)
        .fillna(0.0)
    )
    pivot["Kinch"] = pivot.sum(axis=1) / _N_KINCH_EVENTS
    result = pivot.reset_index()

    if persons is not None:
        result = (
            result
            .merge(persons, left_on=id_col, right_on="wca_id", how="left")
            .drop(columns=["wca_id"])
            .rename(columns={"name": "Name"})
        )
        cols_front = ["Name", id_col]
    else:
        cols_front = [id_col]

    result = (
        result[cols_front + ["Kinch"] + _ALL_KINCH_EVENTS]
        .sort_values("Kinch", ascending=False)
        .reset_index(drop=True)
    )
    result.index += 1

    numeric_cols = ["Kinch"] + _ALL_KINCH_EVENTS
    result[numeric_cols] = result[numeric_cols].round(2)

    return result


###################################################################
########################### KINCH PLOTS ###########################
###################################################################
 
 
def _build_best_x_matrix(ranking: pd.DataFrame, label_col: str) -> pd.DataFrame:
    """
    From the wide Kinch ranking DataFrame, build a matrix where row i
    contains the cumulative sum of that entity's top-k event scores for
    k = 1..18. Missing events already scored 0 in the ranking.
 
    Returns a DataFrame indexed by label_col with columns 1..18.
    """
    # Extract only the 18 per-event score columns, sort each row descending,
    # then cumsum along columns. np.sort is vectorized on the underlying
    # numpy array — much faster than per-row Python work.
    event_cols = [e for e in _ALL_KINCH_EVENTS if e in ranking.columns]
    scores = ranking[event_cols].to_numpy(dtype=float)
    sorted_desc = -np.sort(-scores, axis=1)   # descending sort per row
    cumulative = np.cumsum(sorted_desc, axis=1)
 
    return pd.DataFrame(
        cumulative,
        index=ranking[label_col].values,
        columns=range(1, _N_KINCH_EVENTS + 1),
    )
 
 
def _plot_kinch_lines(
    data: pd.DataFrame,
    title: str,
    xlabel: str,
    ylabel: str,
    logger: logging.Logger,
    invert_y: bool = False,
    ylim_top: float | None = None,
    legend_loc: str = "best",
) -> plt.Figure:
    """
    Shared matplotlib line-plot for all three Kinch analysis figures.
    `data` is a DataFrame indexed by entity label with columns = X values.
    """
    try:
        fig, ax = plt.subplots()
 
        for label, row in data.iterrows():
            ax.plot(data.columns, row.values, marker="o", markersize=3, label=label)
 
        ax.set_title(title, fontweight="bold")
        ax.set_xlabel(xlabel, fontweight="bold")
        ax.set_ylabel(ylabel, fontweight="bold")
        ax.set_xticks(list(data.columns))
        ax.grid(True, which="major", zorder=1)
        ax.legend(loc=legend_loc, fontsize=8, ncol=2)
 
        if invert_y:
            ax.invert_yaxis()
        if ylim_top is not None:
            ax.set_ylim(top=ylim_top)
 
        fig.tight_layout()
        plt.close(fig)
        return fig
 
    except Exception as e:
        logger.critical(f"Error creating Kinch line plot '{title}': {e}", exc_info=True)
 
 
def _kinch_plot_suite(
    ranking: pd.DataFrame,
    label_col: str,
    benchmark_label: str,
    top_n: int,
    logger: logging.Logger,
) -> dict:
    """
    Build the 3 Kinch analysis plots (cumulative, relative-to-leader, rank)
    for the top_n entities in `ranking`. Returns {figure_name: Figure}.
 
    `ranking` must already be sorted by Kinch descending and contain the
    18 per-event score columns plus `label_col` for display.
    """
    if ranking is None or ranking.empty:
        logger.warning(f"No ranking provided for '{benchmark_label}', skipping plots.")
        return {}
 
    # --- Full-population cumulative matrix (needed for fair rank plot) ---
    full_best_x = _build_best_x_matrix(ranking, label_col)
 
    # --- Ranks computed against the full population, column-wise ---
    full_ranks = full_best_x.rank(axis=0, method="min", ascending=False)
 
    # --- Slice top_n (ranking is already sorted by Kinch desc) ---
    top_best_x = full_best_x.iloc[:top_n]
    top_ranks = full_ranks.iloc[:top_n]
 
    # --- Relative to leader per X (leader = top_n max, which equals full max) ---
    leader = full_best_x.max(axis=0)
    relative = top_best_x.div(leader, axis=1) * 100
 
    suffix = f"Top {top_n} - {benchmark_label}"
 
    return {
        f"Kinch Cumulative ({suffix})": _plot_kinch_lines(
            data=top_best_x,
            title=f"Cumulative Kinch score vs events included - {benchmark_label}",
            xlabel="Number of events included (X)",
            ylabel="Cumulative Kinch score",
            logger=logger,
            legend_loc="upper left",
        ),
        f"Kinch Relative ({suffix})": _plot_kinch_lines(
            data=relative,
            title=f"Kinch score relative to leader - {benchmark_label}",
            xlabel="Number of events included (X)",
            ylabel="Score relative to leader (%)",
            logger=logger,
            ylim_top=101,
            legend_loc="lower left",
        ),
        f"Kinch Rank ({suffix})": _plot_kinch_lines(
            data=top_ranks,
            title=f"Kinch rank vs events included - {benchmark_label}",
            xlabel="Number of events included (X)",
            ylabel="Rank",
            logger=logger,
            invert_y=True,
            legend_loc="lower right",
        ),
    }
 
 
def plot_kinch_analysis(
    config: configparser.ConfigParser,
    logger: logging.Logger,
    kinch_ranking: pd.DataFrame,
    top_n: int = 10,
) -> dict:
    """
    World-benchmark Kinch analysis plots (cumulative / relative / rank)
    for the top-N competitors of the configured nationality.
    """
    logger.info(f"Creating world-level Kinch analysis plots (top {top_n})")
    return _kinch_plot_suite(
        ranking=kinch_ranking,
        label_col="Name",
        benchmark_label=f"World benchmark, {config.nationality}",
        top_n=top_n,
        logger=logger,
    )
 
 
def plot_kinch_analysis_national(
    config: configparser.ConfigParser,
    logger: logging.Logger,
    kinch_ranking: pd.DataFrame,
    top_n: int = 10,
) -> dict:
    """
    National-benchmark Kinch analysis plots (cumulative / relative / rank)
    for the top-N competitors of the configured nationality.
    """
    logger.info(f"Creating national-level Kinch analysis plots (top {top_n})")
    return _kinch_plot_suite(
        ranking=kinch_ranking,
        label_col="Name",
        benchmark_label=f"National benchmark, {config.nationality}",
        top_n=top_n,
        logger=logger,
    )
 
 
###################################################################
############################### RUN ###############################
###################################################################
 
 
def run(db_tables, config):
 
    logger = logging.getLogger(__name__)
    logger.info("Producing stats for SOR & Kinch module")
 
    # --- Tables ---
    kinch_world = compute_kinch_score(db_tables=db_tables, config=config, logger=logger)
    kinch_national = compute_national_level_kinch_score(db_tables=db_tables, config=config, logger=logger)
    kinch_country = compute_country_kinch_score(db_tables=db_tables, config=config, logger=logger)
 
    results = {
        "SOR Single": compute_sor_single(db_tables=db_tables, config=config, logger=logger),
        "SOR Average": compute_sor_average(db_tables=db_tables, config=config, logger=logger),
        "Kinch": kinch_world,
        "Kinch (National)": kinch_national,
        "Country Kinch": kinch_country,
    }
 
    # --- Figures ---
    figures = {}
    figures.update(plot_kinch_analysis(config=config, logger=logger, kinch_ranking=kinch_world, top_n=10))
    figures.update(plot_kinch_analysis_national(config=config, logger=logger, kinch_ranking=kinch_national, top_n=10))
 
    section_name = __name__.split(".")[-1]
    uw.export_data(results, figures=figures, section_name=section_name, config=config, logger=logger)
 