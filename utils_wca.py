from pathlib import Path
import configparser
import urllib.request
import zipfile
import pandas as pd
import logging
import sys
from datetime import datetime
import inspect
import numpy as np
import matplotlib.pyplot as plt
from cycler import cycler
import traceback


############ CONSTANTS ############

# Shared WCA domain constants.
# Use via e.g. uw.WCA_CONSTANTS['final_rounds'].
# Inside pandas .query() rebind locally, because @name can't access dict subscripts:
#     final_rounds = uw.WCA_CONSTANTS['final_rounds']
#     df.query("round_type_id in @final_rounds")

WCA_CONSTANTS = {
    # Round type ids representing a final or combined-final.
    'final_rounds': ('c', 'f'),

    # Sentinel values for WCA result columns (best / average / attempt value).
    # -1 = DNF, -2 = DNS, 0 = no attempt / not submitted.
    'dnf': -1,
    'dns': -2,
    'no_attempt': 0,
    'invalid_results': (0, -1, -2),
}



############ LOGGER ############


class ExitOnCriticalHandler(logging.Handler):
    """
    Custom handler that immediately terminates execution
    when a CRITICAL log is emitted.
    """

    def emit(self, record):
        try:
            if record.levelno >= logging.CRITICAL:
                logging.shutdown()

                # Print final CRITICAL message cleanly to stderr
                sys.stderr.write(
                    f"\n\n[CRITICAL] {record.getMessage()}\n"
                )
                if record.exc_info:
                    traceback.print_exception(*record.exc_info)

                sys.stderr.write("\nExecution halted due to critical error.\n")
                sys.stderr.flush()

                # Exit with a nonzero status code
                sys.exit(1)

        except Exception:
            # Fallback if logging itself fails
            sys.stderr.write("\n[CRITICAL] Failed to terminate cleanly.\n")
            sys.exit(1)


def setup_logger(name: str, level=logging.INFO, log_root: Path | str = "./logs") -> logging.Logger:
    """
    Set up a logger that writes to both console and file.
    Applies globally to all modules in the project.
    """
    # --- Root logger ---
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Prevent adding duplicate handlers
    if not root_logger.handlers:
        formatter = logging.Formatter(
            "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # --- Console handler ---
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

        # --- File handler ---
        log_root = Path(log_root)
        date_subfolder = datetime.now().strftime("%Y-%m-%d")
        log_dir = log_root / date_subfolder
        log_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        log_filename = f"{name}_{timestamp}.log"
        log_path = log_dir / log_filename

        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        # stop execution on critical
        root_logger.addHandler(ExitOnCriticalHandler())

        root_logger.info(f"Logger initialized. Writing logs to {log_path.resolve()}")

    # Return a named child logger
    return logging.getLogger(name)


############ CONFIG ############


def load_config(logger: logging.Logger, config_path: str | Path = "config.ini") -> configparser.ConfigParser:
    """
    Import the config file and attach commonly-used attributes to it.

    Attributes set here (available immediately after this call):
        config.current_events, config.multivenue : list[str]
        config.year                              : int
        config.country, config.nationality       : str   (must match countries.tsv `name`)
        config.championship_type                 : str   (e.g. "IT", "US")
        config.figure_size, config.dpi           : plot defaults

    Attributes set later by `process_tables` (require db_tables to be loaded):
        config.continent_id            : str   (e.g. "_Europe", "_Asia") — derived from countries
        config.continental_record_name : str   (e.g. "ER", "AsR")       — derived from continents
        config.nats                    : list[str] — competition_ids of national championships
        config.countries, config.real_countries : list[str]
    """

    logger.info(f"Gathering info from config {config_path}")

    config = configparser.ConfigParser()
    config.read(config_path)

    # Lettura variabili globali
    current_events = [
        x.strip() for x in config["global_variables"]["current_events"].split(",")
    ]
    multivenue = [
        x.strip() for x in config["global_variables"]["multivenue"].split(",")
    ]

    config.current_events = current_events
    config.multivenue = multivenue
    config.year = int(config["global_variables"]["year"])
    config.country = config["global_variables"]["country"]
    config.nationality = config["global_variables"]["nationality"]
    config.championship_type = config["global_variables"]["championship_type"]

    if config.has_section("plot"):
        fig_size = tuple(
            float(x.strip()) for x in config["plot"]["figure_size"].split(",")
        )
        config.figure_size = fig_size
        config.dpi = int(config["plot"]["dpi"])
    else:
        config.figure_size = (12, 6)
        config.dpi = 100

    return config


############ PLOTS ############

# Font families that cover CJK (Chinese/Japanese/Korean) glyphs.
# Order: cross-platform Noto first, then common OS-bundled fallbacks
# (macOS, Windows, Linux). matplotlib picks the first one that's
# actually installed.

_CJK_FONT_CANDIDATES = [
    "Noto Sans CJK SC", "Noto Sans CJK JP", "Noto Sans CJK TC",
    "PingFang SC", "Hiragino Sans GB", "Hiragino Sans",   # macOS
    "Microsoft YaHei", "SimHei",                           # Windows
    "WenQuanYi Zen Hei", "Source Han Sans SC",             # Linux
    "Arial Unicode MS",                                    # older macOS catch-all
]


def _resolve_font_chain(configured: list[str], logger: logging.Logger | None = None) -> list[str]:
    """
    Build matplotlib's font fallback chain by combining the user-configured
    fonts with any CJK-capable fonts actually installed on the system.

    Configured fonts always come first (explicit control wins). CJK fonts
    are only appended if they're found in matplotlib's font registry, to
    avoid polluting the fallback chain with missing fonts.
    """
    from matplotlib import font_manager

    installed = {f.name for f in font_manager.fontManager.ttflist}
    detected_cjk = [f for f in _CJK_FONT_CANDIDATES if f in installed]

    # Deduplicate while preserving order: configured first, then CJK.
    seen = set()
    chain = []
    for f in configured + detected_cjk:
        if f not in seen:
            chain.append(f)
            seen.add(f)

    if logger:
        if detected_cjk:
            logger.info(f"CJK-capable fonts detected: {', '.join(detected_cjk)}")
        else:
            logger.warning(
                "No CJK-capable fonts detected on this system. Non-Latin names "
                "(e.g. Chinese/Japanese) will render as missing glyphs. "
                "Install e.g. 'Noto Sans CJK' to fix."
            )

    return chain


def set_plot_style(config: configparser.ConfigParser, logger: logging.Logger | None = None):
    """
    Apply consistent matplotlib styling based on config.ini [plot] section.
    """
    try:
        cfg = config["plot"]

        # --- Parse color cycle ---
        colors = [c.strip() for c in cfg.get("color_cycle", "").split(",") if c.strip()]

        # --- Parse figure size safely ---
        try:
            fig_size = tuple(
                float(x.strip()) for x in cfg.get("figure_size", "8, 5").split(",")
            )
            if len(fig_size) != 2:
                raise ValueError(f"expected 2 values, got {len(fig_size)}")
        except Exception as e:
            fig_size = (8, 5)
            if logger:
                logger.warning(f"Invalid 'figure_size' in config.ini ({e}); using default (8, 5)")

        
        # --- Build font fallback list (config + auto-detected CJK) ---
        configured_fonts = [
            x.strip() for x in cfg.get("font_sans_serif", "DejaVu Sans").split(",") if x.strip()
        ]
        font_list = _resolve_font_chain(configured_fonts, logger)
        
        # --- Update matplotlib rcParams ---
        plt.rcParams.update({
            # --- Figure ---
            "figure.figsize": fig_size,
            "figure.dpi": cfg.getint("dpi", 150),
            "savefig.dpi": cfg.getint("save_dpi", 300),
            "savefig.bbox": cfg.get("bbox", "tight"),
            "figure.autolayout": cfg.getboolean("autolayout", True),

            # --- Axes ---
            "axes.titlesize": cfg.getint("axes_title_size", 14),
            "axes.labelsize": cfg.getint("axes_label_size", 12),
            "axes.grid": cfg.getboolean("axes_grid", True),
            "axes.edgecolor": cfg.get("axes_edge_color", "gray"),
            "grid.alpha": cfg.getfloat("grid_alpha", 0.4),
            "grid.linestyle": cfg.get("grid_linestyle", ":"),
            "grid.color": cfg.get("grid_color", "lightgray"),

            # --- Font ---
            "font.family": cfg.get("font_family", "sans-serif"),
            "font.sans-serif": font_list,
            "font.size": cfg.getint("font_size", 11),
            "text.color": cfg.get("text_color", "black"),

            # --- Ticks and Legend ---
            "xtick.labelsize": cfg.getint("tick_label_size", 10),
            "ytick.labelsize": cfg.getint("tick_label_size", 10),
            "legend.fontsize": cfg.getint("legend_font_size", 10),
            "legend.frameon": cfg.getboolean("legend_frame", False),

            # --- Lines & Colors ---
            "lines.linewidth": cfg.getfloat("line_width", 2.0),
            "axes.prop_cycle": cycler("color", colors or ["#1f77b4", "#ff7f0e", "#2ca02c"]),
        })

        if logger:
            logger.info("Matplotlib plotting style successfully applied from config.ini")

    except Exception as e:
        if logger:
            logger.warning(f"Could not set plotting style: {e}", exc_info=True)


############ UTILS ############


def get_database_dir(config: configparser.ConfigParser) -> Path:
    """
    Return the path to the database_export directory, creating it if needed.
    """

    try:
        db_dir = Path(config["paths"]["database_export_dir"]).resolve()
    except KeyError:
        raise KeyError("Missing [paths] -> database_export_dir in config.ini")

    db_dir.mkdir(parents=True, exist_ok=True)

    return db_dir


def get_regions_dir(config: configparser.ConfigParser) -> Path:
    """
    Return the path to the regions directory, creating it if needed.
    """
    try:
        reg_dir = Path(config["paths"]["regions_dir"]).resolve()
    except KeyError:
        raise KeyError("Missing [paths] -> regions_dir in config.ini")

    reg_dir.mkdir(parents=True, exist_ok=True)
    return reg_dir


def get_current_persons(db_tables: dict, columns: list[str] | None = None) -> pd.DataFrame:
    """
    Return one row per WCA competitor with their CURRENT (most recent) attributes.

    The WCA persons table stores nationality history: for each `wca_id`, the row
    with `sub_id == 1` holds the latest data; `sub_id > 1` rows are historical
    snapshots of past nationalities/names.

    Parameters
    ----------
    db_tables : dict
        Standard db_tables dict; must contain "persons".
    columns : list[str], optional
        Subset of columns to return. Defaults to ["wca_id", "name"].
        Available columns: name, gender, wca_id, sub_id, country_id.

    Returns
    -------
    pd.DataFrame
        Deduplicated by wca_id, restricted to current-nationality rows.
    """
    if columns is None:
        columns = ["wca_id", "name"]

    return (
        db_tables["persons"]
        .loc[db_tables["persons"]["sub_id"] == 1, columns]
        .drop_duplicates(subset="wca_id")
        .reset_index(drop=True)
    )


def update_data(config: configparser.ConfigParser, logger: logging.Logger | None = None) -> None:
    """
    Download the latest WCA export if not already downloaded today.
    Uses logger if provided.
    """
    db_dir = get_database_dir(config)
    meta_file = db_dir / "last_update.txt"
    url = config["url"]["wca_export_url"]

    today_str = datetime.now().date().isoformat()

    # Check if already updated today
    if meta_file.exists():
        last_update = meta_file.read_text().strip()
        if last_update == today_str:
            if logger:
                logger.info(f"Data already up to date (last update: {last_update})")
            else:
                print(f"Data already up to date (last update: {last_update})")
            return

    if logger:
        logger.info(f"Downloading new WCA export from {url} ...")
    else:
        print(f"Downloading new WCA export from {url} ...")

    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(db_dir)

    meta_file.write_text(today_str)
    if logger:
        logger.info(f"WCA data updated successfully on {today_str} in {db_dir}")
    else:
        print(f"WCA data updated successfully on {today_str} in {db_dir}")


def read_table(table_name: str, config: configparser.ConfigParser, logger: logging.Logger | None = None) -> pd.DataFrame:
    """
    Reads a WCA table (.tsv) based on config mappings.
    Logs messages if logger is provided.
    """
    db_dir = get_database_dir(config)

    if not config.has_section("tables"):
        if logger:
            logger.critical("Missing [tables] section in config.ini")
        else:
            raise KeyError("Missing [tables] section in config.ini")

    tables_map = dict(config.items("tables"))
    if table_name not in tables_map:
        if logger:
            logger.critical(f"Table '{table_name}' not found in [tables] section of config.ini")
        else:
            raise KeyError(f"Table '{table_name}' not found in [tables] section of config.ini")

    file_name = tables_map[table_name]
    file_path = db_dir / file_name

    if not file_path.exists():
        if logger:
            logger.critical(f"File not found: {file_path}. You may need to run update_data().")
        else:
            raise FileNotFoundError(f"File not found: {file_path}. You may need to run update_data().")

    df = pd.read_csv(file_path, sep="\t", low_memory=False)

    if logger:
        logger.info(f"Loaded '{table_name}' from {file_path.name} ({len(df):,} rows)")
    else:
        print(f"Loaded '{table_name}' from {file_path.name} ({len(df):,} rows)")

    return df


def read_aux_file(file_key: str, db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger) -> pd.DataFrame:
    """
    Reads auxiliary .csv files defined in [aux_files] section of config.ini
    and stores them into db_tables under the same key.

    Example
    -------
    read_aux_file("regions", db_tables, config, logger)
    -> loads the file pointed to by config["aux_files"]["regions"]
       (e.g. "city_to_region_map_ita.csv") into db_tables["regions"]
    """

    # --- Validate config ---
    if not config.has_section("aux_files"):
        logger.critical("Missing [aux_files] section in config.ini")
    aux_files = dict(config.items("aux_files"))

    if file_key not in aux_files:
        logger.critical(f"File key '{file_key}' not found in [aux_files] section of config.ini")

    file_name = aux_files[file_key]

    # --- Determine path by file_key ---
    if file_key == "regions":
        base_dir = get_regions_dir(config)
        sep = ";"
    else:
        logger.critical(f"No handler defined for auxiliary file '{file_key}'")

    file_path = base_dir / file_name

    # --- Check file existence ---
    if not file_path.exists():
        logger.critical(f"Auxiliary file not found: {file_path}")

    # --- Load CSV ---
    df = pd.read_csv(file_path, sep=sep)

    # --- Save to db_tables ---
    db_tables[file_key] = df
    logger.info(f"Loaded auxiliary file '{file_key}' from {file_path.name} ({len(df):,} rows)")
    return df


def make_localized_results_df(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger):
    
    """
    Create 'results_nationality' — same as results, but with all person_country_id values
    filtered by the nationality of config - and 'results_country' - same as results, 
    but with all competition_id filtered by the country of config
    """
    
    try:

        logger.info("Creating pre-filtered result tables")

        # Rename column in competitions
        db_tables["competitions"] = (
            db_tables["competitions"]
            .rename(columns = {'name':'competition_name', 'id':'competition_id'})
        )
        db_tables["competitions"]['date'] = pd.to_datetime(db_tables["competitions"][['year','month','day']])

        db_tables["rounds"] = db_tables["rounds"].rename(columns = {'id':'round_type_id'})

        # Filter for nationality
        results_nationality = (
            db_tables["results"]
            .query("person_country_id == @config.nationality")
            .copy()
            .merge(
                db_tables["competitions"],
                on="competition_id",
                how="left",
                validate = "m:1"
            )
            .merge(
                db_tables["rounds"][['round_type_id','rank']],
                how='left',
                on='round_type_id',
            )
        )

        db_tables["results_nationality"] = results_nationality

        # Explode with result detail
        results_nationality_detailed = (
            results_nationality
            .merge(
                db_tables["attempts"],
                left_on = 'id',
                right_on = 'result_id',
                how = 'left'
            )
            .drop('id', axis=1)
        )

        db_tables["results_nationality_detailed"] = results_nationality_detailed

        # Filter for host country
        competitions_filtered = db_tables["competitions"].query("country_id == @config.country").copy()

        results_country = (
            db_tables["results"]
            .merge(
                competitions_filtered,
                on="competition_id",
                how="inner",
                validate = "m:1"
            )
            .merge(
                db_tables["rounds"][['round_type_id','rank']],
                how='left',
                on='round_type_id',
            )
        )

        db_tables["results_country"] = results_country

        # Explode with result detail
        results_country_detailed = (
            results_country
            .merge(
                db_tables["attempts"],
                left_on = 'id',
                right_on = 'result_id',
                how = 'left'
            )
            .drop('id', axis=1)
        )

        db_tables["results_country_detailed"] = results_country_detailed

        logger.info(f"Created 'results_nationality' from results+competitions with only competitors from country = {config.nationality}.")
        logger.info(f"Created 'results_nationality_detailed' from results+competitions+attempts with only competitors from country = {config.nationality}.")
        logger.info(f"Created 'results_country' from results+competitions with only competitions from country = {config.country}.")
        logger.info(f"Created 'results_country_detailed' from results+competitions+attempts with only competitions from country = {config.country}.")

    except Exception as e:
        logger.critical(f"Error creating localized results dataframes: {e}", exc_info=True)


def fix_results_nationality(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger):
    
    """
    Create 'results_fixed' — same as results_nationality, but with all person_country_id values
    replaced by the competitor's latest nationality (sub_id=1).
    """
    
    try:
        persons = db_tables["persons"]
        results = db_tables["results"].copy()
        competitions = db_tables["competitions"]
        rounds = db_tables["rounds"][['round_type_id','rank']]

        logger.info("Creating results table with standardized nationality (subid=1)...")

        # Build mapping person_id → latest country_id
        nat_map = (
            persons.loc[persons["sub_id"] == 1, ["wca_id", "country_id"]]
            .drop_duplicates(subset="wca_id")
            .set_index("wca_id")["country_id"]
            .to_dict()
        )

        # Replace nationality
        results["person_country_id"] = results["person_id"].map(nat_map).fillna(results["person_country_id"])

        results_fixed = (
            results
            .query("person_country_id == @config.nationality")
            .copy()
            .merge(
                competitions,
                on="competition_id",
                how="left",
                validate = "m:1"
            )
            .merge(
                rounds,
                how='left',
                on='round_type_id',
            )
            .drop('id', axis=1)
        )

        results_fixed['date'] = pd.to_datetime(results_fixed[['year','month','day']])

        db_tables["results_fixed"] = results_fixed
        logger.info("Added 'results_fixed' to db_tables.")

    except Exception as e:
        logger.critical(f"Error creating results_fixed: {e}", exc_info=True)
        return db_tables


def make_localized_rankings(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger):

    """
    Filter ranks_single and ranks_average for the given nationality
    """

    persons = db_tables["persons"].query("sub_id == 1")[['wca_id','name','country_id']]

    try:
        db_tables["ranks_single"] = (
            db_tables["ranks_single"]
            .merge(
                persons, 
                how='left', 
                left_on='person_id', 
                right_on='wca_id'
            )
            .drop('wca_id', axis=1)
        )

        db_tables["ranks_single_nationality"] = db_tables["ranks_single"].query("country_id == @config.nationality").copy()

        logger.info("Merged persons with ranks_single")
        logger.info(f"Created 'ranks_single_nationality' with only competitors from country = {config.nationality}.")

    except Exception as e:
        logger.critical(f"Error during ranks_single/persons merge: {e}", exc_info=True)

    try:
        db_tables["ranks_average"] = (
            db_tables["ranks_average"]
            .merge(
                persons, 
                how='left', 
                left_on='person_id', 
                right_on='wca_id'
            )
            .drop('wca_id', axis=1)
        )

        db_tables["ranks_average_nationality"] = db_tables["ranks_average"].query("country_id == @config.nationality").copy()

        logger.info("Merged persons with ranks_average")
        logger.info(f"Created 'ranks_average_nationality' with only competitors from country = {config.nationality}.")

    except Exception as e:
        logger.critical(f"Error during ranks_average/persons merge: {e}", exc_info=True)


def make_better_multi_results(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger):

    """
    Decode the wca multi encoding for easier computation of statistics
    """

    results = db_tables["results"].query("event_id == '333mbf'").copy()

    try:
        results = (
            db_tables["attempts"]
            .copy()
            .merge(
                results,
                left_on = 'result_id',
                right_on = 'id',
                how = 'inner'
            )
            .drop(columns = ["best", "average", "id"])
            .query("value != 0")
            .copy()
        )

        results["attempted"] = results["value"].apply(multiattempted)
        results["solved"] = results["value"].apply(multisolved)
        results["wrong"] = results["attempted"] - results["solved"]
        results["points"] = results["solved"] - results["wrong"]
        results["time"] = results["value"].apply(multitime)
        results["display"] = results["value"].apply(multiresult)

        db_tables["multi_results"] = results

        logger.info("Added multi_results table to db_tables")


    except Exception as e:
        logger.critical(f"Error during better multi results creation: {e}", exc_info=True)


def process_tables(db_tables: dict[str, pd.DataFrame], config: configparser.ConfigParser, logger: logging.Logger) -> dict[str, pd.DataFrame]:
    
    """
    Perform common operations on db_tables. e.g. Merges the 'results' and 'competitions' tables on 'competition_id' and filters rows to include only competitors from the configured country.

    Also attaches country-agnostic attributes to `config`:
        config.continent_id            (e.g. "_Europe")
        config.continental_record_name (e.g. "ER", "AsR", "AfR", "NAR", "SAR", "OcR")
        config.nats                    (list of national championship competition_ids)
    """

    logger.info("Preprocessing tables...")
       
    make_localized_results_df(db_tables, config, logger)
    fix_results_nationality(db_tables, config, logger)
    make_localized_rankings(db_tables, config, logger)
    make_better_multi_results(db_tables, config, logger)

    # --- Derive continent + continental record name from the configured country ---
    # This makes records, podiums, etc. country-agnostic: an Italian setup gets "ER",
    # a Chinese setup gets "AsR", a Brazilian setup gets "SAR", and so on.
    countries = db_tables["countries"]
    continents = db_tables["continents"]

    country_row = countries[countries["id"] == config.country]
    if country_row.empty:
        logger.critical(
            f"Country '{config.country}' not found in countries table. "
            f"Make sure config.country matches a row in countries.tsv (the `id`/`name` field)."
        )

    config.continent_id = country_row["continent_id"].iloc[0]

    continent_row = continents[continents["id"] == config.continent_id]
    if continent_row.empty:
        logger.critical(
            f"Continent '{config.continent_id}' not found in continents table."
        )

    config.continental_record_name = continent_row["record_name"].iloc[0]
    logger.info(
        f"Country '{config.country}' is in continent '{config.continent_id}' "
        f"(continental record name: '{config.continental_record_name}')."
    )

    # --- Extract list of championships ---
    try:
        config.nats = list(
            db_tables["championships"]
            [db_tables["championships"]['championship_type'] == config.championship_type]
            ['competition_id']
        )
        logger.info(f"Extracted {len(config.nats)} national championship(s) for type '{config.championship_type}'")

    except Exception as e:
        logger.critical(
            f"Error during the listing of championships for championship type "
            f"{config.championship_type}: {e}",
            exc_info=True,
        )
        raise

    # Useful country lists
    config.countries = list(db_tables["competitions"]['country_id'].drop_duplicates())
    config.real_countries = [x for x in config.countries if x not in config.multivenue]

    return db_tables


def check_missing_regions(db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger | None = None) -> set: 
    """
    Check for competitions in the WCA database that are missing from the region mapping file.

    Note: this is currently only meaningful for countries that have a region-mapping
    auxiliary file (today: Italy). For other countries, `db_tables["regions"]` will be
    absent or empty and this check should not be invoked.
    """

    competitions = db_tables["competitions"].query("country_id == @config.country").copy()
    mapping = db_tables["regions"]

    # --- Find unmapped competitionIds ---
    mapped_ids = set(mapping["city_name"])
    unmapped = set(competitions.loc[~(competitions["city_name"].isin(mapped_ids)), "city_name"])
    unmapped.discard('Multiple cities')   # discard = no error if absent

    if not unmapped:
        logger.info(f"All {config.country} competitions have a region assigned. No missing mappings.")
        return unmapped   # nothing to save, skip writing an empty file

    # --- Save to CSV (filename is country-tagged) ---
    regions_dir = get_regions_dir(config)
    country_tag = config.country.lower().replace(" ", "_")
    out_path = regions_dir / f"missing_region_mappings_{country_tag}.csv"
    pd.DataFrame(unmapped, columns=['city_name']).to_csv(out_path, index=False, sep=";")

    logger.info(f"Saved missing mapping list to: {out_path}")

    return unmapped


def export_db_schema(
    db_tables: dict, config: configparser.ConfigParser, logger: logging.Logger | None = None) -> None:
    """
    Export the schema of database tables (column names and dtypes) to a text file.
    The output is intended for LLM consumption to improve prompting.
    """

    # --- Output folder ---
    try:
        output_dir = Path(config["paths"]["output_dir"])
    except KeyError:
        output_dir = Path("./output")
        if logger:
            logger.warning("Missing [paths]->output_dir in config.ini. Using './output'.")

    # Ensure directory exists
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "db_schema.txt"

    if logger:
        logger.info("Starting schema export...")

    with open(output_file, "w") as f:

        f.write(f"Nationality: {config.nationality}\n")
        f.write(f"Country: {config.country}\n\n")

        for table_name, df in db_tables.items():
            f.write(f"### {table_name}\n")

            for col, dtype in df.dtypes.items():
                f.write(f"{col}: {dtype}\n")

            f.write("\n")

    if logger:
        logger.info(f"Schema successfully written to {output_file}")
    


def export_data(results: dict, figures: dict | None, section_name: str, config: configparser.ConfigParser, logger: logging.Logger | None = None) -> None:
    """
    Export module results (one CSV per entry + optional figures) under a
    country-scoped output directory:

        <output_dir>/<country>/<section_name>/<entry_name>_<timestamp>.csv
        <output_dir>/<country>/<section_name>/<figures_subfolder>/<fig>_<timestamp>.png

    The country folder is derived from config.country (lowercased, spaces
    replaced with underscores). This keeps results from different countries
    cleanly separated when the pipeline is run for multiple configurations.

    Each entry in `results` becomes its own CSV file (separator=';'). This
    avoids Excel's 31-char sheet name limit and the impracticality of
    producing dozens/hundreds of sheets in a single workbook.
    """

    # --- Timestamp ---
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # --- Output folder (country-scoped) ---
    try:
        base_output_dir = Path(config["paths"]["output_dir"])
    except KeyError:
        base_output_dir = Path("./output")
        if logger:
            logger.warning("Missing [paths]->output_dir in config.ini. Using './output'.")

    country_tag = config.country.lower().replace(" ", "_")
    module_dir = base_output_dir / country_tag / section_name
    module_dir.mkdir(parents=True, exist_ok=True)

    # --- CSVs (one per entry) ---
    try:
        template = config["output"].get("csv_template", "{entry_name}_{timestamp}.csv")
    except KeyError:
        template = "{entry_name}_{timestamp}.csv"

    for entry_name, df in results.items():
        if df is None:
            continue
        # Sanitize file name (avoid path separators / spaces)
        safe_name = entry_name.replace("/", "_").replace("\\", "_").replace(" ", "_")
        csv_file = module_dir / template.format(entry_name=safe_name, timestamp=timestamp)
        df.to_csv(csv_file, index=False, sep=";", encoding="utf-8")
        if logger:
            logger.info(f"{section_name} | wrote CSV: {csv_file.resolve()}")

    # --- Figures ---
    if figures:
        try:
            figures_sub = config["output"]["figures_subfolder"]
        except KeyError:
            figures_sub = "figures"

        fig_dir = module_dir / figures_sub
        fig_dir.mkdir(exist_ok=True)

        for fig_name, fig in figures.items():
            if fig is None:
                continue
            fig_path = fig_dir / f"{fig_name}_{timestamp}.png"
            fig.savefig(fig_path)
            plt.close(fig)
            if logger:
                logger.info(f"Figure saved to {fig_path.resolve()}")


############ Functions ############


def drop_invalid_results(df: pd.DataFrame, cols: str | list[str] = "best") -> pd.DataFrame:
    """
    Mask WCA DNF/DNS/no-attempt sentinels as NaN in the given column(s) and
    drop rows where all listed columns became NaN.

    Use this when you want to keep rows for grouping/counting but exclude
    invalid times from aggregations (e.g. best-of-event, averages, rankings).
    For simple query-based filters prefer the idiomatic `.query("best > 0")`.

    Parameters
    ----------
    df : pd.DataFrame
    cols : str | list[str]
        Column(s) to sanitize. Default "best". Pass e.g. ["best", "average"]
        to drop rows invalid on both.

    Returns
    -------
    pd.DataFrame
        Copy with sentinels replaced by NaN in `cols` and rows entirely
        invalid on `cols` dropped.
    """
    if isinstance(cols, str):
        cols = [cols]

    out = df.copy()
    invalid = list(WCA_CONSTANTS['invalid_results'])
    out[cols] = out[cols].replace(invalid, np.nan)
    return out.dropna(subset=cols, how="all")


def truncate(num: float, n: int) -> float:
    """
    Truncate a number to a given number of decimal places.

    The WCA uses n=2 for all officially recorded times and 
    rounds for averages.

    Parameters
    ----------
    num : float
        The number to truncate.
    n : int
        The number of decimal places to keep.

    Returns
    -------
    float
        The truncated number, or NaN if the input is NaN.
    """
    if np.isnan(num):
        return np.nan

    integer = int(num * (10 ** n)) / (10 ** n)
    return float(integer)


def timeconvert(x: float) -> str:
    """
    Convert a WCA time in centiseconds to a human-readable string.

    Converts a time value (in centiseconds) to the format:
    - 'SS.CC' if under one minute
    - 'M:SS.CC' if one minute or more

    Parameters
    ----------
    x : float
        The time value in centiseconds.

    Returns
    -------
    str
        The formatted time string.
    """
    if x < 6000:
        return f"{x / 100:.2f}"
    else:
        a = x % 6000
        if a < 1000:
            return f"{int(x / 6000)}:0{a / 100:.2f}"
        return f"{int(x / 6000)}:{a / 100:.2f}"


def multisolved(x, logger=None):

    if x <= 0:
        return np.nan

    try:
        if pd.isna(x):
            return np.nan
        x = int(x)
        DD = (x // 10_000_000) % 100
        MM = x % 100
        difference = 99 - DD
        solved = difference + MM
        return float(solved)
    except Exception as e:
        if logger:
            logger.warning(f"multisolved() failed for {x}: {e}")
        return np.nan


def multiwrong(x, logger=None):

    if x <= 0:
        return np.nan

    try:
        if pd.isna(x):
            return np.nan
        x = int(x)
        return float(x % 100)
    except Exception as e:
        if logger:
            logger.warning(f"multiwrong() failed for {x}: {e}")
        return np.nan


def multiattempted(x, logger=None):

    if x <= 0:
        return np.nan
    
    try:
        if pd.isna(x):
            return np.nan
        x = int(x)
        DD = (x // 10_000_000) % 100
        MM = x % 100
        difference = 99 - DD
        attempted = difference + 2 * MM
        return float(attempted)
    except Exception as e:
        if logger:
            logger.warning(f"multiattempted() failed for {x}: {e}")
        return np.nan


def multitime(x, logger=None):

    if x <= 0:
        return np.nan

    try:
        if pd.isna(x):
            return np.nan
        x = int(x)
        TT = (x // 100) % 100_000
        # 99999 means "unknown"
        return np.nan if TT == 99_999 else float(TT*100) #restituisce tempo in centisecondi per uniformità
    except Exception as e:
        if logger:
            logger.warning(f"multitime() failed for {x}: {e}")
        return np.nan


def format_result(value, event_id: str, logger=None) -> str:
    """
    Format a raw WCA result value according to event conventions:
      - 333fm        → moves count, e.g. '24' (single) or '24.33' (average, stored *100)
      - 333mbf       → encoded multi result, e.g. '47/50 37:15'
      - everything   → time, e.g. '1:15.23'

    Note: For FMC averages the WCA stores them *100; this helper assumes
    `value` follows that convention. Pass single-attempt FMC values as-is.
    """
    try:
        if pd.isna(value):
            return ""
        if event_id == "333mbf":
            return multiresult(value, logger)
        if event_id == "333fm":
            # Heuristic: FMC averages are stored *100 (e.g. 2433 → 24.33).
            # Single attempts are integers <= ~80.
            if value > 200:
                return f"{value / 100:.2f}"
            return str(int(value))
        return timeconvert(float(value))
    except Exception as e:
        if logger:
            logger.warning(f"format_result() failed for value={value}, event={event_id}: {e}")
        return str(value)


def multiresult(x, logger=None):
    """Return string like '47/50 37:15' or '47/50 (unknown)'."""
    try:
        s = int(multisolved(x, logger))
        a = int(multiattempted(x, logger))
        t = multitime(x, logger)
        if pd.isna(t) | pd.isna(s) | pd.isna(a):
            t_str = ''
        else:
            t_str = timeconvert(t)
        return f"{s}/{a} {t_str}"
    except Exception as e:
        if logger:
            logger.warning(f"multiresult() failed for {x}: {e}")
        return ""
