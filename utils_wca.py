from pathlib import Path
import configparser
import urllib.request
import zipfile
import datetime
import pandas as pd
import logging
import sys
from datetime import datetime
import inspect
import numpy as np


############ LOGGER ############


def setup_logger(name: str, level=logging.INFO, log_root: Path | str = "./logs") -> logging.Logger:
    """
    Set up a logger that writes to both console and file.

    Parameters
    ----------
    name : str
        Usually __name__ of the calling module.
    level : int
        Logging level (default: logging.INFO)
    log_root : Path | str
        Root folder for all log files (default: ./logs)

    Returns
    -------
    logging.Logger
        Configured logger instance
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # If already configured, return the same logger
    if logger.hasHandlers():
        return logger

    # --- Formatter ---
    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- Console handler ---
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # --- File handler ---
    log_root = Path(log_root)
    date_subfolder = datetime.now().strftime("%Y-%m-%d")
    log_dir = log_root / date_subfolder
    log_dir.mkdir(parents=True, exist_ok=True)

    # Identify caller script (e.g. "main.py" → "main")
    caller_filename = Path(inspect.stack()[1].filename).stem

    # Unique timestamp for this run
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    log_filename = f"{caller_filename}_{timestamp}.log"
    log_path = log_dir / log_filename

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"Logger initialized. Writing logs to {log_path.resolve()}")

    return logger


############ CONFIG ############


def load_config(logger: logging.Logger, config_path: str | Path = "config.ini") -> configparser.ConfigParser:
    """
    Import the config file.
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


def update_data(config: configparser.ConfigParser, force: bool = False, logger: logging.Logger | None = None) -> None:
    """
    Download the latest WCA export if not already downloaded today.
    Uses logger if provided.
    """
    db_dir = get_database_dir(config)
    meta_file = db_dir / "last_update.txt"
    url = config["url"]["wca_export_url"]

    today_str = datetime.now().date().isoformat()

    if logger is None:
        import logging
        logger = logging.getLogger(__name__)

    # Check if already updated today
    if not force and meta_file.exists():
        last_update = meta_file.read_text().strip()
        if last_update == today_str:
            logger.info(f"Data already up to date (last update: {last_update})")
            return

    logger.info(f"Downloading new WCA export from {url} ...")
    zip_path, _ = urllib.request.urlretrieve(url)
    with zipfile.ZipFile(zip_path, "r") as f:
        f.extractall(db_dir)

    meta_file.write_text(today_str)
    logger.info(f"WCA data updated successfully on {today_str} in {db_dir}")


def read_table(table_name: str, config: configparser.ConfigParser, logger: logging.Logger | None = None) -> pd.DataFrame:
    """
    Reads a WCA table (.tsv) based on config mappings.
    Logs messages if logger is provided.
    """
    db_dir = get_database_dir(config)

    if logger is None:
        import logging
        logger = logging.getLogger(__name__)

    if not config.has_section("tables"):
        raise KeyError("Missing [tables] section in config.ini")

    tables_map = dict(config.items("tables"))
    if table_name not in tables_map:
        raise KeyError(f"Table '{table_name}' not found in [tables] section of config.ini")

    file_name = tables_map[table_name]
    file_path = db_dir / file_name

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}. You may need to run update_data().")

    df = pd.read_csv(file_path, sep="\t", low_memory=False)
    logger.info(f"Loaded '{table_name}' from {file_path.name} ({len(df):,} rows)")
    return df


from pathlib import Path
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt

def export_data(results: dict, figures: dict | None, section_name: str, config, logger=None) -> dict:
    """
    Export module results: Excel + optional figures.

    Parameters
    ----------
    results : dict
        Dictionary {sheet_name: DataFrame} for Excel sheets.
    figures : dict, optional
        Dictionary {figure_name: matplotlib.figure.Figure} to save.
    section_name : str
        Name of the module/section.
    config : ConfigParser
        Loaded config.ini object.
    logger : logging.Logger, optional
        Logger for messages.

    """

    # --- Timestamp ---
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    # --- Output folder ---
    try:
        base_output_dir = Path(config["paths"]["output_dir"])
    except KeyError:
        base_output_dir = Path("./output")
        if logger:
            logger.warning("Missing [paths]->output_dir in config.ini. Using './output'.")

    module_dir = base_output_dir / section_name
    module_dir.mkdir(parents=True, exist_ok=True)

    # --- Excel ---
    try:
        template = config["output"].get("excel_template", "{section_name}_{timestamp}.xlsx")
    except KeyError:
        template = "{section_name}_{timestamp}.xlsx"

    excel_file = module_dir / template.format(section_name=section_name, timestamp=timestamp)
    with pd.ExcelWriter(excel_file, engine="openpyxl") as writer:
        for sheet_name, df in results.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    if logger:
        logger.info(f"{section_name} results exported to Excel: {excel_file.resolve()}")

    # --- Figures ---
    figure_paths = []
    if figures:
        try:
            figures_sub = config["output"]["figures_subfolder"]
        except KeyError:
            figures_sub = "figures"

        fig_dir = module_dir / figures_sub
        fig_dir.mkdir(exist_ok=True)

        for fig_name, fig in figures.items():
            fig_path = fig_dir / f"{fig_name}_{timestamp}.png"
            fig.savefig(fig_path)
            plt.close(fig)
            figure_paths.append(fig_path)
            if logger:
                logger.info(f"Figure saved to {fig_path.resolve()}")


############ Functions ############


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
