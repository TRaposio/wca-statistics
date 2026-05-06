⚠️ **Work in Progress**  

This project is currently under development. Most analysis modules are not yet implemented, but the **SQL queries** contained in the `sql/` folder are fully ready to use against the database.

Setup (conda — recommended for geopandas)
    conda env create -f environment.yml
    conda activate wca-stats
    pip install -e .

Setup (pip — works if geopandas installs cleanly on your system)
    python -m venv .venv
    source .venv/bin/activate
    pip install -e ".[dev]"

## Repository Structure

- `modules/` - Python modules for different kinds of statistics (in progress)
- `data/database_export/` - TSV exports from the WCA
- `output/` - Module outputs (Excel files and figures)
- `logs/` - Automatically generated logs
- `sql/` - Ready-to-use SQL queries
- `misc/` - Input figures used in the README or reports