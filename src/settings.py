# Import standard library modules
import os
from pathlib import Path

# The ROOT_DIR should represent the absolute path of the project root folder
ROOT_DIR = Path(__file__).absolute().parent.parent
DATA_DIR = ROOT_DIR / "data"
SRC_DIR = ROOT_DIR / "src"
CONFIG_DIR = ROOT_DIR / "src/config"
PROJ_CRS = "EPSG:4326"

# GCS ID and BQ Dataset IDs
GCP_PROJ_ID = "clearwind"  # Replace with project ID

# data subdirectories
CSV_DATA_DIR = DATA_DIR / "csv"
CARBON_POOLS_OUTDIR = CSV_DATA_DIR / "carbon_pools_inventory"
CARBON_STOCK_OUTDIR = CSV_DATA_DIR / "carbon_stocks"

for data_dir in [CSV_DATA_DIR, CARBON_POOLS_OUTDIR, CARBON_STOCK_OUTDIR]:
    data_dir.mkdir(exist_ok=True)
