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
GEOJSON_DATA_DIR = DATA_DIR / "geojson"
GPKG_DATA_DIR = DATA_DIR / "gpkg"
PARQUET_DATA_DIR = DATA_DIR / "parquet"
SHP_DATA_DIR = DATA_DIR / "shp"
TMP_OUT_DIR = DATA_DIR / "tmp"
CARBON_POOLS_OUTDIR = CSV_DATA_DIR / "biomass_inventory"
CARBON_STOCK_OUTDIR = CSV_DATA_DIR / "carbon_stock"

# lookup tables
SPECIES_LOOKUP_CSV = SRC_DIR / "lookup" / "species_lookup.csv"
PC_PLOT_LOOKUP_CSV = SRC_DIR / "lookup" / "pc_plot_lookup_20240802.csv"

for data_dir in [CSV_DATA_DIR, CARBON_POOLS_OUTDIR, CARBON_STOCK_OUTDIR, GEOJSON_DATA_DIR, GPKG_DATA_DIR, PARQUET_DATA_DIR, SHP_DATA_DIR, TMP_OUT_DIR]:
    data_dir.mkdir(exist_ok=True)
