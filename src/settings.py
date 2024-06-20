# Import standard library modules
from pathlib import Path

# The ROOT_DIR should represent the absolute path of the project root folder
ROOT_DIR = Path(__file__).absolute().parent.parent
DATA_DIR = ROOT_DIR / "data"
SRC_DIR = ROOT_DIR / "src"
CONFIG_DIR = ROOT_DIR / "src/config"
PROJ_CRS = "EPSG:4326"

# GCS ID and BQ Dataset IDs
GCP_PROJ_ID = "geo-retail-data-mart"  # Replace with project ID
ADM_BQ_ID = GCP_PROJ_ID + ".02_adm_bounds.adm4"
GRIDS_300m_BQ_ID = (
    GCP_PROJ_ID + ".01_aoi.grids_wadm_zoom17_300m"
)  # Replace resolution, if not using bing tiles or h3 remove zoom level from name
