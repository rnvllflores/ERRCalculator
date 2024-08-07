# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.16.0
#   kernelspec:
#     display_name: onebase
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Extract data from GCS and upload to BigQuery

# %%
# Standard Imports
import sys
import os
import re
from tqdm import tqdm
import pandas as pd
import geopandas as gpd

# Google Cloud Imports

# %%
# Util imports
sys.path.append("../../../")  # include parent directory
from src.settings import GEOJSON_DATA_DIR

# %%
# Variables
SILUP_DIR = GEOJSON_DATA_DIR / "SILUP"

# GCS Variables
SILUP_GCS_DIR = "gs://silup-gis/onebase/"

# BigQuery Variables
SRC_DATASET_ID = "biomass_inventory"
DATASET_ID = "carbon_stock"
IF_EXISTS = "replace"

# %% [markdown]
# ## Downlaod data

# %%
SILUP_DIR.mkdir(exist_ok=True)

# %%
# !gsutil -m cp $SILUP_GCS_DIR"*.geojson" $SILUP_DIR

# %% [markdown]
# # Combine Separate SILUP and Format data

# %%
file_list = os.listdir(SILUP_DIR)

# %%
silup_gdf = []

for filename in tqdm(file_list):
    data = gpd.read_file(SILUP_DIR / filename)

    # Extract CADT number
    cadt_num = re.findall(r"CADT (\d+)", filename)[0]

    data = data[["ELI_TYPE", "geometry"]].copy()

    # Extract version
    if "v0" in file_list[2]:
        version = "v0"
    else:
        version = "final"

    data["cadt_num"] = cadt_num
    data["version"] = version
    silup_gdf.append(data)

# %%
combined_gdf = pd.concat(silup_gdf)

# %%
combined_gdf
