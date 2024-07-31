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
sys.path.append("../../")  # include parent directory
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
file_list

# %%
columns_to_check = ["BAU_0"]

# %%
silup_gdf = []

for filename in tqdm(file_list):
    data = gpd.read_file(SILUP_DIR / filename)

    # Extract CADT number
    cadt_num = re.findall(r"CADT (\d+)", filename)[0]

    # Extract version
    if "v0" in file_list[2]:
        version = "v0"
    else:
        version = "final"

    for col in columns_to_check:
        if "BAU" in col:
            data.rename(columns={col: "BAU"}, inplace=True)
        if "PLAN" in col:
            data.rename(columns={col: "PLAN"}, inplace=True)

    data.reset_index(drop=True, inplace=True)
    data["cadt_num"] = cadt_num
    data["version"] = version
    silup_gdf.append(data)

# %%
for df in silup_gdf:
    print(df.shape)

# %%
silup_gdf[0].columns

# %%
silup_gdf[2].columns

# %%
silup_gdf[3].columns

# %%
file_list[3]

# %%
silup_gdf[3]

# %%
combined_gdf = pd.concat(silup_gdf)

# %%
combined_gdf.columns

# %%
filename = file_list[1]
number = re.findall(r"CADT (\d+)", filename)[0]
print(number)

# %%
if "v0" in file_list[2]:
    print("The filename contains 'v0'")
else:
    print("The filename does not contain 'v0'")

# %%
test = gpd.read_file(SILUP_DIR / "CADT 002.geojson")

# %%
test.info()

# %%
test2 = gpd.read_file(SILUP_DIR / file_list[3])

# %%
test2.info()

# %%
test.explore
