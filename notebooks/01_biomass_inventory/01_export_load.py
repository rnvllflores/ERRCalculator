# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.1
#   kernelspec:
#     display_name: onebase
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Preprocess ODK data to organized tables

# %% [markdown]
# # Imports and Set-up

# %%
import sys
import urllib.request

import pandas as pd

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.biomass_inventory import extract_trees
from src.settings import DATA_DIR

# %%
# Variables
URL = "https://api.ona.io/api/v1/data/763932.csv"
FILE_RAW = DATA_DIR / "csv" / "biomass_inventory_raw.csv"

# %% [markdown]
# # Get Data from ONA

# %%
if FILE_RAW.exists():
    data = pd.read_csv(FILE_RAW, low_memory=False)
else:
    urllib.request.urlretrieve(URL, FILE_RAW)
    data = pd.read_csv(FILE_RAW, low_memory=False)

# %%
data.head(2)

# %% [markdown]
# ## Add a unique ID

# %%
# Create a new column with "1" for Primary and "2" for Backup
data["plot_info/plot_type_short"] = data["plot_info/plot_type"].apply(
    lambda x: "1" if x == "Primary" else "2"
)

# Extract subplot letters (assuming they are included in the 'plot_info.sub_plot' column)
data["subplot_letter"] = data["plot_info/sub_plot"].str.replace("sub_plot", "")

# Create the unique ID by concatenating the specified columns
data["unique_id"] = (
    data["plot_info/plot_code_nmbr"].astype(str)
    + data["subplot_letter"]
    + data["plot_info/plot_type_short"]
)

# %% [markdown]
# # Extract Plot info

# %%
plot_info_cols = [
    "unique_id",
    "plot_info/data_recorder",
    "plot_info/team_no",
    "plot_info/plot_code_nmbr",
    "plot_info/plot_type",
    "plot_info/sub_plot",
    "plot_info/yes_no",
    "plot_shift/sub_plot_shift",
    "plot_GPS/GPS_waypt",
    "plot_GPS/GPS_id",
    "plot_GPS/GPS",
    "plot_GPS/_GPS_latitude",
    "plot_GPS/_GPS_longitude",
    "plot_GPS/_GPS_altitude",
    "plot_GPS/_GPS_precision",
    "plot_GPS/photo",
    "access/access_reason/slope",
    "access/access_reason/danger",
    "access/access_reason/distance",
    "access/access_reason/water",
    "access/access_reason/prohibited",
    "access/access_reason/other",
    "access/manual_reason",
    "lc_data/lc_type",
    "lc_class/lc_class",
    "lc_class/lc_class_other",
    "disturbance/disturbance_yesno",
    "disturbance_data/disturbance_type",
    "disturbance_class/disturbance_class",
    "slope/slope",
    "canopy/avg_height",
    "canopy/can_cov",
]

# %%
plot_info = data[plot_info_cols]

# %%
plot_info.head(2)

# %% [markdown]
# # Extract info per carbon pool

# %% [markdown]
# ## Living Trees

# %%
extract_trees(data, [2, 3, 4])

# %%
