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
#
# import os
# import re

# %%
# Standard Imports
import sys
import urllib.request

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Google Cloud Imports
import pandas_gbq

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.biomass_inventory import (
    extract_dead_trees_class1,
    extract_dead_trees_class2s,
    extract_dead_trees_class2t,
    extract_ldw_with_hollow,
    extract_ldw_wo_hollow,
    extract_stumps,
    extract_trees,
)
from src.settings import DATA_DIR, GCP_PROJ_ID

# %%
# Variables
URL = "https://api.ona.io/api/v1/data/763932.csv"
FILE_RAW = DATA_DIR / "csv" / "biomass_inventory_raw.csv"
CARBON_POOLS_OUTDIR = DATA_DIR / "csv" / "carbon_pools"
NESTS = [2, 3, 4]

# BigQuery Variables
DATASET_ID = "biomass_inventory"
IF_EXISTS = "replace"

# %%
# Create output directory
CARBON_POOLS_OUTDIR.mkdir(parents=True, exist_ok=True)

# %% [markdown]
# # Get Data from ONA

# %%
column_types = {
    col: str
    for col in (
        28,
        399,
        400,
        407,
        408,
        415,
        416,
        845,
        846,
        853,
        854,
        861,
        862,
        869,
        870,
        877,
        878,
        885,
        886,
        893,
        894,
        901,
        902,
        909,
        910,
        1179,
        1180,
        1187,
        1188,
        1195,
        1196,
        1203,
        1204,
        1211,
        1212,
        1219,
        1220,
        1286,
        1337,
        1342,
        1347,
        1352,
        1357,
        1362,
        1378,
        1392,
    )
}

# %%
if FILE_RAW.exists():
    data = pd.read_csv(FILE_RAW, dtype=column_types)
else:
    urllib.request.urlretrieve(URL, FILE_RAW)
    data = pd.read_csv(FILE_RAW, dtype=column_types)

# %% [markdown]
# ## Add a unique ID

# %%
# Create a new column with "1" for Primary and "2" for Backup
data["plot_type_short"] = data["plot_info/plot_type"].apply(
    lambda x: "1" if x == "Primary" else "2"
)

# Extract subplot letters (assuming they are included in the 'plot_info.sub_plot' column)
data["subplot_letter"] = data["plot_info/sub_plot"].str.replace("sub_plot", "")

# Create the unique ID by concatenating the specified columns
data["unique_id"] = (
    data["plot_info/plot_code_nmbr"].astype(str)
    + data["subplot_letter"]
    + data["plot_type_short"]
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
    "sapling_data/count_saplings",
    "ntv_data/litter_data/litter_bag_weight",
    "ntv_data/litter_data/litter_sample_weight",
    "ntv_data/ntv_bag_weight",
    "ntv_data/ntv_sample_weight",
]

# %%
plot_info = data[plot_info_cols].copy()

# %%
# rename columns
plot_info_cols = {
    "plot_info/data_recorder": "data_recorder",
    "plot_info/team_no": "team_no",
    "plot_info/plot_code_nmbr": "plot_code_nmbr",
    "plot_info/plot_type": "plot_type",
    "plot_info/sub_plot": "sub_plot",
    "plot_info/yes_no": "yes_no",
    "plot_shift/sub_plot_shift": "sub_plot_shift",
    "plot_GPS/GPS_waypt": "GPS_waypt",
    "plot_GPS/GPS_id": "GPS_id",
    "plot_GPS/GPS": "GPS",
    "plot_GPS/_GPS_latitude": "GPS_latitude",
    "plot_GPS/_GPS_longitude": "GPS_longitude",
    "plot_GPS/_GPS_altitude": "GPS_altitude",
    "plot_GPS/_GPS_precision": "GPS_precision",
    "plot_GPS/photo": "photo",
    "access/access_reason/slope": "access_reason_slope",
    "access/access_reason/danger": "access_reason_danger",
    "access/access_reason/distance": "access_reason_distance",
    "access/access_reason/water": "access_reason_water",
    "access/access_reason/prohibited": "access_reason_prohibited",
    "access/access_reason/other": "access_reason_other",
    "access/manual_reason": "manual_reason",
    "lc_data/lc_type": "lc_type",
    "lc_class/lc_class": "lc_class",
    "lc_class/lc_class_other": "lc_class_other",
    "disturbance/disturbance_yesno": "disturbance_yesno",
    "disturbance_data/disturbance_type": "disturbance_type",
    "disturbance_class/disturbance_class": "disturbance_class",
    "slope/slope": "slope",
    "canopy/avg_height": "canopy_avg_height",
    "canopy/can_cov": "canopy_cover",
    "sapling_data/count_saplings": "count_saplings",
    "ntv_data/litter_data/litter_bag_weight": "litter_bag_weight",
    "ntv_data/litter_data/litter_sample_weight": "litter_sample_weight",
    "ntv_data/ntv_bag_weight": "ntv_bag_weight",
    "ntv_data/ntv_sample_weight": "ntv_sample_weight",
}

# %%
plot_info.rename(columns=plot_info_cols, inplace=True)

# %% [markdown]
# ### Set correct data types

# %%
column_types = {
    "unique_id": str,
    "data_recorder": str,
    "team_no": int,
    "plot_code_nmbr": int,
    "plot_type": str,
    "sub_plot": str,
    "yes_no": str,
    "sub_plot_shift": str,
    "GPS": str,
    "photo": str,
    "access_reason_slope": str,
    "access_reason_danger": str,
    "access_reason_distance": str,
    "access_reason_water": str,
    "access_reason_prohibited": str,
    "access_reason_other": str,
    "manual_reason": str,
    "lc_type": str,
    "lc_class": str,
    "lc_class_other": str,
    "disturbance_yesno": str,
    "disturbance_type": str,
    "disturbance_class": str,
}

# %%
plot_info = plot_info.astype(column_types)

# %% [markdown]
# ### Compress access reasons to one column

# %%
plot_info["access_reason"] = np.nan
plot_info["access_reason"] = plot_info["access_reason"].astype(str)
for index, row in plot_info.iterrows():
    if row["access_reason_slope"] == "True":
        plot_info.loc[index, "access_reason"] = "slope"
    elif row["access_reason_danger"] == "True":
        plot_info.loc[index, "access_reason"] = "danger"
    elif row["access_reason_distance"] == "True":
        plot_info.loc[index, "access_reason"] = "distance"
    elif row["access_reason_water"] == "True":
        plot_info.loc[index, "access_reason"] = "water"
    elif row["access_reason_prohibited"] == "True":
        plot_info.loc[index, "access_reason"] = "prohibited"
    elif row["access_reason_other"] == "True":
        plot_info.loc[index, "access_reason"] = row["manual_reason"]

# %%
# Categorize manual reasons
plot_info.loc[plot_info.access_reason == "90 degree slope ", "access_reason"] = "slope"
plot_info.loc[
    plot_info.access_reason == "Slippery due to rainfall and sharp stones..too risky.",
    "access_reason",
] = "danger"
plot_info.loc[
    plot_info.access_reason == "Creek plot and slope 90 degree", "access_reason"
] = "slope"
plot_info.loc[
    plot_info.access_reason == "Near creek 90 degree slope", "access_reason"
] = "slope"

# %%
# drop access_reason columns
plot_info.drop(
    columns=[
        "access_reason_slope",
        "access_reason_danger",
        "access_reason_distance",
        "access_reason_water",
        "access_reason_prohibited",
        "access_reason_other",
    ],
    inplace=True,
)

# %%
plot_info.access_reason.value_counts()

# %%
plot_info.info(), plot_info.head(2)

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export CSV
if len(plot_info) != 0:
    plot_info.to_csv(CARBON_POOLS_OUTDIR / "plot_info.csv", index=False)

# %%
# Upload to BQ
if len(plot_info) != 0:
    pandas_gbq.to_gbq(
        plot_info,
        f"{DATASET_ID}.plot_info",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )

# %% [markdown]
# # Extract info per carbon pool

# %% [markdown]
# # Living Trees

# %%
trees = extract_trees(data, NESTS)

# %%
trees.info(), trees.head(2)

# %%
trees.describe()

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export to CSV
trees.to_csv(CARBON_POOLS_OUTDIR / "trees.csv", index=False)

# %%
# Upload to BQ
pandas_gbq.to_gbq(
    trees, f"{DATASET_ID}.trees", project_id=GCP_PROJ_ID, if_exists=IF_EXISTS
)

# %% [markdown]
# # Tree Stumps

# %% [markdown]
# [delete when fixed] Note: removed `'biomass_per_kg_tree': [biomass_per_kg_tree],`. In the original code there was a placeholder column created, this can be added later in the process when biomass per tree is actually calculated

# %%
stumps = extract_stumps(data, NESTS)

# %%
stumps.info(), stumps.head(2)

# %%
stumps.describe()

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export to CSV
stumps.to_csv(CARBON_POOLS_OUTDIR / "stumps.csv", index=False)

# %%
# Upload to BQ
pandas_gbq.to_gbq(
    stumps, f"{DATASET_ID}.stumps", project_id=GCP_PROJ_ID, if_exists=IF_EXISTS
)

# %% [markdown]
# # Dead Trees: Class 1

# %%
dead_trees_c1 = extract_dead_trees_class1(data, NESTS)

# %%
dead_trees_c1.info(), dead_trees_c1.head(2)

# %%
dead_trees_c1.describe()

# %% [markdown]
# ## Export data and upload to BQ

# %%
dead_trees_c1.to_csv(CARBON_POOLS_OUTDIR / "dead_trees_class1.csv", index=False)

# %%
# Upload to BQ
pandas_gbq.to_gbq(
    dead_trees_c1,
    f"{DATASET_ID}.dead_trees_c1",
    project_id=GCP_PROJ_ID,
    if_exists=IF_EXISTS,
)

# %% [markdown]
# # Dead Trees: Class 2 - short

# %%
dead_trees_c2s = extract_dead_trees_class2s(data, NESTS)

# %%
dead_trees_c2s.info(), dead_trees_c2s.head(2)

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export CSV
if len(dead_trees_c2s) != 0:
    dead_trees_c2s.to_csv(CARBON_POOLS_OUTDIR / "dead_trees_class2.csv", index=False)

# %%
# Upload to BQ
if len(dead_trees_c2s) != 0:
    pandas_gbq.to_gbq(
        dead_trees_c2s,
        f"{DATASET_ID}.dead_trees_c2s",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )

# %% [markdown]
# # Dead Trees: Class 2 - Tall

# %% [markdown]
# [delete when fixed] Note: This is still class 2 but tall trees. rename variables and functions accordingly

# %%
dead_trees_c2t = extract_dead_trees_class2t(data, NESTS)

# %%
dead_trees_c2t.info(), dead_trees_c2t.head(2)

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export CSV
if len(dead_trees_c2t) != 0:
    dead_trees_c2t.to_csv(
        CARBON_POOLS_OUTDIR / "dead_trees_class2_tall.csv", index=False
    )

# %%
# Upload to BQ
if len(dead_trees_c2t) != 0:
    pandas_gbq.to_gbq(
        dead_trees_c2t,
        f"{DATASET_ID}.dead_trees_c2t",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )

# %% [markdown]
# # Lying Deadwood: Hollow

# %%
ldw_hollow = extract_ldw_with_hollow(data)

# %%
ldw_hollow.info(), ldw_hollow.head(2)

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export CSV
if len(ldw_hollow) != 0:
    ldw_hollow.to_csv(CARBON_POOLS_OUTDIR / "lying_deadwood_hollow.csv", index=False)

# %%
# Upload to BQ
if len(ldw_hollow) != 0:
    pandas_gbq.to_gbq(
        ldw_hollow,
        f"{DATASET_ID}.lying_deadwood_hollow",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )

# %% [markdown]
# # Lying Deadwood without hollow

# %%
ldw_wo_hollow = extract_ldw_wo_hollow(data)

# %%
ldw_wo_hollow.info(), ldw_wo_hollow.head(2)

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export CSV
if len(ldw_wo_hollow) != 0:
    ldw_wo_hollow.to_csv(
        CARBON_POOLS_OUTDIR / "lying_deadwood_wo_hollow.csv", index=False
    )

# %%
# Upload to BQ
if len(ldw_wo_hollow) != 0:
    pandas_gbq.to_gbq(
        ldw_wo_hollow,
        f"{DATASET_ID}.lying_deadwood_wo_hollow",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )
