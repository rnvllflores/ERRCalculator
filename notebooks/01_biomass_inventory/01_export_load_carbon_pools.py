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
# # Preprocess ODK data to organized tables

# %% [markdown]
# This script downloads the biomass inventory data collected on the ground using ODK and processes it to extract individual measurements for each carbon pool, per plot

# %% [markdown]
# # Imports and Set-up

# %%
# Standard Imports
import sys
import urllib.request
import pandas as pd
import numpy as np
from math import atan

# geospatial imports
import geopandas as gpd

# Google Cloud Imports
import pandas_gbq

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import DATA_DIR, GCP_PROJ_ID, CARBON_POOLS_OUTDIR
from src.odk_data_parsing import (
    extract_trees,
    extract_stumps,
    extract_dead_trees_class1,
    extract_dead_trees_class2s,
    extract_dead_trees_class2t,
    extract_ldw_with_hollow,
    extract_ldw_wo_hollow,
)

# %%
# Variables
URL = "https://api.ona.io/api/v1/data/763932.csv"
FILE_RAW = DATA_DIR / "csv" / "biomass_inventory_raw.csv"
NESTS = [2, 3, 4]

# BigQuery Variables
DATASET_ID = "biomass_inventory"
IF_EXISTS = "replace"

# %% [markdown]
# ## Get Data from ONA

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
plot_types = {"primary": 1, "backup": 2}

# %%
# Create a new column with "1" for Primary and "2" for Backup
data["plot_type_short"] = data["plot_info/plot_type"].replace(plot_types)

# Extract subplot letters (assuming they are included in the 'plot_info.sub_plot' column)
data["subplot_letter"] = data["plot_info/sub_plot"].str.replace("sub_plot", "")

# Create the unique ID by concatenating the specified columns
data["unique_id"] = (
    data["plot_info/plot_code_nmbr"].astype(str)
    + data["subplot_letter"]
    + data["plot_type_short"].astype(str)
)

# %% [markdown]
# ## Check for duplicate plot IDs

# %%
data[
    data.unique_id.isin(
        data.loc[data.duplicated(subset="unique_id"), "unique_id"].unique()
    )
].sort_values("unique_id")

# %% [markdown]
# ### Assign corrected plot IDs to each duplicate
# The subset of duplicates were manually inspected to correct the issue. The common source of the duplicates were typo errors in the plot ID or sublot letter, there were other instances that abandoned subplots persisted in the dataset

# %%
# load dataframe with manually annotated plot id corrections
plot_id_corrections = gpd.read_file(
    DATA_DIR / "gpkg" / "duplicate_plots_corrected.gpkg"
)

# %%
plot_id_corrections.head(2)

# %% vscode={"languageId": "markdown"}
# Drop rows where the geometry is empty, this indicates that these plots were abandoned
plot_id_corrections = plot_id_corrections[~plot_id_corrections.geometry.is_empty].copy()

# %%
plot_id_corrections = plot_id_corrections[
    plot_id_corrections["unique_id_updated"] != "flag"
]

# %%
plot_id_corrections.shape

# %%
# create a uuid to match duplicates from the original data
plot_id_corrections["uuid"] = (
    plot_id_corrections["unique_id"]
    + plot_id_corrections["slope"].astype(str)
    + plot_id_corrections["team_no"].astype(str)
)

# %%
plot_id_corrections.uuid.nunique()

# %%
# define a dictionary to map the updated unique_id to the uuid
uuid_dict = (
    plot_id_corrections[["unique_id_updated", "uuid"]]
    .set_index("uuid")
    .to_dict()["unique_id_updated"]
)

# %%
data["uuid"] = (
    data["unique_id"]
    + data["slope/slope"].astype(str)
    + data["plot_info/team_no"].astype(str)
)

# %%
data["unique_id_updated"] = data["uuid"].map(uuid_dict).fillna(np.nan)

# %%
# update the unique_id where it is necessary
data["unique_id"] = data["unique_id_updated"].fillna(data["unique_id"])

# %%
# check for remaining duplicates
duplicates = data[
    data.unique_id.isin(
        data.loc[data.duplicated(subset="unique_id"), "unique_id"].unique()
    )
].sort_values("unique_id")

# %%
# these are duplicate entries where both abandoned and active plots
# are present, drop those without an updated unique_id
duplicates

# %%
# save uuid to a list, do not drop here yet since it appears that these rows contain data
duplicates_drop = duplicates.loc[duplicates["unique_id_updated"].isna(), "uuid"]

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
    "uuid",
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
}

# %%
plot_info.rename(columns=plot_info_cols, inplace=True)

# %%
# drop duplicate plot id here since remaining duplicates
# have empty geometry
plot_info = plot_info[~plot_info["uuid"].isin(duplicates_drop)].copy()

# %%
plot_info.drop(columns=["uuid"], inplace=True)

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

# %% [markdown]
# ## Calculate corrected plot area

# %%
# Convert slope from percentage to radians
plot_info["slope_radians"] = plot_info["slope"].apply(lambda x: atan(x / 100))

# %%
# Calculate corrected radius based on slope (in radians)
corrected_radius_n2 = 5 / np.cos(plot_info["slope_radians"])
corrected_radius_n3 = 15 / np.cos(plot_info["slope_radians"])
corrected_radius_n4 = 20 / np.cos(plot_info["slope_radians"])

# %%
# Calculate new total subplot area based on corrected radius
plot_info["corrected_plot_area_n2_m2"] = np.pi * corrected_radius_n2**2
plot_info["corrected_plot_area_n3_m2"] = np.pi * corrected_radius_n3**2
plot_info["corrected_plot_area_n4_m2"] = np.pi * corrected_radius_n4**2

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
# # Saplings, Non tree vegetation and litter

# %%
cols = [
    "unique_id",
    "sapling_data/count_saplings",
    "ntv_data/litter_data/litter_bag_weight",
    "ntv_data/litter_data/litter_sample_weight",
    "ntv_data/ntv_bag_weight",
    "ntv_data/ntv_sample_weight",
    "slope/slope",
    "plot_info/team_no",
]

# %%
# rename columns
col_names = {
    "sapling_data/count_saplings": "count_saplings",
    "ntv_data/litter_data/litter_bag_weight": "litter_bag_weight",
    "ntv_data/litter_data/litter_sample_weight": "litter_sample_weight",
    "ntv_data/ntv_bag_weight": "ntv_bag_weight",
    "ntv_data/ntv_sample_weight": "ntv_sample_weight",
}

# %%
ntv = data[cols].copy()

# %%
ntv.rename(columns=col_names, inplace=True)

# %% [markdown]
# ## remove duplicates

# %%
ntv["uuid"] = (
    ntv["unique_id"]
    + ntv["slope/slope"].astype(str)
    + ntv["plot_info/team_no"].astype(str)
)

# %%
# drop duplicate plot id here since remaining duplicates
# have empty geometry
ntv = ntv[~ntv["uuid"].isin(duplicates_drop)].copy()

# %%
ntv[ntv.duplicated(subset="unique_id")]

# %%
ntv.drop(columns=["uuid", "slope/slope", "plot_info/team_no"], inplace=True)

# %%
ntv.info(), ntv.head(2)

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export CSV
if len(ntv) != 0:
    ntv.to_csv(CARBON_POOLS_OUTDIR / "saplings_ntv_litter.csv", index=False)

# %%
# Upload to BQ
if len(ntv) != 0:
    pandas_gbq.to_gbq(
        ntv,
        f"{DATASET_ID}.saplings_ntv_litter",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )

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
if not dead_trees_c1.empty:
    dead_trees_c1["class"] = 1
    dead_trees_c1["subclass"] = np.nan

# %%
dead_trees_c1.info(), dead_trees_c1.head(2)

# %%
dead_trees_c1.describe()

# %% [markdown]
# # Dead Trees: Class 2 - short

# %%
dead_trees_c2s = extract_dead_trees_class2s(data, NESTS)

# %%
dead_trees_c2s.info(), dead_trees_c2s.head(2)

# %%
if not dead_trees_c2s.empty:
    dead_trees_c2s["class"] = 2
    dead_trees_c2s["subclass"] = "short"

# %% [markdown]
# # Dead Trees: Class 2 - Tall

# %%

# %%
dead_trees_c2t = extract_dead_trees_class2t(data, NESTS)

# %%
dead_trees_c2t.info(), dead_trees_c2t.head(2)

# %%
if not dead_trees_c2t.empty:
    dead_trees_c2t["class"] = 2
    dead_trees_c2t["subclass"] = "tall"

# %% [markdown]
# # Combine into one table

# %%
dead_trees = pd.concat([dead_trees_c1, dead_trees_c2s, dead_trees_c2t])

# %%
dead_trees.info(), dead_trees.head(2)

# %% [markdown]
# ## Export data and upload to BQ

# %%
# Export CSV
if len(dead_trees) != 0:
    dead_trees.to_csv(CARBON_POOLS_OUTDIR / "dead_trees.csv", index=False)

# %%
# Upload to BQ
if len(dead_trees) != 0:
    pandas_gbq.to_gbq(
        dead_trees,
        f"{DATASET_ID}.dead_trees",
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
