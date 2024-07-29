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
# # Calculate emission factors and confidence intervals

# %% [markdown]
# # Imports and Set-up

# %%
# Standard Imports
import sys
import pandas as pd

# Google Cloud Imports
import pandas_gbq

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import (
    GCP_PROJ_ID,
    CARBON_STOCK_OUTDIR,
    CARBON_POOLS_OUTDIR,
    PC_PLOT_LOOKUP_CSV,
)

from src.biomass_equations import calculate_statistics

# %%
import datetime

# Variables
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"
LITTER_CSV = CARBON_STOCK_OUTDIR / "litter_carbon_stock.csv"
NTV_CSV = CARBON_STOCK_OUTDIR / "ntv_carbon_stock.csv"
DEADWOOD_CSV = CARBON_STOCK_OUTDIR / "deadwood_carbon_stock.csv"
TREES_CSV = CARBON_STOCK_OUTDIR / "trees_carbon_stock.csv"
SAPLINGS_CSV = CARBON_POOLS_OUTDIR / "saplings_carbon_stock.csv"

# Version Control
today = datetime.date.today()
VERSION = today.strftime("%Y%m%d")

# BigQuery Variables
SRC_DATASET_ID = "biomass_inventory"
DATASET_ID = "carbon_stock"
IF_EXISTS = "replace"

# %% [markdown]
# ## Load data

# %% [markdown]
# ### Plot Data

# %%
if PLOT_INFO_CSV.exists():
    plot_info = pd.read_csv(PLOT_INFO_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.plot_info"""

    # Read the BigQuery table into a dataframe
    plot_info = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    plot_info.to_csv(PLOT_INFO_CSV, index=False)

# %%
plot_info.info()

# %%
if PC_PLOT_LOOKUP_CSV.exists():
    plot_info_lookup = pd.read_csv(PC_PLOT_LOOKUP_CSV)
else:
    print("PC Plot Lookup CSV not found")

# %%
plot_info_lookup.info()

# %%
plot_info = pd.merge(plot_info, plot_info_lookup, on="unique_id", how="left")

# %% [markdown]
# ### Trees

# %%
if TREES_CSV.exists():
    trees = pd.read_csv(TREES_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.trees_carbon_stock"""

    # Read the BigQuery table into a dataframe
    trees = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    trees.to_csv(TREES_CSV, index=False)

# %%
trees.info()

# %% [markdown]
# ### Deadwood

# %%
if DEADWOOD_CSV.exists():
    deadwood = pd.read_csv(DEADWOOD_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.deadwood_carbon_stock"""

    # Read the BigQuery table into a dataframe
    deadwood = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    deadwood.to_csv(DEADWOOD_CSV, index=False)

# %%
deadwood.info()

# %% [markdown]
# ### Litter

# %%
if LITTER_CSV.exists():
    litter = pd.read_csv(LITTER_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.litter_carbon_stock"""

    # Read the BigQuery table into a dataframe
    litter = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    litter.to_csv(LITTER_CSV, index=False)

# %%
litter.info()

# %% [markdown]
# ### Non-tree Vegetation

# %%
if NTV_CSV.exists():
    ntv = pd.read_csv(NTV_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.ntv_carbon_stock"""

    # Read the BigQuery table into a dataframe
    ntv = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    ntv.to_csv(NTV_CSV, index=False)

# %%
ntv.info()

# %% [markdown]
# # Create plot level summary

# %%
merged_df = plot_info[["unique_id", "plot_code_nmbr", "Strata"]].merge(
    trees, on="unique_id", how="left"
)
merged_df = merged_df.merge(deadwood, on="unique_id", how="left")
merged_df = merged_df.merge(ntv, on="unique_id", how="left")
merged_df = merged_df.merge(litter, on="unique_id", how="left")

# %%
merged_df.info()

# %%
merged_df.head(2)

# %%
plot_count = (
    merged_df[["unique_id", "plot_code_nmbr"]]
    .groupby("plot_code_nmbr")
    .count()
    .reset_index()
)

# %%
merged_df.drop(columns=["unique_id"], inplace=True)

# %%
# get mean value of emission factor for each plot
plot_CO2e_ha = merged_df.groupby(["plot_code_nmbr", "Strata"]).mean().reset_index()

# %%
# add count of subplots within each plot
plot_CO2e_ha = plot_CO2e_ha.merge(plot_count, on="plot_code_nmbr", how="left")

# rename unique_id to subplot_count
plot_CO2e_ha.rename(columns={"unique_id": "subplot_count"}, inplace=True)

# %%
# Drop plots that do not have any recorded data
plot_CO2e_ha.dropna(
    subset=[
        "aboveground_CO2e_per_ha",
        "belowground_CO2e_per_ha",
        "deadwood_CO2e_per_ha",
        "ntv_CO2e_per_ha",
        "litter_CO2e_per_ha",
    ],
    inplace=True,
)

# Drop saplings column and aboveground column since these are summarized in the total_aboveground
plot_CO2e_ha.drop(
    columns=["sapling_CO2e_per_ha", "aboveground_CO2e_per_ha"], inplace=True
)

# %%
plot_CO2e_ha.info()

# %% [markdown]
# ## Export data and Upload to BQ

# %%
# Upload to BQ
if len(plot_CO2e_ha) != 0:
    plot_CO2e_ha.to_csv(
        CARBON_STOCK_OUTDIR / f"plot_emission_factors_{VERSION}.csv", index=False
    )
    pandas_gbq.to_gbq(
        plot_CO2e_ha,
        f"{DATASET_ID}.plot_emission_factors_{VERSION}",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
        progress_bar=True,
    )
else:
    raise ValueError("Dataframe is empty.")

# %% [markdown]
# # Create Strata Level Summary

# %%
CO2e_ha_cols = plot_CO2e_ha.filter(like="CO2e_per_ha").columns
subset_cols = CO2e_ha_cols.insert(0, ["plot_code_nmbr", "Strata", "subplot_count"])

# %%
data = plot_CO2e_ha[subset_cols].copy()

# %%
data.rename(
    columns={"total_aboveground_CO2e_per_ha": "aboveground_CO2e_per_ha"}, inplace=True
)

# %%
data.head(2)

# %%
columns = [
    "belowground_CO2e_per_ha",
    "aboveground_CO2e_per_ha",
    "deadwood_CO2e_per_ha",
    "ntv_CO2e_per_ha",
    "litter_CO2e_per_ha",
]

results_list = []

for strata, group in data.groupby("Strata"):
    for column in columns:
        stats = calculate_statistics(group, column)
        stats["Strata"] = strata
        stats["tCO2e_per_ha"] = column.split("_")[0]
        results_list.append(stats)

results_df = pd.DataFrame(results_list)

# Reordering columns for better readability
results_df = results_df[
    [
        "Strata",
        "tCO2e_per_ha",
        "weighted_mean",
        "confidence_interval_lower",
        "confidence_interval_upper",
        "uncertainty_90",
        "uncertainty_95",
        "margin_of_error",
        "weighted_std",
        "standard_error",
        "standard_error_perc_mean",
    ]
]

# %%
results_df.head(2)

# %%
results_df.sort_values(by=["Strata", "tCO2e_per_ha"], inplace=True)

# %%
results_df[
    [
        "Strata",
        "tCO2e_per_ha",
        "weighted_mean",
        "confidence_interval_lower",
        "confidence_interval_upper",
        "uncertainty_90",
        "uncertainty_95",
        "standard_error_perc_mean",
    ]
]

# %%
results_df.groupby("Strata")["weighted_mean"].sum()

# %% [markdown]
# ## Export data and Upload to BQ

# %%
# Upload to BQ
if len(results_df) != 0:
    results_df.to_csv(
        CARBON_STOCK_OUTDIR / f"strata_emission_factors_{VERSION}.csv", index=False
    )
    pandas_gbq.to_gbq(
        results_df,
        f"{DATASET_ID}.strata_emission_factors_{VERSION}",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
        progress_bar=True,
    )
else:
    raise ValueError("Dataframe is empty.")
