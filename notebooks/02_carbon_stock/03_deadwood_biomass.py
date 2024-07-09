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
# # Calculate biomass from deadwood

# %% [markdown]
# # Imports and Set-up

# %%
# Standard Imports
import sys
import pandas as pd
import numpy as np

# Google Cloud Imports
import pandas_gbq

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import (
    GCP_PROJ_ID,
    CARBON_POOLS_OUTDIR,
)

from src.biomass_equations import vmd0002_eq2, vmd0002_eq7, vmd0002_eq8

# %%
# Variables
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"
STUMPS_CSV = CARBON_POOLS_OUTDIR / "stumps.csv"
LDW_CSV = CARBON_POOLS_OUTDIR / "lying_deadwood_wo_hollow.csv"
LDW_HOLLOW_CSV = CARBON_POOLS_OUTDIR / "lying_deadwood_hollow.csv"

# BigQuery Variables
DATASET_ID = "carbon_stock"
IF_EXISTS = "replace"

# Processing Conditions
OUTLIER_REMOVAL = "get_ave"  # Options: "get_ave", "drop_outliers", "eq_150"

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
    FROM {GCP_PROJ_ID}.{DATASET_ID}.plot_info"""

    # Read the BigQuery table into a dataframe
    plot_info = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    plot_info.to_csv(PLOT_INFO_CSV, index=False)

# %%
plot_info.info()

# %% [markdown]
# ### Stumps

# %%
if STUMPS_CSV.exists():
    stumps = pd.read_csv(STUMPS_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{DATASET_ID}.stumps"""

    # Read the BigQuery table into a dataframe
    stumps = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    stumps.to_csv(STUMPS_CSV, index=False)

# %%
stumps.info()

# %% [markdown]
# ### Lying deadwood

# %%
if LDW_CSV.exists():
    ldw = pd.read_csv(LDW_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{DATASET_ID}.lying_deadwood_wo_hollow"""

    # Read the BigQuery table into a dataframe
    ldw = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    ldw.to_csv(LDW_CSV, index=False)

# %%
ldw.info()

# %%
if LDW_HOLLOW_CSV.exists():
    ldw_hollow = pd.read_csv(LDW_HOLLOW_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{DATASET_ID}.lying_deadwood_hollow"""

    # Read the BigQuery table into a dataframe
    ldw_hollow = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    ldw_hollow.to_csv(LDW_HOLLOW_CSV, index=False)

# %%
ldw_hollow.info()

# %% [markdown]
# # Calculate stump biomass

# %%
stumps.head(2)

# %%
# get wood density equivalent for each density class

density_val = {1: 0.54, 2: 0.35, 3: 0.21}
stumps["stump_density_val"] = stumps["stump_density"].replace(density_val).fillna(0.21)

# %%
# Get biomass for each stump
stumps = vmd0002_eq2(stumps, "Diam1", "Diam2", "height", "stump_density_val")

# %%
# Get biomass of each stump that is hollow
stumps_hollow = vmd0002_eq2(
    stumps, "hollow_d1", "hollow_d2", "height", "stump_density_val"
)

# %%
# Get biomass to subtract due to hollow stumps
stumps["biomass_hollow"] = stumps_hollow["biomass"]

# %%
# Subtract biomass of hollow stumps from total biomass
stumps["biomass"] = np.where(
    (~stumps["biomass_hollow"].isna()) & (stumps["biomass_hollow"] > 0),
    stumps["biomass"] - stumps["biomass_hollow"],
    stumps["biomass"],
)

# %%
# Remove biomass_hollow column to avoid confusion
stumps.drop(columns=["biomass_hollow"], inplace=True)

# %%
stumps.head(2)

# %% [markdown]
# # Calculate Lying deadwood biomass

# %%
ldw.head(2)

# %% [markdown]
# ## No hollow

# %% [markdown]
# ### Outlier removal

# %%
ldw.describe()

# %%
# Filter the ldw DataFrame to keep rows where diameter is less than or equal to the 98th percentile
ldw = ldw[ldw["diameter"] <= 150]

# %%
ldw = vmd0002_eq7(ldw, "diameter")

# %%
ldw = vmd0002_eq8(ldw, "density")

# %%
ldw.head(2)

# %% [markdown]
# ## Hollow Lying Deadwood
