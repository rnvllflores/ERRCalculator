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
from src.settings import GCP_PROJ_ID, CARBON_STOCK_OUTDIR, CARBON_POOLS_OUTDIR

# %%
# Variables
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"
LITTER_CSV = CARBON_STOCK_OUTDIR / "litter_carbon_stock.csv"
NTV_CSV = CARBON_STOCK_OUTDIR / "ntv_carbon_stock.csv"
DEADWOOD_CSV = CARBON_STOCK_OUTDIR / "deadwood_carbon_stock.csv"
TREES_CSV = CARBON_STOCK_OUTDIR / "trees_carbon_stock.csv"

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
# # Create subplot level summary

# %%
merged_df = plot_info[[]].merge(trees, on="unique_id", how="left")
merged_df = merged_df.merge(deadwood, on="unique_id", how="left")
merged_df = merged_df.merge(ntv, on="unique_id", how="left")
merged_df = merged_df.merge(litter, on="unique_id", how="left")

# %%
merged_df.head(2)

# %%
merged_df.columns
