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
# # Calculate Biomass from Non-tree Vegetation and Litter

# %% [markdown]
# # Imports and Set-up
#
# import os

# %%
# Standard Imports
import sys

import numpy as np
import pandas as pd

# Google Cloud Imports
import pandas_gbq

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.biomass_equations import vmd0003_eq1
from src.settings import CARBON_POOLS_OUTDIR, CARBON_STOCK_OUTDIR, DATA_DIR, GCP_PROJ_ID

# %%
# Variables
NTV_LITTER_CSV = CARBON_POOLS_OUTDIR / "saplings_ntv_litter.csv"
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"

# BigQuery Variables
DATASET_ID = "biomass_inventory"
IF_EXISTS = "replace"

# %% [markdown]
# ## Load data

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

# %%
if NTV_LITTER_CSV.exists():
    ntv_litter = pd.read_csv(NTV_LITTER_CSV)
else:
    query = f"""
    SELECT
        *
    FROM {GCP_PROJ_ID}.{DATASET_ID}.saplings_ntv_litter"""

    # Read the BigQuery table into a dataframe
    ntv_litter = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    ntv_litter.to_csv(PLOT_INFO_CSV, index=False)

# %%
ntv_litter.info()

# %% [markdown]
# # Calculate carbon stock for litter

# %%
# get weight of bag contents
ntv_litter["litter_biomass_kg"] = (
    ntv_litter.ntv_sample_weight - ntv_litter.ntv_bag_weight
) / 1000

# %%
litter = ntv_litter[["unique_id", "litter_biomass_kg"]].copy()
litter = vmd0003_eq1(litter, "litter_biomass_kg", 0.15, 0.37)

# %%
litter.info(), litter.head(2)

# %%
litter.rename(columns={"carbon_stock": "litter_carbon_stock"}, inplace=True)

# %% [markdown]
# ## Export data and upload to BQ

# %%
if len(litter) != 0:
    litter.to_csv(CARBON_STOCK_OUTDIR / "litter_carbon_stock.csv", index=False)

# %%
# Upload to BQ
if len(litter) != 0:
    pandas_gbq.to_gbq(
        litter,
        f"{DATASET_ID}.litter_carbon_stock",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )

# %% [markdown]
# # Calculate carbon stock for non-tree vegetation

# %%
ntv_litter["ntv_biomass_kg"] = (
    ntv_litter.ntv_sample_weight - ntv_litter.litter_bag_weight
) / 1000

# %%
ntv = ntv_litter[["unique_id", "ntv_biomass_kg"]].copy()
ntv = vmd0003_eq1(ntv, "ntv_biomass_kg", 0.15, 0.37)

# %%
ntv.info(), ntv.head(2)

# %%
ntv.rename(columns={"carbon_stock": "ntv_carbon_stock"}, inplace=True)

# %%
ntv.to_csv(CARBON_STOCK_OUTDIR / "ntv_carbon_stock.csv", index=False)

# %%
# Upload to BQ
if len(ntv) != 0:
    pandas_gbq.to_gbq(
        ntv,
        f"{DATASET_ID}.ntv_carbon_stock",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )
