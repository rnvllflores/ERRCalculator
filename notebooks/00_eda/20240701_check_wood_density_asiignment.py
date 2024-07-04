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
from src.settings import (
    CARBON_POOLS_OUTDIR,
    CARBON_STOCK_OUTDIR,
    DATA_DIR,
    GCP_PROJ_ID,
    SPECIES_LOOKUP_CSV,
)

# from src.biomass_equations import vmd0003_eq1

# %%
# Variables
TREES_CSV = CARBON_POOLS_OUTDIR / "trees.csv"
SAPLING_CSV = CARBON_POOLS_OUTDIR / "saplings_ntv_litter.csv"
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"

# BigQuery Variables
DATASET_ID = "carbon_stock"
IF_EXISTS = "replace"

# %% [markdown]
# ## Load data

# %% [markdown]
# ### Trees data

# %%
if TREES_CSV.exists():
    trees = pd.read_csv(TREES_CSV)
else:
    query = f"""
    SELECT
        *
    FROM {GCP_PROJ_ID}.{DATASET_ID}.trees"""

    # Read the BigQuery table into a dataframe
    trees = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    trees.to_csv(TREES_CSV, index=False)

# %%
trees.rename(
    columns={"species_name": "code_species", "family_name": "code_family"}, inplace=True
)

# %%
trees.loc[trees["code_species"] == 999, "code_species"] = np.nan

# %%
trees.info()

# %%
trees.head(2)

# %% [markdown]
# ### Tree species

# %%
species = pd.read_csv(SPECIES_LOOKUP_CSV)

# %%
species.info()

# %%
species.head(2)

# %% [markdown]
# ## Add species using lookup table
#
# Wood density in this table was generated using [BIOMASS](https://www.rdocumentation.org/packages/BIOMASS/versions/2.1.11) library from R

# %%
merged_df = trees.merge(species, on="code_family", how="left")

# %%
# add family name based on lookup file
trees["family_name"] = merged_df["family"]

# %%
merged_df = trees.merge(species, on="code_species", how="left")

# %%
# add species name based on lookup file
trees["scientific_name"] = merged_df["scientific_name"]

# Add wood density based on scientific name
trees["wood_density"] = merged_df["wood_density"]

# %%
trees.fillna({"scientific_name": "Unknown", "family_name": "Unknown"}, inplace=True)

# %%
trees[(trees["code_species"].isna()) & (trees["code_family"])]

# %%
unique_family_names = trees.loc[trees["code_species"].isna(), "family_name"].unique()
print(unique_family_names)

# %%
trees.loc[
    (trees["code_family"].isna()) & (trees["code_species"].isna()),
]

# %%
species[species["family"] == "Sapotaceae"]

# %%
test = pd.read_csv("/Users/renflores/Documents/OneBase/data/csv/trees_processed_r.csv")

# %%
test[(test["species_name"] == "Unknown") & (test["family_name"] == "Unknown")]

# %%
unknown_wood_density = test[
    (test["species_name"] == "Unknown") & (test["family_name"] == "Unknown")
]["WD"].unique()
print(unknown_wood_density)
