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
# # Calculate biomass from Living trees and saplings

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
from src.biomass_equations import (
    allometric_peatland_tree,
    allometric_tropical_tree,
    calculate_tree_height,
    vmd0001_eq1,
)
from src.settings import (
    CARBON_POOLS_OUTDIR,
    CARBON_STOCK_OUTDIR,
    DATA_DIR,
    GCP_PROJ_ID,
    PC_PLOT_LOOKUP_CSV,
    SPECIES_LOOKUP_CSV,
)

# %%
# Variables
TREES_CSV = CARBON_POOLS_OUTDIR / "trees.csv"
TREES_WD_CSV = CARBON_POOLS_OUTDIR / "trees_with_wood_density.csv"
SAPLING_CSV = CARBON_POOLS_OUTDIR / "saplings_ntv_litter.csv"
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"

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
# ### Saplings data

# %%
if SAPLING_CSV.exists():
    saplings = pd.read_csv(SAPLING_CSV)
else:
    query = f"""
    SELECT
        *
    FROM {GCP_PROJ_ID}.{DATASET_ID}.saplings_ntv_litter"""

    # Read the BigQuery table into a dataframe
    saplings = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    saplings.to_csv(SAPLING_CSV, index=False)

# %%
saplings.info()

# %% [markdown]
# ### Tree species

# %%
species = pd.read_csv(SPECIES_LOOKUP_CSV)

# %%
species.info()

# %%
species.head(2)

# %% [markdown]
# ### Plot lookup

# %%
plot_strata = pd.read_csv(PC_PLOT_LOOKUP_CSV)

# %%
plot_strata.info()

# %% [markdown]
# # Calculate tree biomass

# %% [markdown]
# ## Remove outliers
#

# %%
if OUTLIER_REMOVAL == "get_ave":
    mean_dbh = pd.DataFrame(trees.groupby("unique_id")["DBH"].mean()).reset_index()
    trees.loc[trees["DBH"] >= 150, "DBH"] = trees.loc[
        trees["DBH"] >= 150, "unique_id"
    ].map(mean_dbh.set_index("unique_id")["DBH"])
elif OUTLIER_REMOVAL == "drop_outliers":
    trees = trees[trees["DBH"] < 150].copy()
elif OUTLIER_REMOVAL == "eq_150":
    trees.loc[trees["DBH"] >= 150, "DBH"] = 150

# %% [markdown]
# ## Add species using lookup table

# %%
species_trees = trees.merge(species, on="code_species", how="left")

# %%
# add species name based on lookup file
trees["scientific_name"] = species_trees["scientific_name"]

# add family name based on lookup file
trees["family_name"] = species_trees["family"]

# %%
species_family = species[["code_family", "family"]].drop_duplicates()

# %%
family_trees = trees.merge(species_family, on="code_family", how="left")

# %%
trees.loc[(trees.code_family.notna()), "family_name"] = family_trees.loc[
    (family_trees.code_family.notna()), "family"
]

# %%
trees.fillna({"scientific_name": "Unknown", "family_name": "Unknown"}, inplace=True)

# %%
trees[(trees["scientific_name"] == "Unknown") & (trees["family_name"] == "Unknown")]

# %%
trees.to_csv(CARBON_POOLS_OUTDIR / "trees_with_names.csv", index=False)

# %% [markdown]
# ## Get genus and wood density using BIOMASS R library
#
# Wood density was generated using [BIOMASS](https://www.rdocumentation.org/packages/BIOMASS/versions/2.1.11) library from R. For further information,

# %% [markdown]
# [To do]: insert running r script to get wood density and genus using R

# %%
trees = pd.read_csv(CARBON_POOLS_OUTDIR / "trees_with_wood_density.csv")

# %%
trees.head(2)

# %% [markdown]
# ## Estimate tree height

# %%
trees = calculate_tree_height(trees, "DBH")

# %%
trees.head(2)

# %% [markdown]
# ## Add strata to trees
#

# %%
trees = trees.merge(plot_strata[["unique_id", "Strata"]], on="unique_id", how="left")

# %%
trees.head(2)

# %% [markdown]
# ## Calculate biomass and carbon stock for tree AGB

# %%
tropical_trees = trees.loc[trees["Strata"].isin([1, 2, 3])].copy()

# %%
tropical_trees = allometric_tropical_tree(tropical_trees, "meanWD", "DBH", "height")

# %%
peatland_trees = trees.loc[trees["Strata"].isin([4, 5, 6])].copy()

# %%
peatland_trees = allometric_peatland_tree(peatland_trees, "DBH")

# %%
trees = pd.concat([tropical_trees, peatland_trees])

# %%
trees = vmd0001_eq1(trees, 0.47)

# %%
trees.head(2)


# %% [markdown]
# ## Calculate below ground biomass


# %%
def vmd0001_eq5(
    df: pd.DataFrame,
    carbon_stock_col: str = "aboveground_carbon_stock",
    eco_zone: str = "tropical_rainforest",
):
    if eco_zone == "tropical_rainforest" or eco_zone == "subtropical_humid":
        df["below_ground_carbon_stock"] = np.where(
            df[carbon_stock_col] < 125,
            df[carbon_stock_col] * 0.20,
            df[carbon_stock_col] * 0.24,
        )
    elif eco_zone == "subtropical_dry":
        df["below_ground_carbon_stock"] = np.where(
            df[carbon_stock_col] < 20,
            df[carbon_stock_col] * 0.56,
            df[carbon_stock_col] * 0.28,
        )
    else:
        raise ValueError(
            "Invalid eco_zone value. Please choose a valid eco_zone or add root-to-shoot ratio for the desired ecological zone."
        )

    return df


# %%
trees = vmd0001_eq5(trees)

# %% [markdown]
# # Calculate sapling biomass

# %%
saplings = vmd0001_eq1(saplings, is_sapling=True)

# %%
# Calculate corrected radius for sapling nest based on slope (in radians)
corrected_radius = 2 / np.cos(plot_info["slope_radians"])

# %%
# Calculate new total subplot area based on corrected radius
plot_info["corrected_sapling_area_m2"] = np.pi * corrected_radius * 2

# %%
