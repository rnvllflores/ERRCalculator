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
# # Calculate biomass from Living trees and saplings

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
    CARBON_STOCK_OUTDIR,
    SPECIES_LOOKUP_CSV,
    PC_PLOT_LOOKUP_CSV,
)

from src.biomass_equations import (
    calculate_tree_height,
    allometric_tropical_tree,
    allometric_peatland_tree,
    vmd0001_eq1,
    vmd0001_eq2,
    vmd0001_eq5,
)

# %%
# Variables
TREES_CSV = CARBON_POOLS_OUTDIR / "trees.csv"
SAPLING_CSV = CARBON_POOLS_OUTDIR / "saplings_ntv_litter.csv"
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"
TREES_SPECIES_CSV = CARBON_POOLS_OUTDIR / "trees_with_names.csv"
TREES_WD_CSV = CARBON_POOLS_OUTDIR / "trees_with_wood_density.csv"

# BigQuery Variables
SRC_DATASET_ID = "biomass_inventory"
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
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.plot_info"""

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
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.trees"""

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
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.saplings_ntv_litter"""

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
species_dict = (
    species[["scientific_name", "code_species"]]
    .set_index("code_species")
    .to_dict()["scientific_name"]
)

# %%
trees["scientific_name"] = trees["code_species"].replace(species_dict)

# %%
# create lookup table for family name and code
species_family = species[["code_family", "family"]].drop_duplicates()

# %%
family_dict = species_family.set_index("code_family").to_dict()["family"]

# %%
trees["family_name"] = trees["code_family"].replace(family_dict)

# %%
trees

# %%
trees[(trees.scientific_name.notnull()) & (trees.code_family.isnull())]

# %%
trees.info()

# %%
trees.to_csv(TREES_SPECIES_CSV, index=False)

# %% [markdown]
# ## Get genus and wood density using BIOMASS R library
#
# Wood density was generated using [BIOMASS](https://www.rdocumentation.org/packages/BIOMASS/versions/2.1.11) library from R. For further information, 

# %%
# !Rscript $SRC_DIR"/get_wood_density.R" $TREES_SPECIES_CSV $TREES_WD_CSV

# %%
trees = pd.read_csv(TREES_WD_CSV)

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
tropical_trees = allometric_tropical_tree(
    tropical_trees, "wood_density", "DBH", "height"
)

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
trees = vmd0001_eq5(
    trees,
)

# %% [markdown]
# ## Export data and Upload to BQ

# %%
trees.info()

# %%
trees.head(2)

# %%
# Upload to BQ
if len(trees) != 0:
    trees.to_csv(CARBON_STOCK_OUTDIR / "trees_carbon_tonnes.csv", index=False)
    pandas_gbq.to_gbq(
        trees,
        f"{DATASET_ID}.trees_carbon_tonnes",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
        progress_bar=True,
    )
else:
    raise ValueError("Dataframe is empty.")

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
saplings = saplings.merge(
    plot_info[["unique_id", "corrected_sapling_area_m2"]], on="unique_id"
)

# %%
saplings = vmd0001_eq2(saplings)

# %%
saplings.info()

# %%
# Upload to BQ
if len(saplings) != 0:
    saplings.to_csv(CARBON_STOCK_OUTDIR / "saplings_carbon_stock.csv", index=False)
    pandas_gbq.to_gbq(
        saplings,
        f"{DATASET_ID}.saplings_carbon_stock",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
        progress_bar=True,
    )
else:
    raise ValueError("Dataframe is empty.")
