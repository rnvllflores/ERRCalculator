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
    TMP_OUT_DIR,
    SPECIES_LOOKUP_CSV,
    PC_PLOT_LOOKUP_CSV,
)

from src.biomass_equations import (
    calculate_tree_height,
    allometric_tropical_tree,
    allometric_peatland_tree,
    vmd0001_eq1,
    vmd0001_eq2a,
    vmd0001_eq2b,
    vmd0001_eq5,
)

# %%
# Variables
TREES_CSV = CARBON_POOLS_OUTDIR / "trees.csv"
SAPLING_CSV = CARBON_POOLS_OUTDIR / "saplings_ntv_litter.csv"
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"
TREES_SPECIES_CSV = TMP_OUT_DIR / "trees_with_names.csv"
TREES_WD_CSV = TMP_OUT_DIR / "trees_with_wood_density.csv"

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

# %%
# get the slope adjusted area per nest per subplot and creaste dict for substitution
plot_info_subset = plot_info[
    [
        "unique_id",
        "corrected_plot_area_n2_m2",
        "corrected_plot_area_n3_m2",
        "corrected_plot_area_n4_m2",
    ]
].copy()
plot_info_subset.dropna(inplace=True)
plot_info_subset.drop_duplicates(subset=["unique_id"], inplace=True)
plot_info_subset_dict = plot_info_subset.to_dict(orient="records")

# %%
plot_info_subset_dict[1]

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
    mean_dbh = pd.DataFrame(
        trees.groupby(["unique_id", "nest"])["DBH"].mean()
    ).reset_index()
    trees.loc[trees["DBH"] >= 150, "DBH"] = trees.loc[
        trees["DBH"] >= 150, "unique_id"
    ].map(mean_dbh.set_index(["unique_id", "nest"])["DBH"])

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
trees.head()

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
if not TREES_WD_CSV.exists():
    # !Rscript {SRC_DIR}/get_wood_density.R {TREES_SPECIES_CSV} {TREES_WD_CSV}

# %%
trees = pd.read_csv(TREES_WD_CSV)

# %%
trees.head(2)

# %%
trees.info()

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

# %%
trees.info()

# %% [markdown]
# ## Calculate biomass and carbon stock for tree AGB 

# %%
plot_strata.Strata.unique()

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
trees.head()

# %%
# convert aboveground biomass to tonnes
trees["aboveground_biomass"] = trees["aboveground_biomass"] / 1000

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

# %%
trees.head(2)

# %% [markdown]
# ## Calculate biomass sum per plot

# %% [markdown]
# ### AGB

# %%
trees_agg_agb = vmd0001_eq2a(trees, ["unique_id", "nest"], "aboveground_carbon_tonnes")

# %%
# add the correct area using the unique_id and nest number
trees_agg_agb["corrected_area_m2"] = trees_agg_agb.apply(
    lambda x: next(
        (
            item["corrected_plot_area_n" + str(x["nest"]) + "_m2"]
            for item in plot_info_subset_dict
            if item["unique_id"] == x["unique_id"]
        ),
        None,
    ),
    axis=1,
)

# %%
trees_agg_agb = vmd0001_eq2b(
    trees_agg_agb, "aboveground_carbon_tonnes", "corrected_area_m2"
)

# %%
# convert tonnes/sqm to tonnes/ha
trees_agg_agb["CO2e_per_ha"] = trees_agg_agb["CO2e_per_ha"] * 10_000

# %%
trees_agg_agb.head()

# %%
# calculate tonnes of Carbon per sqm; convert tonnes/sqm to tonnes/ha
trees_agg_agb["tC_per_ha"] = (
    trees_agg_agb["aboveground_carbon_tonnes"] / trees_agg_agb["corrected_area_m2"]
) * 10_000

# %%
trees_agg_agb = (
    trees_agg_agb.groupby("unique_id")[["CO2e_per_ha", "tC_per_ha"]]
    .mean()
    .reset_index()
)

# %%
trees_agg_agb.rename(
    columns={
        "CO2e_per_ha": "aboveground_CO2e_per_ha",
        "tC_per_ha": "aboveground_tC_per_ha",
    },
    inplace=True,
)

# %%
trees_agg_agb.head()

# %% [markdown]
# ### BGB

# %%
trees_agg_bgb = vmd0001_eq2a(trees, ["unique_id", "nest"], "belowground_carbon_tonnes")

# %%
# add the correct area using the unique_id and nest number
trees_agg_bgb["corrected_area_m2"] = trees_agg_bgb.apply(
    lambda x: next(
        (
            item["corrected_plot_area_n" + str(x["nest"]) + "_m2"]
            for item in plot_info_subset_dict
            if item["unique_id"] == x["unique_id"]
        ),
        None,
    ),
    axis=1,
)

# %%
trees_agg_bgb = vmd0001_eq2b(
    trees_agg_bgb, "belowground_carbon_tonnes", "corrected_area_m2"
)

# %%
# convert tonnes/sqm to tonnes/ha
trees_agg_bgb["CO2e_per_ha"] = trees_agg_bgb["CO2e_per_ha"] * 10_000

# %%
trees_agg_bgb.head()

# %%
# calculate tonnes of Carbon per sqm; convert tonnes/sqm to tonnes/ha
trees_agg_bgb["tC_per_ha"] = (
    trees_agg_bgb["belowground_carbon_tonnes"] / trees_agg_bgb["corrected_area_m2"]
) * 10_000

# %%
trees_agg_bgb = (
    trees_agg_bgb.groupby("unique_id")[["CO2e_per_ha", "tC_per_ha"]]
    .mean()
    .reset_index()
)

# %%
trees_agg_bgb.rename(
    columns={
        "CO2e_per_ha": "belowground_CO2e_per_ha",
        "tC_per_ha": "belowground_tC_per_ha",
    },
    inplace=True,
)

# %%
trees_agg_bgb.head()

# %%
trees = trees_agg_agb.merge(trees_agg_bgb, on="unique_id", how="left")

# %%
trees.head()

# %% [markdown]
# ## Export data and Upload to BQ

# %%
trees.info()

# %% [markdown]
# # Calculate sapling biomass

# %%
saplings = vmd0001_eq1(saplings, is_sapling=True)

# %%
# Calculate corrected radius for sapling nest based on slope (in radians)
corrected_radius = 2 / np.cos(plot_info["slope_radians"])

# %%
# Calculate new total subplot area based on corrected radius
plot_info["corrected_sapling_area_m2"] = np.pi * corrected_radius**2

# %%
saplings = saplings.merge(
    plot_info[["unique_id", "corrected_sapling_area_m2"]], on="unique_id"
)

# %%
saplings = vmd0001_eq2b(saplings)

# %%
saplings.head()

# %%
saplings["saplings_tC_per_ha"] = (
    saplings["aboveground_carbon_tonnes"] / saplings["corrected_sapling_area_m2"]
)

# %%
saplings = saplings[["unique_id", "CO2e_per_ha", "saplings_tC_per_ha"]].copy()

# %%
saplings.rename(columns={"CO2e_per_ha": "sapling_CO2e_per_ha"}, inplace=True)

# %%
saplings.info()

# %% [markdown]
# # Add saplings to aboveground biomass

# %%
trees = trees.merge(saplings, on="unique_id", how="left")

# %%
trees.info()

# %%
trees["total_aboveground_CO2e_per_ha"] = (
    trees["aboveground_CO2e_per_ha"] + trees["sapling_CO2e_per_ha"]
)

# %%
# Upload to BQ
if len(trees) != 0:
    trees.to_csv(CARBON_STOCK_OUTDIR / "trees_carbon_stock.csv", index=False)
    pandas_gbq.to_gbq(
        trees,
        f"{DATASET_ID}.trees_carbon_stock",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
        progress_bar=True,
    )
else:
    raise ValueError("Dataframe is empty.")
