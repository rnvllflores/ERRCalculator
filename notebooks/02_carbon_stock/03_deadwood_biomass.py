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
    SPECIES_LOOKUP_CSV,
    PC_PLOT_LOOKUP_CSV,
    TMP_OUT_DIR,
)

from src.biomass_equations import (
    vmd0001_eq1,
    vmd0002_eq1,
    vmd0002_eq2,
    vmd0002_eq3,
    vmd0002_eq4,
    vmd0002_eq7,
    vmd0002_eq8,
    get_solid_diamter,
    calculate_tree_height,
    allometric_tropical_tree,
    allometric_peatland_tree,
)

# %%
# Variables
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"
STUMPS_CSV = CARBON_POOLS_OUTDIR / "stumps.csv"
LDW_CSV = CARBON_POOLS_OUTDIR / "lying_deadwood_wo_hollow.csv"
LDW_HOLLOW_CSV = CARBON_POOLS_OUTDIR / "lying_deadwood_hollow.csv"
DEAD_TREES_CSV = CARBON_POOLS_OUTDIR / "dead_trees.csv"

# Temporary Output Files
tmp_dead_trees_c1 = TMP_OUT_DIR / "c1_dead_trees.csv"
tmp_dead_trees_c1_wd = TMP_OUT_DIR / "c1_dead_trees_wd.csv"

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
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.lying_deadwood_wo_hollow"""

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
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.lying_deadwood_hollow"""

    # Read the BigQuery table into a dataframe
    ldw_hollow = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    ldw_hollow.to_csv(LDW_HOLLOW_CSV, index=False)

# %%
ldw_hollow.info()

# %% [markdown]
# ### Standing Deadwood

# %%
if DEAD_TREES_CSV.exists():
    dead_trees = pd.read_csv(DEAD_TREES_CSV)
else:
    query = f"""
    SELECT
        * 
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.dead_trees"""

    # Read the BigQuery table into a dataframe
    dead_trees = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    dead_trees.to_csv(DEAD_TREES_CSV, index=False)

# %%
dead_trees.info()

# %% [markdown]
# ### Tree species

# %%
species = pd.read_csv(SPECIES_LOOKUP_CSV)

# %%
species.info()

# %%
species.head(2)

# %%
species_dict = (
    species[["scientific_name", "code_species"]]
    .set_index("code_species")
    .to_dict()["scientific_name"]
)

# %%
# create lookup table for family name and code
species_family = species[["code_family", "family"]].drop_duplicates()

# %%
family_dict = species_family.set_index("code_family").to_dict()["family"]

# %% [markdown]
# ### Plot lookup

# %%
plot_strata = pd.read_csv(PC_PLOT_LOOKUP_CSV)

# %%
plot_strata.info()

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
stumps["tonnes_dry_matter_hollow"] = stumps_hollow["tonnes_dry_matter"]

# %%
# Subtract biomass of hollow stumps from total biomass
stumps["tonnes_dry_matter"] = np.where(
    (~stumps["tonnes_dry_matter_hollow"].isna())
    & (stumps["tonnes_dry_matter_hollow"] > 0),
    stumps["tonnes_dry_matter"] - stumps["tonnes_dry_matter_hollow"],
    stumps["tonnes_dry_matter"],
)

# %%
# Remove biomass_hollow column to avoid confusion
stumps.drop(columns=["tonnes_dry_matter_hollow"], inplace=True)

# %%
stumps.head(2)

# %%
# get sum opf dry matter per subplot
stumps_agg = vmd0002_eq3(stumps, ["unique_id", "nest"], "tonnes_dry_matter")

# %%
# get the slope adjusted area per nest per subplot
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
# add the correct area using the unique_id and nest number
stumps_agg["corrected_area_m2"] = stumps_agg.apply(
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
# convert square meters to hectares
stumps_agg["corrected_area_ha"] = stumps_agg["corrected_area_m2"] / 10_000

# %%
stumps_agg = vmd0002_eq4(stumps_agg, "tonnes_dry_matter", "corrected_area_ha")

# %%
stumps_agg.rename(
    columns={"tonnes_dry_matter_ha": "stumps_tonnes_dry_matter_ha"}, inplace=True
)

# %% [markdown]
# ## Get total stump biomass per hectare per subplot

# %%
stumps_agg

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
# ## Get total lyind deadwood biomass from 

# %% [markdown]
# ## Hollow Lying Deadwood

# %%
ldw_hollow = get_solid_diamter(ldw_hollow, "hollow_d1", "hollow_d2", "diameter")

# %%
ldw_hollow = vmd0002_eq7(ldw_hollow, "solid_diameter")

# %%
ldw_hollow = vmd0002_eq8(ldw_hollow, "density")

# %%
ldw_hollow.head(2)

# %% [markdown]
# # Calculate Standing Deadwood Biomass

# %% [markdown]
# ## Calculate biomass for Class 1 standing deadwood trees
# Class 1 standing dead trees that is fresh and can be treated as living trees in terms of biomass. The method applied mimics the process from living trees

# %%
dead_trees.head(2)

# %%
dead_trees.rename(
    columns={"family_name": "code_family", "species_name": "code_species"}, inplace=True
)

# %%
c1_dead_trees = dead_trees.loc[dead_trees["class"] == 1].copy()

# %%
c1_dead_trees["family_name"] = c1_dead_trees["code_family"].replace(family_dict)

# %%
c1_dead_trees["scientific_name"] = c1_dead_trees["code_species"].replace(species_dict)

# %%
c1_dead_trees.to_csv(tmp_dead_trees_c1)

# %%
c1_dead_trees

# %% [markdown]
# ### Get genus and wood density using BIOMASS R Library

# %%
# !Rscript $SRC_DIR"/get_wood_density.R" $tmp_dead_trees_c1 $tmp_dead_trees_c1_wd

# %%
c1_dead_trees = pd.read_csv(tmp_dead_trees_c1_wd)

# %%
c1_dead_trees

# %%
c1_dead_trees = calculate_tree_height(c1_dead_trees, "DBH_cl1")

# %%
c1_dead_trees = c1_dead_trees.merge(
    plot_strata[["unique_id", "Strata"]], on="unique_id", how="left"
)

# %%
c1_dead_trees_tropical = c1_dead_trees.loc[
    c1_dead_trees["Strata"].isin([1, 2, 3])
].copy()

# %%
c1_dead_trees_tropical = allometric_tropical_tree(
    c1_dead_trees_tropical, "wood_density", "DBH_cl1", "height"
)

# %%
c1_dead_trees_peatland = c1_dead_trees.loc[
    c1_dead_trees["Strata"].isin([4, 5, 6])
].copy()

# %%
c1_dead_trees_peatland = allometric_peatland_tree(c1_dead_trees_peatland, "DBH_cl1")

# %%
c1_dead_trees = pd.concat([c1_dead_trees_tropical, c1_dead_trees_peatland])

# %%
c1_dead_trees = vmd0001_eq1(c1_dead_trees, 0.47)

# %%
c1_dead_trees.drop(columns=["X"], inplace=True)

# %%
c1_dead_trees

# %% [markdown]
# ## Calculate biomass for Class 2  standing deadwood tall trees
#
# Class 2 are standing dead trees with assigned density class

# %%
c2_dead_trees_t = dead_trees.loc[
    (dead_trees["class"] == 2) & (dead_trees["subclass"] == "tall")
].copy()

# %%
# convert slope to radians
c2_dead_trees_t["slope_t_tall_radians"] = np.atan(c2_dead_trees_t["slope_t_tall"]) / 100
c2_dead_trees_t["slope_b_tall_radians"] = np.atan(c2_dead_trees_t["slope_b_tall"]) / 100

# %%
# estimate tree height
c2_dead_trees_t = calculate_tree_height(
    c2_dead_trees_t,
    trig_leveling=True,
    dist_col="dist_t_tall",
    slope_b_col="slope_b_tall_radians",
    slope_t_col="slope_t_tall_radians",
)

# %%
# set wood density equivalent for each density class
density_val = {1: 0.54, 2: 0.35, 3: 0.21}
c2_dead_trees_t["density_val"] = (
    c2_dead_trees_t["tall_density"].replace(density_val).fillna(0.21)
)

# %%
c2_dead_trees_t = vmd0002_eq1(c2_dead_trees_t, "db_tall", "height", "density_val")

# %%
c2_dead_trees_t