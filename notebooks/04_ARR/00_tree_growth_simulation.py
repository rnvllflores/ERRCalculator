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
# # Imports and Set-up

# %%
import sys
import pandas as pd

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import CSV_DATA_DIR

from src.biomass_equations import allometric_tropical_tree, vmd0001_eq1

# %%
# Filepaths
trees_reference = CSV_DATA_DIR / "arr_growth" / "tree-species.csv"
output_fpath = CSV_DATA_DIR / "arr_growth" / "yoy-sequestration.csv"

# %%
# variables
years = 25

# %% [markdown]
# ## Read Data

# %%
tree_lookup = pd.read_csv(trees_reference)

# %%
tree_lookup

# %% [markdown]
# # Project annual size and sequestration of trees

# %%
# Create empty lists to store the results
scientific_name = []
years_list = []
dbh_growth = []
height_growth = []

# Loop through each tree species to calculate the yearly growth
for _, row in tree_lookup.iterrows():
    for t in range(years + 1):  # From year 0 to 25
        scientific_name.append(row["scientific_name"])
        years_list.append(t)
        dbh_growth.append(t * row["maid_cm_yr"])  # Start from 0, and increase with MAI
        height_growth.append(
            t * row["maih_m_yr"]
        )  # Start from 0, and increase with MAI

# Create a new dataframe with the results
growth_df = pd.DataFrame(
    {
        "scientific_name": scientific_name,
        "year": years_list,
        "dbh_cm": dbh_growth,
        "height_m": height_growth,
    }
)

# %%
growth_df

# %%
growth_df = growth_df.merge(tree_lookup[["scientific_name", "wood_density_g_cm3"]])

# %% [markdown]
# # Calculate biomass

# %%
growth_df = allometric_tropical_tree(
    growth_df, "wood_density_g_cm3", "dbh_cm", "height_m"
)

# %%
growth_df[growth_df["scientific_name"] == "Shorea contorta"]

# %% [markdown]
# # Calculate carbon stock of trees per year

# %%
# convert aboveground biomass to tonnes
growth_df["aboveground_biomass"] = growth_df["aboveground_biomass"] / 1000

# %%
growth_df = vmd0001_eq1(growth_df)

# %%
growth_df

# %%
growth_df["annual_change_carbon_stock"] = growth_df.groupby("scientific_name")[
    "aboveground_carbon_tonnes"
].diff()

# %%
growth_df[growth_df["scientific_name"] == "Shorea contorta"]

# %%
pivot_df = growth_df.pivot_table(
    index="year",  # Each row will correspond to a year
    columns="scientific_name",  # Each column will correspond to a tree species
    values="annual_change_carbon_stock",  # The annual change in carbon stock
    aggfunc="sum",  # In case there are multiple rows for the same year and species, sum the values
)

# %%
pivot_df

# %%
pivot_df.to_csv(output_fpath)
