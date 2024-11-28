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

# %%
# Filepaths
seq_reference = CSV_DATA_DIR / "arr_growth" / "yoy-sequestration.csv"
output_fpath = CSV_DATA_DIR / "arr_growth" / "yoy-sequestration_per_ha.csv"

# %%
seq_per_tree = pd.read_csv(seq_reference)

# %%
seq_per_tree.columns

# %%
# variables
total_trees_per_hectare = 400
species_percentage = {
    "Dipterocarpus grandiflorus": 0.40,
    "Hopea plagata": 0.30,
    "Parashorea malaanonan": 0.30,
}
seq_per_ha = seq_per_tree.copy()
seq_per_ha.set_index("year", inplace=True)

# %%
# Filter the table to include only the species in the species_percentage dictionary
seq_per_ha = seq_per_ha[species_percentage.keys()]

# %%
# For each species, calculate the sequestration per hectare by multiplying the sequestration by the number of trees of that species
for species, percentage in species_percentage.items():
    # Calculate the number of trees for this species in the hectare
    num_trees_per_species = total_trees_per_hectare * percentage

    # Multiply the sequestration per year for each species by the number of trees per species
    seq_per_ha[species] = seq_per_ha[species] * num_trees_per_species

# Now sum across all species for each year to get the total sequestration per hectare per year
seq_per_ha["total_sequestration_per_hectare"] = seq_per_ha.sum(axis=1)

# %%
seq_per_ha
