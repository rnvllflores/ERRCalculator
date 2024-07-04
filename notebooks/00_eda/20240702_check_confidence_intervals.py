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
# # Plot confidence interval changes

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
from src.settings import DATA_DIR

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

import glob

# %%
import pandas as pd

csv_files = glob.glob(
    "/Users/renflores/Documents/OneBase/data/csv/test_confidence_intervals/*.csv"
)
dataframes = {}

for file in csv_files:
    file_name = file.split("/")[-1].split(".")[
        0
    ]  # Extract the file name without extension
    df = pd.read_csv(file)
    dataframes[file_name] = df

# %%
ci_avg_substitution = dataframes["ci_avg_substitution"]

# %%
ci_avg_substitution

# %%
ci_avg_substitution.drop(columns=["Unnamed: 0"], inplace=True)

# %%
ci_avg_substitution.drop(columns=["carbon_type"]).groupby("Strata").mean().reset_index()

# %%
ci_avg_substitution.sort_values(by="Strata")

# %% [markdown]
# # droping trees >150

# %%
dataframes.keys()

# %%
csv_files

# %%
ci_remove_trees_150 = pd.read_csv(
    "/Users/renflores/Documents/OneBase/data/csv/test_confidence_intervals/ci_remove_trees_150.csv"
)

# %%
ci_remove_trees_150.drop(columns=["Unnamed: 0"], inplace=True)

# %%
ci_remove_trees_150.drop(columns=["carbon_type"]).groupby("Strata").mean().reset_index()

# %%
merged_df = ci_remove_trees_150.merge(
    ci_avg_substitution,
    on=["Strata", "carbon_type"],
    suffixes=("_remove_trees_150", "_avg_substitution"),
)

# %%
(
    merged_df["margin_err_perc_95_remove_trees_150"]
    - merged_df["margin_err_perc_95_avg_substitution"]
)

# %%
ci_capped_150 = dataframes["ci_capped_150"]

# %%
merged_df = merged_df.merge(ci_capped_150, on=["Strata", "carbon_type"])

# %%
merged_df["margin_err_perc_95"] - merged_df["margin_err_perc_95_avg_substitution"]

# %%
dataframes["ci_capped_150"]

# %%
dataframes.keys()

# %%
ef_avg_substitution = dataframes[
    "emission_factors_biomass_inventories_avg_substitution"
]

# %%
ef_avg_substitution.drop(columns=["Unnamed: 0"], inplace=True)

# %%
ef_avg_substitution.columns
