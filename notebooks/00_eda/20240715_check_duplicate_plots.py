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

# %%
# Standard Imports
import sys
import pandas as pd

# Google Cloud Imports
import pandas_gbq

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import CARBON_POOLS_OUTDIR, GCP_PROJ_ID, TMP_OUT_DIR

PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"
SRC_DATASET_ID = "biomass_inventory"

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
plot_info.plot_type.unique()

# %%
plot_info.loc[plot_info.duplicated(subset="unique_id"), "unique_id"].unique()

# %%
duplicates = plot_info[
    plot_info.unique_id.isin(
        plot_info.loc[plot_info.duplicated(subset="unique_id"), "unique_id"].unique()
    )
].sort_values("unique_id")

# %%
duplicates.groupby("unique_id")["sub_plot_shift"].nunique().sort_values()

# %%
duplicates

# %%
duplicates.to_csv(TMP_OUT_DIR / "duplicate_plot_ids.csv")
