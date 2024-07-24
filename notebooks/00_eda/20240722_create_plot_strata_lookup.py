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
import sys
import pandas as pd
import geopandas as gpd

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import DATA_DIR, SRC_DIR

# %%
STRATA_PATH = DATA_DIR / "shp" / "caraga_strata-natural-forest.shp"

# %%
strata_gdf = gpd.read_file(STRATA_PATH)

# %%
# strata_gdf = strata_gdf[['Strata_No','geometry']].copy()

# %%
primary_plots = gpd.read_file(
    DATA_DIR / "shp" / "subplot_map" / "ALL-recoded_plots_primary.shp"
)

# %%
primary_plots.head(2)

# %%
primary_plots = primary_plots[["Plot_ID", "Strata_No"]].copy()

# %%
backup_plots = gpd.read_file(
    DATA_DIR / "shp" / "subplot_map" / "ALL-recoded_plots_backup.shp"
)

# %%
backup_plots.head(2)

# %%
backup_plots = backup_plots[["Plot_ID", "Strata_No"]].copy()

# %%
plots = pd.concat([primary_plots, backup_plots], ignore_index=True)

# %%
plots["Plot_ID"] = plots["Plot_ID"].str.lstrip("0")

# %%
plots["Strata_No"] = plots["Strata_No"].str.extract(r"(\d+)")

# %%
plots

# %%
plots.rename(columns={"Strata_No": "Strata", "Plot_ID": "unique_id"}, inplace=True)

# %%
plots.to_csv(SRC_DIR / "lookup" / "pc_plot_lookup_20240722.csv", index=False)
