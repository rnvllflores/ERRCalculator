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
import geopandas as gpd

# Google Cloud Imports

# %%
# Util imports
sys.path.append("../../../")  # include parent directory
from src.settings import PARQUET_DATA_DIR, DATA_DIR, SRC_DIR

# %%
# Variables
STRATA_DIR = PARQUET_DATA_DIR / "pre-strata"

# GCS Variables
STRATA_GCS_DIR = "gs://00_extract_vectors/"

# BigQuery Variables
SRC_DATASET_ID = "biomass_inventory"
DATASET_ID = "carbon_stock"
IF_EXISTS = "replace"

# %% [markdown]
# ## Downlaod data

# %%
STRATA_DIR.mkdir(exist_ok=True)

# %%
# !gsutil -m cp $STRATA_GCS_DIR"pre-strata*.parquet.gzip" $STRATA_DIR

# %%
import pathlib

file_list = list(pathlib.Path(STRATA_DIR).glob("*"))

# %%
# Read all files in file_list as separate dataframes
dfs = [pd.read_parquet(file) for file in file_list]

# Concatenate the dataframes into one
combined_df = pd.concat(dfs)

# %%
combined_df["geometry"] = gpd.GeoSeries.from_wkt(combined_df["geometry"])

# %%
combined_df = gpd.GeoDataFrame(combined_df, geometry="geometry", crs="EPSG:4326")

# %%
combined_df["strata"].unique()

# %%
primary_plots = gpd.read_file(
    DATA_DIR / "shp" / "subplot_map" / "ALL-recoded_plots_primary.shp"
)
primary_plots.head(2)

# %%
primary_plots["Plot_ID"] = (
    primary_plots["Plot_No"].astype(str) + primary_plots["Letter_CD"] + "1"
)

# %%
primary_plots[primary_plots.duplicated(subset="Plot_ID", keep=False)]

# %%
# primary_plots = primary_plots.loc[primary_plots.Plot_type == "Primary_point"].copy()

# %%
primary_plots.Plot_type.unique()

# %%
backup_plots = gpd.read_file(
    DATA_DIR / "shp" / "subplot_map" / "ALL-recoded_plots_backup.shp"
)
backup_plots.head(2)

# %%
backup_plots["Plot_ID"] = backup_plots["Plot_ID"].str.lstrip("0")

# %%
backup_plots[backup_plots.duplicated(subset="Plot_ID", keep=False)]

# %%
backup_plots.loc[backup_plots.ID == 100988, "Plot_ID"] = "425D2"

# %%
backup_plots = backup_plots.loc[backup_plots.Plot_type == "Subplot"].copy()

# %%
subset = backup_plots[backup_plots["Plot_ID"].str.endswith("1")]
subset

# %%
backup_plots = backup_plots[~backup_plots["Plot_ID"].str.endswith("1")]

# %%
backup_plots.loc[backup_plots.Plot_No == 296].explore()

# %%
plots = pd.concat([primary_plots, backup_plots], ignore_index=True)

# %%
plots.drop(columns=["Strata_No"], inplace=True)

# %%
plots.to_crs("EPSG:4326", inplace=True)

# %%
plots = plots.sjoin(
    combined_df[["strata", "geometry"]], how="left", predicate="intersects"
)

# %%
plots["Plot_ID"] = plots["Plot_ID"].str.lstrip("0")
plots["strata"] = plots["strata"].str.extract(r"(\d+)")

# %%
plots

# %%
plots.rename(columns={"strata": "Strata", "Plot_ID": "unique_id"}, inplace=True)
# plots.to_csv(SRC_DIR / "lookup" / "pc_plot_lookup_20240802.csv", index=False)

# %%
plots.loc[plots.ID == 118481, "Strata"] = 1

# %%
plots.dropna(subset=["Strata"], inplace=True)

# %%
plots.shape

# %%
plots = plots[["unique_id", "Strata"]]

# %%
plots.to_csv(SRC_DIR / "lookup" / "pc_plot_lookup_20240802.csv", index=False)

# %%
plots.shape

# %%
plots.unique_id.nunique()

# %%
subset = plots[plots.duplicated(subset="unique_id", keep=False)]

# %%
subset

# %%
plots[plots.unique_id.isin(subset.unique_id)].to_csv(
    DATA_DIR / "tmp" / "duplicate_plots_new_strata.csv", index=False
)

# %%

# %%
plots.drop_duplicates(subset=["Strata", "unique_id"], inplace=True)

# %%
combined_df.to_file(DATA_DIR / "gpkg" / "strata.gpkg")
