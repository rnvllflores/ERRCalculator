# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.13.1
#   kernelspec:
#     display_name: site-scoring
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Generate 2x2km grids on areas of interest

# %% [markdown]
# # Set Up

# %% [markdown]
# ## Imports

# %%
# %load_ext autoreload
# %autoreload 2

# Standard imports
import sys

# Geospatial processing packages
import geopandas as gpd
import numpy as np
import pandas as pd
import pandas_gbq as gbq
from geowrangler import grids
from geowrangler.validation import GeometryValidation
from shapely import wkt
from shapely.geometry.polygon import orient

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import ADM1_BQ_ID, CADT_BQ_ID, GCP_PROJ_ID, GRIDS_2K_BQ_ID

# %% [markdown]
# ## Load Data

# %% [markdown]
# ### CADT
# Exclude all areas with official recognition of ancestral domain

# %%
# When running a query, note that there may be a one-time authentication required, just click on the link in the prompt and follow the auth steps
# Load CADT
cadt_query = f"""
  SELECT
  *
  FROM
  `{CADT_BQ_ID}`
  """
# convert from gbq table to gdf
cadt_df = gbq.read_gbq(cadt_query, "tm-geospatial", dialect="standard")
cadt_gdf = gpd.GeoDataFrame(
    cadt_df, geometry=cadt_df.geometry.apply(wkt.loads), crs="EPSG:4326"
)
cadt_gdf.head(2)

# %%
# visually inspect the cadt boundary
cadt_gdf.plot()

# %% [markdown]
# #### Geometry Validation

# %%
# %%time
GeometryValidation(cadt_gdf)

cadt_gdf_val = GeometryValidation(cadt_gdf).validate_all()
cadt_gdf_val.head(2)

# %%
# check if any of the rows returned false in the tests
cadt_gdf_val[cadt_gdf_val.any(axis="columns") == False].shape

# %%
# add uid for each row
cadt_gdf["id"] = (
    cadt_gdf[["CADT_NO_", "SURVEY_NO_", "LOCATION", "AREA"]].sum(axis=1).map(hash)
)
cadt_gdf.columns

# %% [markdown]
# ### Admin bounds

# %%
# query for subset of adm_zone
adm_zone_query = f"""
  SELECT
    ADM1_PCODE,
    ADM1_EN,
    ADM2_PCODE,
    ADM2_EN,
    ADM3_PCODE,
    ADM3_EN,
    ADM4_PCODE,
    ADM4_EN,
    geometry
  FROM
  `{ADM1_BQ_ID}`
  """
# convert from adm_zone table to gdf
adm_df = gbq.read_gbq(adm_zone_query, "tm-geospatial", dialect="standard")
adm_gdf = gpd.GeoDataFrame(
    adm_df, geometry=adm_df.geometry.apply(wkt.loads), crs="EPSG:4326"
)
adm_gdf.head(2)

# %% [markdown]
# # Generate grids using geowrangler

# %% [markdown]
# ## Remove CADT areas from admin bounds

# %%
adm_diff = adm_gdf.overlay(cadt_gdf, how="symmetric_difference")
adm_diff.head(2)

# %%
adm_diff.plot()

# %% [markdown]
# ## Create grids

# %%
# %%time
# set grid parameters
grid_generator = grids.SquareGridGenerator(2000)

# create grids
grid_gdf = grid_generator.generate_grid(adm_diff)
grid_gdf.head(2)

# %%
grid_gdf.shape

# %%
grid_gdf.plot()

# %%
# add admin info
grids_wadm = grid_gdf.sjoin(adm_diff, predicate="intersects")
grids_wadm.head(2)

# %%
# Plot for sense checking
ax = adm_diff.plot(
    facecolor="none", edgecolor="black", legend=True, figsize=[8, 8], linewidth=2
)
ax = grids_wadm.plot(ax=ax, facecolor="none", edgecolor="red", alpha=0.5, linewidth=0.5)

# %%
grids_wadm

# %%

# %% [markdown]
# ## Upload grids to bq

# %%
# define data type for geometry
table_schema = [
    {"name": "bing_id", "type": "STRING"},
    {"name": "x", "type": "NUMERIC"},
    {"name": "y", "type": "NUMERIC"},
    {"name": "ADM1_EN", "type": "STRING"},
    {"name": "ADM1_PCODE", "type": "STRING"},
    {"name": "ADM2_EN", "type": "STRING"},
    {"name": "ADM2_PCODE", "type": "STRING"},
    {"name": "ADM3_EN", "type": "STRING"},
    {"name": "ADM3_PCODE", "type": "STRING"},
    {"name": "ADM4_EN", "type": "STRING"},
    {"name": "ADM4_PCODE", "type": "STRING"},
    {"name": "geometry", "type": "GEOGRAPHY"},
]

gbq.to_gbq(
    grids_wadm,
    GRIDS_2K_BQ_ID,
    GCP_PROJ_ID,
    if_exists="fail",
    table_schema=table_schema,
    progress_bar=True,
    chunksize=10000,  # chunk the upload to stay within BQ limits
    api_method="load_csv",  # chunksize only works if load_csv (default is load_parquet)
)
