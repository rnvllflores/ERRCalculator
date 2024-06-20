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
# # Generate ~300x300m grids on areas of interest

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
from tqdm import tqdm

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import ADM_BQ_ID, GCP_PROJ_ID, GRIDS_300m_BQ_ID

# %% [markdown]
# ## Load Data

# %% [markdown]
# ### Admin bounds

# %%
# query for subset of adm_zone
adm_zone_query = f"""
  SELECT
    *
  FROM
  `{ADM_BQ_ID}`
  """
# convert from adm_zone table to gdf
adm_df = gbq.read_gbq(adm_zone_query, GCP_PROJ_ID, dialect="standard")
adm_gdf = gpd.GeoDataFrame(
    adm_df, geometry=adm_df.geometry.apply(wkt.loads), crs="EPSG:4326"
)
adm_gdf.head(2)

# %% [markdown]
# # Generate grids using geowrangler

# %% [markdown]
# ## Create grids by province

# %%
# %%time
# set grid parameters - See grid resolution here https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system
bing_tile_grid_generator = grids.BingTileGridGenerator(17)

# %%
table_schema = [
    {"name": "quadkey", "type": "STRING"},
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

grids_wadm = []

# %%
# %%time
for adm2 in tqdm(adm_gdf.ADM2_PCODE.unique()):
    _adm_gdf = adm_gdf[adm_gdf.ADM2_PCODE == adm2]
    _grid_gdf = bing_tile_grid_generator.generate_grid(_adm_gdf)
    _grids_wadm = _grid_gdf.sjoin(_adm_gdf, predicate="intersects")
    _grids_wadm.drop(columns=["index_right"], inplace=True)

    # export results to BQ
    gbq.to_gbq(
        _grids_wadm,
        GRIDS_300m_BQ_ID,
        GCP_PROJ_ID,
        if_exists="append",
        table_schema=table_schema,
        progress_bar=True,
        chunksize=10000,  # chunk the upload to stay within BQ limits
        api_method="load_csv",  # chunksize only works if load_csv (default is load_parquet)
    )
    grids_wadm.append(_grids_wadm)

# Concat tables for inspection
grids_wadm = pd.concat(grids_wadm)

# %% [markdown]
# ## Grid QA

# %%
grids_wadm.shape

# %%
grids_wadm.head(2)

# %%
# Plot for sense checking
ax = adm_gdf.plot(
    facecolor="none", edgecolor="black", legend=True, figsize=[8, 8], linewidth=0.5
)
ax = grids_wadm.plot(ax=ax, facecolor="none", edgecolor="red", alpha=0.5, linewidth=0.5)
