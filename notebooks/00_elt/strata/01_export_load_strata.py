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
# # Create Strata for Activity Areas from LCC

# %%
# Standard Imports
import sys
import pandas as pd
import datetime

# Geospatial Imports
import geopandas as gpd
from shapely.geometry import Polygon
from geowrangler.validation import GeometryValidation

# %%
# Util imports
sys.path.append("../../../")  # include parent directory
from src.settings import GPKG_DATA_DIR, TMP_OUT_DIR

# %%
# Variables
VERSION = datetime.datetime.now().strftime("%Y%m%d")

# GCS Variables
LCC_GCS_DIR = "NULL"
STRATA_GCS_DIR = "gs://00_extract_vectors/"
SILUP_GCS_DIR = "gs://silup-gis/onebase/"

# Filepaths
LCC_FILEPATH = GPKG_DATA_DIR / "lcc_cadt.gpkg"
LCC_252_FILEPATH = TMP_OUT_DIR / "lcc_252.gpkg"
LCC_FINAL_FPATH = GPKG_DATA_DIR / "lcc_cadt_validated_20240812.gpkg"

# %% [markdown]
# # Load Data

# %% [markdown]
# Insert auto download of raster and converting raster to vector --  for now this loads a file processed using qgis

# %%
lcc_all = gpd.read_file(LCC_FILEPATH)
lcc_252 = gpd.read_file(LCC_252_FILEPATH)

# %%
lcc = pd.concat([lcc_252, lcc_all])

# %% [markdown]
# ## Validate geometries

# %%
# check for invalid polygons
lcc[~lcc.geometry.is_valid].shape

# %%
# convert multipolygons to polygons
lcc = lcc.explode(index_parts=False)

# %%
# check and correct invalid polygons
lcc = GeometryValidation(lcc).validate_all()

# %%
lcc.geometry.type.unique()

# %%
# create separate gdf for multipolygons for 2nd level of explode and validation
lcc_poly = lcc[lcc["geometry"].type == "Polygon"].copy()
lcc_multi = lcc[lcc["geometry"].type == "MultiPolygon"].copy()

# %%
lcc_multi = lcc_multi.explode(index_parts=False)

# %%
lcc_multi.geometry.type.unique()

# %%
lcc_multi = GeometryValidation(lcc_multi).validate_all()

# %%
lcc_multi.geometry.type.unique()

# %%
# Combine the datasets again
lcc = pd.concat([lcc_poly, lcc_multi])

# %%
lcc[~lcc.geometry.is_valid].shape

# %%
# buffer at 0m to remove invalid vertices
lcc["geometry"] = lcc["geometry"].buffer(0)

# %%
lcc[~lcc.geometry.is_valid].shape

# %%
lcc.to_file(GPKG_DATA_DIR / "lcc_cadt_validated_20240812.gpkg")

# %% [markdown]
# # Generate Strata

# %%
lcc = gpd.read_file(LCC_FINAL_FPATH)

# %%
from typing import Union
from shapely.geometry import MultiPolygon


def one_sided_poly_buffer(
    poly: Polygon, buffer_m: float
) -> Union[Polygon, MultiPolygon]:
    bound = poly.boundary
    # estimate how big the buffer zone will be
    poly_len = min(poly.length, buffer_m)
    buffer_area_estimate = buffer_m * poly_len
    # no need to buffer for small polygons
    if poly.area < buffer_area_estimate:
        buffered_bound = poly
    # buffer for sufficiently large polygons
    else:
        buffered_bound = bound.buffer(-buffer_m / 2, single_sided=True)
        # take the intersection to remove the outward spikes from a single sided buffer
        buffered_bound = buffered_bound.intersection(poly)
        # buffer it again to fill in inward spikes
        buffered_bound = buffered_bound.buffer(buffer_m / 2)
        # take intersection to remove outward spikes again
        buffered_bound = buffered_bound.intersection(poly)
    return buffered_bound


# %%
lcc_dipterocarp = lcc[lcc["DN"].isin([1, 4])]

# %%
lcc_dipterocarp.shape

# %%
test = lcc_dipterocarp[:100]

# %%
lc_swamp = lcc[lcc["DN"].isin([2, 5])]

# %%
# # %%time
# buffered_diptercarp_1km = lcc_dipterocarp["geometry"].apply(
#     lambda x: one_sided_poly_buffer(x, -1_000)
# )

# %%
lcc_dipterocarp["lines_geometry"] = lcc_dipterocarp["geometry"].boundary

# %%
lcc_dipterocarp
