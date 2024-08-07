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
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads

df = pd.read_parquet("../../data/parquet/caraga-davao_lcc_2023_0.parquet.gzip")

# %% vscode={"languageId": "javascript"}
df["geometry"] = df["geometry"].apply(loads)
gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:3125")

# %%
gdf["area_sqm"] = gdf.geometry.to_crs("EPSG:3857").area

# %%
gdf.describe()

# %%
subset_gdf = gdf[gdf.area_sqm > 1_500_000]

# %%
subset_gdf.shape

# %%
import dask_geopandas as dgpd
from dask.distributed import Client

client = Client()  # Connect to a Dask cluster

subset_buffer = dgpd.from_geopandas(
    subset_gdf, npartitions=10
)  # Convert GeoDataFrame to Dask GeoDataFrame

subset_buffer = subset_buffer.map_partitions(
    lambda gdf: gdf.buffer(-200)
)  # Apply buffer operation to each partition

subset_buffer = subset_buffer.compute()  # Compute the result

subset_buffer.head()  # Display the result

# %%
subset_bufffer = subset_gdf.buffer(-200)

# %%
import folium

# Create a Folium map instance
map_instance = folium.Map()

# # Display the map
# map_instance

# %%
gdf.loc[gdf.LCC == "Swamp forest, closed", "buffered_geometry_buffer"].explore(
    m=map_instance, fill_color="red"
)

# %%
gdf.loc[gdf.LCC == "Swamp forest, closed", "geometry"].explore(m=map_instance)

# %%
from typing import Union
from shapely.geometry import Polygon, MultiPolygon


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
gdf.to_crs("EPSG:3857", inplace=True)

# %%
gdf.crs

# %%
# %%time
gdf["buffered_geometry_buffer"] = gdf["geometry"].apply(
    lambda x: one_sided_poly_buffer(x, 200)
)

# %%
gdf["area_buffered"] = gdf["buffered_geometry_buffer"].to_crs("EPSG:3857").area

# %%
gdf["diff"] = gdf["area_sqm"] - gdf["area_buffered"]

# %%
gdf["diff"].describe()

# %%
from shapely import Polygon
import geopandas as gpd

# Define the coordinates of the bounding box
min_lon = 120.9
max_lon = 121.2
min_lat = 14.4
max_lat = 14.8

# Create a Polygon object representing the bounding box
bbox = Polygon(
    [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)]
)

# Create a GeoDataFrame with the bounding box
gdf_bbox = gpd.GeoDataFrame(geometry=[bbox])

# Set the coordinate reference system (CRS) of the GeoDataFrame
gdf_bbox.crs = "EPSG:4326"

# Print the GeoDataFrame
print(gdf_bbox)

# %%
from shapely.geometry import Polygon
import geopandas as gpd

# %%
# Define the coordinates of the bounding box
min_lon = 120.9
max_lon = 121.2
min_lat = 14.4
max_lat = 14.8

# Create a Polygon object representing the bounding box
bbox = Polygon(
    [(min_lon, min_lat), (max_lon, min_lat), (max_lon, max_lat), (min_lon, max_lat)]
)

# Create a GeoDataFrame with the bounding box
gdf_bbox = gpd.GeoDataFrame(geometry=[bbox])

# Set the coordinate reference system (CRS) of the GeoDataFrame
gdf_bbox.crs = "EPSG:3857"

# Print the GeoDataFrame
print(gdf_bbox)

# %%
from shapely.geometry import Polygon

# Create a negative buffer of gdf_bbox with a distance of 200m
gdf_bbox_explode = gdf_bbox.explode()
negative_buffer = gdf_bbox_explode.geometry.buffer(-200)

# Create a new GeoDataFrame with the negative buffer
gdf_negative_buffer = gpd.GeoDataFrame(geometry=[negative_buffer])

# # Set the coordinate reference system (CRS) of the GeoDataFrame
# gdf_negative_buffer.crs = 'EPSG:3857'

# Print the GeoDataFrame
print(gdf_negative_buffer)

# %%
import geopandas as gpd

print(gpd.__version__)
