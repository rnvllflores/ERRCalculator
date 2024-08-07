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
# # Draft

# %%
# Standard Imports
import sys
import pandas as pd
import pandas_gbq
import duckdb
import geopandas as gpd
from shapely.wkt import loads
from shapely.geometry import Polygon
from geowrangler.validation import GeometryValidation

# Google Cloud Imports

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import GEOJSON_DATA_DIR, PARQUET_DATA_DIR, GPKG_DATA_DIR, TMP_OUT_DIR

# %%
# Variables
SILUP_DIR = GEOJSON_DATA_DIR / "SILUP"
STRATA_DIR = PARQUET_DATA_DIR / "pre-strata"

# GCS Variables
STRATA_GCS_DIR = "gs://00_extract_vectors/"
SILUP_GCS_DIR = "gs://silup-gis/onebase/"

# BigQuery Variables
SRC_DATASET_ID = "biomass_inventory"
DATASET_ID = "carbon_stock"
IF_EXISTS = "replace"

# %%
# # !gsutil -m cp "gs://00_extract_vectors/caraga-davao-gee_lcc_2023_*" $PARQUET_DATA_DIR"/lcc"

# %%
# # !gsutil -m cp "gs://00_extract_rasters/CaragaDavao_LandCoverMap_L4_AREA2_2024.tif" $TMP_OUT_DIR

# %%
# Connect to the duckdb database
db = duckdb.connect(database=str(TMP_OUT_DIR / "activity_area.db"))

# %%
db.execute("INSTALL spatial; LOAD spatial;")

# %%
query = "SELECT name FROM sqlite_master WHERE type='table';"
db.execute(query).df()

# %%
activity_area_gdf = gpd.read_file(TMP_OUT_DIR / "activity_area.gpkg", driver="GPKG")

# %%
activity_area_gdf.info()

# %%
activity_area_gdf.geometry.type.unique()

# %%
activity_area_gdf = activity_area_gdf.explode(index_parts=False)

# %%
activity_area_gdf = GeometryValidation(activity_area_gdf).validate_all()


# %%
def polygon_z_to_2d(geom):
    if geom.has_z:
        return Polygon([(x, y) for x, y, z in geom.exterior.coords])
    return geom


# %%
# Apply the conversion to the GeoDataFrame
activity_area_gdf["geometry"] = activity_area_gdf["geometry"].apply(polygon_z_to_2d)

# %%
activity_area_df = activity_area_gdf.copy()
activity_area_df["geometry"] = activity_area_gdf["geometry"].to_wkt()

# %%
activity_area_df[activity_area_df["geometry"].str.contains("POLYGON")]

# %%
activity_area_gdf.to_file(TMP_OUT_DIR / "activity_area.gpkg")

# %%
query = """
DROP TABLE activity_area;
CREATE OR REPLACE TABLE activity_area AS 
SELECT 
    CADT, 
    ELI_TYPE, 
    ST_GeomFromText(geometry) as geometry FROM activity_area_df
"""

db.execute(query)

# %%
lcc_cadt = gpd.read_file(GPKG_DATA_DIR / "lcc_cadt.gpkg", driver="GPKG")

# %%
lcc_cadt = lcc_cadt.explode(index_parts=False)

# %%
# Apply the conversion to the GeoDataFrame
lcc_cadt["geometry"] = lcc_cadt["geometry"].apply(polygon_z_to_2d)

# %%
lcc_cadt = GeometryValidation(lcc_cadt).validate_all()

# %%
lcc_cadt.geometry.type.unique()

# %%
lcc_cadt_df = lcc_cadt.copy()

# %%
lcc_cadt_df["geometry"] = lcc_cadt_df["geometry"].to_wkt()

# %%
lcc_cadt_df.head(2)

# %%
query = """
CREATE OR REPLACE TABLE lcc_cadt AS
SELECT
    CASE 
        WHEN DN = 1 THEN 'dipterocarp_open' 
        WHEN DN = 4 THEN 'dipterocarp_closed' 
        ELSE 'Non-Forest' 
    END as lcc_type,
    ST_GeomFromText(geometry) as geometry
FROM lcc_cadt_df
WHERE DN = 1 OR DN = 4
"""

# %%
db.execute(query)

# %%
query = """
SELECT 
    activity_area.CADT,
    activity_area.ELI_TYPE,
    lcc_cadt.lcc_type,
    ST_AsText(ST_Intersection(activity_area.geometry, lcc_cadt.geometry)) as geometry
FROM activity_area
JOIN lcc_cadt ON ST_Intersects(lcc_cadt.geometry, activity_area.geometry)
WHERE activity_area.ELI_TYPE = 'APD'
"""

# %%
apd = db.execute(query).df()

# %%
apd["geometry"] = apd["geometry"].apply(lambda x: loads(x))
apd = gpd.GeoDataFrame(apd, geometry="geometry", crs="EPSG:4326")

# %%
apd.geometry.type.unique()

# %%
query = "SELECT DISTINCT ST_GeometryType(geometry) FROM activity_area"

# %%
db.execute(query).df()

# %%
apd_explode = apd.explode(index_parts=False)

# %%
apd.head(2)

# %%
apd["area_ha"] = apd.to_crs("EPSG:3123").area / 10_000

# %%
apd.groupby("lcc_type")["area_ha"].sum()

# %%
activity_area_gdf.CADT.unique()

# %%
activity_area_gdf.loc[activity_area_gdf["ELI_TYPE"] == "ARR"].to_crs(
    "EPSG:3123"
).area.sum() / 10_000

# %%
apd["area_ha"].sum()

# %%
lcc_cadt.loc[lcc_cadt["DN"].isin([1, 4])].explore()

# %%
query = """
SELECT 
    activity_area.CADT,
    activity_area.ELI_TYPE,
    lcc_cadt.lcc_type,
    ST_AsText(ST_Intersection(activity_area.geometry, lcc_cadt.geometry)) as geometry
FROM activity_area
JOIN lcc_cadt ON ST_Intersects(lcc_cadt.geometry, activity_area.geometry)
WHERE activity_area.ELI_TYPE = 'APD' OR activity_area.ELI_TYPE = 'ARR'
"""

# %%
apd_arr = db.execute(query).df()

# %%
# .groupby("lcc_type")['area_ha'].sum()

# %%
cadt252 = "NULL"

# %%
cadt252 = gpd.read_file(
    GEOJSON_DATA_DIR / "SILUP" / "v0 CADT 252.geojson", driver="GeoJSON"
)

# %%
cadt252.to_crs("EPSG:4326", inplace=True)

# %%
cadt252.head(2)

# %%
cadt252.geometry.type.unique()

# %%
cadt252_poly = cadt252[cadt252.geometry.type == "Polygon"].copy()

# %%
cadt252_poly.plot()

# %%
cadt252_multi = (
    cadt252[cadt252.geometry.type == "MultiPolygon"].copy().explode(index_parts=False)
)

# %%
cadt252_multi.geometry.type.unique()

# %%
cadt252_multi.plot()

# %%
cadt252_collection = (
    cadt252[cadt252.geometry.type == "GeometryCollection"]
    .copy()
    .explode(index_parts=False)
)

# %%
cadt252_collection.geometry.type.unique()

# %%
cadt252_collection_poly = cadt252_collection[
    cadt252_collection.geometry.type == "Polygon"
]

# %%
cadt252_collection_multi = cadt252_collection[
    cadt252_collection.geometry.type == "MultiPolygon"
].explode(index_parts=False)

# %%
cadt252_all = [
    cadt252_poly,
    cadt252_multi,
    cadt252_collection_poly,
    cadt252_collection_multi,
]

# %%
cadt_252 = pd.concat(cadt252_all)

# %%
cadt_252.geometry.type.unique()

# %%
cadt_252.reset_index(drop=True, inplace=True)

# %%
cadt_252.plot()

# %%
cadt_252.geometry.type.unique()

# %%
cadt_252 = cadt_252.explode(index_parts=False)

# %%
# cadt252["geometry"] = cadt252.buffer(0)

# %%
# cadt252 = GeometryValidation(cadt252).validate_all()


# %%
cadt_252 = cadt_252[["CADT_No", "ELI_TYPE", "geometry"]].copy()

# %%
cadt_252.rename(columns={"CADT_No": "CADT"}, inplace=True)

# %%
activity_area_gdf = pd.concat(
    [activity_area_gdf[activity_area_gdf["CADT"] != 252], cadt_252]
)

# %%
cadt_252.to_file(TMP_OUT_DIR / "cadt252_validated.gpkg")

# %% [markdown]
# # New

# %%
# Standard Imports
import sys
import pandas as pd
import duckdb
import geopandas as gpd
from geowrangler.validation import GeometryValidation

# Google Cloud Imports

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import GEOJSON_DATA_DIR, PARQUET_DATA_DIR, GPKG_DATA_DIR, TMP_OUT_DIR

# %%
# Connect to the duckdb database
db = duckdb.connect(database=str(TMP_OUT_DIR / "activity_area.db"))

# %%
db.execute("INSTALL spatial; LOAD spatial;")

# %%
query = "SELECT name FROM sqlite_master WHERE type='table';"
db.execute(query).df()

# %%
query = """
SELECT
    * 
FROM `phl-caraga-apd.00_transform.aud_risk_buffer`"""

# Read the BigQuery table into a dataframe
aud_risk_buffer = pandas_gbq.read_gbq(query, project_id="phl-caraga-apd")
# plot_info.to_csv(PLOT_INFO_CSV, index=False)

# %%
aud_risk_buffer.info()

# %%
# aud_risk_buffer['geometry'] = aud_risk_buffer['geometry'].apply(lambda x: loads(x))
# aud_risk_buffer = gpd.GeoDataFrame(aud_risk_buffer, geometry='geometry', crs="EPSG:4326")

# %%
aud_risk_buffer = aud_risk_buffer.explode(index_parts=False)

# %%
# aud_risk_buffer = GeometryValidation(aud_risk_buffer).validate_all()

# %%
aud_risk_buffer["geometry"] = aud_risk_buffer["geometry"].apply(polygon_z_to_2d)

# %%
aud_risk_buffer.head()

# %%
aud_risk_buffer_df = aud_risk_buffer.copy()
aud_risk_buffer_df["geometry"] = aud_risk_buffer_df["geometry"].to_wkt()

# %%
query = """
CREATE OR REPLACE TABLE aud_buffer AS
SELECT
    aud_clipping_layer,
    ST_GeomFromText(geometry) as geometry
FROM aud_risk_buffer_df
"""

# %%
db.execute(query)

# %%
query = """
CREATE OR REPLACE TABLE aud_adjusted AS
SELECT 
    activity_area.CADT,
    activity_area.ELI_TYPE,
    ST_AsText(ST_difference(activity_area.geometry, aud_buffer.geometry)) as geometry
FROM activity_area, aud_buffer
WHERE activity_area.ELI_TYPE = 'AUD'
"""

# %%
db.execute(query)

# %%
query = """ 
SELECT 
    SUM(ST_Area(geometry))/10000 as area_ha
FROM 
    activity_area
WHERE ELI_TYPE = 'AUD'
"""

# %%
db.execute(query).df()

# %%
query = """
SELECT 
    activity_area.CADT,
    activity_area.ELI_TYPE,
    lcc_cadt.lcc_type,
    ST_AsText(ST_Intersection(activity_area.geometry, lcc_cadt.geometry)) as geometry
FROM activity_area
JOIN lcc_cadt ON ST_Intersects(lcc_cadt.geometry, activity_area.geometry)
WHERE activity_area.ELI_TYPE = 'AUD'
"""

# %%
aud_lcc = db.execute(query).df()

# %%
aud_lcc["geometry"] = aud_lcc["geometry"].apply(lambda x: loads(x))
aud_lcc = gpd.GeoDataFrame(aud_lcc, geometry="geometry", crs="EPSG:4326")

# %%
aud_lcc.to_file(TMP_OUT_DIR / "aud_lcc.gpkg")

# %%
query = """
SELECT
    aud_clipping_layer,
    ST_AsText(geometry) as geometry
FROM aud_buffer
"""

# %%
aud_buffer = db.execute(query).df()

# %%
aud_buffer["geometry"] = aud_buffer["geometry"].apply(lambda x: loads(x))
aud_buffer = gpd.GeoDataFrame(aud_buffer, geometry="geometry", crs="EPSG:4326")

# %%
aud_risk_buffer.to_file(TMP_OUT_DIR / "aud_buffer.gpkg")

# %%
aud_risk_buffer = gpd.read_file(TMP_OUT_DIR / "aud_buffer.gpkg")

# %%
aud_risk_buffer.geometry.type.unique()

# %%
aud_risk_buffer = aud_risk_buffer.explode(index_parts=False)

# %%
aud_risk_buffer = GeometryValidation(aud_risk_buffer).validate_all()

# %%
lcc_cadt

# %%
activity_area_gdf

# %%
intersection = gpd.overlay(activity_area_gdf, lcc_cadt, how="intersection")

# %%
intersection.columns

# %%
intersection_subset = intersection[["CADT", "ELI_TYPE", "DN", "geometry"]].copy()

# %%
intersection_subset["area_ha"] = intersection_subset.to_crs("EPSG:3123").area / 10_000

# %%
intersection_subset.loc[intersection_subset.DN.isin([1, 4])].groupby(
    ["ELI_TYPE", "DN"]
)["area_ha"].sum().reset_index()

# %%
8352.38 + 77484.27

# %%
intersection_subset.to_file(TMP_OUT_DIR / "lcc_cadt.gpkg")

# %%
aud_subset = intersection_subset[intersection_subset["ELI_TYPE"] == "AUD"].copy()

# %%
aud_buffered = gpd.overlay(
    aud_subset, aud_risk_buffer, how="difference", keep_geom_type=False, make_valid=True
)

# %%
aud_buffered.plot()

# %%
aud_buffered["area_ha"] = aud_buffered.to_crs("EPSG:3123").area / 10_000

# %%
aud_buffered.loc[aud_buffered.DN.isin([1, 4])].groupby(["ELI_TYPE", "DN"])[
    "area_ha"
].sum().reset_index()

# %%
1980.08 + 32114.01

# %%
activity_area_gdf.loc[activity_area_gdf.ELI_TYPE == "AUD"].to_crs(
    "EPSG:3123"
).area.sum() / 10_000

# %%
lcc_252 = gpd.read_file(TMP_OUT_DIR / "lcc_252.gpkg")

# %%
lcc_252_forest = lcc_252.loc[lcc_252["DN"].isin([1, 4])].copy()

# %%
cadt252_lcc = gpd.overlay(cadt_252, lcc_252_forest, how="intersection", make_valid=True)

# %%
cadt252_lcc["area_ha"] = cadt252_lcc.to_crs("EPSG:3123").area / 10_000

# %%
cadt252_lcc.loc[cadt252_lcc.DN.isin([1, 4])].groupby(["ELI_TYPE", "DN"])[
    "area_ha"
].sum().reset_index()

# %%
cadt_252["area_ha"] = cadt_252.to_crs("EPSG:3123").area / 10_000

# %%
8325.38 + 4968.13 + 79127.77

# %%
cadt_252.groupby("ELI_TYPE")["area_ha"].sum()

# %%
activity_area_gdf["area_ha"] = activity_area_gdf.to_crs("EPSG:3123").area / 10_000

# %%
activity_area_gdf.groupby("ELI_TYPE")["area_ha"].sum()

# %%
aud_subset_252 = cadt252_lcc[cadt252_lcc["ELI_TYPE"] == "AUD"].copy()

# %%
aud_subset_252.plot()

# %%
aud_risk_buffer.plot()

# %%
aud_buffered_252 = gpd.overlay(
    aud_subset_252,
    aud_risk_buffer,
    how="difference",
    keep_geom_type=False,
    make_valid=True,
)

# %%
aud_buffered_252.plot()

# %%
aud_buffered_252

# %%
aud_risk_buffer

# %%
aud_buffered_252["area_ha"] = aud_buffered_252.to_crs("EPSG:3123").area / 10_000

# %%
aud_buffered_252.loc[aud_buffered_252.DN.isin([1, 4])].groupby(["ELI_TYPE", "DN"])[
    "area_ha"
].sum().reset_index()
