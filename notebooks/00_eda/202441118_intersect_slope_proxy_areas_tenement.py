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
# Standard Imports
import sys
import pandas as pd
import geopandas as gpd
from shapely.wkt import loads
from tqdm import tqdm

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import GPKG_DATA_DIR, TMP_OUT_DIR
from src.duckdb_utils import create_default_connection

# %% [markdown]
# ## Filepaths

# %%
slope_fpath = GPKG_DATA_DIR / "reclass_slope_linework.gpkg"
elev_fpath = GPKG_DATA_DIR / "caraga-davao-reclassified-elevation.gpkg"
strata_fpath = GPKG_DATA_DIR / "BL-PL" / "caraga-davao-strata-fixed.gpkg"
geotype_fpath = GPKG_DATA_DIR / "BL-PL" / "caraga-davao-ultramafic-rocks.gpkg"
proxy_fpath = GPKG_DATA_DIR / "BL-PL" / "davao_CADT_mining.gpkg"
hrp_fpath = (
    GPKG_DATA_DIR / "BL-PL" / "caraga-davao-gee_deforestation_activity_data.gpkg"
)
hrp2_fpath = (
    GPKG_DATA_DIR
    / "BL-PL"
    / "caraga-davao-gee_deforestation_activity_data_2nd_half_hrp.gpkg"
)
output_slope_fpath = GPKG_DATA_DIR / "davao_proxy_slope.gpkg"
output_elevation_fpath = GPKG_DATA_DIR / "davao-proxy-elevation.gpkg"

# %% [markdown]
# ## Create/open duckdb instance

# %%
# Connect to the duckdb database
db = create_default_connection(filepath=str(TMP_OUT_DIR / "proxy_area.db"))

# %%
query = "SELECT name FROM sqlite_master WHERE type='table';"
db.execute(query).df()

# %% [markdown]
# ## Read GPKGs

# %%
slope_reclass = gpd.read_file(slope_fpath)
proxy = gpd.read_file(proxy_fpath)
elev_reclass = gpd.read_file(elev_fpath)
geo = gpd.read_file(geotype_fpath)
strata_gdf = gpd.read_file(strata_fpath)
hrp = gpd.read_file(hrp_fpath)

# %% [markdown]
# ## Proxy Area

# %%
proxy.head(2)

# %%
proxy.shape

# %%
proxy.CADT_No = proxy.CADT_No.fillna(0)

# %%
proxy = proxy.reset_index()

# %%
proxy["uid"] = (
    proxy["index"].astype(str)
    + "-"
    + proxy["CADT_No"].astype(int).astype(str)
    + "-"
    + proxy["ID_CODE"]
)

# %%
proxy_df = proxy.copy()

# %%
proxy_df["geometry"] = proxy_df["geometry"].to_wkt()

# %%
proxy_df.columns

# %%
query = """
CREATE OR REPLACE TABLE proxy AS 
SELECT 
    uid,
    CADT_No,
    Davao_CADT_No,
    HOLDER,
    TENEMENT_N, 
    DATE_FILED, 
    DATE_APPRO, 
    COMMODITY, 
    REMARKS_2,
    EXPIRYDATE,
    ST_GeomFromText(geometry) as geometry FROM proxy_df
"""

db.execute(query)

# %%
total_area = proxy.to_crs("EPSG:3857").geometry.area.sum() / 10_000

# %%
proxy["area_ha"] = proxy.to_crs("EPSG:3857").geometry.area / 10_000

# %%
tenement_area = proxy[["TENEMENT_N", "area_ha"]].groupby("TENEMENT_N").sum()

# %%
tenement_area

# %%
proxy.Davao_CADT_No.nunique()

# %% [markdown]
# ## Slope

# %%
slope_reclass.head(2)

# %%
slope_reclass_df = slope_reclass.copy()

# %%
slope_reclass_df["geometry"] = slope_reclass_df["geometry"].to_wkt()

# %%
query = """
CREATE OR REPLACE TABLE slope AS 
SELECT 
    DN,
    ST_GeomFromText(geometry) as geometry FROM slope_reclass_df
"""

db.execute(query)

# %%
query = """ 
CREATE OR REPLACE TABLE proxy_slope AS 
SELECT
    uid,
    DN,
    CADT_No,
    Davao_CADT_No,
    HOLDER,
    TENEMENT_N, 
    DATE_FILED, 
    DATE_APPRO, 
    COMMODITY, 
    REMARKS_2,
    EXPIRYDATE,
    ST_INTERSECTION(proxy.geometry, slope.geometry) as geometry
FROM
    proxy 
JOIN 
    slope
ON 
    ST_INTERSECTS(proxy.geometry, slope.geometry)"""

db.execute(query).df()

# %%
query = """ 
SELECT
   *EXCLUDE(geometry),
   ST_astext(geometry) as geometry
FROM
    proxy_slope 
"""

proxy_slope = db.execute(query).df()

# %%
proxy_slope.head()

# %%
proxy_slope_gdf = proxy_slope.copy()

# %%
proxy_slope_gdf["geometry"] = proxy_slope_gdf["geometry"]

# %%
proxy_slope_gdf["geometry"] = proxy_slope_gdf["geometry"].apply(lambda x: loads(x))
proxy_slope_gdf = gpd.GeoDataFrame(
    proxy_slope_gdf, geometry="geometry", crs="EPSG:4326"
)

# %%
proxy_slope_gdf.to_crs("EPSG:3857", inplace=True)

# %%
proxy_slope_gdf["area_ha"] = proxy_slope_gdf.geometry.area / 10_000

# %%
slope_profile = (
    proxy_slope_gdf[["TENEMENT_N", "DN", "area_ha"]].groupby(["DN", "TENEMENT_N"]).sum()
)

# %%
slope_profile_mng = slope_profile.join(tenement_area, lsuffix="_slope")

# %%
slope_profile_mng["perc"] = (
    slope_profile["area_ha"] / slope_profile_mng["area_ha"]
) * 100

# %%
slope_profile_mng.reset_index(inplace=True)
slope_profile_mng.set_index("TENEMENT_N", inplace=True)

# %%
slope_profile_mng.rename(columns={"DN": "slope_dn"}, inplace=True)

# %%
slope_profile_mng.reset_index(inplace=True)

# %%
slope_profile_mng = (
    slope_profile_mng.pivot(index="TENEMENT_N", columns="slope_dn", values="perc")
    .fillna(0)
    .reset_index()
)

# %%
slope_profile_mng

# %% [markdown]
# ## Elevation

# %%
elev_reclass_df = elev_reclass.copy()

# %%
elev_reclass_df["geometry"] = elev_reclass_df["geometry"].to_wkt()

# %%
query = """
CREATE OR REPLACE TABLE elev AS 
SELECT 
    DN,
    ST_GeomFromText(geometry) as geometry FROM elev_reclass_df
"""

db.execute(query)

# %%
query = """ 
CREATE OR REPLACE TABLE proxy_elev AS
SELECT
    uid,
    CADT_No,
    Davao_CADT_No,
    HOLDER,
    TENEMENT_N, 
    DATE_FILED, 
    DATE_APPRO, 
    COMMODITY, 
    REMARKS_2,
    EXPIRYDATE,
    elev.DN as elev_dn,
    ST_INTERSECTION(proxy.geometry, elev.geometry) as geometry
FROM
    proxy
JOIN 
    elev
ON 
    ST_INTERSECTS(proxy.geometry, elev.geometry)"""

db.execute(query).df()

# %%
query = """ 
SELECT
   *EXCLUDE(geometry),
   ST_astext(geometry) as geometry
FROM
    proxy_elev 
"""

proxy_elev = db.execute(query).df()

# %%
proxy_elev.head(2)

# %%
proxy_elev_gdf = proxy_elev.copy()

# %%
proxy_elev_gdf["geometry"] = proxy_elev_gdf["geometry"].apply(lambda x: loads(x))
proxy_elev_gdf = gpd.GeoDataFrame(proxy_elev_gdf, geometry="geometry", crs="EPSG:4326")

# %%
proxy_elev_gdf.to_crs("EPSG:3857", inplace=True)

# %%
proxy_elev_gdf["area_ha"] = proxy_elev_gdf.geometry.area / 10_000

# %%
elev_profile = (
    proxy_elev_gdf[["elev_dn", "TENEMENT_N", "area_ha"]]
    .groupby(["elev_dn", "TENEMENT_N"])
    .sum()
)

# %%
elev_profile_mng = elev_profile.join(tenement_area, lsuffix="_elev")

# %%
elev_profile_mng["perc"] = (
    elev_profile_mng["area_ha_elev"] / elev_profile_mng["area_ha"]
) * 100

# %%
elev_profile_mng.reset_index(inplace=True)

# %%
elev_profile_mng = (
    elev_profile_mng.pivot(index="TENEMENT_N", columns="elev_dn", values="perc")
    .fillna(0)
    .reset_index()
)

# %% [markdown]
# ## Geology

# %%
geo_df = geo.copy()

# %%
geo_df["geometry"] = geo["geometry"].to_wkt()

# %%
geo_df.head()

# %%
query = """
CREATE OR REPLACE TABLE geotype AS 
SELECT 
    *EXCLUDE(geometry),
    ST_GeomFromText(geometry) as geometry FROM geo_df
"""

db.execute(query)

# %%
query = """ 
CREATE OR REPLACE TABLE proxy_geo AS
SELECT
    uid,
    CADT_No,
    Davao_CADT_No,
    HOLDER,
    TENEMENT_N, 
    DATE_FILED, 
    DATE_APPRO, 
    COMMODITY, 
    REMARKS_2,
    EXPIRYDATE,
    Litho,
    remarks,
    ST_INTERSECTION(proxy.geometry, geotype.geometry) as geometry
FROM
    proxy
JOIN 
    geotype
ON 
    ST_INTERSECTS(proxy.geometry, geotype.geometry)"""

db.execute(query).df()

# %%
query = """ 
SELECT
   *EXCLUDE(geometry),
   ST_astext(geometry) as geometry
FROM
    proxy_geo
"""

proxy_geo = db.execute(query).df()

# %%
proxy_geo.head(2)

# %%
proxy_geo_gdf = proxy_geo.copy()

# %%
proxy_geo_gdf["geometry"] = proxy_geo_gdf["geometry"].apply(lambda x: loads(x))
proxy_geo_gdf = gpd.GeoDataFrame(proxy_geo_gdf, geometry="geometry", crs="EPSG:4326")

# %%
proxy_geo_gdf.to_crs("EPSG:3857", inplace=True)

# %%
proxy_geo_gdf["area_ha"] = proxy_geo_gdf.geometry.area / 10_000

# %%
geo_profile = (
    proxy_geo_gdf[["Litho", "TENEMENT_N", "area_ha"]]
    .groupby(["Litho", "TENEMENT_N"])
    .sum()
)

# %%
geo_profile_mng = geo_profile.join(tenement_area, lsuffix="_geotype")

# %%
geo_profile_mng["perc_geotype"] = (
    geo_profile_mng["area_ha_geotype"] / geo_profile_mng["area_ha"]
) * 100

# %%
geo_profile_mng.reset_index(inplace=True)

# %%
geo_profile_mng = (
    geo_profile_mng.pivot(index="TENEMENT_N", columns="Litho", values="perc_geotype")
    .fillna(0)
    .reset_index()
)

# %% [markdown]
# ## Strata

# %%
strata_gdf.to_crs("EPSG:4326", inplace=True)

# %%
strata_df = strata_gdf.copy()

# %%
strata_df["geometry"] = strata_df["geometry"].to_wkt()

# %%
strata_df.head(2)

# %%
query = """
CREATE OR REPLACE TABLE strata_type AS 
SELECT 
    *EXCLUDE(geometry),
    ST_GeomFromText(geometry) as geometry FROM strata_df
"""

db.execute(query)

# %%
query = """ 
CREATE OR REPLACE TABLE proxy_strata AS
SELECT
    uid,
    CADT_No,
    Davao_CADT_No,
    HOLDER,
    TENEMENT_N, 
    DATE_FILED, 
    DATE_APPRO, 
    COMMODITY, 
    REMARKS_2,
    EXPIRYDATE,
    strata,
    ST_INTERSECTION(proxy.geometry, strata_type.geometry) as geometry
FROM
    proxy
JOIN 
    strata_type
ON 
    ST_INTERSECTS(proxy.geometry, strata_type.geometry)"""

db.execute(query).df()

# %%
query = """ 
SELECT
   *EXCLUDE(geometry),
   ST_astext(geometry) as geometry
FROM
    proxy_strata
"""

proxy_strata = db.execute(query).df()

# %%
proxy_strata.head(2)

# %%
proxy_strata_gdf = proxy_strata.copy()

# %%
proxy_strata_gdf["geometry"] = proxy_strata_gdf["geometry"].apply(lambda x: loads(x))
proxy_strata_gdf = gpd.GeoDataFrame(
    proxy_strata_gdf, geometry="geometry", crs="EPSG:4326"
)

# %%
proxy_strata_gdf.to_crs("EPSG:3857", inplace=True)

# %%
proxy_strata_gdf["area_ha"] = proxy_strata_gdf.geometry.area / 10_000

# %%
strata_profile = (
    proxy_strata_gdf[["strata", "TENEMENT_N", "area_ha"]]
    .groupby(["strata", "TENEMENT_N"])
    .sum()
)

# %%
strata_profile_mng = strata_profile.join(tenement_area, lsuffix="_strata")

# %%
strata_profile_mng["perc_strata"] = (
    strata_profile_mng["area_ha_strata"] / strata_profile_mng["area_ha"]
) * 100

# %%
strata_profile_mng.reset_index(inplace=True)

# %%
strata_profile_mng

# %%
strata_profile_mng = strata_profile_mng.pivot(
    index="TENEMENT_N", columns="strata", values="perc_strata"
).fillna(0)

# %%
strata_profile_mng.reset_index(inplace=True)

# %% [markdown]
# ## HRP

# %%
hrp.FOR_NFOR.unique()

# %%
hrp_df = hrp.copy()

# %%
hrp_df["geometry"] = hrp["geometry"].to_wkt()

# %%
hrp_df.head(2)

# %%
hrp_df.reset_index(inplace=True)

# %%
query = """
CREATE OR REPLACE TABLE hrp AS 
SELECT 
    *EXCLUDE(geometry),
    ST_GeomFromText(geometry) as geometry FROM hrp_df
"""

db.execute(query)

# %%
query = """ 
CREATE OR REPLACE TABLE proxy_hrp AS
SELECT
    uid,
    CADT_No,
    Davao_CADT_No,
    HOLDER,
    TENEMENT_N, 
    DATE_FILED, 
    DATE_APPRO, 
    COMMODITY, 
    REMARKS_2,
    EXPIRYDATE,
    FOR_NFOR,
    ST_INTERSECTION(proxy.geometry, hrp.geometry) as geometry
FROM
    proxy
JOIN 
    hrp
ON 
    ST_INTERSECTS(proxy.geometry, hrp.geometry)"""

db.execute(query).df()

# %%
query = """ 
SELECT
   *EXCLUDE(geometry),
   ST_astext(geometry) as geometry
FROM
    proxy_hrp
"""

proxy_hrp = db.execute(query).df()

# %%
proxy_hrp.head(2)

# %%
proxy_hrp_gdf = proxy_hrp.copy()

# %%
proxy_hrp_gdf["geometry"] = proxy_hrp_gdf["geometry"].apply(lambda x: loads(x))
proxy_hrp_gdf = gpd.GeoDataFrame(proxy_hrp_gdf, geometry="geometry", crs="EPSG:4326")

# %%
proxy_hrp_gdf.to_crs("EPSG:3857", inplace=True)

# %%
proxy_hrp_gdf["area_ha"] = proxy_hrp_gdf.geometry.area / 10_000

# %%
hrp_profile = (
    proxy_hrp_gdf[["FOR_NFOR", "TENEMENT_N", "area_ha"]]
    .groupby(["FOR_NFOR", "TENEMENT_N"])
    .sum()
)

# %%
hrp_profile_mng = hrp_profile.join(tenement_area, lsuffix="_fornfor")

# %%
hrp_profile_mng["perc_fornfor"] = (
    hrp_profile_mng["area_ha_fornfor"] / hrp_profile_mng["area_ha"]
) * 100

# %%
hrp_profile_mng.reset_index(inplace=True)

# %%
hrp_profile_mng

# %%
hrp_profile_mng = hrp_profile_mng.pivot(
    index="TENEMENT_N", columns="FOR_NFOR", values="perc_fornfor"
).fillna(0)

# %%
hrp_profile_mng.reset_index(inplace=True)

# %%
hrp_profile_mng

# %% [markdown]
# # Get pivot table

# %%
slope_profile_mng

# %%
slope_profile_mng.rename(columns={0: "below_15", 1: "above_15"}, inplace=True)

# %%
elev_profile_mng

# %%
elev_profile_mng.rename(
    columns={1: "below_500", 2: "500_1000", 3: "1000_1500", 4: "above_1500"},
    inplace=True,
)

# %%
geo_profile_mng

# %%
strata_profile_mng

# %%
pivot_df = (
    proxy.merge(strata_profile_mng, how="left")
    .merge(geo_profile_mng, how="left")
    .merge(elev_profile_mng, how="left")
    .merge(slope_profile_mng, how="left")
    .merge(hrp_profile_mng, how="left")
)

# %%
pivot_df.columns

# %%
pivot_df.to_csv(GPKG_DATA_DIR / "tenement_percentage_profile.csv")

# %%
pivot_df = pd.read_csv(GPKG_DATA_DIR / "tenement_percentage_profile.csv")

# %%
strata_1 = (pivot_df["pre_strata_1"] >= 40.88) & (pivot_df["pre_strata_1"] <= 61.32)

# %%
strata_2 = (pivot_df["pre_strata_2"] >= 24.88) & (pivot_df["pre_strata_2"] <= 37.32)

# %%
strata_3 = (pivot_df["pre_strata_3"] >= 5.92) & (pivot_df["pre_strata_3"] <= 8.88)

# %%
pivot_df[strata_1 & strata_2 & strata_3]

# %% [markdown]
# # Drafts

# %%
hrp.reset_index(inplace=True)
hrp.rename(columns={"index": "index_hrp1"}, inplace=True)

# %%
hrp.head(2)

# %%
slope_reclass.reset_index(inplace=True)
slope_reclass.rename(columns={"DN": "DN_slope", "index": "index_slope"}, inplace=True)

# %%
slope_reclass.head()

# %%
elev_reclass.reset_index(inplace=True)
elev_reclass.rename(columns={"DN": "DN_elev", "index": "index_elev"}, inplace=True)

# %%
elev_reclass.head(2)

# %%
geo.reset_index(inplace=True)
geo.rename(columns={"index": "index_geotype"}, inplace=True)

# %%
geo.columns

# %%
geotype = geo[["index_geotype", "Litho", "remarks", "geometry"]].copy()

# %%
geotype.head(2)

# %%
strata_gdf.reset_index(inplace=True)
strata_gdf.rename(columns={"index": "index_strata"}, inplace=True)

# %%
strata_type = strata_gdf[["index_strata", "strata", "geometry"]].copy()

# %%
strata_type.head(2)

# %%
union_result = gpd.overlay(
    proxy, slope_reclass, how="intersection", keep_geom_type=False
)

# %%
union_result = gpd.overlay(
    union_result, elev_reclass, how="intersection", keep_geom_type=False
)

# %%
union_result.to_csv(GPKG_DATA_DIR / "union_table_slope_elev.csv")

# %%
union_result = union_result[
    union_result.geometry.type.isin(["Polygon", "MultiPolygon", "GeometryCollection"])
]

# %%
union_result = gpd.read_file(GPKG_DATA_DIR / "BL-PL" / "union_elev_slope.gpkg")

# %%
union_result.columns

# %%
union_result.drop(
    columns=[
        "Remarks",
        "field_1",
        "fid_2",
    ],
    inplace=True,
)

# %%
union_result.to_file(GPKG_DATA_DIR / "BL-PL" / "union_geo_slope_elev.gpkg")

# %%
union_result = gpd.read_file(GPKG_DATA_DIR / "BL-PL" / "union_geo_slope_elev.gpkg")

# %%
union_result.shape

# %%
# %%time
# List of GeoDataFrames
gdfs = [strata_type, hrp]
for gdf in tqdm(gdfs):
    union_result = gpd.overlay(union_result, gdf, how="union")

# %%
union_result.geometry.type.unique()

# %%
union_result.to_csv(GPKG_DATA_DIR / "union_table_all.csv")

# %% [markdown]
# # Create Grid

# %%
from geowrangler import grids

# %%
aoi = proxy.to_crs("EPSG:3857").copy()

# %%
grid_generator5k = grids.SquareGridGenerator(5_000)

# %%
grid_gdf5k = grid_generator5k.generate_grid(aoi)

# %%
grid_gdf5k.plot()

# %%
grid_gdf5k.to_crs("EPSG:4326", inplace=True)

# %%
grid_gdf5k_df = grid_gdf5k.copy()

# %%
grid_gdf5k_df["geometry"] = grid_gdf5k_df["geometry"].to_wkt()

# %%
query = """
CREATE OR REPLACE TABLE grid_5k AS 
SELECT 
    *EXCLUDE(geometry),
    ST_GeomFromText(geometry) as geometry FROM grid_gdf5k_df
"""

db.execute(query)

# %%
query = """ 
CREATE OR REPLACE TABLE proxy_grid AS 
SELECT
    uid,
    x,
    y,
    CADT_No,
    Davao_CADT_No,
    HOLDER,
    TENEMENT_N, 
    DATE_FILED, 
    DATE_APPRO, 
    COMMODITY, 
    REMARKS_2,
    EXPIRYDATE,
    ST_INTERSECTION(proxy.geometry, grid_5k.geometry) as geometry
FROM
    proxy 
JOIN 
    grid_5k
ON 
    ST_INTERSECTS(proxy.geometry, grid_5k.geometry)"""

db.execute(query).df()

# %%
query = """ 
SELECT
   *EXCLUDE(geometry),
   ST_astext(geometry) as geometry
FROM
    proxy_grid 
"""

proxy_grid = db.execute(query).df()

# %%
proxy_grid.head()

# %%
proxy_grid_gdf = proxy_grid.copy()

# %%
proxy_grid_gdf["geometry"] = proxy_grid_gdf["geometry"]

# %%
proxy_grid_gdf["geometry"] = proxy_grid_gdf["geometry"].apply(lambda x: loads(x))
proxy_grid_gdf = gpd.GeoDataFrame(proxy_grid_gdf, geometry="geometry", crs="EPSG:4326")

# %%
proxy_grid_gdf.explore()

# %%
proxy_grid_gdf.drop_duplicates(subset=["geometry"], inplace=True)
