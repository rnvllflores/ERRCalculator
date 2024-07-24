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
# # Calculate Biomass from Non-tree Vegetation and Litter

# %% [markdown]
# # Imports and Set-up

# %%
# Standard Imports
import sys
import pandas as pd

# Google Cloud Imports
import pandas_gbq

# %%
# Util imports
sys.path.append("../../")  # include parent directory
from src.settings import GCP_PROJ_ID, CARBON_POOLS_OUTDIR, CARBON_STOCK_OUTDIR

from src.biomass_equations import vmd0003_eq1

# %%
# Variables
NTV_LITTER_CSV = CARBON_POOLS_OUTDIR / "saplings_ntv_litter.csv"
PLOT_INFO_CSV = CARBON_POOLS_OUTDIR / "plot_info.csv"

# BigQuery Variables
SRC_DATASET_ID = "biomass_inventory"
DATASET_ID = "carbon_stock"
IF_EXISTS = "replace"

# %% [markdown]
# ## Load data

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
plot_info.info()

# %%
if NTV_LITTER_CSV.exists():
    ntv_litter = pd.read_csv(NTV_LITTER_CSV)
else:
    query = f"""
    SELECT 
        * 
    FROM {GCP_PROJ_ID}.{SRC_DATASET_ID}.saplings_ntv_litter"""

    # Read the BigQuery table into a dataframe
    ntv_litter = pandas_gbq.read_gbq(query, project_id=GCP_PROJ_ID)
    ntv_litter.to_csv(PLOT_INFO_CSV, index=False)

# %%
ntv_litter.info()

# %% [markdown]
# # Calculate carbon stock for litter

# %%
# get weight of bag contents - convert grams to kg
ntv_litter["litter_biomass_kg"] = (
    ntv_litter.ntv_sample_weight - ntv_litter.ntv_bag_weight
) / 1000

# %%
litter = ntv_litter[["unique_id", "litter_biomass_kg"]].copy()

# %%
litter = vmd0003_eq1(litter, "litter_biomass_kg", 0.15, 0.37)

# %%
litter.rename(
    columns={
        "CO2e_per_ha": "litter_CO2e_per_ha",
        "kg_dry_matter": "litter_kg_dry_matter",
    },
    inplace=True,
)

# %%
litter.head(2)

# %%
litter.info()

# %% [markdown]
# ## Export data and upload to BQ

# %%
if len(litter) != 0:
    litter.to_csv(CARBON_STOCK_OUTDIR / "litter_carbon_stock.csv", index=False)

# %%
# Upload to BQ
table_schema = [
    {"name": "unique_id", "type": "STRING"},
    {"name": "litter_biomass_kg", "type": "FLOAT64"},
    {"name": "litter_kg_dry_matter", "type": "FLOAT64"},
    {"name": "litter_CO2e_per_ha", "type": "FLOAT64"},
]
if len(litter) != 0:
    pandas_gbq.to_gbq(
        litter,
        f"{DATASET_ID}.litter_carbon_stock",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
        table_schema=table_schema,
    )

# %% [markdown]
# # Calculate carbon stock for non-tree vegetation

# %%
# get weight of bag contents - convert grams to kg
ntv_litter["ntv_biomass_kg"] = (
    ntv_litter.ntv_sample_weight - ntv_litter.litter_bag_weight
) / 1000

# %%
ntv = ntv_litter[["unique_id", "ntv_biomass_kg"]].copy()
ntv = vmd0003_eq1(ntv, "ntv_biomass_kg", 0.15, 0.47)

# %%
ntv.rename(
    columns={
        "CO2e_per_ha": "ntv_CO2e_per_ha",
        "kg_dry_matter": "ntv_kg_dry_matter",
    },
    inplace=True,
)

# %%
ntv.head(2)

# %%
ntv.info()

# %%
ntv.to_csv(CARBON_STOCK_OUTDIR / "ntv_carbon_stock.csv", index=False)

# %%
# Upload to BQ
if len(ntv) != 0:
    pandas_gbq.to_gbq(
        ntv,
        f"{DATASET_ID}.ntv_carbon_stock",
        project_id=GCP_PROJ_ID,
        if_exists=IF_EXISTS,
    )
