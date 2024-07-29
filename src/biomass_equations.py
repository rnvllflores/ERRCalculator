import numpy as np
import pandas as pd
from scipy.stats import t
from scipy.stats import norm


# height model
def calculate_tree_height(df: pd.DataFrame, 
                          dbh_column: str = np.nan, 
                          trig_leveling: bool = False, 
                          dist_col: str = np.nan, 
                          slope_b_col: str = np.nan,
                          slope_t_col: str = np.nan) -> pd.DataFrame:
    """
    Calculates the height of trees based on the diameter at breast height (DBH).
    The equation is based on T. R. Feldpausch, et al. which assumes Ht = 35.83 − 31.15 × exp(−0.029 × DBH)

    References:
    Feldpausch TR, Banin L, Phillips OL, Baker TR, Lewis SL, Quesada CA, et al. Height-diameter allome- try of tropical forest trees. Biogeosciences. 2011; 8: 1081–1106. https://doi.org/10.5194/bg-8-1081- 2011

    Parameters:
    - df (pandas.DataFrame): The input DataFrame containing the tree data.
    - dbh_column (str): The name of the column in the DataFrame that represents the DBH.

    Returns:
    - df (pandas.DataFrame): The input DataFrame with an additional 'height' column representing the calculated tree height.
    """
    
    df = df.copy()
    
    if not trig_leveling:
        height_column = np.minimum(35.83 - 31.15 * np.exp(-0.029 * df[dbh_column]), 30)
        df["height"] = height_column
    else:
        df["height"] = np.where(
        df[slope_t_col] > df[slope_b_col],
        df[dist_col] * np.tan(np.tan(df[slope_t_col] - df[slope_b_col])),
        df[dist_col] * np.tan(np.tan(df[slope_b_col] - df[slope_t_col]))
)
    return df


def allometric_tropical_tree(df, wooddensity_col, dbh_col, height_col):
    """
    Calculates the aboveground biomass of tropical trees using allometric equation based on Chave, et al. (2015) which assumes
    10 * (0.0673 * ((wood density * height * dbh^2)^0.976)). The output is divided by 1000 to convert from kg to metric tonnes.

    References:
    Chave, Jérôme & Réjou-Méchain, Maxime & Burquez, Alberto & Chidumayo, Emmanuel & Colgan, Matthew & Delitti, Welington & Duque, Alvaro & Eid, Tron & Fearnside, Philip & Goodman, Rosa & Henry, Matieu & Martinez-Yrizar, Angelina & Mugasha, Wilson & Muller-Landau, Helene & Mencuccini, Maurizio & Nelson, Bruce & Ngomanda, Alfred & Nogueira, Euler & Ortiz, Edgar & Vieilledent, Ghislain. (2014). Improved allometric models to estimate the aboveground biomass of tropical trees. Global Change Biology. 20. 3177-3190. 10.1111/gcb.12629.
    https://forestgeo.si.edu/sites/default/files/aboveground_biomass_protocol_accessible.pdf
    Parameters:
    - df (pandas.DataFrame): The input dataframe containing tree data.
    wooddensity_col (str): The column name in the dataframe that represents wood density in grams per cubic cm.
    dbh_col (str): The column name in the dataframe that represents diameter at breast height in cm.
    height_col (str): The column name in the dataframe that represents tree height in m.

    Formula:
    The aboveground biomass is calculated using the following equation:
    aboveground_biomass = (0.0673 * ((wood density * height * dbh** 2) ** 0.976)). The factor 10 is used to convert the biomass from kg to metric tons.

    Returns:
    pandas.DataFrame: The input dataframe with an additional column 'aboveground_biomass' representing the calculated aboveground biomass in tons.
    """

    df = df.copy()
    df["aboveground_biomass"] = (
        0.0673 * ((df[wooddensity_col] * df[height_col] * df[dbh_col] ** 2) ** 0.976)
    )

    return df


def allometric_peatland_tree(df, dbh_col):
    """
    Calculates the aboveground biomass of trees in a peatland using allometric equation based on Alibo, et al. (2012) which assumes
    (21.297 - (67.953 * DBH) + (0.74 * DBH^2)). The output is divided by 1000 to convert from kg to metric tonnes.

    References:
    Alibo, L.B. & Lasco, Rodel. (2012). Carbon Storage of Caimpugan Peatland in Agusan Marsh, Philippines and its Role in Greenhouse Gas Mitigation. Journal of Environmental Science and Management. 15. 50-58.
    https://www.researchgate.net/publication/283926446_Carbon_Storage_of_Caimpugan_Peatland_in_Agusan_Marsh_Philippines_and_its_Role_in_Greenhouse_Gas_Mitigation

    Parameters:
    - df: DataFrame
        The input DataFrame containing tree data.
    - dbh_col: str
        The name of the column in the DataFrame that represents the diameter at breast height (DBH) in cm.

    Returns:
    - df: DataFrame
        The input DataFrame with an additional column 'aboveground_biomass' representing the calculated aboveground biomass.

    Formula:
    The aboveground biomass is calculated using the following equation:
    aboveground_biomass = (21.297 - (67.953 * trees[dbh_col]) + (0.74 * trees[dbh_col]**2)) The factor 10 is used to convert the biomass from kg to metric tons.

    """
    df = df.copy()
    df["aboveground_biomass"] = (
        21.297 - 67.953 * df[dbh_col] + 0.74 * df[dbh_col] ** 2
    )
    return df

def get_solid_diamter(df: pd.DataFrame, 
                       hollow_diameter_1_col: str, 
                       hollow_diameter_2_col: str,
                       diameter_col: str) -> pd.DataFrame:
    df = df.copy()

    #Get area of ldw based on the diameter
    ldw_area = np.pi * (df[diameter_col]/2)**2

    # Get the average of the two hollow diameters
    avg_hollow_diameter = df[[hollow_diameter_1_col, hollow_diameter_2_col]].mean(axis=1)

    # Get the area of the hollow
    hollow_area = np.pi * (avg_hollow_diameter/2)**2

    # Get the area of the solid part of the ldw
    solid_area = ldw_area - hollow_area

    # Get the diameter of the solid part of the ldw
    df['solid_diameter'] = np.sqrt((solid_area/np.pi))*2

    return df

# Draft that uses t-distribution
def calculate_statistics(df, column, confidence=0.95):
    # Calculate weighted mean
    weights = df['subplot_count']
    weighted_mean = np.average(df[column], weights=weights)
    
    # Calculate variance and standard deviation
    variance = np.average((df[column] - weighted_mean)**2, weights=weights)
    weighted_std = np.sqrt(variance)
    
    # Calculate the total number of subplots
    total_subplots = weights.sum()
    
    # Calculate standard error
    standard_error = weighted_std / np.sqrt(total_subplots)
    
    # Determine the critical value (z or t-score)
    df_deg_of_freedom = total_subplots - 1
    critical_value = t.ppf((1 + confidence) / 2., df_deg_of_freedom) # Use t-distribution
    
    # Calculate margin of error
    margin_of_error = critical_value * standard_error
    
    # Calculate confidence interval
    confidence_interval = (weighted_mean - margin_of_error, weighted_mean + margin_of_error)
    
    return {
        'weighted_mean': weighted_mean,
        'weighted_std': weighted_std,
        'standard_error': standard_error,
        'margin_of_error': margin_of_error,
        'confidence_interval_lower': confidence_interval[0],
        'confidence_interval_upper': confidence_interval[1]
    }

def calculate_statistics(df, column, confidence=0.95):
    # Calculate weighted mean
    weights = df['subplot_count']
    weighted_mean = np.average(df[column], weights=weights)
    
    # Calculate variance and standard deviation
    variance = np.average((df[column] - weighted_mean)**2, weights=weights)
    weighted_std = np.sqrt(variance)
    
    # Calculate the total number of subplots
    total_subplots = weights.sum()
    
    # Calculate standard error
    standard_error = weighted_std / np.sqrt(total_subplots)
    se_perc_mean = ((weighted_std / np.sqrt(total_subplots)) / weighted_mean) * 100

    # Calculate confidence intervals
    ci_val = norm.ppf(.95) * standard_error
    
    # Calculate margin of error
    margin_of_error_per_90 = (ci_val / weighted_mean) * 100
    margin_of_error_per_95 = ((norm.ppf(.975) * standard_error) / weighted_mean) * 100
    
    # Calculate confidence interval
    confidence_interval = (weighted_mean - ci_val, weighted_mean + ci_val)
    
    return {
        'weighted_mean': weighted_mean,
        'confidence_interval_lower': confidence_interval[0],
        'confidence_interval_upper': confidence_interval[1],
        'uncertainty_90': margin_of_error_per_90,
        'uncertainty_95': margin_of_error_per_95,
        'margin_of_error': ci_val,
        'weighted_std': weighted_std,
        'standard_error': standard_error,
        'standard_error_perc_mean': se_perc_mean,
    }

def vmd0001_eq1(
    df: pd.DataFrame,
    carbon_fraction: float = 0.47,
    is_sapling: bool = False,
    sapling_cnt: str = "count_saplings",
    wc: float = 0.25,
    avg_weight: float = 0.000184,
):
    """
    Calculate the carbon stock based on the aboveground biomass and carbon fraction.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the aboveground biomass.
    - carbon_fraction (float, optional): The carbon fraction. Default value is 0.47.
    - is_sapling (bool, optional): Flag indicating if the calculation is for saplings. Default value is False.
    - sapling_cnt (str, optional): The column name in the DataFrame representing the count of saplings. Default value is 'count_saplings'.
    - wc (float, optional): The wood carbon content. Default value is 0.25.
    - avg_weight (float, optional): The average weight of the saplings. Default value is 0.000184 tonnes.

    Returns:
    - DataFrame: The input data with additional columns for carbon stock.
    """
    df = df.copy()

    if not is_sapling:
        df["aboveground_carbon_tonnes"] = df["aboveground_biomass"] * carbon_fraction

    else:
        df["aboveground_carbon_tonnes"] = (
            df[sapling_cnt] * avg_weight * wc
        ) * carbon_fraction
        df["aboveground_carbon_tonnes"] = df["aboveground_carbon_tonnes"].fillna(0)

    return df

def vmd0001_eq2a(df: pd.DataFrame, agg_cols: list, tc_col: str):
    """
    Perform aggregation and summation on a calculated biomass based on specified columns.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    agg_cols (list): A list of columns to group by and aggregate.
    tc_col (str): The column containing the values to be summed.

    Returns:
    pd.DataFrame: The resulting DataFrame after aggregation and summation.
    """

    subset = agg_cols.copy()
    subset.extend([tc_col])
    df = df[subset].copy()
    df = df.groupby(agg_cols).sum().reset_index()

    return df

def vmd0001_eq2b(
    df: pd.DataFrame,
    biomass_col: str = "aboveground_carbon_tonnes",
    area_col: str = "corrected_sapling_area_m2",
) -> pd.DataFrame:
    """
    Calculate CO2e per hectare based on biomass and area.

    Args:
        df (pd.DataFrame): The input DataFrame containing biomass and area columns.
        biomass_col (str, optional): The column name of the biomass data. Defaults to "aboveground_carbon_tonnes".
        area_col (str, optional): The column name of the area data. Defaults to "corrected_sapling_area_m2".

    Returns:
        pd.DataFrame: The input DataFrame with an additional column "CO2e_per_ha" representing CO2e per hectare.
    """
    df = df.copy()
    df["CO2e_per_ha"] = ((df[biomass_col] / df[area_col])) * 44 / 12

    return df


def vmd0001_eq5(
    df: pd.DataFrame,
    carbon_stock_col: str = "aboveground_carbon_tonnes",
) -> pd.DataFrame:
    """
    Calculate the belowground carbon stock based on the aboveground carbon stock and eco zone.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the aboveground carbon stock.
    - carbon_stock_col (str, optional): The column name of the aboveground carbon stock data. Default value is "aboveground_carbon_tonnes".
    - eco_zone (str, optional): The ecological zone. Default value is "tropical_rainforest".

    Returns:
    - DataFrame: The input data with an additional column for belowground carbon stock.
    
    References
    https://www.ipcc-nggip.iges.or.jp/public/2006gl/pdf/4_Volume4/V4_04_Ch4_Forest_Land.pdf
    """
    df = df.copy()

    df["belowground_carbon_tonnes"] = df[carbon_stock_col] * 0.36

    return df

def vmd0002_eq1(df: pd.DataFrame,
                diameter_col:str, 
                height_col:str,
                density_col:str) -> pd.DataFrame:
    df = df.copy()

    # Calculate the biomass of each tree
    df['tonnes_dry_matter'] = (1/3) * np.pi * (df[diameter_col]/200)**2 * df[height_col] * df[density_col]

    return df

def vmd0002_eq2(df: pd.DataFrame, base_diameter_col: str, top_diamter_col: str, height_col: str, density_col: str):
    """
    Calculate the biomass based on the given parameters.

    Parameters:
    df (pd.DataFrame): The input DataFrame.
    base_diameter_col (str): The column name for the base diameter.
    top_diamter_col (str): The column name for the top diameter.
    height_col (str): The column name for the height.
    density_col (str): The column name for the density.

    Returns:
    pd.DataFrame: The DataFrame with an additional 'biomass' column.
    """
    df = df.copy()
    df['tonnes_dry_matter'] = ((df[base_diameter_col] + df[top_diamter_col]) / 200) * df[height_col] * df[density_col]

    return df

def vmd0002_eq3(df:pd.DataFrame, agg_cols:list, tdm_col:str):
    subset = agg_cols.copy()
    subset.extend([tdm_col])
    df = df[subset].copy()
    df = df.groupby(agg_cols).sum().reset_index()
    return df

def vmd0002_eq4(df:pd.DataFrame, biomass_col:str, area_ha_col:str):
    df =df.copy()

    df['tonnes_dry_matter_ha'] = df[biomass_col] / df[area_ha_col]

    return df

def vmd0002_eq7(df: pd.DataFrame, diamter_col: str, transect_l: int = 100) -> pd.DataFrame:
    df = df.copy()
    df['deadwood_volume'] = ((df[diamter_col])**2) / (8 * transect_l)
    return df

def vmd0002_eq8a(df: pd.DataFrame, density_col: str, density_equivalent: dict = {1: 0.54, 2: 0.35, 3: 0.21}, default_density: float = 0.21) -> pd.DataFrame:
    df = df.copy()
    
    density = df[density_col].replace(density_equivalent).fillna(default_density)
    df['tonnes_dry_matter_ha'] = df['deadwood_volume'] * density
    return df

def vmd0002_eq8b(df: pd.DataFrame,
                agg_col:list,
                tdm_col: str = 'tonnes_dry_matter_ha',
                ) -> pd.DataFrame:
    df = df.copy()
    subset = agg_col.copy()
    subset.extend([tdm_col])

    df = df[subset]
    df = df.groupby(agg_col).sum().reset_index()

    return df

def vmd0002_eq9(df_stumps: pd.DataFrame, 
                df_ldw: pd.DataFrame, 
                df_sdw: pd.DataFrame,
                tdm_pattern: str = "tonnes_dry_matter_ha",
                carbon_fraction: float = 0.47):
    df = pd.merge(df_stumps, df_ldw, on="unique_id", how="outer")
    df = pd.merge(df, df_sdw, on="unique_id", how="outer")

    df['all_tonnes_dry_matter_ha'] = df.filter(like=tdm_pattern).sum(axis=1)
    df['tC_per_ha'] = df['all_tonnes_dry_matter_ha'] * carbon_fraction
    df['CO2e_per_ha'] = (df['all_tonnes_dry_matter_ha'] * carbon_fraction)* 44/12
    
    return df

def vmd0003_eq1(
    df: pd.DataFrame,
    kdm_col: str,
    water_content: float,
    carbon_fraction: float = 0.37,
):
    """
    Calculate the carbon stock from forest litter and non-tree vegetation based on VCS module VMD0003 - equation 1.


    Reference here:
    VCS Module: https://verra.org/wp-content/uploads/2023/11/VMD0003-Estimation-of-Carbon-Stocks-in-the-Litter-Pool-CP-L-v1.1.pdf
    Water content: Assumed to be 85% based on...
    Carbon fraction: Assumed to be 0.37 and 0.47 for ;litter and non-tree vegetation respectively based on...

    Parameters:
    data (DataFrame): The input data containing the plot information and biomass in kilograms.
    col_name (str): The name of the column for the biomass in kilograms.
    water_content (float): The water content of the biomass. Used as multiplier to remove water weight
    carbon_fraction (float): Carbon fraction of dry matter, default is 0.37 based on based on VMD0003.

    Returns:
    DataFrame: The input data with additional columns for dry biomass and carbon stock.
    """
    df = df.copy()

    # remove water content
    df["kg_dry_matter"] = df[kdm_col] * water_content

    # calculate carbon stock
    df["CO2e_per_ha"] = (
        (10 / 0.25) * df["kg_dry_matter"] * carbon_fraction * 44 / 12
    )

    return df
