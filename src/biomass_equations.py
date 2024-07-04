import numpy as np
import pandas as pd


# height model
def calculate_tree_height(df, dbh_column):
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
    height_column = np.minimum(35.83 - 31.15 * np.exp(-0.029 * df[dbh_column]), 30)
    df["height"] = height_column
    return df


def allometric_tropical_tree(df, wooddensity_col, dbh_col, height_col):
    """
    Calculates the aboveground biomass of tropical trees using allometric equation based on Chave, et al. (2015) which assumes
    10 * (0.0673 * ((wood density * height * dbh^2)^0.976))

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
    aboveground_biomass = 10 * (0.0673 * ((wood density * height * dbh** 2) ** 0.976)). The factor 10 is used to convert the biomass from kg to metric tons.

    Returns:
    pandas.DataFrame: The input dataframe with an additional column 'aboveground_biomass' representing the calculated aboveground biomass in tons.
    """
    df["aboveground_biomass"] = 10 * (
        0.0673 * ((df[wooddensity_col] * df[height_col] * df[dbh_col] ** 2) ** 0.976)
    )

    return df


def allometric_peatland_tree(df, dbh_col):
    """
    Calculates the aboveground biomass of trees in a peatland using allometric equation based on Alibo, et al. (2012) which assumes
    10 * (21.297 - (6.53 * DBH) + (0.74 * DBH^2))

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
    aboveground_biomass = 10 * (21.297 - (6.53 * trees[dbh_col]) + (0.74 * trees[dbh_col]**2)) The factor 10 is used to convert the biomass from kg to metric tons.

    """
    df["aboveground_biomass"] = 10 * (
        21.297 - (6.53 * df[dbh_col]) + (0.74 * df[dbh_col] ** 2)
    )
    return df


def vmd0001_eq1(
    df: pd.DataFrame,
    carbon_fraction: float = 0.47,
    is_sapling: bool = False,
    sapling_cnt: str = "count_saplings",
    wc: float = 0.25,
    avg_weight: float = 184,
):
    """
    Calculate the carbon stock based on the aboveground biomass and carbon fraction.

    Parameters:
    - df (pd.DataFrame): The input DataFrame containing the aboveground biomass.
    - carbon_fraction (float, optional): The carbon fraction. Default value is 0.47.
    - is_sapling (bool, optional): Flag indicating if the calculation is for saplings. Default value is False.
    - sapling_cnt (str, optional): The column name in the DataFrame representing the count of saplings. Default value is 'count_saplings'.
    - wc (float, optional): The wood carbon content. Default value is 0.25.
    - avg_weight (float, optional): The average weight of the saplings. Default value is 184.

    Returns:
    - DataFrame: The input data with additional columns for carbon stock.
    """

    if not is_sapling:
        df["aboveground_carbon_tonnes"] = df["aboveground_biomass"] * carbon_fraction

    else:
        df["aboveground_carbon_tonnes"] = (
            df[sapling_cnt] * avg_weight * wc
        ) * carbon_fraction
        df["aboveground_carbon_tonnes"] = df["aboveground_carbon_tonnes"].fillna(0)

    return df


def vmd0001_eq2(
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
    df["CO2e_per_ha"] = ((df[biomass_col] / df[area_col]) * 10) * 44 / 12

    return df


def vmd0001_eq5(
    df: pd.DataFrame,
    carbon_stock_col: str = "aboveground_carbon_tonnes",
    eco_zone: str = "tropical_rainforest",
):
    if eco_zone == "tropical_rainforest" or eco_zone == "subtropical_humid":
        df["below_ground_carbon_tonnes"] = np.where(
            df[carbon_stock_col] < 125,
            df[carbon_stock_col] * 0.20,
            df[carbon_stock_col] * 0.24,
        )
    elif eco_zone == "subtropical_dry":
        df["below_ground_carbon_tonnes"] = np.where(
            df[carbon_stock_col] < 20,
            df[carbon_stock_col] * 0.56,
            df[carbon_stock_col] * 0.28,
        )
    else:
        raise ValueError(
            "Invalid eco_zone value. Please choose a valid eco_zone or add root-to-shoot ratio for the desired ecological zone."
        )

    return df


def vmd0003_eq1(
    data: pd.DataFrame,
    col_name: str,
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
    # remove water content
    data.loc[:, "dry_biomass"] = data[col_name] * water_content

    # calculate carbon stock
    data.loc[:, "CO2e_per_ha"] = (
        (10 / 0.25) * data["dry_biomass"] * carbon_fraction * 44 / 12
    )

    return data
