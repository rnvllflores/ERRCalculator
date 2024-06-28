import pandas as pd


def vmd0003_eq1(
    data: pd.DataFrame, col_name: str, water_content: float, carbon_fraction: float
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
    carbon_fraction (float): The carbon fraction.

    Returns:
    DataFrame: The input data with additional columns for dry biomass and carbon stock.
    """
    # remove water content
    data.loc[:, "dry_biomass"] = data[col_name] * water_content

    # calculate carbon stock
    data.loc[:, "carbon_stock"] = (
        (10 / 0.25) * data["dry_biomass"] * carbon_fraction * 44 / 12
    )

    return data
