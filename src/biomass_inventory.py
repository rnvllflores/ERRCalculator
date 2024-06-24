#  Imports
import numpy as np
import pandas as pd

# Functions used to format data retrieved from ONA


def extract_trees(data, nest_list):
    """
    Extracts tree data from the given DataFrame for a list of nest numbers.

    Parameters:
    - data (pd.DataFrame): The DataFrame containing the biomass inventory data retrieved from ONA.
    - nest_list (list): The list of nest numbers for which to extract tree data.

    Returns:
    - trees_nest (DataFrame): A DataFrame containing the extracted tree data for the specified nests.
    """
    # Initialize an empty DataFrame to store trees for all nests
    all_trees_nest = pd.DataFrame()

    for nest_number in nest_list:
        # Find the relevant columns for DBH, Live/Dead, Species Name, and Family Name
        dbh_columns = [col for col in data.columns if f"t_dbh_nest{nest_number}" in col]
        livedead_columns = [
            col for col in data.columns if f"livedead_nest{nest_number}" in col
        ]
        species_name_columns = [
            col for col in data.columns if f"t_species_name_nest{nest_number}" in col
        ]
        family_name_columns = [
            col for col in data.columns if f"t_family_name_nest{nest_number}" in col
        ]

        # Loop through each row of the data
        for i, row in data.iterrows():
            # Loop through each relevant column for this nest
            for j in range(len(dbh_columns)):
                try:
                    # Check if the tree is alive and not NA
                    if (
                        not pd.isna(row[livedead_columns[j]])
                        and row[livedead_columns[j]] == 1
                    ):
                        # Create a DataFrame row for this individual tree
                        tree = pd.DataFrame(
                            {
                                "unique_id": [row["unique_id"]],
                                "nest": [nest_number],
                                "species_name": [row[species_name_columns[j]]],
                                "family_name": [row[family_name_columns[j]]],
                                "DBH": [row[dbh_columns[j]]],
                            }
                        )

                        # Add this tree to the list of trees for all nests
                        all_trees_nest = pd.concat(
                            [all_trees_nest, tree], ignore_index=True
                        )
                except Exception as e:
                    # Handle any errors (e.g., NA values or missing trees)
                    # For now, simply continue to the next iteration
                    pass

    return all_trees_nest


def extract_stumps(data, nest_numbers):
    """
    Extracts stump data from the given DataFrame based on the specified nest numbers.

    Args:
        data (pd.DataFrame): The DataFrame containing the biomass inventory data retrieved from ONA.
        nest_numbers (list): A list of nest numbers to extract stump data for.

    Returns:
        pd.DataFrame: A DataFrame containing the extracted stump data.
    """
    all_stumps = pd.DataFrame()

    for nest_number in nest_numbers:
        # Existing diameter and height columns
        diameter1_cols = [
            col for col in data.columns if f"diameter1_nest{nest_number}" in col
        ]
        diameter2_cols = [
            col for col in data.columns if f"diameter2_nest{nest_number}" in col
        ]
        height_cols = [
            col for col in data.columns if f"height_st_nest{nest_number}" in col
        ]

        # New columns for hollow stumps
        stump_cut_cl_cols = [
            col for col in data.columns if f"stump_cut_cl_nest{nest_number}" in col
        ]
        stump_hollow_go_cols = [
            col for col in data.columns if f"stump_hollow_go_nest{nest_number}" in col
        ]
        stump_hollow_d1_cols = [
            col for col in data.columns if f"stump_hollow_d1_nest{nest_number}" in col
        ]
        stump_hollow_d2_cols = [
            col for col in data.columns if f"stump_hollow_d2_nest{nest_number}" in col
        ]
        stump_density_cols = [
            col for col in data.columns if f"stump_density_nest{nest_number}" in col
        ]

        stumps_nest = pd.DataFrame()

        for i in range(len(data)):
            for j in range(len(diameter1_cols)):
                Diam1 = pd.to_numeric(data.loc[i, diameter1_cols[j]], errors="coerce")
                Diam2 = pd.to_numeric(data.loc[i, diameter2_cols[j]], errors="coerce")
                height = pd.to_numeric(data.loc[i, height_cols[j]], errors="coerce")
                slope = pd.to_numeric(data.loc[i, "slope/slope"], errors="coerce")
                cut_cl = data.loc[i, stump_cut_cl_cols[j]]
                hollow_go = data.loc[i, stump_hollow_go_cols[j]]
                hollow_d1 = pd.to_numeric(
                    data.loc[i, stump_hollow_d1_cols[j]], errors="coerce"
                )
                hollow_d2 = pd.to_numeric(
                    data.loc[i, stump_hollow_d2_cols[j]], errors="coerce"
                )
                stump_density = pd.to_numeric(
                    data.loc[i, stump_density_cols[j]], errors="coerce"
                )

                if pd.isna(Diam1) or pd.isna(Diam2) or pd.isna(height):
                    continue

                mean_Diam = np.mean([Diam1, Diam2])
                # biomass_per_kg_tree = np.nan

                stump = pd.DataFrame(
                    {
                        "unique_id": [data.loc[i, "unique_id"]],
                        "nest": [nest_number],
                        "Diam1": [Diam1],
                        "Diam2": [Diam2],
                        "slope": [slope],
                        "height": [height],
                        # 'biomass_per_kg_tree': [biomass_per_kg_tree],
                        "cut_cl": [cut_cl],
                        "hollow_go": [hollow_go],
                        "hollow_d1": [hollow_d1],
                        "hollow_d2": [hollow_d2],
                        "stump_density": [stump_density],
                    }
                )
                stumps_nest = pd.concat([stumps_nest, stump], ignore_index=True)

        all_stumps = pd.concat([all_stumps, stumps_nest], ignore_index=True)

    return all_stumps


def extract_dead_trees_class1(data, nest_numbers):
    all_dead_trees = pd.DataFrame()

    for nest_number in nest_numbers:
        # Columns for class 1 dead trees
        dbh_columns = [col for col in data.columns if f"t_dbh_nest{nest_number}" in col]
        livedead_columns = [
            col for col in data.columns if f"livedead_nest{nest_number}" in col
        ]
        species_name_columns = [
            col for col in data.columns if f"t_species_name_nest{nest_number}" in col
        ]
        class_columns = [
            col
            for col in data.columns
            if f"tree_dead_nest{nest_number}/t_deadcl_nest{nest_number}" in col
        ]

        dead_trees_nest = pd.DataFrame()

        for i in range(len(data)):
            for j in range(len(dbh_columns)):
                if (
                    not pd.isna(data.loc[i, livedead_columns[j]])
                    and data.loc[i, livedead_columns[j]] == 2
                    and data.loc[i, class_columns[j]] == 1
                ):

                    dead_tree = pd.DataFrame(
                        {
                            "unique_id": [data.loc[i, "unique_id"]],
                            "nest": [nest_number],
                            "species_name": [data.loc[i, species_name_columns[j]]],
                            "DBH_cl1": [data.loc[i, dbh_columns[j]]],
                            "class": [1],
                            "subclass": ["n/a"],
                        }
                    )

                    dead_trees_nest = pd.concat(
                        [dead_trees_nest, dead_tree], ignore_index=True
                    )

        all_dead_trees = pd.concat([all_dead_trees, dead_trees_nest], ignore_index=True)

    return all_dead_trees
