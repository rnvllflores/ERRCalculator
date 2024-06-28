#  Imports
import re

import numpy as np
import pandas as pd

# Functions used to format data retrieved from ONA


def extract_trees(data, nest_numbers):
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

    for nest_number in nest_numbers:
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
    """
    Extracts information about class 1 dead trees from the given data for the specified nest numbers.

    Args:
        data (pandas.DataFrame): The data containing information about trees.
        nest_numbers (list): A list of nest numbers to extract dead trees from.

    Returns:
        pandas.DataFrame: A DataFrame containing information about class 1 dead trees.

    """
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
                try:
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
                except Exception as e:
                    # Handle any errors
                    # For now, simply continue to the next iteration
                    pass

        if len(dead_trees_nest) == 0:
            print(f"No class 1 dead trees found in nest {nest_number}")

        all_dead_trees = pd.concat([all_dead_trees, dead_trees_nest], ignore_index=True)

    return all_dead_trees


def extract_dead_trees_class2s(data, nest_numbers):
    """
    Extracts information about dead trees of class 2 from the given data.

    Args:
        data (pd.DataFrame): The input data containing tree information.
        nest_numbers (list): A list of nest numbers to extract dead trees from.

    Returns:
        pd.DataFrame: A DataFrame containing information about the extracted dead trees.
    """
    all_dead_trees = pd.DataFrame()

    for nest_number in nest_numbers:
        # Columns for dead trees and specific to class 2 short trees
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
        short_columns = [
            col
            for col in data.columns
            if f"tree_dead_nest{nest_number}_cl2_short" in col
        ]

        # New: Add short_density columns
        short_density_columns = [
            col for col in data.columns if f"short_density_nest{nest_number}" in col
        ]

        dead_trees_nest = pd.DataFrame()

        for i in range(len(data)):
            dead_tree_added = False
            for j in range(len(dbh_columns)):
                try:
                    if (
                        not pd.isna(data.loc[i, livedead_columns[j]])
                        and data.loc[i, livedead_columns[j]] == 2
                        and not pd.isna(data.loc[i, class_columns[j]])
                        and data.loc[i, class_columns[j]] == 2
                        and not pd.isna(data.loc[i, short_columns[0]])
                    ):

                        dead_tree = pd.DataFrame(
                            {
                                "unique_id": [data.loc[i, "unique_id"]],
                                "nest": [nest_number],
                                "species_name": [data.loc[i, species_name_columns[j]]],
                                "short_density": [
                                    data.loc[i, short_density_columns[j]]
                                ],  # New: Added short_density
                                "class": [data.loc[i, class_columns[j]]],
                                "subclass": ["short"],
                                "DB_short": [data.loc[i, short_columns[0]]],
                                "DBH_short": [data.loc[i, short_columns[1]]],
                                "DT_short": [data.loc[i, short_columns[2]]],
                                "height_short": [data.loc[i, short_columns[3]]],
                            }
                        )
                        dead_trees_nest = pd.concat(
                            [dead_trees_nest, dead_tree], ignore_index=True
                        )
                        dead_tree_added = True
                except Exception as e:
                    pass

        if len(dead_trees_nest) == 0:
            print(f"No dead trees of class 2 found in nest {nest_number}")

        all_dead_trees = pd.concat([all_dead_trees, dead_trees_nest], ignore_index=True)

    return all_dead_trees


def extract_dead_trees_class2t(data, nest_numbers):
    """
    Extracts data for dead trees of class 3 from the given data frame based on the provided nest numbers.

    Args:
        data (pandas.DataFrame): The data frame containing the tree data.
        nest_numbers (list): A list of nest numbers to filter the data.

    Returns:
        pandas.DataFrame: A data frame containing the extracted data for dead trees of class 3.
    """
    all_dead_trees = pd.DataFrame()

    for nest_number in nest_numbers:
        # Compile regex patterns outside the loop for efficiency
        species_name_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*t_species_name_nest{nest_number}"
        )
        family_name_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*t_family_name_nest{nest_number}"
        )
        livedead_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*t_livedead_nest{nest_number}"
        )
        class_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*t_deadcl_nest{nest_number}"
        )
        subclass_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*cl2_tallshort/t_deadcl2_nest{nest_number}_tallshort"
        )
        dbhtall_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*cl2_tall/t_dead_nest{nest_number}_DBH_tall"
        )
        dbtall_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*cl2_tall/t_dead_nest{nest_number}_D[bB]_tall"
        )
        tall_density_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*cl2_tall/t_dead_nest{nest_number}_tall_density"
        )
        slope_t_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*cl2_tall/t_dead_nest{nest_number}_slope_t_tall"
        )
        slope_b_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*cl2_tall/t_dead_nest{nest_number}_slope_b_tall"
        )
        dist_t_pattern = re.compile(
            f"tree_data_nest{nest_number}/.*cl2_tall/t_dead_nest{nest_number}_dist_t_tall"
        )

        # Filter columns using the compiled regex patterns
        species_name_columns = [
            col for col in data.columns if species_name_pattern.match(col)
        ]
        family_name_columns = [
            col for col in data.columns if family_name_pattern.match(col)
        ]
        livedead_columns = [col for col in data.columns if livedead_pattern.match(col)]
        class_columns = [col for col in data.columns if class_pattern.match(col)]
        subclass_columns = [col for col in data.columns if subclass_pattern.match(col)]
        dbhtall_columns = [col for col in data.columns if dbhtall_pattern.match(col)]
        dbtall_columns = [col for col in data.columns if dbtall_pattern.match(col)]
        tall_density_columns = [
            col for col in data.columns if tall_density_pattern.match(col)
        ]
        slope_t_columns = [col for col in data.columns if slope_t_pattern.match(col)]
        slope_b_columns = [col for col in data.columns if slope_b_pattern.match(col)]
        dist_t_columns = [col for col in data.columns if dist_t_pattern.match(col)]

        for i in range(len(data)):
            # Check for missing 'unique_ID' or other critical fields before proceeding
            if pd.isna(data.loc[i, "unique_id"]):
                continue  # Skip this row if 'unique_ID' is missing

            for j, species_name_col in enumerate(species_name_columns):
                # Use a more robust check for missing values
                if (
                    pd.isna(data.loc[i, livedead_columns[j]])
                    or pd.isna(data.loc[i, class_columns[j]])
                    or pd.isna(data.loc[i, subclass_columns[j]])
                ):
                    continue  # Skip this iteration if critical values are missing

                if (
                    data.loc[i, livedead_columns[j]] == 2
                    and data.loc[i, class_columns[j]] == 2
                    and data.loc[i, subclass_columns[j]] == 2
                ):
                    # Extract relevant data, handling missing values appropriately
                    species_name = data.loc[i, species_name_columns[j]]
                    family_name = data.loc[i, family_name_columns[j]]
                    dbhtall = data.loc[i, dbhtall_columns[j]]
                    dbtall = data.loc[i, dbtall_columns[j]]
                    tall_density = data.loc[i, tall_density_columns[j]]
                    slope_t = data.loc[i, slope_t_columns[j]]
                    slope_b = data.loc[i, slope_b_columns[j]]
                    dist_t = data.loc[i, dist_t_columns[j]]

                    # Combine all data into a single row
                    new_row = pd.DataFrame(
                        {
                            "unique_id": [data.loc[i, "unique_id"]],
                            "nest": [nest_number],
                            "species_name": [species_name],
                            "family_name": [family_name],
                            "dbh_tall": [dbhtall],
                            "db_tall": [dbtall],
                            "tall_density": [tall_density],
                            "slope_t_tall": [slope_t],
                            "slope_b_tall": [slope_b],
                            "dist_t_tall": [dist_t],
                            "class": [2],
                        }
                    )

                    # Append the new row to the result data frame
                    all_dead_trees = pd.concat(
                        [all_dead_trees, new_row], ignore_index=True
                    )

    return all_dead_trees


def extract_ldw_with_hollow(data):
    ldw_with_hollow = []

    for i in range(len(data)):
        row = data.iloc[i]
        unique_id = row["unique_id"]
        tree_class = row["lc_class/lc_class"]

        for tr in ["tr1", "tr2"]:
            for rep_num in range(1, 1000000):
                hollow_go_column = f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_basic_data/ldw_{tr}_hollow_go"

                if hollow_go_column in row.index:
                    hollow_go = row[hollow_go_column]

                    if pd.notna(hollow_go) and hollow_go == "yes":
                        hollow_d1 = row.get(
                            f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_hollow_data/ldw_{tr}_hollow_d1",
                            None,
                        )
                        hollow_d2 = row.get(
                            f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_hollow_data/ldw_{tr}_hollow_d2",
                            None,
                        )
                        diameter = row.get(
                            f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_basic_data/ldw_{tr}_diameter",
                            None,
                        )
                        density = row.get(
                            f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_basic_data/ldw_{tr}_density",
                            None,
                        )

                        result_row = {
                            "unique_id": unique_id,
                            "repetition": rep_num,
                            "type": tr,
                            "class": tree_class,
                            "hollow_d1": pd.to_numeric(hollow_d1, errors="coerce"),
                            "hollow_d2": pd.to_numeric(hollow_d2, errors="coerce"),
                            "diameter": pd.to_numeric(diameter, errors="coerce"),
                            "density": pd.to_numeric(density, errors="coerce"),
                        }
                        ldw_with_hollow.append(result_row)
                else:
                    break

    return pd.DataFrame(ldw_with_hollow)


def extract_ldw_wo_hollow(data):
    ldw_without_hollow = []

    for i in range(len(data)):
        row = data.iloc[i]
        unique_id = row["unique_id"]
        tree_class = row["lc_class/lc_class"]

        for tr in ["tr1", "tr2"]:
            for rep_num in range(1, 1000000):
                hollow_go_column = f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_basic_data/ldw_{tr}_hollow_go"

                if hollow_go_column in row.index:
                    hollow_go = row[hollow_go_column]

                    if pd.notna(hollow_go) and hollow_go == "no":
                        diameter = row.get(
                            f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_basic_data/ldw_{tr}_diameter",
                            None,
                        )
                        density = row.get(
                            f"ldw_{tr}/ldw_{tr}_data_rep[{rep_num}]/ldw_{tr}_basic_data/ldw_{tr}_density",
                            None,
                        )

                        result_row = {
                            "unique_id": unique_id,
                            "repetition": rep_num,
                            "type": tr,
                            "class": tree_class,
                            "diameter": pd.to_numeric(diameter, errors="coerce"),
                            "density": pd.to_numeric(density, errors="coerce"),
                        }
                        ldw_without_hollow.append(result_row)
                else:
                    break

    return pd.DataFrame(ldw_without_hollow)
