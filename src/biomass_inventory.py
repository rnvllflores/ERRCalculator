#  Imports
import pandas as pd

# Functions used to format data retrieved from ONA


def extract_trees(data, nest_list):
    """
    Extracts tree data from the given DataFrame for a list of nest numbers.

    Parameters:
    - data (DataFrame): The input DataFrame containing tree data.
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
