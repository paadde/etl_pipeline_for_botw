# reference site:
# https://gadhagod.github.io/Hyrule-Compendium-API/#/

# Load relevant libraries
import requests
import ast
import pandas as pd
import numpy as np
import json

# Set up endpoints and categories
base_url = 'https://botw-compendium.herokuapp.com/api/v3/compendium/category/monsters'
categories = ['creatures', 'equipment', 'materials', 'monsters', 'treasure']

# Make a request to the api endpoint
response = requests.get(base_url)

# Print result or status code of the request
print(response.status_code)

# Save the response into a variable named game_data
game_data = response.json()

# normalize the data and save it into a dataframe
monster_data_df = pd.json_normalize(game_data['data'])
monster_data_df2 = pd.json_normalize(game_data['data'])

# fill missing values as empty list
monster_data_df['drops'] = monster_data_df['drops'].fillna('[]')
monster_data_df['drops'].head()
monster_data_df[(monster_data_df['drops'] == '[]')]

# create additional columns to extend drops column
max_columns = monster_data_df['drops'].apply(len).max()
drops_column = [f'drops{i+1}' for i in range(max_columns)]
drops_df = pd.DataFrame(
        monster_data_df['drops'].to_list(), columns=drops_column
        )

# Combine drops_df to the main data frame and drop original 'drops' column
monster_data_df = (
        pd.concat([monster_data_df, drops_df], axis=1)
        .drop(columns='drops')
        )

# fill missing values as empty list for common_locations column
monster_data_df['common_locations'] = (
        monster_data_df['common_locations'].fillna('[]')
        )

# create additional columns to extend common_locations column
max_columns_location = (
        monster_data_df['common_locations'].apply(len).max()
        )
location_column = [f'location{i+1}' for i in range(max_columns_location)]
location_df = pd.DataFrame(
        monster_data_df['common_locations'].to_list(), columns=location_column
        )
# Combine both loation_df to the main data frame
monster_data_df = (
        pd.concat([monster_data_df, location_df], axis=1)
        .drop(columns='common_locations')
        )
monster_data_df.info()

# Drop unrelated columns
monster_data_df = monster_data_df.drop(columns=['category', 'image'])

# reorder columns
monster_data_df = monster_data_df[['id', 'name', 'description', 'location1',
                                   'location2', 'dlc', 'drops1', 'drops2',
                                   'drops3', 'drops4', 'drops5', 'drops6',
                                   'drops7', 'drops8', 'drops9', 'drops10',
                                   'drops11', 'drops12']]
monster_data_df.info()


# Create a function to create additional custom columns based on number of
# variables


def column_creator(df, column, new_column):
    """
    Create an x number of columns based on the maximum number of elemens found
    in a the specified dataframe column.

    Parameters:
    df: The dataframe containing the data
    column: The name of column to which elements are to be counted
    new_column: The prefix for the generated column names
    """

    # Calculate the maximum elements on a specified dataframe and column
    max_element = df[column].apply(len).max()

    # Create new column names with the specified prefix
    column_names = [f'{new_column}{i+1}' for i in range(max_element)]
    return column_names


test_drop = column_creator(monster_data_df2, 'drops', 'dropss')
print(test_drop)

drops_df2 = pd.DataFrame(
        monster_data_df2['drops']
        .to_list(),
        columns=column_creator(monster_data_df2, 'drops', 'dropss')
        )

drops_df2.head()
drops_df2.info()
