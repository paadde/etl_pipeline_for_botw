# reference site:
# https://gadhagod.github.io/Hyrule-Compendium-API/#/

# Load relevant libraries
import requests
import ast
import pandas as pd
import json

# Set up endpoints and categories
base_url = 'https://botw-compendium.herokuapp.com/api/v3/compendium/category/monsters'
categories = ['creatures', 'equipment', 'materials', 'monsters', 'treasure']

# Make a request using the endpoint
response = requests.get(base_url)

# Check the status of the request
print(response.status_code)

# Create a variable for the result (json)
compendium_data = response.json()
print(compendium_data)

# normalize data from drops column
compendium_df = pd.json_normalize(compendium_data['data'])
compendium_df.info()

compendium_df['drops'].head()

# convert drops column in list


def convert_to_list(x):
    '''convert value of a column into a list'''
    if isinstance(x, str):
        try:
            return ast.literal_eval(x) if x else []
        except (ValueError, SyntaxError):
            return []
    elif isinstance(x, list):
        return x
    else:
        return []


compendium_df['drops'] = compendium_df['drops'].apply(convert_to_list)


max_drops = compendium_df['drops'].apply(len).max()

drops_column_name = [f'drops{i+1}' for i in range(max_drops)]

compendium_drops = pd.DataFrame(
        compendium_df['drops'].to_list(), columns=drops_column_name
       )
compendium_drops.head()

compendium_df = (
        pd.concat([compendium_df, compendium_drops], axis=1)
        .drop(columns='drops')
        )

compendium_df.info()
