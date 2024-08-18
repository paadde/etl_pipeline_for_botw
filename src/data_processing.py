import pandas as pd
import logging
from src.tools_functions import column_creator


def transform_monster_data(monster_data):
    """
    This do transformational data automation for the fetched data
    """
    logger = logging.getLogger(__name__)
    logger.info('Transforming monster_data...')

    # normalize the data and save it into a dataframe
    monster_data_df = pd.json_normalize(monster_data['data'])

    # Drop unrelated columns
    monster_data_df = monster_data_df.drop(columns=['category', 'image'])

    # Ensure 'drops' and 'common_locations' columns contain lists
    monster_data_df['drops'] = (
            monster_data_df['drops']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    monster_data_df['common_locations'] = (
            monster_data_df['common_locations']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    # Create additional columns to extend the columns
    drops_columns = column_creator(monster_data_df, 'drops', 'drop')

    location_columns = column_creator(
            monster_data_df, 'common_locations', 'location'
            )

    drops_df = pd.DataFrame(
        monster_data_df['drops'].to_list(),
        columns=drops_columns
        )

    location_df = pd.DataFrame(
        monster_data_df['common_locations'].to_list(),
        columns=location_columns
        )

    # Combine new dfs to the main data frame and drop original columns
    monster_data_df = (
            pd.concat([monster_data_df, drops_df], axis=1)
            .drop(columns='drops')
            )

    monster_data_df = (
            pd.concat([monster_data_df, location_df], axis=1)
            .drop(columns='common_locations')
            )

    # Check which columns exist in the DataFrame and only reorder those
    expected_columns = (
            ['id', 'name',
             'description'] + location_columns + ['dlc'] + drops_columns
            )

    existing_columns = (
            [col for col in expected_columns if col in monster_data_df.columns]
            )

    monster_data_df = monster_data_df[existing_columns]
    logger.info(
            f'Successfully transformed the df with dimension of {
                monster_data_df.shape}'
            )

    return monster_data_df
