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
            ['id', 'name'] + location_columns + ['dlc'] + drops_columns + [
                'description']
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


def transform_equipment_data(equipment_data):
    """
    This do transformational data automation for the fetched data

    Parameter:
        equipment_data: data that has been requested from botw API
    """
    logger = logging.getLogger(__name__)
    logger.info('Transforming equipment_data...')

    # normalize json into a Dataframe
    equipment_df = pd.json_normalize(equipment_data['data'], sep='_')

    # drop 'category' and 'image' column
    equipment_df = equipment_df.drop(columns=['category', 'image'])

    # Expand common_locations into different 'location' columns
    equipment_df['common_locations'] = (
            equipment_df['common_locations']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    # create addional columns
    location_columns = column_creator(
            equipment_df, 'common_locations', 'location'
            )

    # create a new dataframe for the created new columns
    location_df = pd.DataFrame(
            equipment_df['common_locations'].to_list(),
            columns=location_columns
            )

    # merge the new location column to the main dataframe then drop original
    # column
    equipment_df = (
            pd.concat([equipment_df, location_df], axis=1)
            .drop(columns='common_locations')
            )

    # reorder the columns of equipment_df dataframe
    expected_columns = (
            ['id', 'name'] + location_columns + ['dlc', 'properties_attack',
                                                 'properties_defense',
                                                 'description']
            )

    existing_columns = (
            [col for col in expected_columns if col in equipment_df.columns]
            )

    equipment_df = equipment_df[existing_columns]
    logger.info(
                f'Successfully transformed the df with dimension of {
                    equipment_df.shape}'
                )

    return equipment_df


def transform_creatures_data(creatures_data):
    """
    This do transformational data automation for the fetched data

    Parameter:
        creatures_data: data that has been requested from botw API
    """
    logger = logging.getLogger(__name__)
    logger.info('Transforming creatures_data...')

    # normalize json file into a dataframe
    creatures_df = pd.json_normalize(creatures_data['data'])

    # Drop 'category' and 'image' column in the dataframe
    creatures_df = creatures_df.drop(columns=['category', 'image'])

    # Expland common_locations column base on its' number of elements
    # Ensure all entries in the columns are lists
    creatures_df['common_locations'] = (
            creatures_df['common_locations']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    creatures_df['drops'] = (
            creatures_df['drops']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    # Create columns based on each elements
    location_column = column_creator(
            creatures_df, 'common_locations', 'location'
            )

    drop_column = column_creator(creatures_df, 'drops', 'drop')

    # Explode each elements in their corresponding columns in a new dataframe
    location_df = pd.DataFrame(
            creatures_df['common_locations'].to_list(),
            columns=location_column
            )

    drops_df = pd.DataFrame(
            creatures_df['drops'].to_list(),
            columns=drop_column
            )

    # Combine extended columns to the main dataframe then drop the orginal
    # column
    creatures_df = pd.concat(
            [creatures_df, location_df], axis=1
            ).drop(columns='common_locations')

    creatures_df = pd.concat(
            [creatures_df, drops_df], axis=1
            ).drop(columns='drops')

    # Organize the order of columns
    expected_columns = (
            ['id', 'name', 'edible', 'hearts_recovered',
             'cooking_effect'] + location_column + ['dlc'] + drop_column + [
                 'description']
            )

    existing_columns = (
            [col for col in expected_columns if col in creatures_df.columns]
            )

    creatures_df = creatures_df[existing_columns]
    logger.info(
            f'Successfully transformed the df with dimension of {
                    creatures_df.shape}'
            )

    return creatures_df


def transform_materials_data(materials_data):
    logger = logging.getLogger(__name__)
    logger.info('Transforming materials_data...')

    # normalize json file into a dataframe
    materials_df = pd.json_normalize(materials_data['data'])

    # Drop 'category' and 'image' column in the dataframe
    materials_df = materials_df.drop(columns=['category', 'image'])

    # Expland common_locations column base on its' number of elements
    # Ensure all entries in the columns are lists
    materials_df['common_locations'] = (
            materials_df['common_locations']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    # Create columns based on each elements
    location_column = column_creator(
            materials_df, 'common_locations', 'location'
            )

    # Explode each elements in their corresponding columns in a new dataframe
    location_df = pd.DataFrame(
            materials_df['common_locations'].to_list(),
            columns=location_column
            )
    # Combine extended columns to the main dataframe then drop the orginal
    # column
    materials_df = pd.concat(
            [materials_df, location_df], axis=1
            ).drop(columns='common_locations')
    # Organize the order of columns
    expected_columns = (
            ['id', 'name', 'hearts_recovered',
             'cooking_effect'] + location_column + ['dlc', 'description']
            )

    existing_columns = (
            [col for col in expected_columns if col in materials_df.columns]
            )

    materials_df = materials_df[existing_columns]
    logger.info(
            f'Successfully transformed the df with dimension of {
                materials_df.shape}'
            )

    return materials_df


def transform_treasure_data(treasure_data):
    logger = logging.getLogger(__name__)
    logger.info('Transforming treasure_data...')

    # normalize json file into a dataframe
    treasure_df = pd.json_normalize(treasure_data['data'])

    # Drop 'category' and 'image' column in the dataframe
    treasure_df = treasure_df.drop(columns=['category', 'image'])

    # Expland common_locations column base on its' number of elements
    # Ensure all entries in the columns are lists
    treasure_df['common_locations'] = (
            treasure_df['common_locations']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    treasure_df['drops'] = (
            treasure_df['drops']
            .apply(lambda x: x if isinstance(x, list) else [])
            )

    # Create columns based on each elements
    location_column = column_creator(
            treasure_df, 'common_locations', 'location'
            )
    drop_column = column_creator(treasure_df, 'drops', 'drop')

    # Explode each elements in their corresponding columns in a new dataframe
    location_df = pd.DataFrame(
            treasure_df['common_locations'].to_list(),
            columns=location_column
            )

    drop_df = pd.DataFrame(
            treasure_df['drops'].to_list(),
            columns=drop_column
            )

    # Combine extended columns to the main dataframe then drop the orginal
    # column
    treasure_df = pd.concat(
            [treasure_df, location_df], axis=1
            ).drop(columns='common_locations')

    treasure_df = pd.concat(
            [treasure_df, drop_df], axis=1
            ).drop(columns='drops')

    # Organize the order of columns
    expected_columns = (
            ['id', 'name'] + location_column + ['dlc'] + drop_column + [
                'description']
            )

    existing_columns = (
            [col for col in expected_columns if col in treasure_df.columns]
            )

    treasure_df = treasure_df[existing_columns]
    logger.info(
            f'Successfully transformed the df with dimension of {
                treasure_df.shape}'
            )
    return treasure_df
