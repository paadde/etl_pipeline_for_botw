# Ochestration file
# Load required libraries
import logging
from src.api_client import get_data
from src.data_processing import transform_monster_data
from src.data_processing import transform_equipment_data
from src.database import upload_to_db
from config.settings import CATEGORIES

# Set up log files to keep track of script execution and error
logging.basicConfig(
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        level=logging.INFO,
        handlers=[
            logging.FileHandler('logs/app.log'),
            logging.StreamHandler()
            ]
        )

logger = logging.getLogger(__name__)


def main():

    logger.info('Starting ETL process...')
    # Create a place holder for the extracted data from API endpoint
    data = {}
    # Loop the function to fetch all data per category
    for name in CATEGORIES:
        data[f'{name}'] = get_data(name)
        logger.info(f'Extraction completed for {name} data.')

    logger.info('Starting data transformation...')
    monster_df = transform_monster_data(data['monsters'])
    equipment_df = transform_equipment_data(data['equipment'])
    logger.info('Transformation of data was successful.')

    logger.info('Uploading transformed data to the database...')
    # Create a dictionary of dataframes and table names
    botw_df = {
            'botw_monster': monster_df,
            'botw_equipment': equipment_df
            }
    # Upload dataframes and corresponding table names to the database
    for name, df in botw_df.items():
        upload_to_db(df, name)
    logger.info('ETL process successful. Script ended.')


if __name__ == '__main__':
    main()
