# Ochestration file
# Load required libraries
import logging
from src.api_client import get_monster_data
from src.data_processing import transform_monster_data
from src.database import upload_to_db

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
    monster_data = get_monster_data()
    logger.info('Data extraction completed.')

    logger.info('Starting data transformation...')
    monster_df = transform_monster_data(monster_data)
    logger.info('Transformation of data was successful.')

    logger.info('Uploading transformed data to the database...')
    upload_to_db(monster_df, 'monster')
    logger.info('Upload successful')


if __name__ == '__main__':
    main()
