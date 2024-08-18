import requests
import logging
from config.settings import API_BASE
from config.settings import CATEGORIES


def get_monster_data():
    """
    retrieve monster data from the API and returns the value in a json format
    """
    logger = logging.getLogger(__name__)

    logger.info(f'Trying to extract {CATEGORIES[3]} data from the API...')
    url = f'{API_BASE}{CATEGORIES[3]}'

    try:
        response = requests.get(url)
        response.raise_for_status()
        logger.info(
            f'The request returned status_code: {response.status_code}'
            )
        logger.info('Continuing data extraction...')

    except Exception as e:
        logger.error(f'An error occured during extraction: {e}')
        logger.info('Data extraction aborted.')

    response = response.json()
    logger.info("The data was successfuly saved to variable 'response'.")
    return response
