import requests
import logging
from config.settings import API_BASE


def get_data(category):
    """
    retrieve monster data from the API and returns the value in a json format

    Parameter:
        category: here is the following available data that can be pulled
        ['creatures', 'equipment', 'materials', 'monsters', 'treasure']
    """
    logger = logging.getLogger(__name__)

    logger.info(f'Trying to extract {category} data from the API...')
    url = f'{API_BASE}{category}'

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
    logger.info(
            f"{category} data was successfuly saved to a variable."
            )
    return response
