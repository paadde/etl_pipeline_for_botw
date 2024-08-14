import requests
from config.settings import API_BASE
from config.settings import CATEGORIES


def get_monster_data():
    """
    retrieve monster data from the API and returns the value in a json format
    """
    url = f'{API_BASE}{CATEGORIES[3]}'
    response = requests.get(url).json()
    return response
