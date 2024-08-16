# Ochestration file
# Load required libraries
from src.api_client import get_monster_data
from src.data_processing import transform_monster_data
from src.database import upload_to_db


def main():
    monster_data = get_monster_data()
    monster_df = transform_monster_data(monster_data)
    print(monster_df.info())
    print(monster_df.head(10))
    upload_to_db(monster_df, 'monster')


if __name__ == '__main__':
    main()
