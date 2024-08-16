# Setup connection and loading of df in a database
import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import psycopg2
from src.tools_functions import get_db_url


def upload_to_db(df, table_name, if_exists='replace', chunksize=1000):
    """
    Upload a dataframe into PostgreSQL Database

    Parameter:
        df = the dataframe to be uploaded
        table_name = name of the table in the database
        if_exists = what will happen to the table if it already existed
        chunksize = number of rows uploaded at a time
    """

    db_url = get_db_url(
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            host=os.environ['DB_HOST'],
            db_name=os.environ['DB_NAME']
            )

    # Creation of SQL engine
    engine = create_engine(db_url)

    # Upload df to postgresql
    try:
        df.to_sql(
                table_name,
                engine,
                if_exists=if_exists,
                index=False,
                chunksize=chunksize
                )
        print(f"Dataframe was successfully uploaded to table '{table_name}'.")
        return True
    except SQLAlchemyError as e:
        print(f'An error occurred: {e}')
        return False
    finally:
        engine.dispose()
