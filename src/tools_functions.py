# for various utility functions


def column_creator(df, column, new_column):
    """
    Create an x number of columns based on the maximum number of elemens found
    in a the specified dataframe column.

    Parameters:
    df: The dataframe containing the data
    column: The name of column to which elements are to be counted
    new_column: The prefix for the generated column names
    """

    # Calculate the maximum elements on a specified dataframe and column
    max_element = df[column].apply(len).max()

    # Create new column names with the specified prefix
    column_names = [f'{new_column}{i+1}' for i in range(max_element)]
    return column_names


def get_db_url(user, password, host, db_name):
    """ Construct database url from the variables """
    return f'postgresql://{user}:{password}@{host}/{db_name}'
