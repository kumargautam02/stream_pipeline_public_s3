
import re
import logging
from pyspark.sql.functions import *
logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')
# logger.info('main.py Script started')

def ingest_data_from_s3(current_working, s3, s3_path):
    try:
        """
        This function is used to Ingest data from Public S3 bucket site using s3fs library and store inside Landing Folder of Current working Directory. 
        Parameters:
        current_working: receive the current working directory to save data ex:- current_working_directory/Landing
        s3: S3 object for Public s3_bucket.
        s3_path: s3 bucket path from which we have to ingest data. 

        returns: None
        """

        # current_working = os.getcwd()
        # s3.get(f'{s3_path}', f'{current_working}/Landing1/',recursive=True, maxdepth=None)
        return f'{current_working}/Landing1/'
    except Exception as e:
        logger.info(f"Error has been encountered at ingest_data_from_s3 {e}")


def get_file_generation_date(column):
    """
    This function is used to extract the date_column from file_name.
    Parameters:
    column[pyspark dataframe column]: column with the file_name.

    returns: the date of file generation.
    """
    try:
        pattern_for_date = "(2024-05-\d{2})"
        pattern_for_whole = "(2024-05-\d{2}-\d{2}-\d{2}-\d{2})"
        date_value = re.search(pattern_for_date, column).group(0)
        whole_value = re.search(pattern_for_whole, column).group(0)
        time_value = whole_value.replace(f'{date_value}-', "")

        return f"{date_value} {time_value}"
    except Exception as e:
        logger.info(f"Error has been encountered at get_file_generation_date {e}")



get_file_generation_date_udf  = udf(lambda column: get_file_generation_date(column),StringType())


def get_unique_column_names(column_names):
    """
    This function is used to give unique names to spark columns based on index of the columns.
    Parameters:
    column[python list]: Python list with the column_names

    returns: [python list with unique names]
    """
    try:
        # print(column_names)
        for i in range(len(column_names)):
            if column_names[i].strip() != "file_creation_date":
                column_names[i] = column_names[i] + f"_{i}" 

        return column_names
    except Exception as e:
        logger.info(f"Error has been encountered at get_unique_column_names {e}")
    

def get_duplicate_column_names(df):
    """
    This function is used to get the duplciate columns from the dataframe.
    Parameters:
    df[pyspark dataframe column]: pyspark dataframe

    returns: [Python list] list of duplicate column names present in the dataframe.
    """
    try:
        duplicate_columns = []
        original_columns = []
        for column in df.columns:
            # print(column)
            if column.rsplit("_", 1)[0] not in original_columns:
                original_columns.append(column.rsplit("_", 1)[0])
            else:
                duplicate_columns.append(column)
        return duplicate_columns
    except Exception as e:
        logger.info(f"Error has been encountered at get_duplicate_column_names {e}")


def get_publisher_id_column_name(df):
    """
    This function is used to get the publisher_id column name.
    Parameters:
    column[pyspark dataframe column]: dataframe.

    returns: [python string] column name of publisher_id
    """
    try:
        # print("yes")
        for column in df.columns:
            # print(column)
            if column.rsplit("_", 1)[0] == 'publisher_id':
                # print(column)
                
                return column
    except Exception as e:
        logger.info(f"Error has been encountered at get_publisher_id_column_name {e}")



