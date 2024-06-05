import os
import s3fs
import time
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')
logger.info('ingestion.py script started')

def ingest_data_in_file_format():
    """
        This function is used to ingest data from the public s3 bucket and store it in file format in landing zone
        Parameters:
        destination_path: This is the path of clean folder where data is being saved. 
        spark: spark object
        returns: transformed pyspark-dataframe
    """
    try:
        currect_working_directory = os.getcwd().replace("\\", "/")
        s3_path = 'datasci-assignment/click_log/2024/05/'
        s3 = s3fs.S3FileSystem(anon =  True)
        files_path_lsit = s3.find(s3_path, maxdepth=None, withdirs=False)[1:]

        for file in files_path_lsit:
            s3.get(file, f'{currect_working_directory}/landing/')
            logger.info(f"{file.rsplit('/', 1)[1]} file loaded successfully at {currect_working_directory}")
            # time.sleep(15)
            # logger.info("waiting for 15 secondds")

    except Exception as e:
        logger.info(f"Error has been encountered at ingest_data_in_file_format {e}")
ingest_data_in_file_format()
