import os
import time
import shutil
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')
logger.info('landing_to_raw.py script started')



def copy_function(source_dir, destination_dir,files):
    """
        This function is used to move the data from landing to raw in a batch of 10 files
        Parameters: None
        returns: None
    """

    try:
        for file_name in files:
                # Construct full file path
                source_file = os.path.join(source_dir, file_name)
                destination_file = os.path.join(destination_dir, file_name)
                # Move the file
                shutil.move(source_file, destination_file)
                print(f'Moved: {source_file} ----> {destination_file}')
    
    except Exception as e:
        logger.info(f"Error has been encountered at copy_function {e}")

def move_landing_to_raw():
    """
        This function is used to move the data from landing to raw in a batch of 10 files
        Parameters: None
        returns: None
    """
    try:
        currect_working_directory = os.getcwd().replace("\\", "/")
        source_dir = f"{currect_working_directory}/stream_task/landing/"
        destination_dir = f"{currect_working_directory}/stream_task/raw/"
        
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
            logger.info("raw directory has been created -->", destination_dir)
        logger.info("waiting for 20 seconds for ingestion")
        time.sleep(20)


        # Move each file from the source directory to the destination directory
        files = os.listdir(source_dir)
        run = True
        while True:
            if len(files) >= 10:
                 logger.info("#########################################################################################\
                             #########################      NEXT BATCH STARTED      ####################################\
                             ############################################################################################\
                             ")
                 copy_function(source_dir, destination_dir, files[0:10])
                 files = os.listdir(source_dir)
                 start_time = time.time()
                 time.sleep(10)
                 logger.info("waiting for 20 seconds for ingestion")


            elif (len(files) < 10) and (start_time - time.time())>30:
                 copy_function(source_dir, destination_dir, files)
                 run = False
        logger.info("No more files to move from landing/ ---->>>> raw/")
                   


    except Exception as e:
        logger.info(f"Error has been encountered at move_landing_to_raw {e}")
move_landing_to_raw()
