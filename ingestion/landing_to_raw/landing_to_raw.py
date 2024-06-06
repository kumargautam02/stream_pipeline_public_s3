import os
import time
import shutil
import logging

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger('Data_Processing')
logger.info('landing_to_raw.py script started')



def copy_function(source_dir, destination_dir,duplicate_dir,files):
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
                if file_name not in os.listdir(destination_dir):
                    shutil.move(source_file, destination_file)
                    print(f'Moved: {source_file} ----> {destination_file}')
                else:
                    if not os.path.exists(f"{duplicate_dir}"):
                        os.makedirs(f"{duplicate_dir}")
                        logger.info("duplicate directory has been created -->", f"{duplicate_dir}/duplicate_files/")
                    shutil.move(source_file, f"{duplicate_dir}")
                    print(f'Move duplicate file from: {source_file} ----> {destination_dir}/duplicate_files/')
    
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
        source_dir = f"{currect_working_directory}/landing/"
        destination_dir = f"{currect_working_directory}/raw/"
        duplicate_dir = f"{currect_working_directory}/duplicate/"
        
        if not os.path.exists(destination_dir):
            os.makedirs(destination_dir)
            logger.info("raw directory has been created -->", destination_dir)
        logger.info("waiting for 30 seconds for ingestion")
        time.sleep(30)


        # Move each file from the source directory to the destination directory
        files = os.listdir(source_dir)
        run = True
        while run:
            if len(files) >= 10:
                 logger.info("#########################################################################################\
                             #########################      NEXT BATCH STARTED      ####################################\
                             ############################################################################################\
                             ")
                 copy_function(source_dir, destination_dir, duplicate_dir,files[0:10])
                 files = os.listdir(source_dir)
                 start_time = time.time()
                 logger.info("waiting for 20 seconds for ingestion")
                 time.sleep(30)
                 
                 continue


            elif (len(files) < 10):
                 copy_function(source_dir, destination_dir, files)
                 run = False
        logger.info("No more files to move from landing/ ---->>>> raw/")
                   


    except Exception as e:
        logger.info(f"Error has been encountered at move_landing_to_raw {e}")
move_landing_to_raw()
