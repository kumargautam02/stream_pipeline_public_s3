import logging

def get_logger():
    try:
        logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
        logger = logging.getLogger('Data_Processing')
        logger.info('main.py Script started')
        return logger

    except Exception as e:
        logger.info(f"Error has been encountered at main {e}")