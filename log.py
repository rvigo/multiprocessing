import logging
logging.basicConfig(format='%(asctime)s - %(name)s - %(process)d - %(levelname)s - %(message)s',
                    datefmt='%d-%b-%y %H:%M:%S', level=logging.DEBUG)


def debug(message):
    logging.debug(message)

def info(message):
    logging.info(message)

def warning(message):
    logging.warning(message)

def error(message, ex=None):
    if ex is None:
        logging.error(message)
    else:
        logging.error(message, ex)