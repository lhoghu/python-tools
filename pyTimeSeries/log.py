import logging
from pyTimeSeries import config


def setup_custom_logger(name):
    # Add the log message handler to the logger
    #logging.basicConfig(level=logging.DEBUG)
    log_filename = '../../%s-fetchbbg-log.txt' % datetime.datetime.strftime(datetime.datetime.now(),"%Y%m%d %H%M%S")
    handler = logging.handlers.RotatingFileHandler(log_filename, maxBytes=1000000, backupCount=50)
    handler.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
    handler.setFormatter(formatter)

    logging.getLogger('').addHandler(handler)
    logging.info('Starting logging')

    logger = logging.getLogger(name)
    logger.addHandler(handler)
    return logger