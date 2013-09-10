import logging
import logging.config

__author__ = 'Zephyre'

def sandbox_logging():
    logging.config.fileConfig('geocode_fetch.cfg')
    logger = logging.getLogger('firenzeLogger')

    logger.debug('debug message')
    logger.info('info message')
    logger.warn('warn message')
    logger.error('error message')
    logger.critical('critical message')

def sandbox_sql():
    pass

if __name__ == "__main__":
    sandbox_logging()