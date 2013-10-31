import logging
import logging.config
import _mysql

__author__ = 'Zephyre'

logging.config.fileConfig('logging.cfg')
logger = logging.getLogger('firenzeLogger')

def proc_idcity_zero(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores'):
    """
    For records where idcity=0: lookup the city in table 'city' and fetch corresponding idcity.
    :param host:
    :param port:
    :param user:
    :param passwd:
    :param db:
    """
    db = _mysql.connect(host='localhost', port=3306, user='root', passwd='123456', db='brand_stores')
    db.query("SET NAMES 'utf8'")


    db.close()
    pass
