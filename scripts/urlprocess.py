# coding=utf-8
import getopt
import logging
import sys

__author__ = 'Ryan'

from core import RoseVisionDb
from urllib2 import quote


def urlencode(url):
    return quote(url, "/?:@&=+$,;#%")


def get_products(db, start=0, count=100):
    """
    取得不重复单品的fingerprint
    """
    rows = db.query(str.format('SELECT idproducts, url from products order by idproducts limit {0}, {1}', start, count))
    result = []
    for row in rows.fetch_row(maxrows=0, how=1):
        result += [row]
    return result


def urlprocess_main():
    db_spec = {
        "host": "127.0.0.1", "port": 3306,
        "username": "rose", "password": "rose123",
        "schema": "editor_stores"
    }
    db = RoseVisionDb()
    db.conn(db_spec)

    idproducts_start = 0
    idproducts_count = 100
    opts, args = getopt.getopt(sys.argv[1:], "s:c:")
    for opt, arg in opts:
        if opt == '-s':
            idproducts_start = int(arg)
        elif opt == '-c':
            idproducts_count = int(arg)

    logger.info(str.format("Url process start"))
    while 1:
        products = get_products(db, idproducts_start, idproducts_count)
        if not products:
            logger.info(str.format("Url process end"))
            break
        else:
            logger.info(str.format("Url process offset : {0} count : {1}", idproducts_start, len(products)))
            idproducts_start += idproducts_count

        for product in products:
            origin_url = product['url']
            url = None
            try:
                url = urlencode(origin_url)
            except:
                url = None
                logger.info(str.format("Error: {0} encode {1} failed", product['idproducts'], origin_url))
                pass

            if url:
                try:
                    db.update({'url': url}, 'products', str.format('idproducts="{0}"', product['idproducts']))
                except:
                    logger.info(str.format("Error: {0} update {1} failed", product['idproducts'], url))
                    pass

if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)-24s%(levelname)-8s%(message)s', level='INFO')
    logger = logging.getLogger()

    logger.info(str.format("Script start"))

    urlprocess_main()

    logger.info(str.format("Script end"))
    pass
