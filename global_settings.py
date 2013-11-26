# coding=utf-8

__author__ = 'Zephyre'

import sys
import core

if sys.platform in ('win32', ):
    STORAGE_PATH = u'd:/Users/Zephyre/Development/mstore/storage'
    HOME_PATH = u'd:/Users/Zephyre/Dropbox/Freelance/MStore/src'
else:
    STORAGE_PATH = u'/home/rose/MStore/storage'
    HOME_PATH = u'/home/rose/MStore/src'

# Database
REMOTE_CONN = True
if sys.platform not in ('win32', ):
    REMOTE_CONN = False

EDITOR_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
               'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'editor_stores'}
RELEASE_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
                'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'release_stores'}
SPIDER_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
               'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'spider_stores'}
TMP_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
            'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'spider_stores_bkp'}

# Email settings for notification
EMAIL_ADDRESSES = ['haizi.zh@gmail.com', 'buddy@mfashion.com.cn']


# Port for remote debugging
DEBUG_HOST = 'localhost'
DEBUG_PORT = 7101
DEBUG_FLAG = True
WRITE_DATABASE = True

# Log settings
LOG_DEBUG = False


def __fetch_brand_info():
    db = core.MySqlDb()
    db.conn(EDITOR_SPEC)
    tmp = db.query('SELECT * FROM brand_info').fetch_row(how=1, maxrows=0)
    return {int(k['brand_id']): {'brandname_e': k['brandname_e'].decode('utf-8') if k['brandname_e'] else None,
                                 'brandname_c': k['brandname_c'].decode('utf-8') if k['brandname_c'] else None,
                                 'brandname_s': k['brandname_s'].decode('utf-8') if k['brandname_s'] else None}
            for k in tmp}


def __fetch_region_info():
    db = core.MySqlDb()
    db.conn(EDITOR_SPEC)
    tmp = db.query('SELECT * FROM region_info').fetch_row(how=1, maxrows=0)
    return {k['iso_code']: {'iso_code3': k['iso_code3'],
                            'name_e': k['name_e'].decode('utf-8'),
                            'name_c': k['name_c'].decode('utf-8') if k['name_c'] else None,
                            'currency': k['currency']}
            for k in tmp}


DECIMAL_MARK = {'.': {'cn', 'au', 'us', 'hk', 'tw'},
                ',': {'fr', 'de', 'it', 'fi'}}

BRAND_NAMES = __fetch_brand_info()
REGION_INFO = __fetch_region_info()
CURRENCY_LIST = set(val['currency'] for val in REGION_INFO.values())
