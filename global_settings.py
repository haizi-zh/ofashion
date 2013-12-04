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

DB_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
           'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'editor_stores'}

# Email settings for notification
EMAIL_ADDRESSES = ['haizi.zh@gmail.com', 'buddy@mfashion.com.cn']

# Port for remote debugging
DEBUG_HOST = 'localhost'
DEBUG_PORT = 7100
DEBUG_FLAG = False
WRITE_DATABASE = True

# Log settings
LOG_DEBUG = False


def __fetch_brand_info():
    db = core.MySqlDb()
    db.conn(DB_SPEC)
    tmp = db.query('SELECT * FROM brand_info').fetch_row(how=1, maxrows=0)
    return {int(k['brand_id']): {'brandname_e': k['brandname_e'].decode('utf-8') if k['brandname_e'] else None,
                                 'brandname_c': k['brandname_c'].decode('utf-8') if k['brandname_c'] else None,
                                 'brandname_s': k['brandname_s'].decode('utf-8') if k['brandname_s'] else None}
            for k in tmp}


def __fetch_region_info():
    db = core.MySqlDb()
    db.conn(DB_SPEC)
    tmp = db.query('SELECT * FROM region_info').fetch_row(how=1, maxrows=0)
    return {k['iso_code']: {'iso_code3': k['iso_code3'],
                            'weight': k['weight'], 'rate': float(k['rate']),
                            'name_e': k['name_e'].decode('utf-8'),
                            'name_c': k['name_c'].decode('utf-8') if k['name_c'] else None,
                            'currency': k['currency'], 'decimal': k['decimal_mark']}
            for k in tmp}


__cached_region_info = None
__cached_brand_info = None
__cached_currency_rate = None


def region_info():
    global __cached_region_info
    if not __cached_region_info:
        __cached_region_info = __fetch_region_info()
    return __cached_region_info


def brand_info():
    global __cached_brand_info
    if not __cached_brand_info:
        __cached_brand_info = __fetch_brand_info()
    return __cached_brand_info


def currency_info():
    rate_data = {}
    for code, data in region_info().items():
        if data['currency'] in rate_data:
            continue
        rate_data[data['currency']] = data['rate']
    return rate_data

