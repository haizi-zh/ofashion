# coding=utf-8
import ConfigParser
import datetime

__author__ = 'Zephyre'

import sys
import core

if sys.platform in ('win32', ):
    STORAGE_PATH = u'd:/Users/Zephyre/Development/mstore/storage'
    HOME_PATH = u'd:/Users/Zephyre/Dropbox/Freelance/MStore/src'
elif sys.platform in ('darwin', ):  # MAC
    STORAGE_PATH = u'/Users/Ryan/Desktop/MStoreSpiderStorage'
    HOME_PATH = u'/Users/Ryan/Desktop/MStoreSpiderGit'
else:
    STORAGE_PATH = u'/home/rose/MStore/storage'
    HOME_PATH = u'/home/rose/MStore/src'


# Database
REMOTE_CONN = True
if sys.platform not in ('win32', ):
    REMOTE_CONN = False

DB_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
           'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'editor_stores'}
SPIDER_SPEC = {}
RELEASE_SPEC = {}
TMP_SPEC = {}


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
                            'weight': int(k['weight']), 'rate': float(k['rate']),
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


def _load_user_cfg(cfg_file='mstore.cfg'):
    # 加载mstore.cfg的设置内容
    config = ConfigParser.ConfigParser()
    config.optionxform = str

    def read_settings(section, option, var=None, proc=lambda x: x):
        """
        从config文件中指定的section，读取指定的option，并写到全局变量var中。
        @param section:
        @param option:
        @param var: 如果为None，则视为等同于option。
        @param proc: 对读取到的option，应该如何处理？
        @return:
        """
        if not var:
            var = option
        self_module = sys.modules[__name__]
        if section in config.sections() and option in config.options(section):
            setattr(self_module, var, proc(config.get(section, option)))
            return True
        else:
            return False

    conv_int = lambda val: int(val)
    conv_bool = lambda val: val.lower() == 'true'

    def conv_datetime(val):
        try:
            return datetime.datetime.strptime(val, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            return None

    try:
        with open(cfg_file, 'r') as cf:
            config.readfp(cf)
    except IOError:
        pass

    # SECTION: DEBUG
    section = 'DEBUG'
    read_settings(section, 'DEBUG_HOST')
    read_settings(section, 'DEBUG_PORT', proc=conv_int)
    read_settings(section, 'DEBUG_FLAG', proc=conv_bool)
    read_settings(section, 'LOG_DEBUG', proc=conv_bool)

    # SECTION: MISC
    read_settings('MISC', 'EMAIL_ADDR', proc=lambda val: [email.strip() for email in val.split('|')])

    # SECTION DATABASE
    read_settings('DATABASE', 'WRITE_DATABASE', proc=conv_bool)

    # SECTION CHECKPOINT
    section = 'CHECKPOINT'
    read_settings(section, 'LAST_CRAWLED', proc=conv_datetime)
    read_settings(section, 'LAST_PROCESS_TAGS', proc=conv_datetime)


_load_user_cfg()