# coding=utf-8
__author__ = 'Zephyre'

import sys

if sys.platform in ('win32', ):
    STORAGE_PATH = u'd:/Users/Zephyre/Development/mstore/storage'
    HOME_PATH = u'd:/Users/Zephyre/Dropbox/Freelance/MStore/src'
else:
    STORAGE_PATH = u'/home/rose/MStore/storage'
    HOME_PATH = u'/home/rose/MStore/src'

# Database
REMOTE_CONN = False
if sys.platform not in ('win32', ):
    REMOTE_CONN = False

EDITOR_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
               'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'editor_stores'}
RELEASE_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
                'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'release_stores'}
SPIDER_SPEC = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123',
               'port': 1228 if REMOTE_CONN else 3306 if REMOTE_CONN else 3306, 'schema': 'spider_stores'}

# Email settings for notification
EMAIL_ADDRESSES = ['haizi.zh@gmail.com', 'buddy@mfashion.com.cn']


# Port for remote debugging
DEBUG_HOST = 'localhost'
DEBUG_PORT = 7105
DEBUG_FLAG = True
WRITE_DATABASE = True

# Log settings
LOG_DEBUG = False

CURRENCY_MAP = {'cn': 'CNY', 'us': 'USD', 'uk': 'GBP', 'hk': 'HKD', 'sg': 'SGD', 'de': 'EUR', 'es': 'EUR',
                'fr': 'EUR', 'it': 'EUR', 'jp': 'JPY', 'kr': 'KRW', 'mo': 'MOP', 'ae': 'AED', 'au': 'AUD',
                'br': 'BRL', 'ca': 'CAD', 'my': 'MYR', 'ch': 'CHF', 'nl': 'EUR', 'ru': 'RUB', 'tw': 'TWD',
                'at': 'EUR', 'th': 'THB', 'dk': 'DKK', 'be': 'EUR', 'fi': 'EUR', 'ie': 'EUR', 'lu': 'EUR',
                'no': 'NOK', 'se': 'SEK', 'mc': 'EUR', 'pt': 'EUR', 'gr': 'EUR', 'ap': 'USD', 'ii': 'USD'}

SUPPORTED_REGION = ['cn', 'us', 'fr', 'uk', 'hk', 'jp', 'it', 'au', 'ae', 'sg', 'de', 'ca', 'es', 'ch', 'ru', 'br',
                    'th', 'kr', 'my', 'nl']

# 必须实现
BRAND_NAMES = {10226: {'brandname_e': 'Louis Vuitton', 'brandname_c': u'路易威登', 'brandname_s': 'louis_vuitton'},
               10135: {'brandname_e': 'Fendi', 'brandname_c': u'芬迪', 'brandname_s': 'fendi'},
               10057: {'brandname_e': 'Burberry', 'brandname_c': u'博柏利', 'brandname_s': 'burberry'},
               10074: {'brandname_e': 'Chanel', 'brandname_c': u'香奈儿', 'brandname_s': 'chanel'},
               10166: {'brandname_e': u'Hermès', 'brandname_c': u'爱马仕', 'brandname_s': 'hermes'},
               10066: {'brandname_e': 'Cartier', 'brandname_c': u'卡地亚', 'brandname_s': 'cartier'},
               10300: {'brandname_e': 'Prada', 'brandname_c': u'普拉达', 'brandname_s': 'prada'}}




