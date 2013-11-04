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
EMAIL_ADDRESSES = ['haizi.zh@gmail.com', 'haizi.zh@qq.com', 'buddy@mfashion.com.cn']




