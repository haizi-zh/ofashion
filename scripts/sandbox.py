#!/usr/bin/env python
# coding=utf-8
import csv
import os
import re
import sys
import hashlib
import json
import urlparse
from core import RoseVisionDb
import core
import global_settings as gs

# import pydevd

# pydevd.settrace('localhost', port=7100, stdoutToServer=True, stderrToServer=True)

from utils.utils_core import gen_fingerprint

SALT_PLAIN = 'roseVision88'
SALT_md5 = hashlib.md5()
SALT_md5.update(SALT_PLAIN)
SALT = SALT_md5.digest()


def encrypt(idstores):
    md5 = hashlib.md5()
    md5.update(idstores)
    d1 = md5.digest()
    d2 = ''.join(map((lambda x, y: '{0:x}'.format((ord(x) + ord(y)) % 256)), d1, SALT))
    return d2


class Sandbox(object):
    """
    随意
    """

    def __init__(self, param=None):
        self.tot = 1
        self.progress = 0
        if param and 'brand' in param:
            self.brand_list = [int(val) for val in param['brand']]
        else:
            self.brand_list = None

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def run(self):
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            try:
                rs = db.query(
                    'SELECT idproducts_image, brand_id, model FROM products_image where fingerprint is null').fetch_row(
                    maxrows=0)
                self.tot = len(rs)
                self.progress = 0
                now_brand = 0
                for iid, brand_id, model in rs:
                    self.progress += 1
                    fp = gen_fingerprint(brand_id, model)
                    db.update({'fingerprint': fp}, 'products_image', str.format('idproducts_image={0}', iid))
            except:
                raise


def func1():
    brand_list = [10074, 13084, 10142, 10008, 10105, 10152, 10109, 10308, 10373, 10080, 10259, 10178, 10270, 10305,
                  10220, 10218, 10006, 10204, 10263, 10076, 10030, 10184, 10149, 10192, 10114, 10333, 10617, 10316,
                  10106, 10212]
    for brand in brand_list:
        cmd1 = str.format('python scripts/mstore.py process-tags --cond brand_id={0}', brand)
        cmd2 = str.format('python scripts/mstore.py release --brand {0}', brand)
        cmd = '; '.join([cmd1, cmd2])
        print cmd
        os.system(cmd)


def func2():
    with open('missing.txt', 'r') as f:
        local = set(json.load(f))

    with open('missing_p.txt', 'r') as f:
        local_p = set(json.load(f))

    with open('fingerprint.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter=' ', quotechar='|')
        remote = set([row[0].replace('"', '') for row in reader])

    with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
        local = set(
            [tmp[0] for tmp in db.query('SELECT DISTINCT fingerprint FROM products_release').fetch_row(maxrows=0)])
        local_p = set(
            [tmp[0] for tmp in db.query('SELECT DISTINCT fingerprint FROM products').fetch_row(maxrows=0)])

    cnt_local = len(local)
    cnt_remote = len(remote)

    delta = remote - local
    cnt_delta = len(delta)

    delta_p = remote - local_p
    cnt_local_p = len(local_p)

    with open('missing.txt', 'w') as f:
        json.dump(list(delta), f)

    with open('missing_p.txt', 'w') as f:
        json.dump(list(delta_p), f)


if __name__ == '__main__':
    func1()