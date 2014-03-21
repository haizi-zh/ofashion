#!/usr/bin/env python
# coding=utf-8
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


if __name__ == '__main__':
    obj = Sandbox()
    # obj.run()
    core.func_carrier(Sandbox(), 0.3)
    pass
