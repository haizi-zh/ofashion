#!/usr/bin/env python
# coding=utf-8
import sys
import hashlib
import json
from core import RoseVisionDb
import global_settings as gs


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
            db.start_transaction()
            try:
                for pid, color in db.query(
                        r'''
SELECT idproducts, color FROM products where color like '%""%'
                        ''').fetch_row(maxrows=0):
                    nc = [tmp for tmp in json.loads(color) if tmp]
                    db.update({'color': json.dumps(nc, ensure_ascii=False)}, 'products', str.format('idproducts={0}', pid))
            except:
                db.rollback()
                raise
            finally:
                db.commit()


if __name__ == '__main__':
    obj = Sandbox()
    obj.run()
    pass