#!/usr/bin/env python
# coding=utf-8
import hashlib
from core import MySqlDb
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
        if 'brand' in param:
            self.brand_list = [int(val) for val in param['brand']]
        else:
            self.brand_list = None

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def run(self):
        db = MySqlDb()
        db.conn(gs.DB_SPEC)

        if not self.brand_list:
            rs = db.query_match(['brand_id'], 'products', distinct=True)
            brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]
            self.brand_list = brand_list
        else:
            brand_list = self.brand_list

        # 获得总数
        self.tot = int(db.query(str.format('SELECT COUNT(*) FROM products WHERE brand_id IN ({0})',
                                           ','.join(str(tmp) for tmp in self.brand_list))).fetch_row()[0][0])
        self.progress = 0

        for brand in brand_list:
            print(str.format('PROCESSING {0}', brand))
            db.start_transaction()
            try:
                records = db.query_match(['model', 'idproducts'], 'products', {'brand_id': brand}).fetch_row(maxrows=0,
                                                                                                             how=1)
                for r in records:
                    self.progress += 1
                    fingerprint = encrypt(str(brand) + r['model'])
                    db.update({'fingerprint': fingerprint}, 'products', str.format('idproducts={0}', r['idproducts']))
            except:
                db.rollback()
                raise
            finally:
                db.commit()

        db.close()