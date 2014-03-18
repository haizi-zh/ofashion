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

import pydevd
pydevd.settrace('localhost', port=7102, stdoutToServer=True, stderrToServer=True)

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
            idx = 0
            count = 50
            while True:
                tot = int(db.query('SELECT COUNT(idproducts) FROM products').fetch_row()[0][0])
                print str.format('idx={0}, tot={1}', idx, tot)
                db.start_transaction()
                try:
                    results = db.query(
                        str.format('SELECT idproducts, brand_id, model FROM products LIMIT {0}, {1}', idx,
                                   count)).fetch_row(maxrows=0)
                    if not results:
                        break

                    for pid, brand, model in results:
                        # 目标checksum
                        target = [tmp[0] for tmp in db.query_match(['checksum'], 'products_image',
                                                                   {'brand_id': brand, 'model': model}).fetch_row(
                            maxrows=0)]
                        for checksum in target:
                            db.insert({'idproducts_image': pid, 'brand_id': brand, 'model': model, 'checksum': checksum,
                                       'processed': 1}, 'products_image')
                    idx += count
                    db.commit()
                except:
                    db.rollback()
                    raise

    def run2(self):
        with RoseVisionDb(getattr(gs, 'DB_SPEC')) as db:
            rs = db.query('''
            SELECT COUNT(idproducts_image) FROM products_image AS p1 JOIN images_store AS p2 ON p1.checksum=p2.checksum;
            ''').fetch_row()
            self.tot = int(rs[0][0])
            self.progress = 0

            rs = db.query('''
            SELECT * FROM products_image AS p1 JOIN images_store AS p2 ON p1.checksum=p2.checksum;
            ''', use_result=True)

            fp_dict = {}
            while True:
                bulk = rs.fetch_row(maxrows=100, how=1)
                if not bulk:
                    break
                for results in bulk:
                    self.progress += 1
                    url_head = urlparse.urlsplit(results['url']).netloc
                    path_head = os.path.split(results['path'])[0]
                    fingerprint = '|'.join((results['brand_id'], url_head, path_head))
                    if fingerprint not in fp_dict:
                        fp_dict[fingerprint] = 1
                    else:
                        fp_dict[fingerprint] += 1

        for fp, count in sorted(fp_dict.items(), key=lambda val: val[0]):
            if not re.search(r'^\d{6}|[^|]+|\d{6}_[^/]+/full', fp):
                print "ERROR!!!"
            print str.format('{0}: {1}', fp, count)


if __name__ == '__main__':
    # obj = Sandbox()
    core.func_carrier(Sandbox(), 0.3)
    pass
