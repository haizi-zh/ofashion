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
#
# pydevd.settrace('localhost', port=7102, stdoutToServer=True, stderrToServer=True)

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
            # self.tot = int(db.query('''
            # SELECT COUNT(*) FROM products AS p1
            # JOIN products_image AS p2 ON p1.fingerprint=p2.fingerprint
            # JOIN images_store AS p3 ON p2.checksum=p3.checksum
            # ''').fetch_row()[0][0])
            self.tot = 10000
            self.progress = 0

            rs = db.query('''
            SELECT p1.brand_id, p3.path FROM products AS p1
            JOIN products_image AS p2 ON p1.fingerprint=p2.fingerprint
            JOIN images_store AS p3 ON p2.checksum=p3.checksum
            ''', use_result=True)

            err_set = set({})
            while True:
                bulk = rs.fetch_row(maxrows=100)
                if not bulk:
                    break

                for brand_id, path in bulk:
                    self.progress += 1
                    if not re.search(str.format('^{0}_', brand_id), path):
                        print str.format('{0}:{1}', brand_id, path)

            print 'DONE'

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
    obj = Sandbox()
    obj.run()
    # core.func_carrier(Sandbox(), 0.3)
    pass
