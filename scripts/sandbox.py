# coding=utf-8

# import _mysql
# import json
# import os
# import shutil
# import Image
# import errno
import time
# import common as cm
import sys
import threading
import global_settings as glob
import core

if glob.DEBUG_FLAG:
    import pydevd

    pydevd.settrace('localhost', port=glob.DEBUG_PORT, stdoutToServer=True, stderrToServer=True)

import datetime
from sync_product import spider2editor, process_editor_price

__author__ = 'Zephyre'

db_spec = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123', 'port': 3306,
           'schema': 'spider_stores'}
home_path = '/home/rose/MStore/storage/products/images'

# db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
#                     passwd=db_spec['password'], db=db_spec['schema'])
# db.query("SET NAMES 'utf8'")

class Object(object):
    pass


def func1():
    try:
        os.makedirs(os.path.join(home_path, '10135_fendi/full'))
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

    db.query('SELECT idproducts_image, path FROM products_image WHERE brand_id=10152')
    rs = db.store_result()
    tot = rs.num_rows()
    db.query('START TRANSACTION')
    for i in xrange(tot):
        record = rs.fetch_row()[0]
        src = os.path.join(home_path, record[1])
        filename = os.path.split(src)[-1]
        dst = os.path.join('10152_gucci/full', filename)
        statement = str.format('UPDATE products_image SET path="{0}" WHERE idproducts_image={1}', dst, record[0])
        db.query(statement)
    db.query('COMMIT')


def func2():
    db.query('SELECT * FROM products WHERE brand_id=10226')
    rs = db.store_result()
    tot = rs.num_rows()
    print str.format('Total records: {0}', tot)
    cnt = 0
    db.query('START TRANSACTION')
    for i in xrange(tot):
        record = rs.fetch_row(how=1)[0]
        idproducts = record['idproducts']
        image_list = json.loads(record['image_list'])
        for val in image_list:
            val['height'] = int(val['height'])
            val['width'] = int(val['width'])
            # image_list = json.dumps(image_list, ensure_ascii=False)
        cover_image = None
        if len(image_list) > 0:
            cover_image = image_list[0]

        cm.update_record(db, {'image_list': json.dumps(image_list, ensure_ascii=False),
                              'cover_image': json.dumps(cover_image)}, 'products',
                         str.format('idproducts={0}', idproducts))
        cnt += 1
        print str.format('#{0} processed', cnt)

    db.query('COMMIT')


def func3(*args, **kwargs):
    tot = 10
    print 'STARTED\n'
    lock = kwargs['lock']
    for i in xrange(tot):
        if lock.acquire():
            text = str.format('Completed: {0:.1%}', float(tot - i) / tot)
            sys.stdout.write(' ' * 80 + '\r' + text + '\r')
            sys.stdout.flush()
            time.sleep(1)
        else:
            print '\nTIMEOUT'
            break

    print '\nDONE'


def func4(*args, **kwargs):
    print datetime.datetime.now()


def run(self):
    self.count = 0
    self.tot = 100

    for i in xrange(self.tot):
        self.count += 1
        time.sleep(0.33)


def get_msg(self):
    return str.format('{0} out of {1} completed({2:.1%})', self.count, self.tot, float(self.count) / self.tot)


# obj = Object()
# obj.run = lambda: run(obj)
# obj.get_msg = lambda: get_msg(obj)
#
# core.func_carrier(obj, 2)

print 'Syncing: editor => spider'
spider2editor()

print 'Processing price info...'
process_editor_price()



# th=threading.Timer(2,func4)
# th.start()
# time.sleep(100)
