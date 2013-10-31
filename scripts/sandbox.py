# coding=utf-8

import _mysql
import json
import os
import Image
import common as cm

__author__ = 'Zephyre'

db_spec = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123', 'port': 1228,
           'schema': 'release_stores'}
home_path = '/home/rose/MStore/storage/products/images'

db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                    passwd=db_spec['password'], db=db_spec['schema'])
db.query("SET NAMES 'utf8'")


def func1():
    db.query('SELECT * FROM products WHERE brand_id=10226')
    rs = db.store_result()

    tot = rs.num_rows()
    cnt = 0
    missing = 0
    print str.format('Total records: {0}', tot)
    for i in xrange(tot):
        record = rs.fetch_row(how=1)
        model = record[0]['model']
        image_list = json.loads(record[0]['image_list'])
        for path in [val['path'] for val in image_list]:
            try:
                img = Image.open(os.path.join(home_path, path))
                if cnt % 100 == 0:
                    print cnt
            except IOError:
                missing += 1
                print str.format('{0} / {1} missing!', model, path)
            finally:
                cnt += 1

    print str.format('{0} missing.', missing)


def func2():
    db.query('SELECT * FROM products WHERE brand_id=10226')
    rs = db.store_result()
    tot = rs.num_rows()
    print str.format('Total records: {0}', tot)
    cnt = 0
    for i in xrange(tot):
        record = rs.fetch_row(how=1)[0]
        idproducts = record['idproducts']
        image_list = json.loads(record['image_list'])
        for val in image_list:
            val['height'] = int(val['height'])
            val['width'] = int(val['width'])
        image_list = json.dumps(image_list, ensure_ascii=False)
        cover_image = None
        if len(image_list) > 0:
            cover_image = json.dumps(image_list[0], ensure_ascii=False)

        cm.update_record(db, {'image_list': image_list, 'cover_image': cover_image}, 'products',
                         str.format('idproducts={0}', idproducts))
        cnt += 1
        print str.format('#{0} processed', cnt)


func2()

