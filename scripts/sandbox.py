#!/usr/bin/env python
# coding=utf-8

import codecs
import hashlib

import json
import os
import re
import shutil
from _mysql_exceptions import OperationalError
import global_settings as glob
from core import MySqlDb
import core
import common as cm
from PIL import Image

import csv
from scripts.sync_product import SyncProducts


if glob.DEBUG_FLAG:
    import pydevd

    pydevd.settrace(host=glob.DEBUG_HOST, port=glob.DEBUG_PORT, stdoutToServer=True, stderrToServer=True)
    print 'REMOTE DEBUG ENABLED'

__author__ = 'Zephyre'

db_spec = {'host': '127.0.0.1', 'username': 'rose', 'password': 'rose123', 'port': 3306,
           'schema': 'release_stores'}


class Object(object):
    def __init__(self, brand_id=None, db_spec=None):
        self.progress = 0
        self.tot = 0
        self.brand_id = brand_id
        self.db_spec = db_spec

    def run(self):
        self.func_oneuse()

    def reduce_duplicate_image(self, db_spec=glob.SPIDER_SPEC, table='products_image'):
        db = MySqlDb()
        db.conn(db_spec)

        db.lock([table])
        db.start_transaction()
        try:
            rs = db.query(
                str.format('SELECT COUNT(*),checksum FROM {0} WHERE checksum IS NOT NULL GROUP BY checksum',
                           table)).fetch_row(maxrows=0)
            # 有重复的checksum
            checksum_list = [val[1] for val in rs if int(val[0]) > 1]
            self.tot = len(checksum_list)
            self.progress = 0
            for checksum in checksum_list:
                results = db.query(
                    str.format('SELECT model,url,path,idproducts_image,brand_id FROM {0} WHERE checksum="{1}"', table,
                               checksum)).fetch_row(maxrows=0,
                                                    how=1)

                # 找出需要删除的id
                model_set = set([])
                id_deleted = set([])
                for r in results:
                    model = r['model']
                    if model in model_set:
                        id_deleted.add(r['idproducts_image'])
                    else:
                        model_set.add(model)

                if len(id_deleted) > 0:
                    db.execute(
                        str.format('DELETE FROM {0} WHERE idproducts_image IN ({1})', table, ', '.join(id_deleted)))

                if len(set(r['path'] for r in results)) > 1:
                    db.update({'path': results[0]['path']}, table,
                              str.format('idproducts_image IN ({0})', ','.join(r['idproducts_image'] for r in results)))
                    # # 删除多余的文件
                # for path in [val['path'] for val in results[1:]]:
                #     try:
                #         os.remove(os.path.join(glob.STORAGE_PATH, 'products/images', path))
                #     except (OSError, IOError):
                #         pass
                self.progress += 1

            db.commit()
        except OperationalError:
            db.rollback()
            raise
        finally:
            db.unlock()
            db.close()

    def proc_price(self):
        """
        原先的价格处理函数有错误。解决方法：根据products.price字段，对价格重新处理。如果和price_history相应单品的最新记录不一致，则
        覆盖、更正之（而不是添加）！
        """
        max_transaction = 10000
        db = MySqlDb()
        db.conn(glob.EDITOR_SPEC)

        self.tot = int(db.query('SELECT COUNT(*) FROM products WHERE price IS NOT NULL').fetch_row()[0][0])
        self.progress = 0
        rs_tot = db.query_match(['idproducts', 'price', 'region', 'brand_id', 'model'], 'products', {},
                                extra='price IS NOT NULL').fetch_row(maxrows=0)

        db.start_transaction()
        for pid, price_str, region, brand, model in rs_tot:
            self.progress += 1

            price = cm.process_price(price_str, region=region)
            if not price:
                continue

            r = {'idproducts': int(pid), 'price': price['price'], 'brand_id': int(brand), 'model': cm.unicodify(model)}
            db.insert({'idproducts': r['idproducts'], 'price': price['price'], 'currency': price['currency']},
                      'products_price_history')

            # rs = db.query_match(['*'], 'products_price_history', {'idproducts': r['idproducts']},
            #                     tail_str='ORDER BY date DESC LIMIT 1').fetch_row(maxrows=0, how=1)
            # if float(rs[0]['price']) != r['price']:
            #     print str.format('\rPRICE MISMATCH: brand:{0}, model:{1}, idproducts:{4} price:{2}=>{3}',
            #                      r['brand_id'], r['model'], rs[0]['price'], r['price'], r['idproducts'])
            #     db.update({'price': r['price']}, 'products_price_history',
            #               str.format('idprice_history={0}', rs[0]['idprice_history']))

        db.commit()
        db.close()

    def func_oneuse(self):
        self.proc_price()

    def proc_image_path(self):
        db = MySqlDb()
        db.conn(glob.EDITOR_SPEC)

        rs = db.query('SELECT * FROM images_store WHERE path NOT LIKE "%/full/%"')
        self.tot = rs.num_rows()
        db.start_transaction()
        for self.progress in xrange(self.tot):
            record = rs.fetch_row(how=1)[0]
            path = record['path']
            checksum = record['checksum']
            path = re.sub(r'/full(?!/)', '/full/', path)
            db.update({'path': path}, 'images_store', str.format('checksum="{0}"', checksum))

        db.commit()
        db.close()


    def image_compact(self):
        """
        检查图片仓库，输出没有引用的图片
        """

        db = MySqlDb()
        db.conn(self.db_spec)

        db.lock(['products_image'])
        db.start_transaction()

        walks = list(os.walk(os.path.join(glob.STORAGE_PATH, 'products/images')))
        tot = 0
        for root, dirs, files in walks:
            tot += len(files)

        self.tot = tot

        self.progress = 0
        for root, dirs, files in walks:
            for image_name in files:
                self.progress += 1
                rs = db.query(str.format('SELECT COUNT(*) FROM products_image WHERE path like "%{0}%"', image_name))
                if int(rs.fetch_row()[0][0]) == 0:
                    print str.format('Orphan image: {0}', os.path.normpath(os.path.join(root, image_name)))
                    shutil.move(os.path.join(root, image_name), os.path.join(glob.STORAGE_PATH, 'orphan', image_name))

        db.rollback()
        db.unlock()
        db.close()

    def func2(self):
        db = MySqlDb()
        db.conn(self.db_spec)

        db.execute('LOCK TABLES products_image WRITE')
        db.start_transaction()

        try:
            db.execute('DROP TEMPORARY TABLES IF EXISTS tmp')
            db.execute(str.format('CREATE TEMPORARY TABLE tmp (SELECT * FROM products_image WHERE brand_id={0})',
                                  self.brand_id))

            rs = db.query('SELECT idproducts_image, path FROM tmp')
            self.tot = rs.num_rows()
            image_dir = os.path.normpath(os.path.join(glob.STORAGE_PATH, 'products/images'))
            for i in xrange(self.tot):
                self.progress = i
                pid, full_path = rs.fetch_row()[0]
                path, tail = os.path.split(full_path)
                mt = re.search(r'_([^_]+\.[a-zA-Z]{3})$', tail)
                if mt:
                    # print tail
                    new_tail = mt.group(1)
                    try:
                        shutil.move(os.path.join(image_dir, path, tail),
                                    os.path.join(image_dir, path, new_tail))
                    except IOError:
                        pass

                    db.update({'path': os.path.normpath(os.path.join(path, new_tail))}, 'products_image',
                              str.format('idproducts_image={0}', pid))


                    # temp = []
                    # path = full_path
                    # for j in xrange(3):
                    #     path, tail = os.path.split(path)
                    #     temp.append(tail)
                    # temp.reverse()
                    #
                    # path = apply(os.path.join, temp)
                    #
                    # rs1 = db.query(str.format('SELECT COUNT(*) FROM tmp WHERE path="{0}"', path))
                    # if int(rs1.fetch_row()[0][0]) == 0:
                    #     db.update({'path': path}, 'products_image', str.format('idproducts_image={0}', pid))
                    # else:
                    #     db.execute(str.format('DELETE FROM products_image WHERE idproducts_image={0}', pid))

            db.commit()
        except:
            db.rollback()
            raise
        finally:
            db.execute('UNLOCK TABLES')
            db.close()


    def func1(self):
        db = MySqlDb()
        db.conn(db_spec)

        db.execute('LOCK TABLES products READ')
        db.start_transaction()
        try:
            rs = db.query('SELECT idproducts,image_list,cover_image FROM products')
            self.tot = rs.num_rows()
            for i in xrange(self.tot):
                record = rs.fetch_row(how=1)[0]
                image_list = json.loads(record['image_list'])
                for item in image_list:
                    item['width'] = int(item['width'])
                    item['height'] = int(item['height'])

                try:
                    cover = json.loads(record['cover_image'])
                except:
                    print record['cover_image']

                cover['width'] = int(cover['width'])
                cover['height'] = int(cover['height'])

                db.update({'cover_image': cover, 'image_list': image_list}, 'products',
                          str.format('idproducts={0}', record['idproducts']))
        finally:
            db.commit()
            # db.execute('UNLOCK TABLES')

    def get_msg(self):
        if self.tot != 0:
            return str.format('{0}/{1}({2:.1%}) completed', self.progress, self.tot, float(self.progress) / self.tot)
        else:
            return ''


def func1():
    base_dir = os.path.join(glob.STORAGE_PATH, 'products/images/10226_louis_vuitton')

    full_list = set(os.listdir(os.path.join(base_dir, 'full')))
    thumb_list = set(os.listdir(os.path.join(base_dir, 'thumb')))

    a = thumb_list - full_list
    b = full_list - thumb_list

    print 'Done'

# cm.process_price('12.30', 'cn', currency='CNY')
core.func_carrier(Object(), 0.3)
# core.func_carrier(SyncProducts(src_spec=glob.TMP_SPEC, cond=['brand_id=10226']), 1)



