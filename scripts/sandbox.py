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

    def reformat(self, text):
        """
        格式化字符串，将多余的空格、换行、制表符等合并
        """
        text = cm.unicodify(text)
        if text is None:
            return None
        text = cm.html2plain(text.strip())
        # <br/>换成换行符
        text = re.sub(ur'<\s*br\s*/?>', u'\r\n', text)
        # 去掉多余的标签
        text = re.sub(ur'<[^<>]*?>', u'', text)
        # # 换行转换
        text = re.sub('[\r\n]+', '\r', text)
        # text = re.subn(ur'(?:[\r\n])+', ', ', text)[0]
        # 去掉连续的多个空格
        text = re.sub(r'[ \t]+', ' ', text)
        return text

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def run(self):
        db = MySqlDb()
        db.conn(glob.EDITOR_SPEC)

        rs = db.query_match(['idproducts', 'name', 'category'], 'products', {},
                            extra=['name like "%&nbsp;%" OR category like "%&nbsp;%"']).fetch_row(maxrows=0, how=1)
        db.start_transaction()
        for record in rs:
            pid = int(record['idproducts'])
            name = self.reformat(record['name'])
            category = self.reformat(record['category'])
            db.update({'name': name, 'category': category}, 'products', str.format('idproducts={0}', pid))
        db.commit()
        #self.func_oneuse()

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
            #db.insert({'idproducts': r['idproducts'], 'price': price['price'], 'currency': price['currency']},
            #          'products_price_history')

            rs = db.query_match(['*'], 'products_price_history', {'idproducts': r['idproducts']},
                                tail_str='ORDER BY date DESC LIMIT 1').fetch_row(maxrows=0, how=1)
            if float(rs[0]['price']) != r['price']:
                print str.format('\rPRICE MISMATCH: brand:{0}, model:{1}, idproducts:{4} price:{2}=>{3}',
                                 r['brand_id'], r['model'], rs[0]['price'], r['price'], r['idproducts'])
                #db.update({'price': r['price']}, 'products_price_history',
                #          str.format('idprice_history={0}', rs[0]['idprice_history']))

        db.commit()
        db.close()

    def func_oneuse(self):
        self.func_1()
        #self.proc_price()

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

    def func_1(self):
        db = MySqlDb()
        db.conn(glob.EDITOR_SPEC)

        self.tot = int(db.query('SELECT COUNT(*) FROM images_store where path like "10109%" order by '
                                'url').fetch_row()[0][0])
        self.progress = 0
        storage_path = os.path.normpath(os.path.join(glob.STORAGE_PATH, 'products/images'))

        rs = db.query_match(['checksum', 'url', 'path'], 'images_store', extra=['path like "10109%"'],
                            tail_str='order by url')
        cur_data = {}
        del_list = []
        db.start_transaction()
        try:
            while True:
                record = rs.fetch_row(how=1)
                if not record:
                    break
                else:
                    record = record[0]
                self.progress += 1

                full_path = os.path.normpath(os.path.join(storage_path, record['path']))
                try:
                    img = Image.open(full_path)
                    dim = img.size[0]
                except IOError:
                    continue

                # 取model和特征值
                mt = re.search(r'/([\da-zA-Z]+)_\d+_([a-z])\.', record['url'])
                if not mt:
                    continue
                model = mt.group(1)
                tail = mt.group(2)
                key = str.format('{0}_{1}', model, tail)

                to_del = None
                if key in cur_data:
                    tmp = cur_data[key]
                    if dim > tmp['dim']:
                        del_list.append(tmp['checksum'])
                        to_del = tmp['checksum']
                        tmp['checksum'] = record['checksum']
                        tmp['dim'] = dim
                    else:
                        del_list.append(record['checksum'])
                        to_del = record['checksum']
                else:
                    cur_data[key] = {'checksum': record['checksum'], 'dim': dim}

                if to_del:
                    db.execute(str.format('DELETE FROM images_store WHERE checksum="{0}"', to_del))
        except:
            db.rollback()
            raise

            ## 开始删除
            #db.execute(str.format('DELETE FROM images_store WHERE checksum in ({0})',
            #                      ','.join(str.format('"{0}"', val) for val in del_list)))

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


core.func_carrier(Object(), 0.3)



