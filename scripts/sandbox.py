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
from PIL import Image
import common as cm
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

    def func_oneuse(self):
        db = MySqlDb()
        db.conn(glob.EDITOR_SPEC)

        rs = db.query('SELECT p1.* FROM editor_stores.products_image as p1 '
                      'left join images_store as p2 on p1.checksum=p2.checksum '
                      'where p2.checksum is null')
        self.tot = rs.num_rows()
        for self.progress in xrange(self.tot):
            record = rs.fetch_row(how=1)[0]
            path = record['path']
            if record['brand_id'] == '10226':
                path = hashlib.sha1(record['url']).hexdigest()+'.png'
            full_path = os.path.normpath(os.path.join(glob.STORAGE_PATH, 'products/images', path))

            try:
                img = Image.open(full_path)
                entry = {'checksum': record['checksum'], 'url': record['url'], 'path': path,
                         'width': img.size[0], 'height': int(img.size[1]), 'format': img.format,
                         'size': os.path.getsize(full_path)}
                db.insert(entry, 'images_store', ignore=True)
            except IOError:
                continue


    def func_oneuse2(self):
        db = MySqlDb()
        db.conn(glob.EDITOR_SPEC)
        for file_name in (tmp for tmp in os.listdir('.') if re.search(r'\d{5}\.csv', tmp)):
            db.start_transaction()
            try:
                with open(file_name, 'r') as f:
                    row_list = list(csv.reader(f))
                    self.tot = len(row_list)
                    self.progress = 0
                    for row in row_list:
                        self.progress += 1
                        if row[0][:3] == codecs.BOM_UTF8:
                            row[0] = row[0][3:]
                        if file_name == '10226.csv':
                            brand_id, region, tag_name = 10226, 'cn', cm.unicodify(row[0])
                            mapping_list = [cm.unicodify(val) for val in row[2:] if val.strip()]
                            rs = db.query_match(['idmappings'], 'original_tags',
                                                {'brand_id': brand_id, 'region': region,
                                                 'tag_name': tag_name}).fetch_row()
                        else:
                            brand_id, region, tag_type, tag_name = (cm.unicodify(row[i]) for i in (0, 2, 3, 4))
                            mapping_list = [cm.unicodify(val) for val in row[6:] if val.strip() and val != 'NULL']
                            rs = db.query_match(['idmappings'], 'original_tags',
                                                {'brand_id': brand_id, 'region': region,
                                                 'tag_type': tag_type,
                                                 'tag_name': tag_name}).fetch_row()
                        if not rs or not mapping_list:
                            continue
                        db.update({'mapping_list': json.dumps(mapping_list, ensure_ascii=False)}, 'original_tags',
                                  str.format('idmappings={0}', rs[0][0]))
                db.commit()
            except:
                db.rollback()
                raise

        db.close()

    def func_oneuse1(self):
        db = MySqlDb()
        db.conn(glob.EDITOR_SPEC)

        rs_all = db.query(
            'SELECT idproducts,brand_id,region,extra,gender FROM products WHERE modified!=1')
        self.progress = 0
        self.tot = rs_all.num_rows()
        while True:
            tmp = rs_all.fetch_row(how=1)
            if not tmp:
                break
            record = tmp[0]
            self.progress += 1

            if not record['extra']:
                continue

            pid = int(record['idproducts'])
            try:
                extra = json.loads(record['extra'].replace('\r', '').replace('\n', ''))
            except ValueError:
                continue

            for tag_type in extra:
                tmp = extra[tag_type]
                if tag_type == 'size':
                    continue

                for tag_name in tmp if cm.iterable(tmp) else [tmp]:
                    db.start_transaction()
                    try:
                        rs = db.query(unicode.format(u'SELECT idmappings FROM original_tags WHERE brand_id={0} AND '
                                                     u'region="{1}" AND tag_type="{2}" AND tag_name="{3}"',
                                                     record['brand_id'], record['region'], tag_type,
                                                     tag_name.replace('\\', '\\\\').replace('"', '\\"')))
                        if rs.num_rows() == 0:
                            rs = db.query(unicode.format(u'SELECT idmappings FROM original_tags WHERE brand_id={0} AND '
                                                         u'region="{1}" AND tag_name="{2}"',
                                                         record['brand_id'], record['region'],
                                                         tag_name.replace('\\', '\\\\').replace('"', '\\"')))
                            if rs.num_rows() == 0:
                                db.insert(
                                    {'brand_id': record['brand_id'], 'region': record['region'], 'tag_name': tag_name,
                                     'tag_type': tag_type, 'tag_text': tag_name}, 'original_tags')
                                tid = db.query(unicode.format(
                                    u'SELECT idmappings FROM original_tags WHERE brand_id={0} AND region="{1}" AND '
                                    u'tag_type="{2}" AND tag_name="{3}"',
                                    record['brand_id'], record['region'], tag_type,
                                    tag_name.replace('\\', '\\\\').replace('"', '\\"'))).fetch_row()[0][0]
                            else:
                                tid = rs.fetch_row()[0][0]
                        else:
                            tid = rs.fetch_row()[0][0]

                        # 找到tag，更新products_original_tags表
                        db.insert({'idproducts': pid, 'id_original_tags': tid}, 'products_original_tags', ignore=True)

                        db.commit()
                    except:
                        db.rollback()
                        raise

            gender = None
            tmp = record['gender']
            if tmp:
                if tmp == 'male':
                    gender = 'male'
                elif tmp == 'female':
                    gender = 'female'
                else:
                    tmp = json.loads(tmp)
                    val = 0
                    for g in tmp:
                        if g in ('female', 'women'):
                            val |= 1
                        elif g in ('male', 'men'):
                            val |= 2
                    if val == 3 or val == 0:
                        gender = None
                    elif val == 1:
                        gender = 'female'
                    elif val == 2:
                        gender = 'male'

            db.update({'modified': 1, 'gender': gender}, 'products', str.format('idproducts={0}', pid))


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


core.func_carrier(Object(), 0.3)
# core.func_carrier(SyncProducts(src_spec=glob.TMP_SPEC, cond=['brand_id=10226']), 1)



