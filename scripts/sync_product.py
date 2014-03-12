# coding=utf-8
import json
import re
from _mysql_exceptions import OperationalError
import time
import core
import common as cm
from utils.utils_core import process_price, unicodify, iterable

__author__ = 'Zephyre'

import global_settings as glob
from core import RoseVisionDb


class SyncProducts(object):
    def __init__(self, src_spec=glob.DB_SPEC, dst_spec=glob.DB_SPEC, cond=None):
        self.progress = 0
        self.tot = 1
        if cond:
            if iterable(cond):
                self.cond = cond
            else:
                self.cond = [cond]
        else:
            self.cond = ['1']
        self.src_spec = src_spec
        self.dst_spec = dst_spec

    def run(self):
        db_src = RoseVisionDb()
        db_src.conn(self.src_spec)
        db_dst = RoseVisionDb()
        db_dst.conn(self.dst_spec)

        # 备选记录
        idproducts_list = [int(val[0]) for val in db_src.query(
            unicode.format(u'SELECT idproducts FROM products WHERE {0}', u' AND '.join(self.cond)).encode(
                'utf-8')).fetch_row(maxrows=0)]

        self.tot = len(idproducts_list)
        self.progress = 0

        db_dst.execute('SET AUTOCOMMIT=0')
        # db_dst.execute('ALTER TABLE products DISABLE KEYS')

        for pid_src in idproducts_list:
            self.progress += 1
            record = db_src.query(str.format('SELECT * FROM products WHERE idproducts={0}', pid_src)).fetch_row(how=1)[
                0]

            db_dst.start_transaction()
            try:
                rs = db_dst.query(
                    str.format('SELECT idproducts FROM products WHERE brand_id={0} AND model="{1}" '
                               'AND region="{2}"', record['brand_id'], record['model'], record['region']))
                pid_dst = int(rs.fetch_row()[0][0]) if rs.num_rows() > 0 else None
                entry = {k: record[k] for k in record if k != 'idproducts'}

                price = process_price(record['price'], record['region'])
                if price:
                    entry['price_rev'] = price['price']
                    entry['currency_rev'] = price['currency']

                if entry['details']:
                    entry['details'] = self.process_text(unicodify(entry['details']))
                if entry['description']:
                    entry['description'] = self.process_text(unicodify(entry['description']))
                if entry['name']:
                    entry['name'] = self.process_text(unicodify(entry['name']))
                if entry['category']:
                    entry['category'] = self.process_text(unicodify(entry['category']))
                if entry['extra']:
                    entry['extra'] = self.process_text(unicodify(entry['extra']))

                if pid_dst:
                    db_dst.update(entry, 'products', str.format('idproducts={0}', pid_dst))
                else:
                    db_dst.insert(entry, 'products')
                    pid_dst = int(
                        db_dst.query(str.format('SELECT idproducts FROM products WHERE brand_id={0} AND model="{1}" '
                                                'AND region="{2}"', record['brand_id'], record['model'],
                                                record['region'])).fetch_row()[0][0])

                # 是否需要处理价格信息
                if price:
                    record_price = db_dst.query(str.format('SELECT price,currency FROM products_price_history '
                                                           'WHERE idproducts={0} ORDER BY date DESC LIMIT 1',
                                                           pid_dst)).fetch_row(how=1)
                    if not record_price or float(record_price[0]['price']) != price['price'] or \
                                    record_price[0]['currency'] != price['currency']:
                        db_dst.insert({'idproducts': pid_dst, 'date': record['update_time'],
                                       'brand_id': record['brand_id'], 'region': record['region'],
                                       'model': record['model'], 'price': price['price'],
                                       'currency': price['currency']}, 'products_price_history')

                # 处理图像信息
                tmp = db_src.query(
                    str.format('SELECT checksum,brand_id,url,path,width,height,format FROM products_image '
                               'WHERE brand_id={0} AND model="{1}"',
                               record['brand_id'], record['model'])).fetch_row(maxrows=0, how=1)
                image_record = {val['checksum']: val for val in tmp}
                checksum_src = set(image_record.keys())

                # 完善images_store信息。如果checksum没有在images_store中出现，则添加之。
                for checksum in checksum_src:
                    if db_dst.query(str.format('SELECT checksum FROM images_store WHERE checksum="{0}"',
                                               checksum)).num_rows() == 0:
                        db_dst.insert({'checksum': checksum, 'brand_id': image_record[checksum]['brand_id'],
                                       'url': image_record[checksum]['url'], 'path': image_record[checksum]['path'],
                                       'width': image_record[checksum]['width'],
                                       'height': image_record[checksum]['height'],
                                       'format': image_record[checksum]['format']}, 'images_store')

                # 补充目标数据库的products_image表，添加相应的checksum
                checksum_dst = set([val[0] for val in db_dst.query(
                    str.format('SELECT checksum FROM products_image WHERE brand_id={0} AND model="{1}"',
                               record['brand_id'], record['model'])).fetch_row(maxrows=0)])
                for checksum in checksum_src - checksum_dst:
                    db_dst.insert({'checksum': checksum, 'brand_id': record['brand_id'], 'model': record['model']},
                                  'products_image')

                db_dst.commit()
            except:
                db_dst.rollback()
                raise

                # db_dst.execute('ALTER TABLE products ENABLE KEYS')

    def process_text(self, val):
        val = cm.html2plain(val.strip())
        # <br/>换成换行符
        val = re.sub(ur'<\s*br\s*/?\s*>', u'\n', val)

        # 去掉多余的标签
        val = re.sub(ur'<[^<>]*?>', u'', val)

        return val

    def get_msg(self):
        if self.tot > 0:
            return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot, float(self.progress) / self.tot)
        else:
            return str.format('{0}/{1} PROCESSED', self.progress, self.tot)


class Spider2EditorHlper(object):
    def __init__(self, src_rs, db):
        self.rs = src_rs
        self.db = db
        self.progress = 0
        self.tot = self.rs.num_rows()

    def run(self):
        for i in xrange(self.tot):
            temp = self.rs.fetch_row(how=1)[0]
            record = dict((k, unicodify(temp[k])) for k in temp)

            rs = self.db.query(
                str.format('SELECT idproducts FROM products WHERE idproducts={0}', record['idproducts']))
            if rs.num_rows() == 0:
                # 新数据
                record['update_flag'] = 'I'
                self.db.insert(record, 'products')
            else:
                # 已经存在，更新
                record['update_flag'] = 'U'
                self.db.update(record, 'products', str.format('idproducts={0}', record['idproducts']))
            self.progress = i + 1

    def get_msg(self):
        return str.format('{0} out of {1} completed({2:.1%})', self.progress, self.tot, float(self.progress) / self.tot)


class EditorPriceProcessor(object):
    def __init__(self, src_rs, db):
        self.rs = src_rs
        self.db = db
        self.progress = 0
        self.tot = self.rs.num_rows()

    def run(self):
        for i in xrange(self.tot):
            temp = self.rs.fetch_row(how=1)[0]
            record = dict((k, unicodify(temp[k])) for k in temp)

            ret = process_price(record['price'], record['region'])
            self.db.update({'price_rev': ret['price'], 'currency_rev': ret['currency']}, 'products',
                           str.format('idproducts={0}', record['idproducts']))
            self.progress = i + 1

    def get_msg(self):
        return str.format('{0} out of {1} completed({2:.1%})', self.progress, self.tot, float(self.progress) / self.tot)


def spider2editor(src=getattr(glob, 'SPIDER_SPEC'), dst=getattr(glob, 'DB_SPEC'), table='products'):
    """
    从spider库到editor库的更新机制
    """
    scr_db = RoseVisionDb()
    scr_db.conn(src)
    dst_db = RoseVisionDb()
    dst_db.conn(dst)

    # 根据update_time字段判断哪些记录是需要更新的
    editor_ts = dst_db.query('SELECT MAX(update_time) FROM products').fetch_row()[0][0]
    if not editor_ts:
        editor_ts = '1900-01-01 00:00:00'
    obj = Spider2EditorHlper(scr_db.query(str.format('SELECT * FROM {0} WHERE update_time>"{1}"',
                                                     table, editor_ts)), dst_db)

    dst_db.lock([table])
    dst_db.start_transaction()
    try:
        cnt = int(
            dst_db.query(str.format('SELECT COUNT(idproducts) FROM {0} WHERE update_flag!="N"', table)).fetch_row()[
                0][0])
        if cnt == 0:
            core.func_carrier(obj, 1)
        else:
            print str.format('{0} uncommited records exist in editor_stores, unable to sync.', cnt)
    except OperationalError:
        dst_db.rollback()
    finally:
        dst_db.commit()
        dst_db.unlock()

    dst_db.close()
    scr_db.close()


def process_editor_price(db_spec=glob.DB_SPEC, table='products', extra_cond=None):
    """
    处理editor库中的价格信息
    :param table: 需要操作的表。默认为products。
    :param db_spec: 需要操作的数据库，默认为editor库。
    :param extra_cond: 筛选条件。
    """
    db = RoseVisionDb()
    db.conn(db_spec)
    extra_cond = ' AND '.join(
        unicode.format(u'({0})', tuple(unicodify(v))) for v in extra_cond) if extra_cond else '1'

    db.lock([table])
    db.start_transaction()
    try:
        # 根据update_flag字段判断哪些记录是需要更新的
        obj = EditorPriceProcessor(db.query(
            unicode.format(u'SELECT idproducts,price,region FROM {0} WHERE price IS NOT NULL AND {1}', table,
                           extra_cond).encode('utf-8')), db)
        core.func_carrier(obj, 1)
        db.commit()
    except OperationalError:
        db.rollback()
    finally:
        db.unlock()
        db.close()


def process_editor_tags(db_spec=getattr(glob, 'DB_SPEC'), db_spider_spec=getattr(glob, 'SPIDER_SPEC'),
                        table='products', extra_cond=None):
    """
    给editor库的数据添加tags字段
    """
    db = RoseVisionDb()
    db.conn(db_spider_spec)
    try:
        extra_cond = ' AND '.join(
            unicode.format(u'({0})', tuple(unicodify(v))) for v in extra_cond) if extra_cond else '1'

        rs = db.query(
            unicode.format(u'SELECT tag_name,mapping_list FROM products_tag_mapping WHERE {1}', extra_cond).encode(
                'utf-8'))
        temp = rs.fetch_row(maxrows=0)
        mapping_rules = dict(temp)
    finally:
        db.close()

    db.conn(db_spec)
    db.start_transaction()
    try:

        rs = db.query(unicode.format(u'SELECT * FROM {0} WHERE {1}', table, extra_cond))
        for i in xrange(rs.num_rows()):
            record = rs.fetch_row(how=1)[0]
            extra = json.loads(record['extra'])
            tags = []
            for k in extra:
                tags.extend(extra[k])
            tags = set(tags)
            tag_names = []
            for v in tags:
                if v in mapping_rules:
                    tag_names.extend(json.loads(mapping_rules[v]))
            tag_names = list(set(tag_names))

            db.update({'tags': json.dumps(tag_names, ensure_ascii=False)},
                      str.format('idproducts={0}', record['idproducts']))

        db.commit()
        pass
    except OperationalError:
        db.rollback()
    finally:
        db.close()
