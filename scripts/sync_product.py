# coding=utf-8
import json
from _mysql_exceptions import OperationalError
import time
import core
import common as cm

__author__ = 'Zephyre'

import global_settings as glob
from core import MySqlDb


class SyncProducts(object):
    def __init__(self, data, extra=None):
        self.data = data
        self.progress = 0
        self.tot = 0
        self.extra = list(extra) if extra else []

    def run(self):
        db_s = MySqlDb()
        db_s.conn(glob.SPIDER_SPEC)
        db_e = MySqlDb()
        db_e.conn(glob.EDITOR_SPEC)
        rs = db_s.query('SELECT * FROM products', use_result=True)
        self.tot = rs.num_rows()
        db_e.start_transaction()
        for i in xrange(self.tot):
            record = rs.fetch_row(how=1).copy()
            self.tot += 1

            brand_id = int(record['brand_id'])
            model = record['model']
            region = record['region']

            self.process_price(record)
            self.process_text(record)

            db_e.start_transaction()
            try:
                results = db_e.query(
                    str.format('SELECT * FROM products WHERE brand_id={0} AND model="{1}" AND region="{2}"',
                               brand_id, model, region)).fetch_row(maxrows=0, how=1)
                if len(results) > 1:
                    str.format('DUPLICATE RECORDS: {0}', ', '.join(val['idproducts'] for val in results))
                    continue

                if results:
                    r = {k: results[0][k] for k in results[0] if k != 'idproducts'}
                    pid = results[0]['idproducts']
                    # 合并
                    price_history = sorted(r['price_history'] if r['price_history'] else [],
                                           key=lambda val: time.strptime(val['time'], '%Y-%m-%d %H:%M:%S'))
                    candidate = {'time': record['update_time'], 'price': record['price_rev'],
                                 'currency': record['currency_rev']}
                    if price_history:
                        if price_history[-1]['price'] != record['price_rev']:
                            price_history.append(candidate)
                    else:
                        price_history = [candidate]
                    record['price_history'] = price_history

                    db_e.update(record, 'products', str.format('idproducts={0}', pid))

                else:
                    # 新增
                    record = {k: record[k] for k in record if k != 'idproducts'}
                    record['price_history'] = [{'time': record['update_time'], 'price': record['price_rev'],
                                           'currency': record['currency_rev']}]
                    db_e.insert(record, 'products')

                db_e.commit()
            except:
                db_e.rollback()
                raise

        return

    def process_price(self, record):
        ret = cm.process_price(record['price'])
        record['price_rev'] = ret['price']
        record['currency_rev'] = ret['currency']

    def process_text(self, record):
        if 'details' in record and record['details']:
            record['details'] = cm.reformat_addr(record['details'])
        if 'description' in record and record['description']:
            record['description'] = cm.reformat_addr(record['description'])


    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot, float(self.progress) / self.tot)


class Spider2EditorHlper(object):
    def __init__(self, src_rs, db):
        self.rs = src_rs
        self.db = db
        self.progress = 0
        self.tot = self.rs.num_rows()

    def run(self):
        for i in xrange(self.tot):
            temp = self.rs.fetch_row(how=1)[0]
            record = dict((k, cm.unicodify(temp[k])) for k in temp)

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
            record = dict((k, cm.unicodify(temp[k])) for k in temp)

            ret = cm.process_price(record['price'], record['region'])
            self.db.update({'price_rev': ret['price'], 'currency_rev': ret['currency']}, 'products',
                           str.format('idproducts={0}', record['idproducts']))
            self.progress = i + 1

    def get_msg(self):
        return str.format('{0} out of {1} completed({2:.1%})', self.progress, self.tot, float(self.progress) / self.tot)


def spider2editor(src=glob.SPIDER_SPEC, dst=glob.EDITOR_SPEC, table='products'):
    """
    从spider库到editor库的更新机制
    """
    scr_db = MySqlDb()
    scr_db.conn(src)
    dst_db = MySqlDb()
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


def process_editor_price(db_spec=glob.EDITOR_SPEC, table='products', extra_cond=None):
    """
    处理editor库中的价格信息
    :param table: 需要操作的表。默认为products。
    :param db_spec: 需要操作的数据库，默认为editor库。
    :param extra_cond: 筛选条件。
    """
    db = MySqlDb()
    db.conn(db_spec)
    extra_cond = ' AND '.join(
        unicode.format(u'({0})', tuple(cm.unicodify(v))) for v in extra_cond) if extra_cond else '1'

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


def process_editor_tags(db_spec=glob.EDITOR_SPEC, db_spider_spec=glob.SPIDER_SPEC, table='products', extra_cond=None):
    """
    给editor库的数据添加tags字段
    """
    db = MySqlDb()
    db.conn(db_spider_spec)
    try:
        extra_cond = ' AND '.join(
            unicode.format(u'({0})', tuple(cm.unicodify(v))) for v in extra_cond) if extra_cond else '1'

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