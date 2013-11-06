# coding=utf-8
import time
import core
import common as cm

__author__ = 'Zephyre'

import global_settings as glob
from core import MySqlDb


class Spider2EditorHlper(object):
    def __init__(self, src_rs, db):
        self.rs = src_rs
        self.db = db
        self.progress = 0
        self.tot = self.rs.num_rows()

    def run(self):
        self.db.start_transaction()
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

        self.db.commit()

    def get_msg(self):
        return str.format('{0} out of {1} completed({2:.1%})', self.progress, self.tot, float(self.progress) / self.tot)


class EditorPriceProcessor(object):
    def __init__(self, src_rs, db):
        self.rs = src_rs
        self.db = db
        self.progress = 0
        self.tot = self.rs.num_rows()

    def run(self):
        self.db.start_transaction()
        for i in xrange(self.tot):
            temp = self.rs.fetch_row(how=1)[0]
            record = dict((k, cm.unicodify(temp[k])) for k in temp)

            ret = cm.process_price(record['price'], record['region'])
            self.db.update({'price_rev': ret['price'], 'currency_rev': ret['currency']}, 'products',
                           str.format('idproducts={0}', record['idproducts']))
            self.progress = i + 1
        self.db.commit()

    def get_msg(self):
        return str.format('{0} out of {1} completed({2:.1%})', self.progress, self.tot, float(self.progress) / self.tot)


def spider2editor():
    """
    从spider库到editor库的更新机制
    """
    db_spider_spec = glob.SPIDER_SPEC
    db_editor_spec = glob.EDITOR_SPEC

    db_spider = MySqlDb()
    db_spider.conn(db_spider_spec)
    db_editor = MySqlDb()
    db_editor.conn(db_editor_spec)

    # 根据update_time字段判断哪些记录是需要更新的
    rs = db_editor.query('SELECT MAX(update_time) FROM products')
    editor_ts = rs.fetch_row()[0][0]

    rs = db_spider.query(str.format('SELECT * FROM products WHERE update_time>"{0}"', editor_ts))
    obj = Spider2EditorHlper(rs, db_editor)

    db_editor.execute('LOCK TABLES products WRITE')
    try:
        rs = db_editor.query('SELECT COUNT(idproducts) FROM products WHERE update_flag!="N"')
        cnt = int(rs.fetch_row()[0][0])
        if cnt == 0:
            core.func_carrier(obj, 1)
        else:
            print str.format('{0} uncommited records exist in editor_stores, unable to sync.', cnt)
    finally:
        db_editor.execute('UNLOCK TABLES')

    db_editor.close()
    db_spider.close()


def process_editor_price():
    """
    处理editor库中的价格信息
    """
    db_editor_spec = glob.EDITOR_SPEC

    db_editor = MySqlDb()
    db_editor.conn(db_editor_spec)

    # 根据update_flag字段判断哪些记录是需要更新的
    rs = db_editor.query('SELECT idproducts,price,region FROM products WHERE update_flag!="N" && price IS NOT NULL')
    obj = EditorPriceProcessor(rs, db_editor)

    db_editor.execute('LOCK TABLES products WRITE')
    try:
        core.func_carrier(obj, 1)
    finally:
        db_editor.execute('UNLOCK TABLES')

    db_editor.close()


# def process_editor_tags():
#     """
#     给editor库的数据添加tags字段
#     """
#     db = _mysql.connect(host=db_spider['host'], port=db_spider['port'], user=db_spider['username'],
#                         passwd=db_spider['password'], db=db_spider['schema'])
#     db.query("SET NAMES 'utf8'")
#     db.query(str.format('SELECT tag_name,mapping_list FROM products_tag_mapping WHERE brand_id={0} && region="{1}"',
#                         brand_id, region))
#     temp = db.store_result().fetch_row(maxrows=0)
#     mapping_rules = dict(temp)
#     db.close()
#
#     db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
#                         passwd=db_spec['password'], db=db_spec['schema'])
#     db.query("SET NAMES 'utf8'")
#
#     db.query(str.format('SELECT * FROM products WHERE brand_id={0} && region="{1}"', brand_id, region))
#     rs = db.store_result()
#
#     db.query('START TRANSACTION')
#     for i in xrange(rs.num_rows()):
#         record = rs.fetch_row(how=1)[0]
#         extra = json.loads(record['extra'])
#         tags = []
#         for k in extra:
#             tags.extend(extra[k])
#         tags = set(tags)
#         tag_names = []
#         for v in tags:
#             if v in mapping_rules:
#                 tag_names.extend(json.loads(mapping_rules[v]))
#         tag_names = list(set(tag_names))
#
#         db.query(unicode.format(u'UPDATE products SET tags="{0}" WHERE idproducts={1}',
#                                 to_sql(json.dumps(tag_names, ensure_ascii=False)),
#                                 record['idproducts']).encode('utf-8'))
#
#     db.query('COMMIT')
#     db.close()
#     pass