# coding=utf-8

# Editor库的处理。包括：
# 读取标签映射文件，得到映射规则，并写入数据库；
# 标签的翻译和规范化
# 价格的提取


import _mysql
import json
import common as cm
import re
import Image
import time

__author__ = 'Zephyre'

import csv

def to_sql(val):
    return val.replace('\\', '\\\\').replace('"', '\\"') if val else ''


def price_processor(price_body, region):
    val = cm.unicodify(price_body)
    currency = {'cn': 'CNY', 'us': 'USD', 'de': 'EUR', 'es': 'EUR', 'fr': 'EUR', 'it': 'EUR', 'jp': 'JPY', 'kr': 'KRW'}[
        region]

    if region == 'de':
        val = re.sub(ur'\s', u'', val, flags=re.U).replace('.', '').replace(',', '.')
    elif region == 'fr':
        val = re.sub(ur'\s', u'', val, flags=re.U).replace(',', '.')
    else:
        val = re.sub(ur'\s', u'', val, flags=re.U).replace(',', '')

    m = re.search(ur'[\d\.]+', val)
    if not m:
        price = ''
    else:
        price = float(m.group())

    return {'currency': currency, 'price': price}


def import_tag_mapping(map_file, region, brand_id, brandname_e):
    data = []
    with open(map_file, 'r') as f:
        rdr = csv.reader(f)
        for row in rdr:
            data.append([val.decode('utf-8').strip() for val in row])

    db_spec = {'host': '127.0.0.1', 'username': 'root', 'password': '123456', 'port': 3306, 'schema': 'spider_stores'}
    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")
    # db.query('START TRANSACTION')

    for rule in data:
        tag_name = rule[0]
        tag_text = rule[1]
        mapping_list = list(set(filter(lambda x: x, rule[1:])))
        m_val = json.dumps(mapping_list, ensure_ascii=False)
        db.query(
            unicode.format(u'SELECT * FROM products_tag_mapping WHERE brand_id={0} && region="{1}" && tag_name="{2}"',
                           brand_id, region, tag_name).encode('utf-8'))
        rs = db.store_result()

        if rs.num_rows() > 0:
            pid = rs.fetch_row()[0][0]
            db.query(
                unicode.format(u'UPDATE products_tag_mapping SET mapping_list="{0}" WHERE idmappings={1}',
                               to_sql(m_val),
                               pid).encode('utf-8'))
        else:
            db.query(unicode.format(u'INSERT INTO products_tag_mapping (brand_id, brandname_e, region, tag_name, '
                                    u'tag_text, mapping_list) VALUES ({0}, "{1}", "{2}", "{3}", "{4}", "{5}")',
                                    brand_id, to_sql(brandname_e), to_sql(region), to_sql(tag_name), to_sql(tag_text),
                                    to_sql(m_val)).encode('utf-8'))

    db.close()


def process_editor(brand_id, region):
    db_spec = {'host': '127.0.0.1', 'username': 'root', 'password': '123456', 'port': 3306, 'schema': 'spider_stores'}
    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")

    db.query(str.format('SELECT tag_name, mapping_list FROM products_tag_mapping WHERE brand_id={0} && region="{1}"',
                        brand_id, region))
    results = db.store_result().fetch_row(maxrows=0)
    tag_mapping = dict((item[0], json.loads(item[1])) for item in results)
    db.close()

    db_spec['schema'] = 'editor_stores'
    db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                        passwd=db_spec['password'], db=db_spec['schema'])
    db.query("SET NAMES 'utf8'")
    db.query('START TRANSACTION')

    db.query('SELECT * FROM products')
    rs = db.store_result()
    for i in xrange(rs.num_rows()):
        record = rs.fetch_row(how=1)[0]
        pid = record['idproducts']
        tag_list = set([item for sublist in
                        [record[k].split('|') for k in ('category', 'color', 'texture') if record[k]]
                        for item in sublist])

        # extra字段中所有的tag
        extra = json.loads(record['extra'])
        extra_values = set([item for sublist in
                            [val.split('|') for val in extra.values() if val]
                            for item in sublist])
        tag_list = list(set(tag_list.union(extra_values)))
        # 转换后的标签
        new_tag_list = set([item for sublist in [tag_mapping.get(k, k) for k in tag_list] for item in sublist])
        db.query(unicode.format(u'UPDATE products SET tags="{0}" WHERE idproducts={1}',
                                to_sql(json.dumps(list(new_tag_list), ensure_ascii=False)), pid).encode('utf-8'))

        ret = price_processor(record['price'], record['region'])

        # 转换后的category
        new_data = dict((val, list(set(tag_mapping.get(k, k))))
                        for val in ('category', 'color', 'texture') if record[val] for k in record[val].split('|'))
        clause = u', '.join(
            unicode.format(u'{0}_rev="{1}"', k, to_sql(json.dumps(v, ensure_ascii=False))) for k, v in new_data.items())

        price_clause = unicode.format(u'price_rev={0}, currency_rev="{1}"', ret['price'], ret['currency'])
        clause = u', '.join([clause, price_clause])
        db.query(unicode.format(u'UPDATE products SET {0} WHERE idproducts={1}', clause, pid).encode('utf-8'))

    db.query('COMMIT')
    db.close()


    # 转换后的

    # import_tag_mapping('../lv_tag_mapping.txt', 'cn', 10226, 'Louis Vuitton')
    # process_editor(10226, 'cn')