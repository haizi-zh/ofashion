# coding=utf-8
from cStringIO import StringIO
import csv
import sys
import datetime
from core import MySqlDb
import global_settings as gs
import common as cm
import json
from utils.utils import unicodify, iterable

__author__ = 'Zephyre'


class SampleExtractor(object):
    """
    将记录导出，方便进行人工检查
    """

    def __init__(self, param=None):
        self.tot = 1
        self.progress = 0
        if 'brand' in param:
            self.brand_list = [int(val) for val in param['brand']]
        else:
            self.brand_list = None

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def run(self):
        db = MySqlDb()
        db.conn(gs.DB_SPEC)

        # 如果没有指定brand_list，则默认使用数据库中所有的brand_list
        if not self.brand_list:
            rs = db.query_match(['brand_id'], 'products', distinct=True)
            brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]
            self.brand_list = brand_list
        else:
            brand_list = self.brand_list

        self.progress = 0
        self.tot = len(brand_list)

        # 最终生成的表格
        results = {}

        for brand in brand_list:
            print unicode.format(u'PROCESSING {0} / {1}', brand, gs.brand_info()[brand]['brandname_e'])
            self.progress += 1

            rs = db.query(str.format('''SELECT p1.idproducts,p1.brand_id,p1.model,p1.region,p2.price,p2.price_discount,p2.currency,p2.date,p1.name,p4.tag,p1.url FROM products AS p1
                            JOIN products_price_history AS p2 ON p1.idproducts=p2.idproducts
                            LEFT JOIN products_mfashion_tags AS p3 ON p3.idproducts=p1.idproducts
                            LEFT JOIN mfashion_tags AS p4 ON p3.id_mfashion_tags=p4.idmfashion_tags
                            WHERE p1.brand_id={0}''', brand))
            records = rs.fetch_row(maxrows=0, how=1)
            for r in records:
                pid = int(r['idproducts'])
                timestamp = datetime.datetime.strptime(r['date'], '%Y-%m-%d %H:%M:%S')
                tag = unicodify(r['tag'])

                if pid in results:
                    # 如果已经存在相应单品的记录
                    old_rec = results[pid]
                    old_rec['tag'].add(tag)
                    old_t = datetime.datetime.strptime(old_rec['date'], '%Y-%m-%d %H:%M:%S')
                    if timestamp > old_t:
                        old_rec['price'] = unicodify(r['price'])
                        old_rec['price_discount'] = unicodify(r['price_discount'])
                        old_rec['currency'] = unicodify(r['currency'])
                        old_rec['date'] = unicodify(r['date'])
                else:
                    # 如果该单品的记录不存在
                    results[pid] = {k: unicodify(r[k]) for k in r}
                    tmp = results[pid]['tag']
                    if tmp:
                        results[pid]['tag'] = {tmp}
                    else:
                        results[pid]['tag'] = set({})

        db.close()

        # 将所有的tag转换为[]
        data = []
        for r in results.values():
            r['tag'] = json.dumps(list(r['tag']), ensure_ascii=False)
            data.append({k: r[k].encode('utf-8') if r[k] else 'NULL' for k in r})

        # 写入CSV文件
        with open(str.format('extract_{0}.csv', datetime.datetime.now().strftime('%Y%m%d%H%M%S')), 'wb') as f:
            f.write(u'\ufeff'.encode('utf8'))
            dict_writer = csv.DictWriter(f, fieldnames=['idproducts', 'brand_id', 'model', 'region', 'price',
                                                        'price_discount', 'currency', 'date',
                                                        'name', 'tag', 'url'])
            dict_writer.writeheader()
            dict_writer.writerows(data)