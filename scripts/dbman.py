# coding=utf-8
import sys
import datetime
from core import MySqlDb
import global_settings as gs
import common as cm
import json

__author__ = 'Zephyre'


class ProcessTags(object):
    """
    标签的映射规则有变动，需要更新
    """
    original_tags_tbl = 'original_tags'
    mfashion_tags_tbl = 'mfashion_tags'
    prod_tag_tbl = 'products_original_tags'
    prod_mt_tbl = 'products_mfashion_tags'
    products = 'products'

    db_spec = gs.EDITOR_SPEC
    tot = 1
    progress = 0

    def __init__(self, last_update=None, extra_cond=None):
        self.db = MySqlDb()
        self.db.conn(self.db_spec)
        self.last_update = last_update
        self.extra_cond = extra_cond

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def run(self):
        last_update = self.last_update
        extra_cond = self.extra_cond

        if not extra_cond:
            extra_cond = []
        elif not cm.iterable(extra_cond):
            extra_cond = [extra_cond]
        if last_update:
            extra_cond.append(unicode.format(u'update_time > "{0}"', last_update))
        extra_cond.append('mapping_list IS NOT NULL')

        # MFashion标签的缓存记录
        cached_mfashion = {}

        # 标签更新原理：original_tags存放原始标签。根据update_time字段可以得到最近更新过的标签。由于标签系统具备一定传染性，所以
        # 该标签对应brand/region下的所有标签都必须重做
        rs = self.db.query_match(['brand_id', 'region'], self.original_tags_tbl, {}, extra=extra_cond, distinct=True)
        # 需要处理的标签
        tag_dict = {}
        for i in xrange(rs.num_rows()):
            brand_id, region = rs.fetch_row()[0]
            for val in self.db.query_match(['idmappings', 'mapping_list'], self.original_tags_tbl,
                                           {'brand_id': brand_id, 'region': region},
                                           extra='mapping_list IS NOT NULL').fetch_row(maxrows=0):
                tag_dict[val[0]] = json.loads(val[1].replace("'", '"'))

            # 删除旧单品/标签关系
            self.db.execute(str.format('DELETE FROM p2 USING {0} AS p1, {1} AS p2 WHERE p1.idproducts=p2.idproducts '
                                       'AND p1.brand_id={2} AND region="{3}"', self.products, self.prod_mt_tbl,
                                       brand_id,
                                       region))

        self.tot = len(tag_dict)
        self.progress = 0
        for tid, rule in tag_dict.items():
            self.progress += 1
            self.db.start_transaction()
            try:
                # 所有相关的单品
                pid_list = [int(val[0]) for val in self.db.query_match(['idproducts'], self.prod_tag_tbl, {
                    'id_original_tags': tid}).fetch_row(maxrows=0)]

                # 添加MFashion标签
                for tag in rule:
                    if tag not in cached_mfashion:
                        self.db.insert({'tag': tag}, self.mfashion_tags_tbl, ignore=True)
                        tid = int(
                            self.db.query_match(['idmfashion_tags'], self.mfashion_tags_tbl, {'tag': tag}).fetch_row()[
                                0][0])
                        cached_mfashion[tag] = tid

                    self.db.insert([{'idproducts': pid, 'id_mfashion_tags': cached_mfashion[tag]} for pid in pid_list],
                                   self.prod_mt_tbl, ignore=True)

                self.db.commit()
            except ValueError:
                self.db.rollback()
            except:
                self.db.rollback()
                raise


class PublishRelease(object):
    def __init__(self, brand_id, extra_cond=None):
        self.brand_id = brand_id
        if not extra_cond:
            extra_cond = ['1']
        elif not cm.iterable(extra_cond):
            extra_cond = [extra_cond]
        self.extra_cond = extra_cond
        self.tot = 0
        self.progress = 0

        region_list = ['cn', 'us', 'uk', 'fr', 'hk']
        self.region_order = {region_list[idx]: idx * 10 for idx in xrange(len(region_list))}

        self.products_tbl = 'products'
        self.prod_mt_tbl = 'products_mfashion_tags'
        self.mt_tbl = 'mfashion_tags'
        self.prod_ot_tbl = 'products_original_tags'
        self.ot_tbl = 'original_tags'
        self.price_hist = 'products_price_history'

    def merge_prods(self, prods):
        """
        按照国家顺序，挑选主记录
        :param prods:
        """
        sorted_prods = sorted(prods, key=lambda k: self.region_order[k['region']] \
            if k['region'] in self.region_order else sys.maxint)
        main_entry = sorted_prods[0]
        entry = {k: cm.unicodify(main_entry[k]) for k in (
            'brand_id', 'region', 'model', 'name', 'description', 'details', 'gender', 'category', 'color', 'url')}
        entry['idproducts_list'] = json.dumps([int(val['idproducts']) for val in prods], ensure_ascii=False)

        mfashion_tags = [cm.unicodify(val[0]) for val in
                         self.db.query(str.format('SELECT DISTINCT p1.tag FROM mfashion_tags AS p1 '
                                                  'JOIN products_mfashion_tags AS p2 ON p1.idmfashion_tags=p2.id_mfashion_tags '
                                                  'WHERE p2.idproducts IN ({0})',
                                                  ','.join(val['idproducts'] for val in prods))).fetch_row(
                             maxrows=0)]

        original_tags = [int(val[0]) for val in
                         self.db.query(str.format('SELECT DISTINCT id_original_tags FROM products_original_tags '
                                                  'WHERE idproducts IN ({0})',
                                                  ','.join(val['idproducts'] for val in prods))).fetch_row(
                             maxrows=0)]

        entry['mfashion_tags'] = json.dumps(mfashion_tags, ensure_ascii=False)
        entry['original_tags'] = json.dumps(original_tags, ensure_ascii=False)

        entry['region_list'] = json.dumps([val['region'] for val in prods], ensure_ascii=False)

        # pid和region之间的关系
        pid_region_dict = {int(val['idproducts']): val['region'] for val in prods}
        price_list = {}
        for item in self.db.query_match(['price', 'currency', 'date', 'idproducts'], self.price_hist, {},
                                        str.format('idproducts IN ({0})',
                                                   ','.join(val['idproducts'] for val in prods))).fetch_row(maxrows=0,
                                                                                                            how=1):
            pid = int(item.pop('idproducts'))
            updated = False
            if pid not in price_list:
                updated = True
            else:
                old_ts = price_list[pid]['date']
                new_ts = datetime.datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S")
                if new_ts > old_ts:
                    updated = True

            if updated:
                region = pid_region_dict[pid]
                price_list[pid] = {'price': float(item['price']), 'currency': item['currency'],
                                   'date': datetime.datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S"),
                                   'code': region,
                                   'country': gs.REGION_INFO[region]['name_c']}

        for val in price_list.values():
            val.pop('date')
        entry['price_list'] = json.dumps(sorted(price_list.values(),
                                                key=lambda val: self.region_order[val['code']] if \
                                                    val['code'] in self.region_order else sys.maxint),
                                         ensure_ascii=False)
        tmp = {k['code']: k for k in price_list.values()}
        if 'cn' in tmp:
            entry['price_cn'] = tmp['cn']['price']

        image_list = []
        checksums = set([])
        cover_checksum = None
        p = prods[0]
        rs = self.db.query_match(['checksum'], 'products_image',
                                 {'brand_id': p['brand_id'], 'model': p['model']}).fetch_row(maxrows=0)
        for val in rs:
            checksums.add(val[0])
            if not cover_checksum:
                cover_checksum = val[0]

        if checksums:
            rs = self.db.query_match(['checksum', 'path', 'width', 'height'], 'images_store', {},
                                     str.format('checksum IN ({0})',
                                                ','.join(str.format('"{0}"', val) for val in checksums))).fetch_row(
                maxrows=0, how=1)
            for val in rs:
                tmp = {'path': val['path'], 'width': int(val['width']), 'height': int(val['height'])}
                image_list.append(tmp)
                if val['checksum'] == cover_checksum:
                    entry['cover_image'] = tmp

            entry['image_list'] = json.dumps(image_list, ensure_ascii=False)

        self.db.insert(entry, 'products_release')

    def run(self):
        self.db = MySqlDb()
        self.db.conn(gs.EDITOR_SPEC)

        self.db.execute(str.format('DELETE FROM products_release WHERE brand_id={0}', self.brand_id))

        # 每一个model，对应哪些pid需要合并？
        model_list = {}
        rs = self.db.query_match(['COUNT(DISTINCT model)'], self.products_tbl, {'brand_id': self.brand_id})
        self.tot = int(rs.fetch_row()[0][0])
        rs = self.db.query_match(['*'], self.products_tbl, {'brand_id': self.brand_id}, tail_str='ORDER BY model')
        for self.progress in xrange(self.tot):
            record = rs.fetch_row(how=1)[0]
            if record['model'] not in model_list:
                if model_list.keys():
                    # 归并上一个model
                    self.merge_prods(model_list.pop(model_list.keys()[0]))

                model_list[record['model']] = [record]
            else:
                model_list[record['model']].append(record)

        self.db.close()

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'
