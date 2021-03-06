# coding=utf-8
from cStringIO import StringIO
import copy
import csv
import sys
import datetime
from utils.db import RoseVisionDb
import global_settings as gs
import common as cm
import json
from scripts.push_utils import price_changed
from utils import info
from utils.filters import release_filter
from utils.text import unicodify, iterable
from utils.utils_core import gen_fingerprint, get_logger

__author__ = 'Zephyre'


class FingerprintCheck(object):
    """
    检查单品的加盐MD5指纹是否正确
    """

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def __init__(self, param=None):
        self.tot = 1
        self.progress = 0
        # 是否处于静默模式
        self.silent = ('s' in param)
        # 是否更新错配的fingerprint
        self.update_fingerprint = ('update' in param)
        # 如果没有指定brand，则对数据库中存在的所有brand进行处理
        self.brand_list = [int(val) for val in param['brand']] if 'brand' in param else None
        # 检查报告
        self.report = []

    def run(self):
        db = RoseVisionDb()
        db.conn(getattr(gs, 'DATABASE')['DB_SPEC'])

        if not self.brand_list:
            rs = db.query_match(['brand_id'], 'products', distinct=True)
            brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]
            self.brand_list = brand_list
        else:
            brand_list = self.brand_list
        if not brand_list:
            # 如果没有任何品牌，则直接退出
            return self.report

        self.progress = 0
        # 获得检查总数
        self.tot = int(db.query(str.format('SELECT COUNT(*) FROM products WHERE brand_id IN ({0})',
                                           ','.join(str(tmp) for tmp in brand_list))).fetch_row()[0][0])
        for brand in brand_list:
            if not self.silent:
                print unicode.format(u'\nPROCESSING {0} / {1}\n', brand, info.brand_info()[brand]['brandname_e'])

            db.start_transaction()
            try:
                for model, pid, fingerprint in db.query_match(['model', 'idproducts', 'fingerprint'], 'products',
                                                              {'brand_id': brand}).fetch_row(maxrows=0):
                    self.progress += 1
                    new_fp = gen_fingerprint(brand, model)
                    if fingerprint != new_fp:
                        self.report.append({'model': model, 'idproducts': pid, 'fingerprint_db': fingerprint,
                                            'fingerprint_gen': new_fp, 'brand_id': brand})
                        if not self.silent:
                            print unicode.format(u'\nMismatched fingerprints! model={0}, idproducts={1}, brand_id={2}, '
                                                 u'fingerprints: {3} => {4}\n',
                                                 model, pid, brand, fingerprint, new_fp)
                        if self.update_fingerprint:
                            # 自动更新MD5指纹
                            db.update({'fingerprint': new_fp}, 'products', str.format('idproducts={0}', pid),
                                      timestamps=['update_time'])
            except:
                db.rollback()
                raise
            finally:
                db.commit()
        db.close()


class PriceCheck(object):
    """
    检查单品价格是否存在可能的错误
    """

    def __init__(self, param=None):
        self.tot = 1
        self.progress = 0
        self.threshold = 10
        if 'brand' in param:
            self.brand_list = [int(val) for val in param['brand']]
        else:
            self.brand_list = None
        if 'threshold' in param and param['threshold']:
            self.threshold = int(param['threshold'][0])

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def run(self):
        db = RoseVisionDb()
        db.conn(getattr(gs, 'DATABASE')['DB_SPEC'])

        if not self.brand_list:
            rs = db.query_match(['brand_id'], 'products', distinct=True)
            brand_list = [int(val[0]) for val in rs.fetch_row(maxrows=0)]
            self.brand_list = brand_list
        else:
            brand_list = self.brand_list

        self.progress = 0
        self.tot = len(brand_list)
        for brand in brand_list:
            print unicode.format(u'PROCESSING {0} / {1}', brand, info.brand_info()[brand]['brandname_e'])
            self.progress += 1
            rs = db.query(str.format(
                'SELECT * FROM (SELECT p2.idprice_history,p2.date,p2.price,p2.currency,p1.idproducts,p1.brand_id,'
                'p1.region,p1.name,p1.model,p1.offline FROM products AS p1 JOIN products_price_history AS p2 ON '
                'p1.idproducts=p2.idproducts '
                'WHERE p1.brand_id={0} ORDER BY p2.date DESC) AS p3 GROUP BY p3.idproducts',
                brand))

            # 以model为键值，将同一个model下，不同区域的价格放在一起。
            records = rs.fetch_row(maxrows=0, how=1)
            price_data = {}
            for r in records:
                model = r['model']
                # # 仅有那些price不为None，且offline为0的数据，才加入到price check中。
                # if r['price'] and int(r['offline']) == 0:
                # 这里更改为不管offline，全检查
                if r['price']:
                    # 首先检查model是否已存在
                    if model not in price_data:
                        price_data[model] = []
                    price_data[model].append(r)

            # 最大值和最小值之间，如果差别过大，则说明价格可能有问题
            for model in price_data:
                for item in price_data[model]:
                    price = float(item['price'])
                    item['nprice'] = info.currency_info()[item['currency']]['rate'] * price

                # 按照nprice大小排序
                sorted_data = sorted(price_data[model], key=lambda item: item['nprice'])
                max_price = sorted_data[-1]['nprice']
                min_price = sorted_data[0]['nprice']
                if min_price > 0 and max_price / min_price > self.threshold:
                    print unicode.format(u'WARNING: {0}:{6} MODEL={1}, {2} / {3} => {4} / {5}',
                                         brand, model,
                                         sorted_data[0]['nprice'], sorted_data[0]['region'],
                                         sorted_data[-1]['nprice'], sorted_data[-1]['region'],
                                         info.brand_info()[brand]['brandname_e'])

        db.close()


class PriceChangeDetect(object):
    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'

    def __init__(self, param=None):
        self.tot = 1
        self.progress = 0
        # 价格变化报告
        self.change_detection = None
        # 是否处于静默模式
        self.silent = ('s' in param)
        # 如果没有指定brand，则对数据库中存在的所有brand进行处理
        self.brand_list = [int(val) for val in param['brand']] if 'brand' in param else None
        self.start_ts = param['start'][0] if 'start' in param else None
        self.end_ts = param['end'][0] if 'end' in param else None

    def run(self):
        change_detection = price_changed(self.brand_list, self.start_ts, self.end_ts)
        changes = {'U': [], 'D': []}
        for change_type in ['discount_down', 'price_down', 'discount_up', 'price_up']:
            for brand in change_detection[change_type]:
                for fingerprint, model_data in change_detection[change_type][brand].items():
                    for product in model_data['products']:
                        pid = product['idproducts']
                        c = '0'
                        if change_type in ['discount_down', 'price_down']:
                            c = 'D'
                        elif change_type in ['discount_up', 'price_up']:
                            c = 'U'
                        if c != '0':
                            changes[c].append(pid)

        with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
            db.start_transaction()
            try:
                for change_type in ['U', 'D']:
                    db.update({'price_change': change_type}, 'products',
                              str.format('idproducts IN ({0})', ','.join(str(tmp) for tmp in changes[change_type])),
                              timestamps=['update_time'])
            except:
                db.rollback()
                raise
            finally:
                db.commit()

        self.change_detection = change_detection
        return change_detection


class ProcessTags(object):
    """
    标签的映射规则有变动，需要更新
    """
    original_tags_tbl = 'original_tags'
    mfashion_tags_tbl = 'mfashion_tags'
    prod_tag_tbl = 'products_original_tags'
    prod_mt_tbl = 'products_mfashion_tags'
    products = 'products'

    db_spec = getattr(gs, 'DATABASE')['DB_SPEC']
    tot = 1
    progress = 0

    def __init__(self, last_update=None, extra_cond=None):
        print str.format('Processing tags (last_update="{0}", extra_cond="{1}")...', last_update, extra_cond)
        self.db = RoseVisionDb()
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
        elif not iterable(extra_cond):
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
                                       brand_id, region))

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
    def __init__(self, brand_id, extra_cond=None, max_images=15):
        print str.format('Publishing (brand_id={0}, max_images={1}, extra_cond="{2}")...', brand_id, max_images,
                         extra_cond)
        # 某一单品最大发布的图片数量
        self.max_images = max_images
        self.brand_id = brand_id
        if not extra_cond:
            extra_cond = ['1']
        elif not iterable(extra_cond):
            extra_cond = [extra_cond]
        self.extra_cond = extra_cond
        self.tot = 0
        self.progress = 0
        # 国家的展示顺序
        self.region_order = {k: info.region_info()[k]['weight'] for k in info.region_info()}

        self.products_tbl = 'products'
        self.prod_mt_tbl = 'products_mfashion_tags'
        self.mt_tbl = 'mfashion_tags'
        self.prod_ot_tbl = 'products_original_tags'
        self.ot_tbl = 'original_tags'
        self.price_hist = 'products_price_history'

    def merge_prods(self, prods, db):
        """
        按照国家顺序，挑选主记录
        :param prods:
        """
        logger = get_logger()
        # 将prods转换为unicode
        for idx in xrange(len(prods)):
            prods[idx] = {k: unicodify(prods[idx][k]) for k in prods[idx]}

        # 挑选primary记录
        sorted_prods = sorted(prods, key=lambda k: self.region_order[k['region']])
        main_entry = sorted_prods[0]
        entry = {k: unicodify(main_entry[k]) for k in (
            'brand_id', 'model', 'name', 'description', 'details', 'gender', 'category', 'color', 'url', 'fingerprint')}
        if not entry['name']:
            entry['name'] = u'单品'

        mfashion_tags = [unicodify(val[0]) for val in
                         db.query(str.format('SELECT DISTINCT p1.tag FROM mfashion_tags AS p1 '
                                             'JOIN products_mfashion_tags AS p2 ON p1.idmfashion_tags=p2.id_mfashion_tags '
                                             'WHERE p2.idproducts IN ({0})',
                                             ','.join(val['idproducts'] for val in prods))).fetch_row(
                             maxrows=0)]
        #
        # original_tags = [int(val[0]) for val in
        #                  db.query(str.format('SELECT DISTINCT id_original_tags FROM products_original_tags '
        #                                      'WHERE idproducts IN ({0})',
        #                                      ','.join(val['idproducts'] for val in prods))).fetch_row(
        #                      maxrows=0)]

        entry['mfashion_tags'] = json.dumps(mfashion_tags, ensure_ascii=False)
        entry['original_tags'] = ''  #json.dumps(original_tags, ensure_ascii=False)

        entry['region_list'] = json.dumps([val['region'] for val in prods], ensure_ascii=False)
        entry['brandname_e'] = info.brand_info()[int(entry['brand_id'])]['brandname_e']
        entry['brandname_c'] = info.brand_info()[int(entry['brand_id'])]['brandname_c']
        # # 该单品在所有国家的记录中，第一次被抓取到的时间，作为release的fetch_time
        # entry['fetch_time'] = \
        #     sorted(datetime.datetime.strptime(tmp['fetch_time'], "%Y-%m-%d %H:%M:%S") for tmp in prods)[
        #         0].strftime("%Y-%m-%d %H:%M:%S")

        url_dict = {int(val['idproducts']): val['url'] for val in prods}
        offline_dict = {int(val['idproducts']): int(val['offline']) for val in prods}
        price_change_dict = {int(val['idproducts']): val['price_change'] for val in prods}
        update_time_dict = {int(val['idproducts']): datetime.datetime.strptime(val['update_time'], "%Y-%m-%d %H:%M:%S")
                            for val in prods}
        # pid和region之间的关系
        region_dict = {int(val['idproducts']): val['region'] for val in prods}
        price_list = {}
        # 以pid为主键，将全部的价格历史记录合并起来
        for item in db.query_match(['price', 'price_discount', 'currency', 'date', 'idproducts'],
                                   self.price_hist, {},
                                   str.format('idproducts IN ({0})',
                                              ','.join(val['idproducts'] for val in prods)),
                                   tail_str='ORDER BY idprice_history DESC').fetch_row(maxrows=0,
                                                                                       how=1):
            pid = int(item['idproducts'])
            region = region_dict[pid]
            offline = offline_dict[pid]
            if pid not in price_list:
                price_list[pid] = []
            price = float(item['price']) if item['price'] else None
            if offline == 0:
                price_discount = float(item['price_discount']) if item['price_discount'] else None
            else:
                price_discount = None
            price_list[pid].append({'price': price, 'price_discount': price_discount, 'currency': item['currency'],
                                    'date': datetime.datetime.strptime(item['date'], "%Y-%m-%d %H:%M:%S"),
                                    'price_change': price_change_dict[pid], 'url': url_dict[pid],
                                    'offline': offline, 'code': region,
                                    'country': info.region_info()[region]['name_c']})

        currency_conv = lambda val, currency: info.currency_info()[currency]['rate'] * val

        # 对price_list进行简并操作。
        # 策略：如果有正常的最新价格，则返回正常的价格数据。
        # 如果最新价格为None，则取回溯第一条不为None的数据，同时将price_discount置空。
        # 如果无法找到不为None的价格，则跳过该pid
        for pid, pid_data in price_list.items():
            # 按照时间顺序逆排序，同时只保留price不为None的数据
            # pid_data = sorted(pid_data, key=lambda val: val['date'], reverse=True)

            # 有价格的pid_data子集
            valid_pid_data = filter(lambda val: val['price'], pid_data)

            if pid_data[0]['price']:
                # 正常情况
                price_list[pid] = pid_data[0]
                # 如果当前没有折扣价，查看是否为一周内原价悄悄下降的情况
                currency = valid_pid_data[0]['currency']
                if price_change_dict[pid]=='D' and len(valid_pid_data) > 1 and currency == valid_pid_data[1]['currency']:
                    if not pid_data[0]['price_discount'] and currency_conv(valid_pid_data[1]['price'],
                                                                           currency) > currency_conv(
                            valid_pid_data[0]['price'], currency) and (
                                datetime.datetime.now() - valid_pid_data[0]['date']) < datetime.timedelta(7):
                        price_list[pid]['price_discount'] = price_list[pid]['price']
                        price_list[pid]['price'] = valid_pid_data[1]['price']
            else:
                # 寻找回溯第一条price不为None的数据。
                # tmp = filter(lambda val: val['price'], pid_data)
                if not valid_pid_data:
                    # 没有价格信息，取消该pid记录
                    price_list.pop(pid)
                else:
                    # 取最近一次价格，同时取消折扣价，保留最新记录的offline状态
                    tmp = valid_pid_data[0]
                    tmp['price_discount'] = None
                    price_list[pid] = tmp

            # 第一次有效价格对应的时间，为fetch_time
            # pid_data = filter(lambda val: val['price'], sorted(pid_data, key=lambda val: val['date']))
            # pid_data = filter(lambda val: val['price'], pid_data)
            if valid_pid_data and pid in price_list:
                price_list[pid]['fetch_time'] = valid_pid_data[-1]['date']
                price_list[pid]['idproducts'] = pid

        # 如果没有价格信息，则不发布
        if not price_list:
            return

        entry['price_list'] = sorted(price_list.values(), key=lambda val: self.region_order[val['code']])
        entry = release_filter(entry, logger)

        if not entry['price_list']:
            return

        entry['offline'] = entry['price_list'][0]['offline']

        # model的fetch_time的确定：所有对应pid中，fetch_time最早的那个。
        entry['fetch_time'] = min(tmp['fetch_time'] for tmp in entry['price_list']).strftime("%Y-%m-%d %H:%M:%S")

        # 价格排序的列表
        alt_prices = []
        for price_item in entry['price_list']:
            # 将datetime序列化，进而保存在release表中。
            price_item['date'] = price_item['date'].strftime("%Y-%m-%d %H:%M:%S")
            price_item['fetch_time'] = price_item['fetch_time'].strftime("%Y-%m-%d %H:%M:%S")
            if price_item['offline'] == 0:
                if price_item['price_discount']:
                    tmp = map(lambda key_name: currency_conv(price_item[key_name], price_item['currency']),
                              ('price', 'price_discount'))
                    tmp.extend(
                        [price_item[key] for key in
                         ('price_change', 'price', 'price_discount', 'currency', 'date', 'idproducts')])
                    alt_prices.append(tmp)
                else:
                    alt_prices.append(
                        [currency_conv(price_item['price'], price_item['currency']), None, price_item['price_change'],
                         price_item['price'], price_item['price_discount'], price_item['currency'], price_item['date'],
                         price_item['idproducts']])
            else:
                alt_prices.append(
                    [currency_conv(price_item['price'], price_item['currency']), None, price_item['price_change'],
                     price_item['price'], price_item['price_discount'], price_item['currency'], price_item['date'],
                     price_item['idproducts']])

        # 返回的价格：如果有折扣价，返回折扣价；如果没有，返回原价
        alt_prices = sorted(alt_prices, key=lambda val: val[1] if val[1] else val[0])

        entry['price'], entry['price_discount'] = alt_prices[0][:2] if alt_prices else (None,) * 2
        entry['price_change'] = alt_prices[0][2] if alt_prices else '0'
        entry['o_price'], entry['o_discount'], entry['o_currency'] = alt_prices[0][3:6]

        # 取消entry['price_list']中的idproducts
        for i in xrange(len(entry['price_list'])):
            entry['price_list'][i].pop('idproducts')
        entry['price_list'] = json.dumps(entry['price_list'], ensure_ascii=False)

        entry['last_price_ts'] = alt_prices[0][6]
        entry['product_update_ts'] = update_time_dict[alt_prices[0][7]].strftime("%Y-%m-%d %H:%M:%S")

        # 搜索字段
        search_text = u' '.join(entry[tmp] if entry[tmp] else '' for tmp in
                                ('name', 'description', 'details', 'model', 'brandname_e', 'brandname_c'))
        search_color = u' '.join(entry['color']) if entry['color'] else u''
        rs = db.query_match(['description_cn', 'description_en', 'details_cn', 'details_en'], 'products_translate',
                            {'fingerprint': entry['fingerprint']}).fetch_row()
        part_translate = u' ' + u' '.join(unicodify(tmp) for tmp in filter(lambda v: v, rs[0])) if rs else ' '
        search_tags = u' '.join(list(set(mfashion_tags)))
        entry['searchtext'] = unicode.format(u'{0} {1} {2} {3}', search_text, part_translate, search_tags, search_color)

        p = prods[0]
        checksums = []
        # 爆照checksums中的数据唯一，且顺序和idproducts_image一致
        for tmp in db.query(str.format('''
          SELECT p1.checksum, p3.width, p3.height, p3.path FROM products_image AS p1
          JOIN products AS p2 ON p1.fingerprint=p2.fingerprint
          JOIN images_store AS p3 ON p1.checksum=p3.checksum
          WHERE p2.fingerprint="{0}" ORDER BY p1.idproducts_image
          ''', p['fingerprint'])).fetch_row(maxrows=0, how=1):
            if tmp not in checksums:
                checksums.append(tmp)

        # 如果没有图片，则暂时不添加到release表中
        if not checksums:
            return

        image_list = []
        for val in checksums:
            tmp = {'path': val['path'], 'width': int(val['width']), 'height': int(val['height'])}
            if not image_list:
                entry['cover_image'] = json.dumps(tmp, ensure_ascii=False)
            image_list.append(tmp)

        entry['image_list'] = json.dumps(image_list[:self.max_images], ensure_ascii=False)

        db.insert(entry, 'products_release')

    def run(self):
        logger = get_logger()
        # 只处理关键国家的数据
        tmp = info.region_info()
        key_regions = filter(lambda val: tmp[val]['status'] == 1, tmp)
        with RoseVisionDb(getattr(gs, 'DATABASE')['DB_SPEC']) as db:
            # 删除原有的数据
            logger.info(str.format('DELETING OLD RECORDS: brand_id={0}', self.brand_id))
            db.execute(str.format('DELETE FROM products_release WHERE brand_id={0}', self.brand_id))
            fp_list = [tmp[0] for tmp in db.query(
                str.format('SELECT fingerprint FROM products WHERE brand_id={0} GROUP BY fingerprint',
                           self.brand_id)).fetch_row(maxrows=0)]
            self.tot = len(fp_list)
            self.progress = 0

            # 最多每100次就需要提交一次事务
            transaction_max = 100

            logger.info(str.format('TOT: {0} fingerprints, brand_id={1}', self.tot, self.brand_id))
            db.start_transaction()
            for self.progress in xrange(self.tot):
                if self.progress % transaction_max == 0:
                    db.commit()
                    db.start_transaction()
                    logger.info(str.format('PROCESSED {0}/{1} fingerprints, brand_id={2}', self.progress, self.tot,
                                           self.brand_id))

                fp = fp_list[self.progress]
                model_list = list(filter(lambda val: val['region'] in key_regions,
                                         db.query_match(['*'], 'products', {'fingerprint': fp}).fetch_row(maxrows=0,
                                                                                                          how=1)))
                if model_list:
                    self.merge_prods(model_list, db)
            db.commit()

            logger.info(str.format('DONE, brand_id={0}', self.brand_id))

    def get_msg(self):
        return str.format('{0}/{1}({2:.1%}) PROCESSED', self.progress, self.tot,
                          float(self.progress) / self.tot) if self.tot > 0 else 'IDLE'


def currency_update(param_dict):
    """
    更新货币的汇率信息
    @param param_dict:
    """
    db = RoseVisionDb()
    db.conn(getattr(gs, 'DATABASE')['DB_SPEC'])
    rs = db.query_match(['iso_code', 'currency'], 'region_info').fetch_row(maxrows=0)
    db.start_transaction()
    try:
        for code, currency in rs:
            print str.format('Fetching for currency data for {0}...', currency)
            data = cm.get_data(url=str.format('http://download.finance.yahoo.com/d/quotes.csv?s={0}CNY=X'
                                              '&f=sl1d1t1ba&e=.json', currency))
            rdr = csv.reader(StringIO(data['body']))
            line_data = [val for val in rdr][0]
            timestamp = datetime.datetime.strptime(str.format('{0} {1}', line_data[2], line_data[3]),
                                                   '%m/%d/%Y %I:%M%p')
            db.update({'rate': line_data[1], 'update_time': timestamp.strftime('%Y-%m-%d %H:%M:%S')},
                      'region_info', str.format('iso_code="{0}"', code))
        db.commit()
    except:
        db.rollback()
        raise
