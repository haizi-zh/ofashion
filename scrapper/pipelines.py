# coding=utf-8

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from cStringIO import StringIO
import copy
import hashlib
import json
import os
import re
from urllib2 import quote
import mmap
import urllib2

from scrapy import log
from scrapy.contrib.pipeline.images import ImagesPipeline, ImageException
from scrapy.exceptions import DropItem
from scrapy.http import Request
from PIL import Image
from upyun import upyun
from scrapper.mfimages import MFImagesPipeline
from scripts.run_crawler import get_images_store

from utils.db import RoseVisionDb
import global_settings as glob
from utils import info
from utils.text import unicodify, iterable
from utils.utils_core import process_price, gen_fingerprint, lxmlparser, get_logger

import sys

sys.path.append('/home/rose/Mstore/scripts')
from tasks import image_download


class MStorePipeline(object):
    @staticmethod
    def update_db_price(metadata, pid, brand, region, db):
        price = None
        discount = None
        # 表示是否可能下线（对应offline=2的状态）
        suggested_offline = False
        # 表示是否更新了价格信息
        price_updated = False
        try:
            # 爬虫指定的货币
            spider_currency = info.spider_info()[brand]['spider_class'].spider_data['currency'][region]
        except KeyError:
            spider_currency = None
        if 'price' in metadata:
            price = process_price(metadata['price'], region, currency=spider_currency)
            try:
                discount = process_price(metadata['price_discount'], region, currency=spider_currency)
            except KeyError:
                discount = None
        if price and price['price'] > 0:
            # 该单品最后的价格信息
            price_value = price['price']
            discount_value = discount['price'] if discount else None

            # 如果折扣价格大于或等于原价，则取消折扣价，并作出相应的警告
            if discount_value and discount_value >= price_value:
                discount_value = None

            rs = db.query_match(['price', 'price_discount', 'currency'], 'products_price_history',
                                {'idproducts': pid}, tail_str='ORDER BY date DESC LIMIT 1')
            insert_flag = False
            if rs.num_rows() == 0:
                insert_flag = True
            else:
                ret = rs.fetch_row()[0]
                db_entry = [float(val) if val else None for val in ret[:2]]
                old_currency = ret[2]
                if db_entry[0] != price_value or db_entry[1] != discount_value or old_currency != price[
                    'currency']:
                    insert_flag = True

            if insert_flag:
                db.insert({'idproducts': pid, 'price': price_value, 'currency': price['currency'],
                           'price_discount': (
                               discount_value if discount_value < price_value else None)},
                          'products_price_history')
            price_updated = insert_flag
        else:
            # 如果原来有价格，现在却没有抓到价格信息，则uggested_offline=True
            rs = db.query_match(['price', 'price_discount', 'currency'], 'products_price_history',
                                {'idproducts': pid}, tail_str='ORDER BY date DESC LIMIT 1')
            tmp = rs.fetch_row(maxrows=0, how=1)
            suggested_offline = True if tmp else False

        return price_updated, suggested_offline


class UpdatePipeline(MStorePipeline):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(getattr(glob, 'DATABASE')['DB_SPEC'])

    def __init__(self, db_spec):
        self.db = RoseVisionDb()
        self.db.conn(db_spec)
        self.processed_tags = set([])

    @staticmethod
    def get_update_data(spider, item, record):
        """
        Compare the pipeline item against the existing record, and determine which fields should be updated.
        @param item: a pipeline item to process.
        @param record: a corresponding record from the database.
        @return: a dictionary object, which indicates the data to be updated, and a boolean value indicates
        """
        metadata = item['metadata']

        model = unicodify(record['model'])
        description = unicodify(lxmlparser(record['description']))
        details = unicodify(lxmlparser(record['details']))
        tmp = unicodify(record['color'])
        # color = json.loads(tmp) if tmp else None
        price = unicodify(record['price'])
        price_discount = unicodify(record['price_discount'])
        offline = int(record['offline'])

        # 如果旧数据和item有不一致的地方，则更新
        update_data = {}
        if 'model' in metadata and metadata['model'] != model:
            # Generally, during the update procedure, a product's model shouldn't be changed.
            # If the model doesn't agree with that in the database, the whole record is considered to be offline.
            spider.log(str.format('The update spider tried to change the model string: idproducts={0}',
                                  record['idproducts']), log.WARNING)
            update_data['offline'] = 1
            return update_data

        if 'description' in metadata and metadata['description'] != description:
            update_data['description'] = metadata['description']
        if 'details' in metadata and metadata['details'] != details:
            update_data['details'] = metadata['details']
        # if 'color' in metadata and metadata['color'] != color:
        #     update_data['color'] = json.dumps(metadata['color'], ensure_ascii=False)

        # 处理价格（注意：价格属于经常变动的信息，需要及时更新）
        if 'price' in metadata and metadata['price'] != price:
            update_data['price'] = metadata['price']
        elif 'price' not in metadata and price:
            # 原来有价格，现在没有价格
            update_data['price'] = None
        if 'price_discount' in metadata:
            if metadata['price_discount'] != price_discount:
                update_data['price_discount'] = metadata['price_discount']
        else:
            if price_discount:
                # 原来有折扣价格，现在没有折扣价格
                update_data['price_discount'] = None

        if item['offline'] != offline:
            update_data['offline'] = item['offline']

        return update_data

    @staticmethod
    def is_update_price(item):
        """
        检查：是否需要进行价格更新
        @param item:
        """
        # item['offline']的取值只有两种可能：0或者1。如果是1，说明已经下线，不需要进行价格更新。
        # 如果是0，则有两种可能：完全正常，或者是其实没抓到价格（这一情况大部分时候意味着疑似下线）。需要进行进一步检查。
        return item['offline'] == 0

    def process_item(self, item, spider):
        pid = item['idproduct']

        self.db.start_transaction()
        try:
            # 获得旧数据
            rs = self.db.query_match({'model', 'description', 'details', 'color', 'price', 'price_discount', 'offline',
                                      'idproducts'}, 'products', {'idproducts': pid})
            # 如果没有找到相应的记录，
            if rs.num_rows() == 0:
                raise DropItem
            record = rs.fetch_row(how=1)[0]
            record['offline'] = int(record['offline'])
            update_data = self.get_update_data(spider, item, record)

            # 如果为True，则无论update_data的状态，都需要更新
            force_update = False

            if self.is_update_price(item):
                # 注意，如果价格更新，所以products的时间戳也需要更新，所以需要force_update为True
                force_update, suggested_offline = self.update_db_price(item['metadata'], pid, item['brand'],
                                                                       item['region'], self.db)

                if suggested_offline:
                    # 保证更新后的offline为1或者2。
                    if 'offline' in update_data and update_data['offline'] == 0:
                        update_data['offline'] = 2
                    elif 'offline' not in update_data and record['offline'] == 0:
                        update_data['offline'] = 2

            if update_data:
                self.db.update(update_data, 'products', str.format('idproducts={0}', pid),
                               timestamps=['update_time', 'touch_time'])
            elif force_update:
                self.db.update({}, 'products', str.format('idproducts={0}', pid),
                                 timestamps=['update_time', 'touch_time'])
            else:
                self.db.update({}, 'products', str.format('idproducts={0}', pid),
                                 timestamps=['touch_time'])

        except:
            self.db.rollback()
            raise
        finally:
            self.db.commit()


class ProductPipeline(MStorePipeline):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(getattr(glob, 'DATABASE')['DB_SPEC'])

    def __init__(self, db_spec):
        self.db = RoseVisionDb()
        self.db.conn(db_spec)
        self.processed_tags = set([])

    @staticmethod
    def process_gender(gender):
        """
        检查entry的gender字段。如果male/female都出现，说明该entry和性别无关，unisex。
        :param entry:
        """
        if not gender:
            return None
        else:
            gender = list(set(gender))

        if 'female' in gender and 'male' in gender:
            return 'unisex'
        else:
            return gender[0]

    def process_tags_mapping(self, tags, entry, pid):
        """
        如果有新的tag，加入到mapping列表中
        :param entry:
        """

        def get_tag_sig(brand_id, region, tag_type, tag_name):
            m = hashlib.md5()
            m.update(u'|'.join([unicode(brand_id), region.lower(), tag_type.lower(), tag_name.lower()]).encode('utf-8'))
            return m.hexdigest()

        # 构造完整的tag集合
        brand_id = entry['brand_id']
        region = entry['region']
        for tag_type in tags:
            for tag_item in tags[tag_type]:
                tag_name = tag_item['name']
                tag_text = tag_item['title']
                # 空的tag_name
                if not tag_name:
                    continue

                tmp = {'brand_id': brand_id,
                       'region': region,
                       'tag_type': tag_type,
                       'tag_name': tag_name, 'tag_text': tag_text}
                self.db.insert(tmp, 'original_tags', ignore=True)
                tmp.pop('tag_text')
                rs = self.db.query_match('idmappings', 'original_tags', tmp)
                if rs.num_rows() > 0:
                    tid = int(rs.fetch_row()[0][0])
                    self.db.insert({'idproducts': pid, 'id_original_tags': tid}, 'products_original_tags', ignore=True)

    @staticmethod
    def product_tags_merge(src, dest):
        """
        合并两个tag列表：把src中的内容合并到dest中
        :param src:
        :param dest:
        """

        def to_set(val):
            """
            如果val是iterable，则转为set，否则……
            :param val:
            :return:
            """
            return set(val) if iterable(val) else {val}

        dest = {k: to_set(dest[k]) for k in dest if dest[k]}
        src = {k: to_set(src[k]) for k in src if src[k]}

        for k in src:
            if k not in dest:
                dest[k] = src[k]
            else:
                dest[k] = dest[k].union(src[k])

        # 整理
        return dict((k, list(dest[k])) for k in dest)

    @staticmethod
    def merge_list(old_list, term):
        """
        将新抓取到的数据，合并到旧数据里面
        @rtype : 返回新列表的JSON字符串。
        @param old_list: 原color数据，为json.dump字符串形式，或者为None
        @param term:
        """
        term = [unicodify(tmp.lower()) for tmp in term] if term else []

        if old_list:
            old_list = [unicodify(tmp.lower()) for tmp in json.loads(old_list) if tmp]
            old_list.extend(term)
        else:
            old_list = term

        if old_list is not None:
            return json.dumps(sorted(list(set(old_list))), ensure_ascii=False)
        else:
            return None

    @staticmethod
    def merge_gender(old_gender, gender):
        if gender:
            gender = gender[0]
        else:
            return old_gender

        if old_gender:
            if old_gender != unicodify(gender).lower():
                old_gender = None
        else:
            if gender:
                old_gender = unicodify(gender).lower()

        return old_gender

    def process_item(self, item, spider):
        entry = item['metadata']
        if 'model' not in entry or not entry['model']:
            raise DropItem()
        entry['fingerprint'] = gen_fingerprint(entry['brand_id'], entry['model'])

        for k in ('name', 'description', 'details'):
            if k in entry:
                entry[k] = lxmlparser(entry[k])

        origin_url = entry['url']
        try:
            encoded_url = quote(origin_url.encode('utf-8'), "/?:@&=+$,;#%")
        except:
            encoded_url = origin_url
            spider.log(unicode.format(u"ERROR: {0} encode url error {1}", entry['fingerprint'], encoded_url))
        entry['url'] = encoded_url

        self.db.start_transaction()
        try:
            tags_mapping = entry.pop('tags_mapping')
            results = self.db.query_match('*', 'products', {'fingerprint': entry['fingerprint'],
                                                            'region': entry['region']}).fetch_row(maxrows=0, how=1)
            if not results:
                # 数据库中无相关数据，插入新记录
                if 'color' in entry and entry['color']:
                    entry['color'] = json.dumps(entry['color'], ensure_ascii=False).lower()
                if 'gender' in entry:
                    entry['gender'] = self.process_gender(entry['gender'])

                self.db.insert(entry, 'products', ['touch_time', 'update_time'])
                pid = int(self.db.query('SELECT LAST_INSERT_ID()').fetch_row()[0][0])
                spider.log(unicode.format(u'INSERT: {0}', entry['model']), log.DEBUG)
            else:
                # 需要将新数据合并到旧数据中
                record = results[0]

                dest = {'offline': 0}
                if 'color' in entry:
                    dest['color'] = self.merge_list(record['color'], entry['color'])
                if 'gender' in entry:
                    dest['gender'] = self.process_gender(entry['gender'])

                for k in (
                        'name', 'url', 'description', 'details', 'price', 'price_discount', 'price', 'price_discount'):
                    if k in entry:
                        dest[k] = entry[k]

                # 比较内容是否发生实质性变化
                md5_o = hashlib.md5()
                md5_n = hashlib.md5()
                flag = True
                for k in dest:
                    tmp = unicodify(dest[k])
                    if tmp:
                        md5_n.update((tmp if tmp else 'NULL').encode('utf-8'))
                    tmp = results[0][k]
                    if tmp:
                        md5_o.update(tmp if tmp else 'NULL')
                    if md5_n.hexdigest() != md5_o.hexdigest():
                        flag = False
                        break

                pid = results[0]['idproducts']
                if flag:
                    self.db.update({}, 'products', str.format('idproducts={0}', pid), ['touch_time'])
                else:
                    self.db.update(dest, 'products', str.format('idproducts={0}', pid), ['update_time', 'touch_time'])

                spider.log(unicode.format(u'UPDATE: {0}', entry['model']), log.DEBUG)

            # 处理价格变化。其中，如果spider提供了货币信息，则优先使用之。
            price_updated, suggested_offline = self.update_db_price(entry, pid, entry['brand_id'], entry['region'],
                                                                    self.db)
            if price_updated:
                self.db.update({}, 'products', str.format('idproducts={0}', pid),
                                 timestamps=['update_time', 'touch_time'])

            if suggested_offline:
                # 单品疑似下架
                self.db.update({'offline': 2}, 'products', str.format('idproducts={0}', pid),
                               timestamps=['update_time', 'touch_time'])

            # 处理标签变化
            self.process_tags_mapping(tags_mapping, entry, pid)

            self.db.commit()
        except:
            self.db.rollback()
            raise
        return item


class ProductImagePipeline(MFImagesPipeline):
    def __init__(self, store_uri):
        super(ProductImagePipeline, self).__init__(store_uri)
        self.url_map = {}
        self.db = RoseVisionDb()
        self.db.conn(getattr(glob, 'DATABASE')['DB_SPEC'])

    def get_images(self, response, request, info):
        """
        和默认版本的get_images函数相比，主要的修改是：支持多种image/*格式。
        @param response:
        @param request:
        @param info:
        @raise ImageException:
        """
        media_guid = hashlib.sha1(request.url).hexdigest()
        # 确定图像类型
        content_type = None
        for k in response.headers:
            if k.lower() == 'content-type':
                try:
                    content_type = response.headers[k].lower()
                except (TypeError, IndexError):
                    pass
        if content_type == 'image/tiff':
            ext = 'tif'
        elif content_type == 'image/png':
            ext = 'png'
        elif content_type == 'image/gif':
            ext = 'gif'
        elif content_type == 'image/bmp':
            ext = 'bmp'
        elif content_type == 'image/jpeg':
            ext = 'jpg'
        else:
            raise DropItem
        key = str.format('full/{0}.{1}', media_guid, ext)

        orig_image = Image.open(StringIO(response.body))

        width, height = orig_image.size
        if width < self.MIN_WIDTH or height < self.MIN_HEIGHT:
            raise ImageException("Image too small (%dx%d < %dx%d)" %
                                 (width, height, self.MIN_WIDTH, self.MIN_HEIGHT))

        self.convert_image(orig_image)
        image, buf = orig_image, StringIO(response.body)
        # image, buf = self.convert_image(orig_image)

        yield key, image, buf

        # for thumb_id, size in self.THUMBS.iteritems():
        #     thumb_key = str.format('thumbs/{0}/{1}.{2}', thumb_id, media_guid, ext)
        #     thumb_image, thumb_buf = self.convert_image(image, size)
        #     yield thumb_key, thumb_image, thumb_buf

    def get_media_requests(self, item, info):
        if 'image_urls' in item:
            for url in item['image_urls']:
                yield Request(url)

    def preprocess(self, results, item):
        brand_id = item['metadata']['brand_id']

        def tiffany_chrt(r):
            """
            提取Tiffany图片链接的特征表达，即URL变量中，?之前的部分，即url的主体
            @param r:
            """
            # TODO 利用urlparser库来解析url地址
            url = r[1]['url']
            idx = url.find('?')
            if idx != -1:
                url = url[:idx]
            return hashlib.md5(url).hexdigest()

        # TODO balenciaga和bottega的特征提取函数都错了，需要重新做
        def balenciaga_chrt(r):
            # 处理Balenciaga图片连接的特征值，即url变量文件名部分的最后一个字母
            return os.path.splitext(r[1]['url'])[0][-1]

        mcqueen_chrt = balenciaga_chrt
        # TODO Dolce Gabbana的图片数据需要重新做
        dolce_chrt = balenciaga_chrt
        valentino_chrt = balenciaga_chrt
        emiliopucci_chrt = balenciaga_chrt
        stella_chrt = balenciaga_chrt
        missoni_chrt = balenciaga_chrt
        roberto_cavalli_chrt = balenciaga_chrt
        marni_chrt = balenciaga_chrt
        bally_chrt = balenciaga_chrt
        sergio_chrt = balenciaga_chrt

        def bottega_chrt(r):
            # 处理Bottega的图片链接特征值，即url变量文件名部分的最后两个字母
            return os.path.splitext(r[1]['url'])[0][-2:]

        def garmani_chrt(r):
            # Giorgio Armani的特征：最后几个字母
            url = r[1]['url']
            mt = re.search(r'\d+([_a-z]+)\.[a-z]+$', url)
            return mt.group(1)

        def func(chrt_func):
            url_dict = {}
            for item in list(filter(lambda val: val[0], results)):
                chrt = chrt_func(item)

                full_path = os.path.normpath(os.path.join(self.store.basedir, item[1]['path']))
                try:
                    img = Image.open(full_path)
                except IOError:
                    continue

                # 两种情况会采用该result：尺寸更大，或第一次出现
                dim = img.size[0]
                if chrt not in url_dict or dim > url_dict[chrt]['dim']:
                    url_dict[chrt] = {'dim': dim, 'item': item}
            return [val['item'] for val in url_dict.values()]

        func_map = {10350: lambda: func(tiffany_chrt),
                    10029: lambda: func(balenciaga_chrt),
                    10008: lambda: func(mcqueen_chrt),
                    10109: lambda: func(dolce_chrt),
                    10049: lambda: func(bottega_chrt),
                    10149: lambda: func(garmani_chrt),
                    10117: lambda: func(emiliopucci_chrt),
                    10333: lambda: func(stella_chrt),
                    10030: lambda: func(bally_chrt),
                    10263: lambda: func(missoni_chrt),
                    10305: lambda: func(roberto_cavalli_chrt),
                    10241: lambda: func(marni_chrt),
                    10316: lambda: func(sergio_chrt),
                    10367: lambda: func(valentino_chrt)}

        if brand_id in func_map:
            return func_map[brand_id]()
        else:
            return results

    def update_products_image(self, brand_id, model, checksum):
        """
        保证products_image的一致性
        :param brand_id:
        :param checksum:
        :param model:
        """
        rs = self.db.query_match('idproducts_image', 'products_image',
                                 {'brand_id': brand_id, 'model': model, 'checksum': checksum})
        if rs.num_rows() == 0:
            self.db.insert({'checksum': checksum, 'brand_id': brand_id, 'model': model}, 'products_image')
        elif rs.num_rows() > 1:
            # brand|model下面有多个同样的文件，保留第一条，删除其余的记录
            entry_list = [tmp[0] for tmp in rs.fetch_row(maxrows=0)]
            self.db.execute(str.format('DELETE FROM products_image WHERE idproducts_image IN ({0})',
                                       ', '.join(entry_list[1:])))

    def update_db(self, data):
        """
        根据新数据的hash值以及文件路径，更新数据库images_store和products_image
        :param data:
        """
        checksum = data['checksum']
        path = data['path']
        brand_id = data['brand_id']
        model = data['model']

        rs = self.db.query_match('checksum', 'images_store', {'checksum': checksum})
        checksum_anchor = rs.num_rows() > 0
        rs = self.db.query_match('checksum', 'images_store', {'path': path})
        path_anchor = rs.num_rows() > 0

        # checksum_anchor: 说明数据库中已经有记录，其checksum和新插入的图像是一致的。
        # path_anchor：说明数据库中已经有记录，其path（也就是图像的url），和新插入的图像是一致的。
        # 这里分为4种情况讨论：
        # 1. checksum_anchor==True and path_anchor==True：说明一切正常，新增加的图像在数据库中已经有记录。
        #    不用对images_store作任何操作。
        # 2. checksum_anchor==True and path_anchor==False：数据库中已经存在这幅图像，但path不符。一般来说，可能是下面这种情况
        #    引起的：url_a和url_b这两个图像链接，指向了同样一张图像。假定数据库中已有图像记录的链接为url_a。由于url_a和url_b都存在，
        #    切对应于同一张图像，所以通常我们可以忽略url_b，不用对images_store作任何操作。
        # 3. checksum_anchor==False and path_anchor=True：二者不一致，说明该path对应图像发生了变化（比如，原网站对图像做了一
        #    些改动等，但并未更改url链接等）。此时，需要更新数据库的记录。
        # 4. checksum_anchor==False and path_anchor==False：说明这是一条全新的图像，直接入库。
        if not checksum_anchor and path_anchor:
            # 说明原来的checksum有误，整体更正
            self.db.update({'checksum': checksum}, 'images_store', str.format('path="{0}"', path))
        elif not checksum_anchor and not path_anchor:
            # 在images_store里面新增一个item
            self.db.insert({'checksum': checksum, 'url': data['url'], 'path': data['path'],
                            'width': data['width'], 'height': data['height'], 'format': data['format'],
                            'size': data['size']}, 'images_store')

        self.db.insert({'checksum': checksum, 'brand_id': brand_id, 'model': model,
                        'fingerprint': gen_fingerprint(brand_id, model)},
                       'products_image', ignore=True)

    def item_completed(self, results, item, pipeline_info):
        # Tiffany需要特殊处理。因为Tiffany的图片下载机制是：下载一批可能的图片，在下载成功的图片中，挑选分辨率最好的那个。
        # results = self.preprocess(results, item)
        brand_id = item['metadata']['brand_id']
        model = unicodify(item['metadata']['model'])
        for status, r in filter(lambda val: val[0], results):
            path = r['path']  #.replace(u'\\', u'/')

            self.db.start_transaction()
            try:
                self.update_db({'brand_id': brand_id, 'model': model, 'checksum': r['checksum'], 'path': r['path'],
                                'width': r['width'], 'height': r['height'], 'format': r['format'], 'url': r['url'],
                                'size': r['size']})
                self.db.commit()
            except:
                self.db.rollback()
                raise

                # # 框架返回的文件名，有可能后缀是错的。需要找到真实的图片文件名称
                # full_path = os.path.normpath(os.path.join(self.store.basedir, path))
                # dir_path, file_name = os.path.split(full_path)
                # file_base, ext = os.path.splitext(file_name)
                #
                # # 找到具有同样base name的文件中，modified time最后的那个
                # tmp = sorted([val for val in os.listdir(dir_path) if re.search(file_base, val)],
                #              key=lambda val: os.stat(os.path.join(dir_path, val)).st_mtime, reverse=True)
                # if not tmp:
                #     continue
                # else:
                #     file_name = tmp[0]
                #     full_path = os.path.normpath(os.path.join(dir_path, file_name))
                #     _, ext = os.path.splitext(file_name)
                #     path_db = os.path.normpath(os.path.join(
                #         unicode.format(u'{0}_{1}/{2}{3}', brand_id, info.brand_info()[brand_id]['brandname_s'],
                #                        os.path.splitext(path)[0], ext)))
                #
                #     file_size = os.path.getsize(full_path)
                #     checksum = r['checksum']
                #     try:
                #         img = Image.open(full_path)
                #         aw, ah = img.size
                #         afmt = img.format
                #     except IOError:
                #         continue
                #
                #     self.db.start_transaction()
                #     try:
                #         self.update_db({'brand_id': brand_id, 'model': model, 'checksum': checksum, 'path': path_db,
                #                         'width': aw, 'height': ah, 'format': afmt, 'url': r['url'], 'size': file_size})
                #         self.db.commit()
                #     except:
                #         self.db.rollback()
                #         raise

        return item


class MonitorPipeline(UpdatePipeline):
    def process_item(self, item, spider):
        pid = item['idproduct']

        self.db.start_transaction()
        try:
            # 获得旧数据
            rs = self.db.query_match(
                {'model', 'description', 'details', 'color', 'price', 'price_discount', 'offline',
                 'idproducts'}, 'products', {'idproducts': pid})
            # 如果没有找到相应的记录，
            if rs.num_rows() == 0:
                raise DropItem
            record = rs.fetch_row(how=1)[0]
            update_data = self.get_update_data(spider, item, record)

            # 这里不判断model和offline的变化
            if 'model' in update_data:
                update_data.pop('model')
            # if 'offline' in update_data:
            #     update_data.pop('offline')

            if update_data:
                # 注意，这里的stop()，并不会立即停止所有爬虫线程
                spider.log(
                    str.format('DIFFERENCE DETECTED: {2}: {0} => {1}', str({k: record[k] for k in update_data}),
                               str(update_data), record['idproducts']), log.INFO)
                spider.crawler.stop()

                logger = get_logger(logger_name='monitor')

                filename = str(item['brand']) + item['region']
                fd = os.open(filename, os.O_RDWR)
                assert os.write(fd, '\x00' * mmap.PAGESIZE) == mmap.PAGESIZE

                mm = mmap.mmap(fd, mmap.PAGESIZE, access=mmap.ACCESS_WRITE)
                mm.write('recrawl')

                logger.info('Monitor ended--> brand_id:%s, region:%s' % (
                    item['brand'], item['region']))
            self.db.commit()
        except Exception as e:
            print e
            self.db.rollback()
            raise


def upyun_upload(brand_id, buf, image_path):
    uri = get_images_store(brand_id)
    assert uri.startswith('up://')
    info, dirpath = uri[5:].split('/', 1)
    UP_USERNAME, UP_PASSWORD, UP_BUCKETNAME = re.split('[:@]', info)
    up = upyun.UpYun(UP_BUCKETNAME, UP_USERNAME, UP_PASSWORD, timeout=30,
                     endpoint=upyun.ED_AUTO)
    full_file = os.path.join(dirpath, image_path)
    for i in range(3):
        try:
            up.put(full_file, buf.getvalue(), checksum=True)
            up.getinfo(full_file)
            break
        except Exception as e:
            if i == 2:
                raise e


def update_images(checksum, url, path, width, height, fmt, size, brand_id, model, buf, image_path):
    db = RoseVisionDb()
    db.conn(getattr(glob, 'DATABASE')['DB_SPEC'])
    rs1 = db.query_match('checksum', 'images_store', {'checksum': checksum})
    checksum_anchor = rs1.num_rows() > 0
    rs2 = db.query_match('checksum', 'images_store', {'path': path})
    path_anchor = rs2.num_rows() > 0

    try:

        if not checksum_anchor and path_anchor:
            # 说明原来的checksum有误，整体更正
            upyun_upload(brand_id, buf, image_path)
            db.update({'checksum': checksum}, 'images_store', str.format('path="{0}"', path))
        elif not checksum_anchor and not path_anchor:
            # 在images_store里面新增一个item
            upyun_upload(brand_id, buf, image_path)
            db.insert({'checksum': checksum, 'url': url, 'path': path,
                       'width': width, 'height': height, 'format': fmt,
                       'size': size}, 'images_store', ignore=True)

        db.insert({'checksum': checksum, 'brand_id': brand_id, 'model': model,
                   'fingerprint': gen_fingerprint(brand_id, model)},
                  'products_image', ignore=True)
    except:
        #todo need to write logs to rsyslog
        print('upload image error:%s,%s' % (url, image_path))


class CeleryPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(getattr(glob, 'DATABASE')['DB_SPEC'])

    def __init__(self, db_spec):
        self.db = RoseVisionDb()
        self.db.conn(db_spec)
        self.processed_tags = set([])

    def process_item(self, item, spider):
        if 'image_urls' in item:
            # spider.log(item)
            data = copy.deepcopy(item)
            if 'description' in data['metadata']:
                data['metadata'].pop('description')
            if 'category' in data['metadata']:
                data['metadata'].pop('category')
            if 'color' in data['metadata']:
                data['metadata'].pop('color')
            if 'name' in data['metadata']:
                data['metadata'].pop('name')
            if 'touch_time' in data['metadata']:
                data['metadata'].pop('touch_time')
            if 'update_time' in data['metadata']:
                data['metadata'].pop('update_time')

            data['metadata']['ua'] = spider.settings.values['USER_AGENT']

            data = dict(data)
            #todo test find duplicate items
            logger = get_logger(logger_name='item')
            logger.info(data)

            image_download.apply_async(kwargs=data)

