# coding=utf-8

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from cStringIO import StringIO
import hashlib
import json
import os
import re

from scrapy import log
from scrapy.contrib.pipeline.images import ImagesPipeline, ImageException
from scrapy.exceptions import DropItem
from scrapy.http import Request
from PIL import Image

from core import RoseVisionDb
import global_settings as glob
from utils.utils_core import process_price, unicodify, iterable, gen_fingerprint
from scripts.urlprocess import urlencode

class MStorePipeline(object):
    @staticmethod
    def update_db_price(metadata, pid, brand, region, db):
        price = None
        discount = None
        # 表示是否更新了价格信息
        price_updated = False
        try:
            currency = glob.spider_info()[brand].spider_data['currency'][region]
        except KeyError:
            currency = None
        if 'price' in metadata:
            price = process_price(metadata['price'], region, currency=currency)
            try:
                discount = process_price(metadata['price_discount'], region, currency=currency)
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
            # 如果原来有价格，现在却没有抓到价格信息，则需要一些额外处理
            rs = db.query_match(['price', 'price_discount', 'currency'], 'products_price_history',
                                {'idproducts': pid}, tail_str='ORDER BY date DESC LIMIT 1')
            tmp = rs.fetch_row(maxrows=0, how=1)
            if tmp and tmp[0]['price']:
                db.insert({'idproducts': pid, 'price': None, 'currency': tmp[0]['currency'],
                           'price_discount': None}, 'products_price_history')
                price_updated = True

        return price_updated


class UpdatePipeline(MStorePipeline):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(getattr(glob, 'DB_SPEC'))

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
        description = unicodify(record['description'])
        details = unicodify(record['details'])
        tmp = unicodify(record['color'])
        color = json.loads(tmp) if tmp else None
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
        if 'color' in metadata and metadata['color'] != color:
            update_data['color'] = json.dumps(metadata['color'], ensure_ascii=False)

        is_price_offline = False
        # 处理价格（注意：价格属于经常变动的信息，需要及时更新）
        if 'price' in metadata and metadata['price'] != price:
            update_data['price'] = metadata['price']
        elif 'price' not in metadata and price:
            # 原来有价格，现在没有价格
            update_data['price'] = None
            is_price_offline = True
        if 'price_discount' in metadata:
            if metadata['price_discount'] != price_discount:
                update_data['price_discount'] = metadata['price_discount']
        else:
            if price_discount:
                # 原来有折扣价格，现在没有折扣价格
                update_data['price_discount'] = None

        if item['offline'] != offline:
            update_data['offline'] = item['offline']

        # 如果在某一次recrawl或update过程中，
        # 发现该商品的offline状态依然是0（即没有下线）,
        # 但是无当前价格，则应该将其offline置为2。
        if 'offline' not in update_data:
            if offline == 0:
                if is_price_offline:
                    update_data['offline'] = 2
        elif update_data['offline'] == 0:
            if is_price_offline:
                update_data['offline'] = 2

        return update_data

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
            update_data = self.get_update_data(spider, item, record)

            if update_data:
                self.db.update(update_data, 'products', str.format('idproducts={0}', pid),
                               timestamps=['update_time', 'touch_time'])
            else:
                # 所有的字段都没有变化，仅仅需要更新一下touch_time
                self.db.update({'offline': item['offline']}, 'products', str.format('idproducts={0}', pid),
                               timestamps=['touch_time'])

            # 更新数据库中的价格记录
            if not (('offline' in update_data and update_data['offline'] == 1) or (
                        'offline' in item and item['offline'] == 1)):
                if self.update_db_price(item['metadata'], pid, item['brand'], item['region'], self.db):
                    self.db.update({}, 'products', str.format('idproducts={0}', pid),
                                       timestamps=['update_time', 'touch_time'])
        except:
            self.db.rollback()
            raise
        finally:
            self.db.commit()


class ProductPipeline(MStorePipeline):
    @classmethod
    def from_crawler(cls, crawler):
        return cls(getattr(glob, 'DB_SPEC'))

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

        origin_url = entry['url']
        encoded_url = None
        try:
            encoded_url = urlencode(origin_url)
        except:
            encoded_url = origin_url
            spider.log(str.format("ERROR: {0} encode url error {1}", entry['fingerprint'], encoded_url))
            pass
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
                    # if 'category' in entry:
                #     dest['category'] = self.merge_list(record['category'], entry['category'])
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
            if self.update_db_price(entry, pid, entry['brand_id'], entry['region'], self.db):
                self.db.update({}, 'products', str.format('idproducts={0}', pid),
                                   timestamps=['update_time', 'touch_time'])

            # 处理标签变化
            self.process_tags_mapping(tags_mapping, entry, pid)

            self.db.commit()
        except:
            self.db.rollback()
            raise
        return item


class ProductImagePipeline(ImagesPipeline):
    def __init__(self, store_uri):
        super(ProductImagePipeline, self).__init__(store_uri)
        self.url_map = {}
        self.db = RoseVisionDb()
        self.db.conn(glob.DB_SPEC)

    def get_images(self, response, request, info):
        media_guid = hashlib.sha1(request.url).hexdigest()
        # 确定图像类型
        content_type = None
        for k in response.headers:
            if k.lower() == 'content-type':
                try:
                    content_type = response.headers[k].lower()
                except (TypeError, IndexError):
                    pass
        ext = 'jpg'
        if content_type == 'image/tiff':
            ext = 'tif'
        elif content_type == 'image/png':
            ext = 'png'
        key = str.format('full/{0}.{1}', media_guid, ext)

        orig_image = Image.open(StringIO(response.body))

        width, height = orig_image.size
        if width < self.MIN_WIDTH or height < self.MIN_HEIGHT:
            raise ImageException("Image too small (%dx%d < %dx%d)" %
                                 (width, height, self.MIN_WIDTH, self.MIN_HEIGHT))

        self.convert_image(orig_image)
        image, buf = orig_image, StringIO(response.body)

        yield key, image, buf

        for thumb_id, size in self.THUMBS.iteritems():
            thumb_key = str.format('thumbs/{0}/{1}.{2}', thumb_id, media_guid, ext)
            thumb_image, thumb_buf = self.convert_image(image, size)
            yield thumb_key, thumb_image, thumb_buf

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

        if not checksum_anchor and path_anchor:
            # 说明原来的checksum有误，整体更正
            self.db.update({'checksum': checksum}, 'images_store', str.format('path="{0}"', path))
        elif not checksum_anchor and not path_anchor:
            # 在images_store里面新增一个item
            self.db.insert({'checksum': checksum, 'url': data['url'], 'path': data['path'],
                            'width': data['width'], 'height': data['height'], 'format': data['format'],
                            'size': data['size']}, 'images_store')

        self.db.insert({'checksum': checksum, 'brand_id': brand_id, 'model': model}, 'products_image', ignore=True)
        # self.update_products_image(brand_id, model, checksum)

    def item_completed(self, results, item, info):
        # Tiffany需要特殊处理。因为Tiffany的图片下载机制是：下载一批可能的图片，在下载成功的图片中，挑选分辨率最好的那个。
        results = self.preprocess(results, item)
        brand_id = item['metadata']['brand_id']
        model = unicodify(item['metadata']['model'])
        for status, r in filter(lambda val: val[0], results):
            path = r['path']  #.replace(u'\\', u'/')

            # 框架返回的文件名，有可能后缀是错的。需要找到真实的图片文件名称
            full_path = os.path.normpath(os.path.join(self.store.basedir, path))
            dir_path, file_name = os.path.split(full_path)
            file_base, ext = os.path.splitext(file_name)

            # 找到具有同样base name的文件中，modified time最后的那个
            tmp = sorted([val for val in os.listdir(dir_path) if re.search(file_base, val)],
                         key=lambda val: os.stat(os.path.join(dir_path, val)).st_mtime, reverse=True)
            if not tmp:
                continue
            else:
                file_name = tmp[0]
                full_path = os.path.normpath(os.path.join(dir_path, file_name))
                _, ext = os.path.splitext(file_name)
                path_db = os.path.normpath(os.path.join(
                    unicode.format(u'{0}_{1}/{2}{3}', brand_id, glob.brand_info()[brand_id]['brandname_s'],
                                   os.path.splitext(path)[0], ext)))

                file_size = os.path.getsize(full_path)
                checksum = r['checksum']
                try:
                    img = Image.open(full_path)
                    aw, ah = img.size
                    afmt = img.format
                except IOError:
                    continue

                self.db.start_transaction()
                try:
                    self.update_db({'brand_id': brand_id, 'model': model, 'checksum': checksum, 'path': path_db,
                                    'width': aw, 'height': ah, 'format': afmt, 'url': r['url'], 'size': file_size})
                    self.db.commit()
                except:
                    self.db.rollback()
                    raise

        return item
