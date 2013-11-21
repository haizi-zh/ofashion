# coding=utf-8

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from cStringIO import StringIO
import datetime
import hashlib
import json
import os
from scrapy import log
from scrapy.contrib.pipeline.images import ImagesPipeline, ImageException
from scrapy.exceptions import DropItem
from scrapy.http import Request
import common as cm
from core import MySqlDb
from scrapper import utils
from PIL import Image
import global_settings as glob


class ProductPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        db_spec = crawler.settings['SPIDER_SPEC']
        return cls(db_spec)

    def __init__(self, db_spec):
        self.db = MySqlDb()
        self.db.conn(db_spec)
        self.processed_tags = set([])

    def process_gender(self, entry):
        """
        检查entry的gender字段。如果male/female都出现，说明该entry和性别无关，设置gender为None。
        :param entry:
        """
        gender = None
        tmp = entry['gender']
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

        entry['gender'] = gender

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
                tid = int(self.db.query_match('idmappings', 'original_tags', tmp).fetch_row()[0][0])
                self.db.insert({'idproducts': pid, 'id_original_tags': tid}, 'products_original_tags', ignore=True)

    def process_item(self, item, spider):
        entry = item['metadata']
        tags_mapping = entry.pop('tags_mapping')
        if 'model' not in entry or not entry['model']:
            raise DropItem()

        self.db.start_transaction()
        try:
            results = self.db.query_match('*', 'products', {'brand_id': entry['brand_id'], 'model': entry['model'],
                                                            'region': entry['region']}).fetch_row(maxrows=0, how=1)
            if not results:
                if 'color' in entry and entry['color']:
                    entry['color'] = json.dumps(entry['color'], ensure_ascii=False)
                if 'category' in entry and entry['category']:
                    entry['category'] = json.dumps(entry['category'], ensure_ascii=False)
                if 'gender' in entry and entry['gender']:
                    entry['gender'] = json.dumps(entry['gender'], ensure_ascii=False)
                    self.process_gender(entry)

                self.db.insert(entry, 'products', ['touch_time', 'update_time', 'fetch_time'])
                pid = int(self.db.query('SELECT LAST_INSERT_ID()').fetch_row()[0][0])
                spider.log(unicode.format(u'INSERT: {0}', entry['model']), log.DEBUG)
            else:
                pid = results[0]['idproducts']
                # 需要处理合并的字段
                dest = {}
                src = {}
                for k in ('category', 'color', 'gender'):
                    tmp = results[0][k]
                    if tmp:
                        try:
                            dest[k] = json.loads(tmp)
                        except ValueError:
                            dest[k] = [cm.unicodify(tmp)]
                    tmp = entry[k]
                    if tmp:
                        try:
                            src[k] = json.loads(tmp)
                        except ValueError:
                            src[k] = [cm.unicodify(tmp)]

                dest = utils.product_tags_merge(src, dest)
                dest = {k: json.dumps(dest[k], ensure_ascii=False) for k in dest}
                self.process_gender(dest)
                for k in ('name', 'url', 'description', 'details', 'price'):
                    dest[k] = entry[k]

                self.db.update(dest, 'products', str.format('idproducts={0}', pid), ['touch_time'])
                spider.log(unicode.format(u'UPDATE: {0}', entry['model']), log.DEBUG)

            # 处理价格变化
            tmp = cm.process_price(entry['price'], entry['region'])
            if not tmp:
                price = tmp['price']
                currency = tmp['currency']
                rs = self.db.query_match('price', 'products_price_history', {'idproducts': pid},
                                         tail_str='ORDER BY date DESC LIMIT 1')
                if rs.num_rows() == 0 or float(rs.fetch_row()[0][0]) != price:
                    self.db.insert({'idproducts': pid, 'price': price, 'currency': currency}, 'products_price_history')

            # 处理标签变化
            self.process_tags_mapping(tags_mapping, entry, pid)
            self.db.commit()
        except:
            self.db.rollback()
            raise
        return item


class ProductImagePipeline(ImagesPipeline):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        db_spec = settings['SPIDER_SPEC']
        image_store = settings.get('IMAGES_STORE')
        ProductImagePipeline.DBSPEC = db_spec
        return cls(image_store, crawler, db_spec)

    def __init__(self, store_uri, crawler=None, db_spec=None):
        self.crawler = crawler
        self.url_map = {}
        self.db = MySqlDb()
        self.db.conn(db_spec)
        super(ProductImagePipeline, self).__init__(store_uri)

        # def image_key(self, url):
        #     val = super(ProductImagePipeline, self).image_key(url)
        #     return val
        # m = self.url_map.pop(url)
        # p, fn = os.path.split(val)
        # fn = unicode.format(u'{0}_{1}', m['model'], fn)
        # return os.path.join(p, unicode.format(u'{0}_{1}', m['brand_id'], cm.simplify_brand_name(m['brandname_e'])), fn)

    # def thumb_key(self, url, thumb_id):
    #     val = super(ProductImagePipeline, self).thumb_key(url, thumb_id)
    #     m = self.url_map[url]
    #     p, fn = os.path.split(val)
    #     fn = unicode.format(u'{0}_{1}', m['model'], fn)
    #     return os.path.join(p, unicode.format(u'{0}_{1}', m['brand_id'], m['brandname_e']), fn)

    def get_images(self, response, request, info):
        key = self.file_key(request.url)

        orig_image = Image.open(StringIO(response.body))

        width, height = orig_image.size
        if width < self.MIN_WIDTH or height < self.MIN_HEIGHT:
            raise ImageException("Image too small (%dx%d < %dx%d)" %
                                 (width, height, self.MIN_WIDTH, self.MIN_HEIGHT))

        image, buf = self.convert_image(orig_image)
        image, buf = orig_image, StringIO(response.body)

        yield key, image, buf

        for thumb_id, size in self.THUMBS.iteritems():
            thumb_key = self.thumb_key(request.url, thumb_id)
            thumb_image, thumb_buf = self.convert_image(image, size)
            yield thumb_key, thumb_image, thumb_buf

    def get_media_requests(self, item, info):
        if 'image_urls' in item:
            for url in item['image_urls']:
                # self.url_map[url] = {'brand_id': m['brand_id'], 'brandname_e': m['brandname_e'], 'model': m['model']}
                yield Request(url)
                # yield Request(url, meta={'handle_httpstatus_list': [403]})

    def preprocess(self, results, item):
        brand_id = item['metadata']['brand_id']

        if brand_id == 10350:
            # 处理Tiffany
            url_dict = {}
            for item in list(filter(lambda val: val[0], results)):
                path = item[1]['path']
                full_path = os.path.normpath(os.path.join(self.store.basedir, path))
                url = item[1]['url']
                idx = url.find('?')
                if idx != -1:
                    url = url[:idx]
                try:
                    img = Image.open(full_path)
                except IOError:
                    continue

                # 两种情况会采用该result：尺寸更大，或第一次出现
                width = img.size[0]
                if url not in url_dict or width > url_dict[url]['size']:
                    url_dict[url] = {'size': width, 'item': item}

            return [val['item'] for val in url_dict.values()]
        else:
            return results

    def update_products_image(self, brand_id, model, checksum):
        """
        保证products_image的一致性
        :param brand_id:
        :param checksum:
        :param model:
        """
        rs = self.db.query_match('idproducts', 'products_image',
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
        根据新数据的hash值以及文件路径，更新数据库image_store和products_image
        :param data:
        """
        checksum = data['checksum']
        path = data['path']
        brand_id = data['brand_id']
        model = data['model']

        rs = self.db.query_match('checksum', 'image_store', {'checksum': checksum})
        checksum_anchor = rs.num_rows() > 0
        rs = self.db.query_match('checksum', 'image_store', {'path': path})
        path_anchor = rs.num_rows() > 0

        # if checksum_anchor and path_anchor:
        #     # checksum和path都已存在，image_store不用更新，仅确保products_image中存在
        #     self.update_products_image(brand_id, model,checksum)
        # elif checksum_anchor and not path_anchor:
        #     # checksum存在，path不存在。说明原path对应于一个错误的URL
        #     self.update_products_image(brand_id,model,checksum)

        if not checksum_anchor and path_anchor:
            # 说明原来的checksum有误，整体更正
            self.db.update({'checksum': checksum}, 'image_store', str.format('path="{0}"', path))
        elif not checksum_anchor and not path_anchor:
            # 在image_store里面新增一个item
            self.db.insert({'checksum': checksum, 'brand_id': brand_id, 'url': data['url'], 'path': data['path'],
                            'width': data['width'], 'height': data['height'], 'format': data['format'],
                            'size': data['size']}, 'image_store')

        self.update_products_image(brand_id, model, checksum)

    def item_completed(self, results, item, info):
        # Tiffany需要特殊处理。因为Tiffany的图片下载机制是：下载一批可能的图片，在下载成功的图片中，挑选分辨率最好的那个。
        results = self.preprocess(results, item)
        brand_id = item['metadata']['brand_id']
        model = cm.unicodify(item['metadata']['model'])
        for status, r in results:
            if not status:
                continue

            path = r['path']    #.replace(u'\\', u'/')
            path_db = os.path.normpath(
                os.path.join(unicode.format(u'{0}_{1}/{2}', brand_id, glob.BRAND_NAMES[brand_id]['brandname_s'], path)))
            full_path = os.path.normpath(os.path.join(self.store.basedir, path))
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
