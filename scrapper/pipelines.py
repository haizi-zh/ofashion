# coding=utf-8

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import hashlib
import json
import os
import _mysql
from scrapy import log
from scrapy.contrib.pipeline.images import ImagesPipeline
from scrapy.http import Request
import common as cm
from core import MySqlDb
from scrapper import utils
from PIL import Image
import global_settings as glob


images_store_map = {10152: '10152_gucci', 10074: '10074_chanel'}

spider_data_map = {}


def fetch_spider_data(brand_id):
    if brand_id in spider_data_map:
        return spider_data_map[brand_id]
    else:
        cm.get_spider_module()
        pass
    pass


class ProductPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        db_spec = settings['SPIDER_SPEC']
        return cls(db_spec)

    def __init__(self, db_spec):
        self.db = MySqlDb()
        self.db.conn(db_spec)
        self.processed_tags = set([])

    def process_tags_mapping(self, tags, entry):
        """
        如果有新的tag，加入到mapping列表中
        :param entry:
        """

        def get_tag_sig(brand_id, region, tag_type, tag_name):
            m = hashlib.md5()
            m.update(u'|'.join([unicode(brand_id), region, tag_type, tag_name]).encode('utf-8'))
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

                sig = get_tag_sig(brand_id, region, tag_type, tag_name)
                if sig in self.processed_tags:
                    continue
                else:
                    # 获得新的tag映射
                    self.processed_tags.add(sig)
                    self.db.start_transaction()
                    try:
                        rs = self.db.query(
                            unicode.format(u'SELECT * FROM products_tag_mapping WHERE brand_id={0} AND region="{1}" '
                                           u'AND tag_type="{2}" AND tag_name="{3}"', brand_id, region, tag_type,
                                           tag_name)).fetch_row(maxrows=0, how=1)
                        if not rs:
                            self.db.insert({'brand_id': brand_id,
                                            'region': region,
                                            'tag_type': tag_type,
                                            'tag_name': tag_name, 'tag_text': tag_text}, 'products_tag_mapping')
                        self.db.commit()
                    except:
                        self.db.rollback()
                        raise

    def process_item(self, item, spider):
        metadata = item['metadata']

        # if ('name' not in metadata) or not metadata['name']:
        #     raise DropItem('Invalid item: ')

        entry = metadata.copy()
        extra = entry['extra']
        tags_mapping = entry.pop('tags_mapping')

        self.process_tags_mapping(tags_mapping, entry)

        self.db.start_transaction()
        try:
            results = self.db.query(
                unicode.format(u'SELECT * FROM products WHERE brand_id={0} AND model="{1}" AND region="{2}"',
                               entry['brand_id'], entry['model'], entry['region']).encode('utf-8')).fetch_row(maxrows=0,
                                                                                                              how=1)
            if not results:
                entry['extra'] = json.dumps(extra, ensure_ascii=False)
                if 'color' in entry and entry['color']:
                    entry['color'] = json.dumps(entry['color'], ensure_ascii=False)
                if 'category' in entry and entry['category']:
                    entry['category'] = json.dumps(entry['category'], ensure_ascii=False)
                if 'gender' in entry and entry['gender']:
                    entry['gender'] = json.dumps(entry['gender'], ensure_ascii=False)
                if 'texture' in entry and entry['texture']:
                    entry['texture'] = json.dumps(entry['texture'], ensure_ascii=False)

                self.db.insert(entry, 'products', ['touch_time', 'fetch_time', 'update_time'])
                spider.log(unicode.format(u'INSERT: {0}', entry['model']), log.DEBUG)
            else:
                # 需要处理合并的字段
                merge_keys = ('gender', 'category', 'color', 'texture')
                dest = dict(
                    (k, json.loads(results[0][k])) for k in merge_keys if results[0][k])
                src = dict((k, entry[k]) for k in merge_keys if k in entry)
                dest = utils.product_tags_merge(src, dest)

                s2 = json.loads(results[0]['extra'])
                dest['extra'] = utils.product_tags_merge(s2, extra)
                dest = dict((k, json.dumps(dest[k], ensure_ascii=False)) for k in merge_keys + ('extra',) for k in dest)

                # 处理product中其它字段（覆盖现有记录）
                skip_keys = merge_keys + ('model', 'region', 'brand_id', 'extra', 'fetch_time')
                for k in entry:
                    if k in skip_keys:
                        continue
                    dest[k] = entry[k]

                # 检查是否有改变
                modified = False
                for k in dest:
                    if cm.unicodify(results[0][k]) != cm.unicodify(dest[k]):
                        modified = True
                        break
                if modified:
                    self.db.update(dest, 'products', str.format('idproducts={0}', results[0]['idproducts']),
                                   ['update_time', 'touch_time'])
                    spider.log(unicode.format(u'UPDATE: {0}', entry['model']), log.DEBUG)
                else:
                    self.db.update({}, 'products', str.format('idproducts={0}', results[0]['idproducts']),
                                       ['touch_time'])
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
    #     m = self.url_map.pop(url)
    #     p, fn = os.path.split(val)
    #     fn = unicode.format(u'{0}_{1}', m['model'], fn)
    #     return os.path.join(p, unicode.format(u'{0}_{1}', m['brand_id'], cm.simplify_brand_name(m['brandname_e'])), fn)

    # def thumb_key(self, url, thumb_id):
    #     val = super(ProductImagePipeline, self).thumb_key(url, thumb_id)
    #     m = self.url_map[url]
    #     p, fn = os.path.split(val)
    #     fn = unicode.format(u'{0}_{1}', m['model'], fn)
    #     return os.path.join(p, unicode.format(u'{0}_{1}', m['brand_id'], m['brandname_e']), fn)

    def get_media_requests(self, item, info):
        if 'image_urls' in item:
            for url in item['image_urls']:
                # self.url_map[url] = {'brand_id': m['brand_id'], 'brandname_e': m['brandname_e'], 'model': m['model']}
                yield Request(url)

    def item_completed(self, results, item, info):
        brand_id = item['metadata']['brand_id']
        spider_data = glob.BRAND_NAMES[brand_id]
        for status, r in results:
            if not status:
                continue

            path = r['path'].replace(u'\\', u'/')
            path_db = os.path.normpath(
                os.path.join(str.format('{0}_{1}/{2}', brand_id, spider_data['brandname_s'], path)))
            full_path = os.path.normpath(os.path.join(self.store.basedir, path))
            md5 = hashlib.md5()
            with open(full_path, 'rb') as f:
                buf = f.read()
                md5.update(buf)
            checksum = md5.hexdigest()

            self.db.lock(['products_image'])
            self.db.start_transaction()
            try:
                # If the file already exists
                rs = self.db.query(
                    str.format('SELECT path,width,height,format,url FROM products_image WHERE checksum="{0}"',
                               checksum)).fetch_row(how=1)
                if rs:
                    path_db = cm.unicodify(rs[0]['path'])
                    width = rs[0]['width']
                    height = rs[0]['height']
                    fmt = rs[0]['format']
                    url = rs[0]['url']
                else:
                    img = Image.open(full_path)
                    width, height = img.size
                    fmt = img.format
                    url = r['url']

                m = item['metadata']
                rs = self.db.query(
                    unicode.format(u'SELECT * FROM products_image WHERE path="{0}" AND model="{1}"', path_db,
                                   m['model'])).fetch_row(maxrows=0)
                if not rs:
                    self.db.insert(
                        {'model': m['model'], 'url': url, 'path': path_db, 'width': width, 'checksum': checksum,
                         'height': height, 'format': fmt, 'brand_id': m['brand_id']}, 'products_image')
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise
            finally:
                self.db.unlock()

        return item
