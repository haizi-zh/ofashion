# coding=utf-8

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import datetime
import hashlib
from itertools import ifilter
import json
import os
import _mysql
import Image
from scrapy import log
from scrapy.contrib.pipeline.images import ImagesPipeline
from scrapy.exceptions import DropItem
from scrapy.http import Request
from scrapy.settings import Settings
import common as cm
from scrapper import utils


class BaiduPipeline(object):
    def __init__(self):
        self.timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def process_item(self, item, spider):
        return item


class ProductPipeline(object):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        db_spec = settings.get('DBSPEC')
        return cls(db_spec)

    def __init__(self, db_spec):
        self.db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                                 passwd=db_spec['password'], db=db_spec['schema'])
        self.db.query("SET NAMES 'utf8'")
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
                    self.db.query('LOCK TABLES products_tag_mapping WRITE')
                    self.db.query(
                        unicode.format(u'SELECT * FROM products_tag_mapping WHERE brand_id={0} AND region="{1}" '
                                       u'AND tag_type="{2}" AND tag_name="{3}"', brand_id, region, tag_type,
                                       tag_name).encode('utf-8'))
                    if len(self.db.store_result().fetch_row(maxrows=0, how=1)) == 0:
                        cm.insert_record(self.db,
                                         {'brand_id': brand_id, 'brandname_e': entry['brandname_e'], 'region': region,
                                          'tag_type': tag_type,
                                          'tag_name': tag_name, 'tag_text': tag_text},
                                         'products_tag_mapping', update_time=False, modified=False)
                    self.db.query('UNLOCK TABLES')

    def process_item(self, item, spider):
        metadata = item['metadata']

        # if ('name' not in metadata) or not metadata['name']:
        #     raise DropItem('Invalid item: ')

        entry = metadata.copy()
        extra = entry['extra']
        tags_mapping = entry['tags_mapping']
        del entry['tags_mapping']

        self.process_tags_mapping(tags_mapping, entry)

        self.db.query('LOCK TABLES products WRITE')    # 检查数据库
        self.db.query(unicode.format(u'SELECT * FROM products WHERE brand_id={0} AND model="{1}" AND region="{2}"',
                                     entry['brand_id'], entry['model'], entry['region']))
        results = self.db.store_result().fetch_row(maxrows=0, how=1)
        if len(results) == 0:
            entry['extra'] = json.dumps(extra, ensure_ascii=False)
            if 'color' in entry and entry['color']:
                entry['color'] = json.dumps(entry['color'], ensure_ascii=False)
            if 'category' in entry and entry['category']:
                entry['category'] = json.dumps(entry['category'], ensure_ascii=False)
            if 'gender' in entry and entry['gender']:
                entry['gender'] = json.dumps(entry['gender'], ensure_ascii=False)
            cm.insert_record(self.db, entry, 'products')
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

            cm.update_record(self.db, dest, 'products',
                             str.format('idproducts={0}', results[0]['idproducts']))
            spider.log(unicode.format(u'UPDATE: {0}', entry['model']), log.DEBUG)
        self.db.query('UNLOCK TABLES')
        return item


class ProductImagePipeline(ImagesPipeline):
    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        db_spec = settings.get('DBSPEC')
        image_store = settings.get('IMAGES_STORE')
        ProductImagePipeline.DBSPEC = db_spec
        return cls(image_store, crawler, db_spec)

    def __init__(self, store_uri, crawler=None, db_spec=None):
        self.crawler = crawler
        self.url_map = {}
        self.db = _mysql.connect(host=db_spec['host'], port=db_spec['port'], user=db_spec['username'],
                                 passwd=db_spec['password'], db=db_spec['schema'])
        self.db.query("SET NAMES 'utf8'")
        super(ProductImagePipeline, self).__init__(store_uri)

    def image_key(self, url):
        val = super(ProductImagePipeline, self).image_key(url)
        m = self.url_map[url]
        p, fn = os.path.split(val)
        fn = unicode.format(u'{0}_{1}', m['model'], fn)
        return os.path.join(p, unicode.format(u'{0}_{1}', m['brand_id'], cm.simplify_brand_name(m['brandname_e'])), fn)

    def thumb_key(self, url, thumb_id):
        val = super(ProductImagePipeline, self).thumb_key(url, thumb_id)
        m = self.url_map[url]
        p, fn = os.path.split(val)
        fn = unicode.format(u'{0}_{1}', m['model'], fn)
        return os.path.join(p, unicode.format(u'{0}_{1}', m['brand_id'], m['brandname_e']), fn)

    def get_media_requests(self, item, info):
        m = item['metadata']
        for url in item['image_urls']:
            self.url_map[url] = {'brand_id': m['brand_id'], 'brandname_e': m['brandname_e'], 'model': m['model']}
            yield Request(url)

    def item_completed(self, results, item, info):
        for status, r in results:
            if not status:
                continue

            path = r['path'].replace(u'\\', u'/')
            url = r['url']
            m = self.url_map[url]
            del self.url_map[url]
            img = Image.open(os.path.join(self.store.basedir, path))
            self.db.query('LOCK TABLES products_image WRITE')    # 检查数据库
            self.db.query(unicode.format(u'SELECT * FROM products_image WHERE path="{0}"', path))
            if len(self.db.store_result().fetch_row(maxrows=0)) == 0:
                cm.insert_record(self.db, {'model': m['model'], 'url': url, 'path': path, 'width': img.size[0],
                                           'height': img.size[1], 'format': img.format, 'brand_id': m['brand_id'],
                                           'fetch_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                                 'products_image')
            self.db.query('UNLOCK TABLES')
        return item
