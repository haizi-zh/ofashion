# coding=utf-8
import scrapy.contrib.spiders
from core import MySqlDb
from scrapy.http import Request
from scrapper.items import UpdateItem
import global_settings as glob

__author__ = 'Zephyre'


class UpdateSpider(scrapy.contrib.spiders.CrawlSpider):
    def __init__(self, brand_list, db_spec, *a, **kw):
        self.name = 'update'
        super(UpdateSpider, self).__init__(*a, **kw)
        self.brand_list = brand_list
        self.db = MySqlDb()
        self.db.conn(db_spec)

    def start_requests(self):
        # 如果未指定brand_list，则默认对所有的品牌进行更新
        # 获得所有的品牌数据
        if not self.brand_list:
            self.brand_list = glob.brand_info().keys()

        for brand in self.brand_list:
            # 获得该品牌下所有记录
            rs = self.db.query_match({'idproducts', 'url', 'region'}, 'products', {'brand_id': brand})
            products_map = {int(tmp['idproducts']): {'url': tmp['url'], 'region': tmp['region']} for tmp in
                            rs.fetch_row(maxrows=0, how=1)}
            for pid, data in products_map.items():
                url = data['url']
                region = data['region']
                yield Request(url=url, callback=self.parse, meta={'brand': brand, 'pid': pid, 'region': region})

    def parse(self, response):
        brand = response.meta['brand']
        item = UpdateItem()
        item['idproduct'] = response.meta['pid']
        item['brand'] = brand
        item['region'] = response.meta['region']
        sc = glob.spider_info()[brand]

        metadata = {}
        item['offline'] = 1 if getattr(sc, 'is_offline')(response) else 0

        if 'fetch_price' in dir(sc):
            ret = getattr(sc, 'fetch_price')(response)
            if 'price' in ret:
                metadata['price'] = ret['price']
            if 'price_discount' in ret:
                metadata['price_discount'] = ret['price_discount']

        item['metadata'] = metadata
        yield item