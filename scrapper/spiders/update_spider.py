# coding=utf-8
import scrapy.contrib.spiders
from scrapy import log
from core import MySqlDb
from scrapy.http import Request
from scrapper.items import UpdateItem
import global_settings as glob

__author__ = 'Zephyre'


class UpdateSpider(scrapy.contrib.spiders.CrawlSpider):
    handle_httpstatus_list = [404]

    def __init__(self, brand_list, region_list, db_spec, *a, **kw):
        self.name = 'update'
        super(UpdateSpider, self).__init__(*a, **kw)
        self.brand_list = brand_list
        self.region_list = region_list
        self.db = MySqlDb()
        self.db.conn(db_spec)

    def start_requests(self):
        # 如果未指定brand_list，则默认对所有的品牌进行更新
        # 获得所有的品牌数据
        if not self.brand_list:
            self.brand_list = glob.brand_info().keys()

        # UpdateSpider的可选区域参数
        region_cond = str.format('region IN ({0})',
                                 ','.join("'" + tmp + "'" for tmp in self.region_list)) if self.region_list else '1'

        rs = self.db.query(str.format('SELECT COUNT(*) FROM products WHERE brand_id IN ({0}) AND {1}',
                                      ','.join(str(tmp) for tmp in self.brand_list), region_cond))
        tot_num = int(rs.fetch_row()[0][0])
        self.log(str.format('Total number of records to update: {0}', tot_num), level=log.INFO)

        for brand in self.brand_list:
            # 获得该品牌下所有记录
            rs = self.db.query_match({'idproducts', 'url', 'region'}, 'products', {'brand_id': brand})
            products_map = {int(tmp['idproducts']): {'url': tmp['url'], 'region': tmp['region']} for tmp in
                            rs.fetch_row(maxrows=0, how=1)}
            for pid, data in products_map.items():
                url = data['url']
                region = data['region']

                # url = 'http://www.michaelkors.cn/catalog/women/handbags/totes/susannah-medium-shoulder-tote.html'
                # region = 'cn'
                # pid = 510556
                #
                # return [Request(url=url,
                #                 callback=self.parse,
                #                 meta={'brand': brand, 'pid': pid, 'region': region},
                #                 errback=self.onerror,
                #                 dont_filter=True)]

                yield Request(url=url,
                              callback=self.parse,
                              meta={'brand': brand, 'pid': pid, 'region': region},
                              errback=self.onerror,
                              dont_filter=True)

    def parse(self, response):
        brand = response.meta['brand']
        item = UpdateItem()
        item['idproduct'] = response.meta['pid']
        item['brand'] = brand
        item['region'] = response.meta['region']
        sc = glob.spider_info()[brand]
        metadata = {}
        item['metadata'] = metadata

        metadata['url'] = response.url

        if response.status < 200 or response.status > 300:
            item['offline'] = 1
            return item
        else:
            item['offline'] = 1 if getattr(sc, 'is_offline')(response) else 0
            if item['offline'] == 1:
                return item

        if 'fetch_price' in dir(sc):
            ret = getattr(sc, 'fetch_price')(response)
            if 'price' in ret:
                metadata['price'] = ret['price']
            if 'price_discount' in ret:
                metadata['price_discount'] = ret['price_discount']

        if 'fetch_name' in dir(sc):
            name = getattr(sc, 'fetch_name')(response)
            if name:
                metadata['name'] = name

        if 'fetch_model' in dir(sc):
            model = getattr(sc, 'fetch_model')(response)
            if model:
                metadata['model'] = model

        if 'fetch_description' in dir(sc):
            description = getattr(sc, 'fetch_description')(response)
            if description:
                metadata['description'] = description

        if 'fetch_details' in dir(sc):
            details = getattr(sc, 'fetch_details')(response)
            if details:
                metadata['details'] = details

        if 'fetch_color' in dir(sc):
            color = getattr(sc, 'fetch_color')(response)
            if color:
                metadata['color'] = color

        return item

    def onerror(self, reason):

        meta = None
        if hasattr(reason.value, 'response'):
            response = reason.value.response
            meta = response.meta
        else:
            meta = reason.request.meta

        if meta:
            brand = meta['brand']
            item = UpdateItem()
            item['idproduct'] = meta['pid']
            item['brand'] = brand
            item['region'] = meta['region']
            sc = glob.spider_info()[brand]
            metadata = {}
            item['metadata'] = metadata

            item['offline'] = 1

            return item
