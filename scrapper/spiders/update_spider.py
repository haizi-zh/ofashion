# coding=utf-8
import scrapy.contrib.spiders
from scrapy import log
from core import RoseVisionDb
from scrapy.http import Request
from scrapper.items import UpdateItem
import global_settings as glob
from scrapper.spiders.mfashion_spider import MFashionBaseSpider
from utils import info

__author__ = 'Zephyre'


# class UpdateSpider(scrapy.contrib.spiders.CrawlSpider):
class UpdateSpider(MFashionBaseSpider):
    handle_httpstatus_list = [404]

    def __init__(self, brand_list, region_list, db_spec, *a, **kw):
        self.name = str.format('update-{0}-{1}', '-'.join(str(tmp) for tmp in brand_list) if brand_list else 'all',
                               '-'.join(region_list) if region_list else 'all')
        super(UpdateSpider, self).__init__(*a, **kw)
        self.brand_list = brand_list
        self.region_list = region_list
        self.db = RoseVisionDb()
        self.db.conn(db_spec)

    def start_requests(self):
        # 如果未指定brand_list，则默认对所有的品牌进行更新
        # 获得所有的品牌数据
        if not self.brand_list:
            self.brand_list = info.brand_info().keys()

        # UpdateSpider的可选区域参数
        region_cond = str.format('region IN ({0})',
                                 ','.join("'" + tmp + "'" for tmp in self.region_list)) if self.region_list else '1'

        rs = self.db.query(str.format('SELECT COUNT(*) FROM products WHERE brand_id IN ({0}) AND {1} AND offline!=1',
                                      ','.join(str(tmp) for tmp in self.brand_list), region_cond))
        tot_num = int(rs.fetch_row()[0][0])
        self.log(str.format('Total number of records to update: {0}', tot_num), level=log.INFO)

        for brand in self.brand_list:
            # 获得该品牌下所有记录
            # 如果未指定region_list，则默认对所有的的确进行更新
            if self.region_list:
                region_list = self.region_list
            else:
                rs = self.db.query(str.format('SELECT DISTINCT region FROM products WHERE brand_id={0}', brand))
                region_list = [tmp['region'] for tmp in rs.fetch_row(maxrows=0, how=1)]

            region_info = info.region_info()
            region_list = filter(lambda val: int(region_info[val]['status']), region_list)

            for region in region_list:
                rs = self.db.query_match({'idproducts', 'url', 'region', 'model'}, 'products',
                                         {'brand_id': brand, 'region': region}, extra='offline!=1')
                products_map = {
                    int(tmp['idproducts']): {'url': tmp['url'], 'region': tmp['region'], 'model': tmp['model']} for tmp
                    in
                    rs.fetch_row(maxrows=0, how=1)}
                for pid, data in products_map.items():
                    url = data['url']
                    region = data['region']
                    model = data['model']

                    url = 'http://www.gucci.com/us/styles/3085353G0109060'
                    region = 'us'
                    pid = 196907

                    return [Request(url=url,
                                    callback=self.parse,
                                    meta={'brand': brand, 'pid': pid, 'region': region},
                                    errback=self.onerror,
                                    dont_filter=True)]
                    # if url:
                    #     try:
                    #         yield Request(url=url,
                    #                       callback=self.parse,
                    #                       meta={'brand': brand, 'pid': pid, 'region': region, 'model': model},
                    #                       errback=self.onerror,
                    #                       dont_filter=True)
                    #     except TypeError:
                    #         continue
                    # else:
                    #     continue

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
            item['offline'] = 1 if getattr(sc, 'is_offline')(response, self) else 0
            if item['offline'] == 1:
                return item

        if 'fetch_price' in dir(sc):
            ret = getattr(sc, 'fetch_price')(response, self)
            if isinstance(ret, Request):
                metadata['price'] = ret
            else:
                if 'price' in ret:
                    metadata['price'] = ret['price']
                if 'price_discount' in ret:
                    metadata['price_discount'] = ret['price_discount']

        if 'fetch_name' in dir(sc):
            name = getattr(sc, 'fetch_name')(response, self)
            if name:
                metadata['name'] = name

        if 'fetch_model' in dir(sc):
            model = getattr(sc, 'fetch_model')(response, self)
            if model:
                metadata['model'] = model

        if 'fetch_description' in dir(sc):
            description = getattr(sc, 'fetch_description')(response, self)
            if description:
                metadata['description'] = description

        if 'fetch_details' in dir(sc):
            details = getattr(sc, 'fetch_details')(response, self)
            if details:
                metadata['details'] = details

        if 'fetch_color' in dir(sc):
            color = getattr(sc, 'fetch_color')(response, self)
            if color:
                metadata['color'] = color

        return self.resolve_requests(item)

    def update_callback(self, response):
        spider_cb = response.meta['spider_cb']
        item = response.meta['item']
        key = response.meta['key']
        ret = spider_cb(response)
        if ret:
            if key == 'price':
                for tmp in ('price', 'price_discount'):
                    if tmp in ret:
                        item['metadata'][tmp] = ret[tmp]
                    else:
                        item['metadata'].pop(tmp)
            else:
                item['metadata'][key] = ret
        else:
            # 如果返回值为None，说明没有相对应的信息。
            if key == 'price':
                for tmp in ('price', 'price_discount'):
                    if tmp in item['metadata']:
                        item['metadata'].pop(tmp)
            else:
                item['metadata'].pop(key)
        return self.resolve_requests(item)

    def resolve_requests(self, item):
        """
        检查metadata里面是否有Request对象。如果有，需要进一步提交这些request。
        仅当所有的对象都不是Request时，该过程才结束。
        应该注意的是，metadata中的键只会处理一次。
        @param metadata:
        """
        metadata = item['metadata']
        while True:
            resolved = True
            for func_key, value in metadata.items():
                if isinstance(value, Request):
                    value.meta['spider_cb'] = value.callback
                    value.meta['item'] = item
                    value.meta['key'] = func_key
                    value.callback = self.update_callback
                    value.dont_filter = True
                    return value

            if resolved:
                break

        return item


    @staticmethod
    def onerror(reason):
        meta = None
        try:
            if hasattr(reason.value, 'response'):
                response = reason.value.response
                # 这里response可能为None，比如出现
                # ERROR: Error downloading <GET xxx>:
                # [<twisted.python.failure.Failure <class 'twisted.internet.error.ConnectionDone'>>]
                if response:
                    meta = response.meta
        except AttributeError:
            pass
        try:
            if not meta:
                meta = reason.request.meta
        except AttributeError:
            pass

        if meta:
            brand = meta['brand']
            item = UpdateItem()
            item['idproduct'] = meta['pid']
            item['brand'] = brand
            item['region'] = meta['region']
            # sc = glob.spider_info()[brand]
            metadata = {}
            item['metadata'] = metadata

            item['offline'] = 1

            return item
