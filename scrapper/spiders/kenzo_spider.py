# coding=utf-8
import json
import os
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


# TODO kenzo的数据不是用这个爬虫爬的，这个文件是后来覆盖过来的


class KenzoSpider(MFashionSpider):
    spider_data = {'brand_id': 10192,
                   'image_data': 'https://www.kenzo.com/en/services/product/',
                   'currency': {'us': 'EUR'},
                   'home_urls': {'us': 'https://www.kenzo.com/en'}}

    @classmethod
    def get_supported_regions(cls):
        return KenzoSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(KenzoSpider, self).__init__('kenzo', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"header-categories")]/ul/li//a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
                gender = cm.guess_gender(cat_name)
                if gender:
                    m['gender'] = [gender]
                yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})
            except (IndexError, TypeError):
                continue

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"header-level-3-inner")]/a[@href]'):
            href = node.xpath('@href').extract()[0]
            if href not in response.url:
                continue
            for node1 in node.xpath('../ul/li/a[@href]'):
                try:
                    cat_title = self.reformat(node1.xpath('text()').extract()[0])
                    cat_name = cat_title.lower()
                    m = copy.deepcopy(metadata)
                    m['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
                    url = self.process_href(node1.xpath('@href').extract()[0], response.url)
                    yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m})
                except (IndexError, TypeError):
                    continue

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[contains(@class,"line")]/li[contains(@class,"product-list-unit")]'
                              '//a[@href and @class="history"]'):
            m = copy.deepcopy(metadata)
            url = self.process_href(node.xpath('@href').extract()[0], response.url)
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        # if 'fetch_price' in dir(self.__class__):
        #     ret = getattr(self.__class__, 'fetch_price')(response)
        #     if 'price' in ret:
        #         metadata['price'] = ret['price']
        #     if 'price_discount' in ret:
        #         metadata['price_discount'] = ret['price_discount']

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        # tmp = sel.xpath('//div[contains(@class,"product-detail")]//*[contains(@class,"JS_price")]/text()').extract()
        # if tmp:
        #     metadata['price'] = self.reformat(tmp[0])
        # tmp = sel.xpath(
        #     '//div[contains(@class,"product-detail")]//*[contains(@class,"JS_specialprice")]/text()').extract()
        # if tmp:
        #     metadata['price_discount'] = self.reformat(tmp[0])

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        # if 'fetch_color' in dir(self.__class__):
        #     colors = getattr(self.__class__, 'fetch_color')(response)
        #     if colors:
        #         metadata['color'] = colors

        url = self.spider_data['image_data'] + str(model)
        m = copy.deepcopy(metadata)
        yield Request(url=url, callback=self.parse_image, errback=self.onerr, meta={'userdata': m})

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        # if image_urls:
        #     item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    def parse_image(self, response):
        metadata = response.meta['userdata']
        try:
            data = json.loads(response.body)
        except ValueError:
            return

        image_urls = []
        infos = self.get_infos_from_json(data)
        if 'image_urls' in infos:
            image_urls = infos['image_urls']
        if 'color' in infos:
            metadata['color'] = infos['color']
        if 'price' in infos:
            metadata['price'] = infos['price']
        if 'price_discount' in infos:
            metadata['price_discount'] = infos['price_discount']

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

    @classmethod
    def get_infos_from_json(cls, data):
        ret = {}

        image_urls = []
        color = []
        price = None
        price_discount = None

        for clr_item in data['data']['colors_list']:
            try:
                image_urls += [val['image_src'] for val in clr_item['images']]
                color += [cls.reformat(clr_item['name'])]
            except (IndexError, KeyError):
                continue
        prod_item = filter(lambda val: val['color_id'], data['data']['products'])
        tmp = prod_item[0]
        try:
            price = str(float(tmp['price']) / 100) if 'price' in tmp else None
        except (TypeError, ValueError):
            price = None
        try:
            price_discount = str(float(tmp['price_sale']) / 100) if 'price_sale' in tmp else None
        except (TypeError, ValueError):
            price_discount = None

        if image_urls:
            ret['image_urls'] = image_urls
        if color:
            ret['color'] = color
        if price:
            ret['price'] = price
        if price_discount:
            ret['price_discount'] = price_discount

        return ret

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        # 获取model
        mt = re.search(r'_(\d+)/?$', response.url)
        if mt:
            try:
                model = mt.group(1)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        model = cls.fetch_model(response)
        if model:
            url = cls.spider_data['image_data'] + str(model)
            return Request(url=url,
                           callback=cls.fetch_price_server,
                           errback=spider.onerror,
                           meta=response.meta)
        else:
            return ret

    @classmethod
    def fetch_price_server(cls, response):
        sel = Selector(response)
        ret = {}

        try:
            data = json.loads(response.body)
        except ValueError:
            return

        old_price = None
        new_price = None
        infos = cls.get_infos_from_json(data)
        if 'price' in infos:
            old_price = infos['price']
        if 'price_discount' in infos:
            new_price = infos['price_discount']

        if old_price:
            ret['price'] = str(old_price)
        if new_price:
            ret['price_discount'] = str(new_price)

        return ret

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        try:
            tmp = sel.xpath('//div[contains(@class,"product-detail")]/*[@class="font-title-product"]/text()').extract()
            if tmp:
                name = cls.reformat(tmp[0])
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        try:
            tmp = sel.xpath('//div[contains(@class,"in-desc")]/descendant-or-self::text()').extract()
            if tmp:
                description = '\r'.join(filter(lambda x: x, [cls.reformat(val) for val in tmp]))
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        model = cls.fetch_model(response)
        if model:
            url = cls.spider_data['image_data'] + str(model)
            return Request(url=url,
                           callback=cls.fetch_color_server,
                           errback=spider.onerror,
                           meta=response.meta)
        else:
            return None

    @classmethod
    def fetch_color_server(cls, response):
        sel = Selector(response)

        try:
            data = json.loads(response.body)
        except ValueError:
            return

        colors = []
        infos = cls.get_infos_from_json(data)
        if 'color' in infos:
            colors = infos['color']

        return colors
