# coding=utf-8
import json
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


# TODO 抽查到货号386.61.h42 没有官网链接。


class LoeweSpider(MFashionSpider):
    spider_data = {'brand_id': 10220,
                   'model_term': {'cn': ur'型号\s*:\s*', 'us': r'Model ID\s*:\s*', 'jp': r'Model ID\s*:\s*'},
                   'home_urls': {'cn': 'http://www.loewe.com/cn_zh_hans',
                                 'jp': 'http://www.loewe.com/jp_ja/',
                                 'us': 'http://www.loewe.com/us_en'}}

    @classmethod
    def get_supported_regions(cls):
        return LoeweSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LoeweSpider, self).__init__('loewe', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul/li[@class="menu_justified"]/a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@class="dispatchIndex"]/li/a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        menu_sels = sel.xpath('//div[@id="menuSelects"]/ul/li')
        if not menu_sels:
            return
        for node in menu_sels[0].xpath('.//ul/li/a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url) + '&context=carousel'
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-2'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(url=url, callback=self.parse_ajax, errback=self.onerr, meta={'userdata': m},
                          headers={'X-Requested-With': 'XMLHttpRequest'})

    def parse_ajax(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@id="carouselWrapper"]/li[@class="slide"]/a[@href]'):
            m = copy.deepcopy(metadata)
            tmp = node.xpath('..//*[@class="product-name"]/text()').extract()
            if tmp:
                m['name'] = self.reformat(tmp[0])
            url = self.process_href(node.xpath('@href').extract()[0], response.url)
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        try:
            sel = Selector(response)
        except AttributeError:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        image_urls = [self.process_href(val, response.url) for val in
                      sel.xpath('//div[@id="product"]//ul[@class="views"]/li/a[@href]/@href').extract()]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

    @classmethod
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        try:
            model_term = cls.spider_data['model_term'][region]
            model_list = filter(lambda val: re.search(model_term, val, flags=re.IGNORECASE),
                                sel.xpath('//*/text()').extract())
            if model_list:
                model = cls.reformat(re.sub(model_term, '', model_list[0], flags=re.IGNORECASE))
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            tmp = sel.xpath('//*[@class="regular-price"]/*[@class="price"]/text()').extract()
            price = cls.reformat(tmp[0]) if tmp else None
            if price:
                old_price = price
        except(TypeError, IndexError):
            pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        try:
            tmp = sel.xpath('//div[@id="aside"]//*[@class="product-name"]/text()').extract()
            if tmp:
                name = cls.reformat(tmp[0])
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            tmp = sel.xpath('//div[@id="tab1"]/descendant-or-self::text()').extract()
            if tmp:
                description = '\r'.join(filter(lambda x: x, [cls.reformat(val) for val in tmp]))
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        details = None
        try:
            tmp = sel.xpath('//div[@id="tab2"]/descendant-or-self::text()').extract()
            if tmp:
                details = '\r'.join(filter(lambda x: x, [cls.reformat(val) for val in tmp]))
        except(TypeError, IndexError):
            pass

        return details
