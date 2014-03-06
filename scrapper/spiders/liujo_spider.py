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


class LiujoSpider(MFashionSpider):
    spider_data = {'brand_id': 10218,

                   'home_urls': {
                       region: str.format('http://www.liujo.com/{0}/shop.html', region if region != 'uk' else 'gb') for
                       region in {'uk', 'de', 'at', 'es', 'nl', 'pl', 'ru', 'fr', 'it', 'be'}}}

    @classmethod
    def get_supported_regions(cls):
        return LiujoSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LiujoSpider, self).__init__('liujo', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[contains(@class,"main-subcategories-list")]/li[contains(@class,"list-elem")]'):
            try:
                tmp = node.xpath('./a[@href]/text()').extract()
                cat_title = self.reformat(tmp[0])
                cat_name = cat_title.lower()
                tmp = node.xpath('./a[@href]/@href').extract()
                url = self.process_href(tmp[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[contains(@class,"main-subcategories-list")]/li[contains(@class,"list-elem")]'):
            try:
                tmp = node.xpath('./a[@href]/text()').extract()
                cat_title = self.reformat(tmp[0])
                cat_name = cat_title.lower()
                tmp = node.xpath('./a[@href]/@href').extract()
                url = self.process_href(tmp[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[contains(@class,"products-grid")]/li[contains(@class,"product-item")]'):
            try:
                tmp = node.xpath('.//div[@class="infos"]/*[@class="product-name"]/a[@href and @title]/@href').extract()
                url = self.process_href(tmp[0], response.url)
                tmp = node.xpath('.//div[@class="infos"]/*[@class="product-name"]/a[@href and @title]/@title').extract()
                name = self.reformat(tmp[0]) if tmp else None
                tmp = node.xpath('.//div[@class="price-box"]/*[@class="regular-price" or @class="old-price"]'
                                 '/*[@class="price"]/text()').extract()
                price = self.reformat(tmp[0]) if tmp else None
                tmp = node.xpath('.//div[@class="price-box"]/*[@class="special-price"]'
                                 '/*[@class="price"]/text()').extract()
                price_discount = self.reformat(tmp[0]) if tmp else None
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            if name:
                m['name'] = name
            if price:
                m['price'] = price
            if price_discount:
                m['price_discount'] = price_discount
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

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        image_urls = [self.process_href(val, response.url) for val in
                      sel.xpath('//a[@data-image-fullscreen]/@data-image-fullscreen').extract()]
        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

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
        try:
            tmp = sel.xpath('//div[@class="product-name"]/span[@class="product-ids"]/text()').extract()[0]
            if tmp:
                model = cls.reformat(tmp)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            tmp = sel.xpath('.//div[@class="price-box"]/*[@class="regular-price" or @class="old-price"]'
                            '/*[@class="price"]/text()').extract()
            old_price = cls.reformat(tmp[0]) if tmp else None
            tmp = sel.xpath('.//div[@class="price-box"]/*[@class="special-price"]'
                            '/*[@class="price"]/text()').extract()
            new_price = cls.reformat(tmp[0]) if tmp else None
        except (IndexError, TypeError):
            pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        try:
            tmp = sel.xpath('//div[@class="product-name"]/*[@itemprop="name"]/text()').extract()
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
            tmp = sel.xpath('//div[@itemprop="description"]/text()').extract()
            if tmp:
                description = '\r'.join(filter(lambda x: x, [cls.reformat(val) for val in tmp]))
        except(TypeError, IndexError):
            pass

        return description
