# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class BvlgariSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10058,
        'home_urls': {
            'us': 'http://us.bulgari.com/gift-guide/'
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(BvlgariSpider, self).__init__('bvlgari', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//section[@class="main-content"]/aside/ul/li/a[@href]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('.//strong[text()]/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text}
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

        nav_nodes = sel.xpath('//div[@class="gift-guide-slider-container"]//ul/li/a[@href]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('.//em[text()]/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text}
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="scroller"]//section[@class="product-details clearfix"]/figure/a[@href]')
        for node in product_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        image_urls = []
        image_nodes = sel.xpath('//div[@id="zoomImage"]/img[@id="zoomImage_Img"][@src]')
        if image_nodes:
            try:
                image_urls = [self.process_href(val, response.url) for val in image_nodes.xpath('./@src').extract()]
            except(TypeError, IndexError):
                pass

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//input[@id="goog_prodId"][@value]')
        if model_node:
            try:
                model = model_node.xpath('./@value').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        # 这个最终页面是没有名字的，这个desc是一个简短的，这里把它作为名字了
        name_node = sel.xpath('//input[@id="pinitdesc"][@value]')
        if name_node:
            try:
                name = name_node.xpath('./@value').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//input[@id="goog_price"][@value]')
        if price_node:
            try:
                old_price = price_node.xpath('./@value').extract()[0]
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = ''
        description_node = sel.xpath('//div[@id="DetailPane"]/div[@id="Description"]//span[contains(@id, "span_")]')
        if description_node:
            # 这里处理 document.write 的形式，并且保持原来网上的样子
            for val in description_node.xpath('.//text()').extract():
                try:
                    mt = re.search(ur'document.write\((\S+)\)', val)
                    if mt:
                        description += mt.group(1)
                        description += ':'
                    else:
                        description += cls.reformat(val)
                        description += '\r'
                except(TypeError, IndexError):
                    continue
            description = cls.reformat(description)

        return description
