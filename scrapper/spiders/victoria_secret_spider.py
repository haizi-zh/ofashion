# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem

import common
import copy
import re

class VictoriaSecretSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10376,
        'home_urls': {
            'us': 'http://www.victoriassecret.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(VictoriaSecretSpider, self).__init__('victoria secret', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//nav[@id="nav-primary"]/ul/li/span/a[@href][text()]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text}
                ]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_cat,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_cat(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        cat_nodes = sel.xpath('//nav[@id="nav-secondary"]/ul/li/ul/li//a[@href][text()]')
        for node in cat_nodes:
            try:
                tag_text = node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text}
                ]

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

        product_nodes = sel.xpath('//section[@id="collection-set"]/ul/li[child::a[@href]]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

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
        if 'price_disount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = []
        image_nodes = sel.xpath('//section[@id="content"]//div[@class="product-image-group"]/ul/li/img[@src]')
        for image_node in image_nodes:
            try:
                image_src = image_node.xpath('./@src').extract()[0]
                image_src = self.process_href(image_src, response.url)
                if image_src:
                    src = re.sub(ur'/\d+x\d+/', ur'/760x1013/', image_src)
                    if src:
                        image_urls += [src]
            except(TypeError, IndexError):
                continue

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
        model_node = sel.xpath('//ul[@class="pdp-info box primary"]//div[@class="more"][text()]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//ul[@class="pdp-info box primary"]//hgroup[@itemprop="name"]/h1[text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
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
        price_node = sel.xpath('//section[@id="content"]//div[@class="price"]/p[text()]')
        if price_node:
            try:
                old_price = price_node.xpath('./text()').extract()[0]
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

            discount_node = sel.xpath('//section[@id="content"]//div[@class="price"]/p/em[text()]')
            if discount_node:
                try:
                    new_price = discount_node.xpath('./text()').extract()[0]
                    new_price = cls.reformat(new_price)
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

        description = None
        description_node = sel.xpath('//ul[@class="pdp-info box primary"]//div[@itemprop="description"]/div[@class="full trunc-on"][text()]')
        if description_node:
            try:
                description = '\r'.join(cls.reformat(val) for val in description_node.xpath('.//text()').extract())
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        color = []
        color_nodes = sel.xpath('//ul[@class="pdp-info box primary"]//section[@data-common-name="color"]//label[child::img][child::span[text()]]')
        for color_node in color_nodes:
            try:
                color_text = color_node.xpath('./span/text()').extract()[0]
                color_text = cls.reformat(color_text)
                color_text = color_text.lower()
                if color_text:
                    color += [color_text]
            except(TypeError, IndexError):
                pass

        return color
