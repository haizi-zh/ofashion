# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class FolliFollieSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10138,
        'home_urls': {
            'cn': 'http://www.follifollie.com.cn/ch-ch',
            'us': 'http://www.follifollie.us.com/us-en',
            'uk': 'http://www.follifollie.co.uk/gb-en',
            'gr': 'http://www.follifollie.gr/gr-el',
            'es': 'http://www.follifollie.es/sp-sp',
            'hk': 'http://www.follifollie.com.hk/hk-ch',
            'jp': 'http://www.follifollie.co.jp/jp-jp',
            'tw': 'http://www.follifollie.com.tw/ch-tw',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(FolliFollieSpider, self).__init__('folli_follie', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="header"]//table[@class="main"]//td/a[@href][text()]')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = nav_node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                try:
                    href = nav_node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_sub_nav,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_sub_nav(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        sub_nav_nodes = sel.xpath('//*[@id="content"]/div[@class="main-slider"]/div[@class="list"]/div/ul/li/a[@href][text()]')
        for sub_nav_node in sub_nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = sub_nav_node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                try:
                    href = sub_nav_node.xpath('./@href').extract()[0]
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

        product_nodes = sel.xpath('//*[@id="gift-finder-main"]/ul/li[child::a]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        next_page_node = sel.xpath('//*[@id="paginationTop"]/ul/li[last()]/a[@href]')
        if next_page_node:
            m = copy.deepcopy(metadata)

            try:
                next_href = next_page_node.xpath('./@href').extract()[0]
                next_href = self.process_href(next_href, response.url)

                yield Request(url=next_href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})
            except(TypeError, IndexError):
                pass

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        other_nodes = sel.xpath('//div[@class="product-container"]//div[@class="prod-options"]/div[@class="colors"]/ul/li/a[@href]')
        for node in other_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

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

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        image_urls = None
        image_node = sel.xpath('//link[@rel="image_src"][@href]')
        if image_node:
            try:
                image_href = image_node.xpath('./@href').extract()[0]
                image_href = re.sub(r'_\d+x\d+\$', '', image_href)

                image_urls = [image_href]
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
    def is_offline(cls, response):
        return not cls.fetch_model(response)

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        model = None
        try:
            model_node = sel.xpath('//div[@class="product-container"]//div[@class="prod-descr-wrap"]//p[@class="code"][text()]')
            if model_node:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = cls.reformat(model_text)
                if model_text:
                    mt = re.search(r'(\w+)$', model_text)
                    if mt:
                        model = mt.group(1)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@class="product-container"]//div[@class="product-inner"]//div[@class="prod-options"]/div[@id="prices"]')
        if price_node:
            old_price_node = price_node.xpath('./div[@class="price-offer"]/span[@class="strike"][text()]')
            if old_price_node:
                try:
                    old_price = old_price_node.xpath('./text()').extract()[0]
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass

                try:
                    new_price = price_node.xpath('./div[@class="price"]/span[text()]/text()').extract()[0]
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
            else:
                try:
                    old_price = price_node.xpath('./div[@class="price"]/span[text()]/text()').extract()[0]
                    old_price = cls.reformat(old_price)
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
        name_node = sel.xpath('//div[@class="product-container"]//div[@class="product-inner"]/div[@class="right"]/h2[text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@class="product-container"]//div[@class="right"]/div[@class="prod-descr-wrap"]/ul/li[1]/div[@class="cnt"]/div[@class="in"][text()]')
        if description_node:
            try:
                description = '\r'.join(
                    cls.reformat(val)
                    for val in description_node.xpath('./text()').extract()
                )
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        detail = None
        detail_node = sel.xpath('//div[@class="product-container"]//div[@class="right"]/div[@class="prod-descr-wrap"]/ul/li[2]/div[@class="cnt"]/div[@class="in"][text()]')
        if detail_node:
            try:
                detail = '\r'.join(
                    cls.reformat(val)
                    for val in detail_node.xpath('./text()').extract()
                )
                detail = cls.reformat(detail)
            except(TypeError, IndexError):
                pass

        return detail

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)

        colors = None
        color_node = sel.xpath('//div[@class="product-container"]//div[@class="prod-options"]/div[@class="colors"]/ul/li[contains(@class, "active")]//div[@class="inner"][text()]')
        if color_node:
            try:
                colors = [cls.reformat(val)
                          for val in color_node.xpath('./text()').extract()]
            except(TypeError, IndexError):
                pass

        return colors
