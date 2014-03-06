# coding=utf-8


__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re


class LanvinSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10212,
        'currency': {
            'se': 'EUR',
            'dk': 'EUR',
        },
        'home_urls': {
            'us': 'http://www.lanvin.com/e-lanvin/US/',
            'uk': 'http://www.lanvin.com/e-lanvin/UK/',
            'se': 'http://www.lanvin.com/e-lanvin/SE/',
            'es': 'http://www.lanvin.com/e-lanvin/ES/',
            'nl': 'http://www.lanvin.com/e-lanvin/NL/',
            'lu': 'http://www.lanvin.com/e-lanvin/LU/',
            'it': 'http://www.lanvin.com/e-lanvin/IT/',
            'ie': 'http://www.lanvin.com/e-lanvin/IE/',
            'hk': 'http://www.lanvin.com/e-lanvin/HK-EN/',
            'de': 'http://www.lanvin.com/e-lanvin/DE/',
            'fr': 'http://www.lanvin.com/e-lanvin/FR-EN/',
            'fi': 'http://www.lanvin.com/e-lanvin/FI/',
            'dk': 'http://www.lanvin.com/e-lanvin/DK/',
            'be': 'http://www.lanvin.com/e-lanvin/BE/',
            'at': 'http://www.lanvin.com/e-lanvin/AT/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LanvinSpider, self).__init__('lanvin', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="header"]//ul[@id="nav"]/li[child::a[@href][text()]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a[text()]/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./ul/li[child::a[@href][text()]]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text, },
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./a[@href]/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

                try:
                    href = node.xpath('./a[@href]/@href').extract()[0]
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

        product_nodes = sel.xpath(
            '//div[@id="content"]/div[@class="col-main"]/div[@class="category-products"]/ul/li[child::a[@href]]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./a[@href][last()]/@href').extract()[0]
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
            # 这里检查有没有more infomation的链接
            more_node = sel.xpath('//div[@class="product-info"]//a[@class="tl-more"][@href]')
            if more_node:
                try:
                    more_href = more_node.xpath('./@href').extract()[0]
                    more_href = self.process_href(more_href, response.url)
                except(TypeError, IndexError):
                    return

                yield Request(url=more_href,
                              callback=self.parse_product,
                              errback=self.onerr,
                              meta={'userdata': metadata})

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

        details = self.fetch_details(response)
        if details:
            metadata['details'] = details

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = None
        image_list = re.findall(ur'"(\S*)"\); return true;', response.body)
        if image_list:
            try:
                image_urls = [
                    self.process_href(val, response.url)
                    for val in image_list
                ]
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
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath(
            '//*[@id="product_addtocart_form"]/div[@class="product-info"]/h3[@class="product-cat"][text()]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        price = None
        price_node = sel.xpath(
            '//*[@id="product_addtocart_form"]/div[@class="product-info"]//span[@class="price"][text()]')
        if price_node:
            try:
                price = price_node.xpath('./text()').extract()[0]
                price = cls.reformat(price)
            except(TypeError, IndexError):
                pass

        if price:
            ret['price'] = price

        return ret

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath(
            '//*[@id="product_addtocart_form"]/div[@class="product-info"]/h2[@class="product-name"][text()]')
        if name_node:
            try:
                name = ' '.join(
                    cls.reformat(val)
                    for val in name_node.xpath('./text()').extract()
                )
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@id="pp-details"]/p[text()]')
        if description_node:
            try:
                description = description_node.xpath('./text()').extract()[0]
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        details = None
        detail_nodes = sel.xpath('//div[@id="pp-details"]/ul/li[text()]')
        if detail_nodes:
            try:
                details = '\r'.join(
                    cls.reformat(val)
                    for val in detail_nodes.xpath('./text()').extract()
                )
                details = cls.reformat(details)
            except(TypeError, IndexError):
                pass
        if not detail_nodes:
            mt = re.search(ur'"productDescriptions":[^<]+"([^"]+)"', response.body)
            if mt:
                detail_html = mt.group(1)
                detail_texts = re.findall(ur'<li[^>]+>([^<]+)<\\/li>', detail_html)
                if detail_texts:
                    details = '\r'.join(
                        cls.reformat(val)
                        for val in detail_texts
                    )
                    details = cls.reformat(details)

        return details

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = None
        color_nodes = sel.xpath(
            '//div[@class="product-box"]//div[@id="product-options-wrapper"]//ul[@class="thumbs"]/li/img[@title]')
        if color_nodes:
            try:
                colors = [
                    cls.reformat(val).lower()
                    for val in color_nodes.xpath('./@title').extract()
                ]
            except(TypeError, IndexError):
                pass
        if not colors:
            mt = re.search(ur'color=(\w+)', response.url)
            if mt:
                try:
                    color_text = mt.group(1)
                    color_text = color_text.lower()
                    if color_text:
                        colors = [color_text]
                except(TypeError, IndexError):
                    pass

        return colors
