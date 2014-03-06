# coding=utf-8


__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re


class LongchampSpider(MFashionSpider):
    """
    这个品牌对于价格的符号写的可能是有问题的，不要信
    """

    spider_data = {
        'brand_id': 10510,
        'currency': {
            'ch': 'EUR',
            'dk': 'EUR',
            'se': 'EUR',
            'ca': 'USD',
            'kr': 'KRW',
            'tw': 'TWD',
            'hk': 'HKD',
            'sg': 'USD',
        },
        'home_urls': {
            'us': 'http://us.longchamp.com/',
            'uk': 'http://uk.longchamp.com/',
            'at': 'http://at.longchamp.com/',
            'fr': 'http://fr.longchamp.com/',
            'it': 'http://it.longchamp.com/',
            'es': 'http://es.longchamp.com/',
            'be': 'http://be.longchamp.com/',
            'de': 'http://de.longchamp.com/',
            'lu': 'http://lu.longchamp.com/',
            'se': 'http://se.longchamp.com/',
            'dk': 'http://dk.longchamp.com/',
            'gr': 'http://gr.longchamp.com/',
            'nl': 'http://nl.longchamp.com/',
            'ch': 'http://ch.longchamp.com/',
            'fi': 'http://fi.longchamp.com/',
            'ie': 'http://ie.longchamp.com/',
            'pt': 'http://pt.longchamp.com/',
            'ca': 'http://ca.longchamp.com/',
            'jp': 'http://jp.longchamp.com/',
            'kr': 'http://kr.longchamp.com/',
            'cn': 'http://cn.longchamp.com/',
            'hk': 'http://hk.longchamp.com/',
            'tw': 'http://tw.longchamp.com/',
            'sg': 'http://sg.longchamp.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LongchampSpider, self).__init__('longchamp', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="footer"]//div[@class="footer-map"]/dl[child::dt[text()]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./dt/text()').extract()[0]
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

                sub_nodes = node.xpath('./dd/ul/li/a[@href][text()]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./text()').extract()[0]
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
                            href = sub_node.xpath('./@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath(
            '//div[@id="page"]/div[@class="region region-content page"]/div[@class="coverflow"]//ul/li/a[@href]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
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

        other_nodes = sel.xpath('//div[@id="aside"]//div[@id="couleurs"]//ul/li/a[@href][@data-label]')
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
        image_nodes = sel.xpath('//div[@id="article"]//ul/li/img[@data-hd]')
        if image_nodes:
            try:
                image_urls = [
                    self.process_href(self.reformat(val), response.url)
                    for val in image_nodes.xpath('./@data-hd').extract()
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
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//div[@id="aside"]//span[@itemprop="productID"][text()]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider):
        sel = Selector(response)
        ret = {}

        price = None
        price_node = sel.xpath('//div[@id="aside"]//span[@itemprop="price"][text()]')
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
    def fetch_name(cls, response, spider):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="aside"]//h1[@itemprop="name"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@id="aside"]//div[@itemprop="description"][text()]')
        if description_node:
            try:
                description = '\r'.join(
                    cls.reformat(val)
                    for val in description_node.xpath('.//text()').extract()
                )
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response, spider):
        sel = Selector(response)

        details = None
        detail_node = sel.xpath('//div[@id="aside"]//div[@id="dimensions"][text()]')
        if detail_node:
            try:
                details = '\r'.join(
                    cls.reformat(val)
                    for val in detail_node.xpath('.//text()').extract()
                )
                details = cls.reformat(details)
            except(TypeError, IndexError):
                pass

        return details

    @classmethod
    def fetch_color(cls, response, spider):
        sel = Selector(response)

        colors = None
        color_nodes = sel.xpath('//div[@id="aside"]//div[@id="couleurs"]//ul/li/a[@href][@data-label]')
        if color_nodes:
            try:
                colors = [
                    cls.reformat(val)
                    for val in color_nodes.xpath('./@data-label').extract()
                ]
            except(TypeError, IndexError):
                pass

        return colors
