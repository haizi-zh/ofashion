# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re
from utils.text import iterable


class DonnakaranSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10110,
        'curreny': {
            'us': 'USD',
            'uk': 'GBP',
            'au': 'AUD',
            'at': 'EUR',
            'be': 'EUR',
            'br': 'BRL',
            'ca': 'CAD',
            'cy': 'EUR',
            'dk': 'EUR',
            'fi': 'EUR',
            'fr': 'EUR',
            'de': 'EUR',
            'gr': 'EUR',
            'in': 'INR',
            'ie': 'EUR',
            'it': 'EUR',
            'li': 'EUR',
            'mx': 'MXN',
            'mc': 'EUR',
            'nl': 'EUR',
            'nz': 'NZD',
            'no': 'NOK',
            'pt': 'EUR',
            'ro': 'RON',
            'si': 'EUR',
            'es': 'EUR',
            'se': 'SEK',
            'ch': 'CHF',
        },
        'home_urls': {
            'common': 'http://www.donnakaran.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['curreny'].keys()

    def __init__(self, region):
        super(DonnakaranSpider, self).__init__('donna_karan', region)

    def start_requests(self):
        for region in self.region_list:
            metadata = {'region': region, 'brand_id': self.spider_data['brand_id'],
                        'tags_mapping': {}}
            tmp = self.spider_data['home_urls']['common']
            cookie = {
                'DKI_FiftyOneInternationalCookie': str.format('{0}-{1}', region.upper(),
                                                              self.spider_data['curreny'][region])
            }
            start_urls = tmp if iterable(tmp) else [tmp]
            for url in start_urls:
                m = copy.deepcopy(metadata)
                yield Request(url=url,
                              meta={'userdata': m},
                              callback=self.parse,
                              errback=self.onerr,
                              cookies=cookie,
                              dont_filter=True)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[contains(@class, "global-nav")]/ul/li')
        for node in nav_nodes:
            try:
                tag_text = ' '.join(node.xpath('./a//text()').extract())
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,}
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                # 这个不包含最后一个nav里边的链接，那里边没单品
                sub_nodes = node.xpath('./div/div/ul/li[child::a[text()][@href]]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,}
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./a/@href').extract()[0]
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

        product_nodes = sel.xpath('//ul[contains(@class, "product-set")]/li/a[@href]')
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
                          meta={'userdata': m})

        next_node = sel.xpath('//ul[contains(@class, "page-set")]/li[@class="page next-page"]/a[@href]')
        if next_node:
            try:
                next_href = next_node.xpath('./@href').extract()[0]
                next_href = self.process_href(next_href, response.url)

                yield Request(url=next_href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': metadata})
            except(TypeError, IndexError):
                pass

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

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        image_urls = []
        image_nodes = sel.xpath('//div[@id="container"]//ul[contains(@class, "thumbnail-set")]/li/a/img[@src]')
        for image_node in image_nodes:
            try:
                src_text = image_node.xpath('./@src').extract()[0]
                if src_text:
                    src_text = re.sub(ur'images/\d+/\d+', u'images/2000/2000', src_text)
                    if src_text:
                        src = self.process_href(src_text, response.url)
                        if src:
                            image_urls += [src]
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
        model = cls.fetch_model(response, spider)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//div[@class="product-info-container"]//div[@class="product-id"][text()]')
        if model_node:
            try:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = cls.reformat(model_text)
                if model_text:
                    mt = re.search(ur'([\w-]+)$', model_text)
                    if mt:
                        model = mt.group(1)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@class="product-info-container"]//ul[@class="price-set"]/li/span[text()]')
        if price_node:
            try:
                price = price_node.xpath('./text()').extract()[0]
                price = cls.reformat(price)
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
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@class="product-info-container"]//h1[@class="product-name"][text()]')
        if name_node:
            try:
                name_text = name_node.xpath('./text()').extract()[0]
                name_text = cls.reformat(name_text)
                if name_text:
                    name = name_text
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@class="product-info-container"]//div[@class="product-description-complete"][text()]')
        if description_node:
            try:
                description = '\r'.join(description_node.xpath('.//text()').extract())
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        color_nodes = sel.xpath('//div[@class="product-info-container"]//ul[@class="option-set"]/li[contains(@class, "variant")]/ul/li/a/img[@alt]')
        for color_node in color_nodes:
            try:
                color_name = color_node.xpath('./@alt').extract()[0]
                color_name = color_name.lower()
                if color_name:
                    colors += [color_name]
            except(TypeError, IndexError):
                continue

        return colors

