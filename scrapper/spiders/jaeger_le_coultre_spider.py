# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import common
from utils.utils import unicodify, iterable


class JaegerLeCoultreSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10178,
        'home_urls': {
            k: str.format('http://www.jaeger-lecoultre.com/{0}/{1}/watch-finder',
                          k.upper() if k != 'uk' else 'GB',
                          'en' if k != 'cn' else 'zh')
            for k in {
            'cn', 'us', 'fr', 'uk', 'hk',
            'jp', 'it', 'ae', 'sg', 'de',
            'es', 'ch', 'ru', 'kr', 'my',
            'nl', 'au',
        }
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return JaegerLeCoultreSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(JaegerLeCoultreSpider, self).__init__('jaeger_le_coultre', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # jaeger_le_coultre所有的产品都是腕表
        metadata['tags_mapping']['category-0'] = [{'name': ur'腕表', 'title': ur'腕表'}]

        collection_nodes = sel.xpath('//div[@class="collectionsList"]//a')
        for node in collection_nodes:
            try:
                tag_text = node.xpath('./h3/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()

                if tag_text and tag_name:
                    m = copy.deepcopy(metadata)
                    m['tags_mapping']['collection'] = [
                        {'name': tag_name, 'title': tag_text, },
                    ]

                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)

                    yield Request(url=href,
                                  callback=self.parse_filter1,
                                  errback=self.onerr,
                                  meta={'userdata': m})
            except(TypeError, IndexError):
                continue

        for val in self.parse_filter1(response):
            yield val

    def parse_filter1(self, response):
        '''
        解析左上，机芯一栏
        '''

        metadata = response.meta['userdata']
        sel = Selector(response)

        filter1_nodes = sel.xpath('//div[@class="filters"]//dd[following-sibling::dt[2]]')
        for node in filter1_nodes:
            try:
                tag_node = node.xpath('.//span')
                if tag_node:
                    tag_text = tag_node.xpath('./text()')
                    if tag_text:
                        tag_text = tag_node.xpath('./text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()

                        if tag_text and tag_name:
                            m = copy.deepcopy(metadata)
                            m['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text, },
                            ]

                            href = node.xpath('./a/@href').extract()[0]
                            href = self.process_href(href, response.url)

                            yield Request(url=href,
                                          callback=self.parse_filter2,
                                          errback=self.onerr,
                                          meta={'userdata': m})
            except(TypeError, IndexError):
                continue

        for val in self.parse_filter2(response):
            yield val

    def parse_filter2(self, response):
        '''
        解析左中，表壳一栏
        '''

        metadata = response.meta['userdata']
        sel = Selector(response)

        filter2_nodes = sel.xpath('//div[@class="filters"]//dd[preceding-sibling::dt[2] and following-sibling::dt]')
        for node in filter2_nodes:
            try:
                tag_node = node.xpath('.//span')
                if tag_node:
                    tag_text = tag_node.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        m = copy.deepcopy(metadata)
                        m['tags_mapping']['category-2'] = [
                            {'name': tag_name, 'title': tag_text, },
                        ]

                        href = node.xpath('./a/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_filter3,
                                      errback=self.onerr,
                                      meta={'userdata': m})
            except(TypeError, IndexError):
                continue

        for val in self.parse_filter3(response):
            yield val


    def parse_filter3(self, response):
        '''
        解析左下，功能一栏
        '''

        metadata = response.meta['userdata']
        sel = Selector(response)

        filter3_nodes = sel.xpath('//div[@class="filters"]//dd[preceding-sibling::dt[3]]')
        for node in filter3_nodes:
            try:
                tag_node = node.xpath('.//span')
                if tag_node:
                    tag_text = tag_node.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        m = copy.deepcopy(metadata)
                        m['tags_mapping']['category-3'] = [
                            {'name': tag_name, 'title': tag_text, },
                        ]

                        href = node.xpath('./a/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_productList,
                                      errback=self.onerr,
                                      meta={'userdata': m})
            except(TypeError, IndexError):
                continue

        for val in self.parse_productList(response):
            yield val

    def parse_productList(self, response):
        '''
        解析单品列表
        '''

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_list_nodes = sel.xpath('//div[@class="models-list"]//li')
        for node in product_list_nodes:
            try:
                m = copy.deepcopy(metadata)

                model_node = node.xpath('.//h5')
                if model_node:
                    model = model_node.xpath('.//div').extract()[0]
                    model = self.reformat(model)
                    if model:
                        m['model'] = model
                    else:
                        continue
                else:
                    continue

                name_node = node.xpath('.//h5')
                if name_node:
                    nameText = name_node.xpath('./text()').extract()[0]
                    nameText = self.reformat(nameText)
                    if nameText:
                        m['name'] = nameText

                if m['name']:
                    gender = common.guess_gender(m['name'])
                    if gender:
                        m['gender'] = [gender]

                href = node.xpath('.//a/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product,
                              errback=self.onerr,
                              meta={'userdata': m})
            except(TypeError, IndexError):
                continue

    def parse_product(self, response):
        '''
        解析单品
        '''

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

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        image_urls = []
        image_nodes = sel.xpath('//a[@class="lightbox_recto"]')
        for node in image_nodes:
            try:
                image_href = node.xpath('./@href').extract()[0]
                image_href = self.process_href(image_href, response.url)

                if image_href:
                    image_urls += [image_href]
            except(TypeError, IndexError):
                pass

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        item = ProductItem()
        if image_urls:
            item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
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
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//div[@class="productDetails"]//a[@data-related]')
        if model_node:
            try:
                model = model_node.xpath('./@data-related').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@class="price"]')
        if price_node:
            try:
                price = price_node.xpath('./h3/text()').extract()[0]
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
        name_node = sel.xpath('//div[@class="informationContainer"]//div[@class="reference-header"]/h1[text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        product_description_node = sel.xpath('//p[contains(@class,"description")]')
        if product_description_node:
            try:
                description = product_description_node.xpath('./text()').extract()[0]
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        details = None
        detail_node = sel.xpath('//div[@class="specifications"]')
        if detail_node:

            def func(node):
                node_name = node._root.tag
                allText = ''.join(cls.reformat(val) for val in node.xpath('.//text()').extract())
                # dt标签说明他是一行的开头
                if node_name == 'dt':
                    return '\r' + allText
                elif node_name == 'dd':
                    return allText
                return allText

            nodes = detail_node.xpath('./dl/child::*')
            detail = ''.join(func(node) for node in nodes)
            detail = cls.reformat(detail)
            if detail:
                details = detail

        return details
