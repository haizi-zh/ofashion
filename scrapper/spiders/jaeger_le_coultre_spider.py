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

        collectionNodes = sel.xpath('//div[@class="collectionsList"]//a')
        for node in collectionNodes:
            try:
                tag_text = node.xpath('./h3/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()

                if tag_text and tag_name:
                    m = copy.deepcopy(metadata)
                    m['tags_mapping']['collection'] = [
                        {'name': tag_name, 'title': tag_text,},
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

        filter1Nodes = sel.xpath('//div[@class="filters"]//dd[following-sibling::dt[2]]')
        for node in filter1Nodes:
            try:
                tagNode = node.xpath('.//span')
                if tagNode:
                    tag_text = tagNode.xpath('./text()')
                    if tag_text:
                        tag_text = tagNode.xpath('./text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()

                        if tag_text and tag_name:
                            m = copy.deepcopy(metadata)
                            m['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text,},
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

        filter2Nodes = sel.xpath('//div[@class="filters"]//dd[preceding-sibling::dt[2] and following-sibling::dt]')
        for node in filter2Nodes:
            try:
                tagNode = node.xpath('.//span')
                if tagNode:
                    tag_text = tagNode.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        m = copy.deepcopy(metadata)
                        m['tags_mapping']['category-2'] = [
                            {'name': tag_name, 'title': tag_text,},
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

        filter3Nodes = sel.xpath('//div[@class="filters"]//dd[preceding-sibling::dt[3]]')
        for node in filter3Nodes:
            try:
                tagNode = node.xpath('.//span')
                if tagNode:
                    tag_text = tagNode.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        m = copy.deepcopy(metadata)
                        m['tags_mapping']['category-3'] = [
                            {'name': tag_name, 'title': tag_text,},
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

        productListNodes = sel.xpath('//div[@class="models-list"]//li')
        for node in productListNodes:
            try:
                m = copy.deepcopy(metadata)

                modelNode = node.xpath('.//h5')
                if modelNode:
                    model = modelNode.xpath('.//div').extract()[0]
                    model = self.reformat(model)
                    if model:
                        m['model'] = model
                    else:
                        continue
                else:
                    continue

                nameNode = node.xpath('.//h5')
                if nameNode:
                    nameText = nameNode.xpath('./text()').extract()[0]
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

        productDescriptionNode = sel.xpath('//p[contains(@class,"description")]')
        if productDescriptionNode:
            try:
                description = productDescriptionNode.xpath('./text()').extract()[0]
                description = self.reformat(description)

                if description:
                    metadata['description'] = description
            except(TypeError, IndexError):
                pass

        detailNode = sel.xpath('//div[@class="specifications"]')
        if detailNode:

            def func(node):
                nodeName = node._root.tag
                allText = ''.join(self.reformat(val) for val in node.xpath('.//text()').extract())
                # dt标签说明他是一行的开头
                if nodeName == 'dt':
                    return '\r'+allText
                elif nodeName == 'dd':
                    return allText
                return allText

            nodes = detailNode.xpath('./dl/child::*')
            detail = ''.join(func(node) for node in nodes)
            detail = self.reformat(detail)
            if detail:
                metadata['details'] = detail


        imageUrls = []
        imageNodes = sel.xpath('//a[@class="lightbox_recto"]')
        for node in imageNodes:
            try:
                imageHref = node.xpath('./@href').extract()[0]
                imageHref = self.process_href(imageHref, response.url)

                if imageHref:
                    imageUrls += [imageHref]
            except(TypeError, IndexError):
                pass

        priceNode = sel.xpath('//div[@class="price"]')
        if priceNode:
            try:
                price = priceNode.xpath('./h3/text()').extract()[0]
                if price:
                    metadata['price'] = price
            except(TypeError, IndexError):
                pass

        item = ProductItem()
        if imageUrls:
            item['image_urls'] = imageUrls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        yield item
