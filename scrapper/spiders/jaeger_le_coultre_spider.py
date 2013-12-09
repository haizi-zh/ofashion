# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy

class JaegerLeCoultreSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10178,
        #'currency': {
        #    'cn': ''
        #}
        'home_urls': {
            'cn': 'http://www.jaeger-lecoultre.com/CN/zh/watch-finder',
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
        metadata['tags_mapping']['category-0'] = [ur'腕表']

        collectionNodes = sel.xpath('//div[@class="collectionsList"]//a')
        for node in collectionNodes:
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

        for val in self.parse_filter1(response):
            yield val

    def parse_filter1(self, response):
        '''
        解析左上，机芯一栏
        '''

        metadata = response.meta['userdata']
        sel = Selector(response)

        filter1Nodes = sel.xpath('//div[@class="filters"]//dt[2]/preceding-sibling::dd')
        for node in filter1Nodes:
            tag_text = node.xpath('.//span/text()').extract()[0]
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
            tag_text = node.xpath('.//span/text()').extract()[0]
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
            tag_text = node.xpath('.//span/text()').extract()[0]
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
                              callback=self.parse_prductList,
                              errback=self.onerr,
                              meta={'userdata': m})

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
            m = copy.deepcopy(metadata)

            model = node.xpath('.//h5//div[@style]').extract()[0]
            model = self.reformat(model)
            if model:
                m['model'] = model
            else:
                continue

            name = node.xpath('.//h5/text()').extract()[0]
            name = self.reformat(name)
            if name:
                m['name'] = name

            href = node.xpath('.//a/@href').extract()[0]
            href = self.process_href(href)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_product(self, response):
        '''
        解析单品
        '''

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        productDescriptionNode = sel.xpath('//p[@class="description"]')
        if productDescriptionNode:
            description = productDescriptionNode.xpath('./text()').extract()[0]
            description = self.reformat(description)

            if description:
                metadata['description'] = description

        imageNode = sel.xpath('//a[@class="lightbox_recto"]')
        if imageNode:
            imageHref = imageNode.xpath('./@href')
            imageHref = self.process_href(imageHref)

            if imageHref:
                metadata['image_urls'] = imageHref

        priceNode = sel.xpath('//div[@class="price"]')
        if priceNode:
            price = priceNode.xpath('./h3/text()')
            if price:
                metadata['price'] = price

        item = ProductItem()
        if metadata['image_urls']:
            item['image_urls'] = metadata['image_urls']
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        yield item
