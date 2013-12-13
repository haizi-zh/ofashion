# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class AlexanderWangSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10009,
        'home_urls': {
            'cn': 'http://www.alexanderwang.cn/',
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(AlexanderWangSpider, self).__init__('alexander wang', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//nav[@id="sitenav"]/ul/li[child::a[@href]]')
        for node in nav_nodes:
            tag_text = node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                subNodes = node.xpath('.//li[child::a[@href]]')
                for subNode in subNodes:
                    tag_text = subNode.xpath('./a/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text},
                        ]

                        gender = common.guess_gender(gender)
                        if gender:
                            mc['gender'] = [gender]

                        href = subNode.xpath('./a/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_left_filter,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

                href = node.xpath('./a/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_left_filter,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_left_filter(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 有些类别有第三级展开，比如中国，促销，女装
        nav_nodes = sel.xpath('//nav[@id="navMenu"]//ul//ul//ul//li//a[@href]')
        for node in nav_nodes:
            tag_text = node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = gender

                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

        for val in self.parse_product_list(response):
            yield val

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[contains(@class, "content")]//ul[@class="productsContainer"]//li')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name = node.xpath('.//div[@class="description"]/a/div[@class="title"]/text()').extract()[0]
            name = self.reformat(name)

            if name:
                m['name'] = name

            priceNode = node.xpath('.//div[@class="productPrice"]/div[@class="oldprice"]')
            if priceNode:
                price = ''.join(self.reformat(val) for val in priceNode.xpath('.//text()').extract())
                price = self.reformat(price)
                if price:
                    m['price'] = price

            color_nodes = node.xpath('.//div[@class="colorsList"]//div[@class="color"]//img[@title]')
            if color_nodes:
                colors = [
                    self.reformat(val)
                    for val in color_nodes.xpath('./@title').extract()
                ]
                if colors:
                    m['color'] = colors


        # TODO 页面下拉到底部会自动加载更多，需要模拟请求，解析返回的json
