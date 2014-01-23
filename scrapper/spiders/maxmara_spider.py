# coding=utf-8


__author__ = 'Ryan'


from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re
from utils.utils import unicodify, iterable


class MaxMaraSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10248,
        'home_urls': {
            'uk': 'http://gb.maxmara.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MaxMaraSpider, self).__init__('max_mara', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="nav_main"]//nav[@id="menu"]/ul/ul/div/li[child::a[text()]]')
        for node in nav_nodes:
            tag_text = node.xpath('./a[text()]/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./ul/li[child::a[@href][text()]]')
                for sub_node in sub_nodes:
                    tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        href = sub_node.xpath('./a[@href]/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)
