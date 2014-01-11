# coding=utf-8
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class LacosteSpider(MFashionSpider):
    spider_data = {'brand_id': 10204,
                   'home_urls': {'cn': 'http://shop.lacoste.com.cn'}}

    @classmethod
    def get_supported_regions(cls):
        return LacosteSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LacosteSpider, self).__init__('lacoste', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//ul[@class="mainNavi"]//li[contains(@class,"mainNavi_item")]'):
            try:
                tmp = node1.xpath('.//a[@href and @class="mainNavi_link"]/text()').extract()
                cat_title = self.reformat(tmp[0])
                cat_name = cat_title.lower()
            except (IndexError, TypeError):
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m1['gender'] = [gender]

            for node2 in node1.xpath('.//ul[@class="nav_category_list"]/li/a[@href]'):
                url = self.process_results(node2.xpath('@href').extract()[0], response.url)
                try:
                    tmp = node2.xpath('text()').extract()
                    cat_title = self.reformat(tmp[0])
                    cat_name = cat_title.lower()
                except (IndexError, TypeError):
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
                yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m2})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)