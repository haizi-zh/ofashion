# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class KenzoSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10192,
        'home_urls': {
            'uk': 'https://www.kenzo.com/en/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(KenzoSpider, self).__init__('kenzo', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)
