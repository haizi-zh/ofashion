# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import common
import re

class AgnesBSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10006,
        'home_urls': {
            'us': 'http://usa.agnesb.com/en/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(AgnesBSpider, self).__init__('agnes b', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

