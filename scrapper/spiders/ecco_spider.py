# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class EccoSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10105,
        'home_urls': {
            'uk': 'http://shopeu.ecco.com/uk/en',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(EccoSpider, self).__init__('ecco', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)
