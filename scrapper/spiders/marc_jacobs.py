# coding=utf-8
import json
import re
import urlparse
from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import copy

__author__ = 'Zephyre'


class ValentinoSpider(MFashionSpider):
    spider_data = {'brand_id': 10367,
                   'currency': {'cn': 'EUR', 'hk': 'EUR', 'tw': 'EUR'},
                   'home_urls': {'cn': 'http://store.valentino.com/VALENTINO/home/tskay/5A81B803/mm/112',
                                 'us': 'http://store.valentino.com/VALENTINO/home/tskay/B60ACEA7/mm/112',
                                 'fr': 'http://store.valentino.com/VALENTINO/home/tskay/D5C4AA66/mm/112',
                                 'it': 'http://store.valentino.com/VALENTINO/home/tskay/CD784FB3/mm/112',
                                 'uk': 'http://store.valentino.com/VALENTINO/home/tskay/112439D7/mm/112',
                                 'jp': 'http://store.valentino.com/VALENTINO/home/tskay/7D74C94E/mm/112',
                                 'hk': 'http://store.valentino.com/VALENTINO/home/tskay/3DC16A52/mm/112',
                                 'tw': 'http://store.valentino.com/VALENTINO/home/tskay/928128F6/mm/112'
                   }}

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(ValentinoSpider, self).__init__('valentino', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)