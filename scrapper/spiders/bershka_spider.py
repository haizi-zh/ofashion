# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

class BershkaSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10040,
        'home_urls': {
            'cn': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkacn/zh/40109502',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(BershkaSpider, self).__init__('bershka', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)
