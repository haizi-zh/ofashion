# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

class AlexanderWangSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10009,
    }

