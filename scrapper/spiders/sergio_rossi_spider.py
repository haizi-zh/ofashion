# coding=utf-8
import json
import urllib
import urlparse
import copy
import re
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'wuya'

_regions = [
    #type one
    #'cn',
    'us',
    'fr',
    'it',
    'gb', #替换uk
    #type two
    'hk',
    'jp',
    'au',
    'ae',
    'sg',
    'de',
    'ca',
    'es',
    'ch',
    'ru',
    #'br',
    'th',
    'kr',
    'my',
    'nl',
]


class SergiorossiSpider(MFashionSpider):
    spider_data = {'brand_id': 10316, }

    def __init__(self, region):
        home_urls = {
            region: ['http://www.sergiorossi.com/%s/women/shoponline/shoes' % region,
                     'http://www.sergiorossi.com/%s/women/shoponline/women-s-bags' % region,
                     'http://www.sergiorossi.com/%s/men/shoponline/shoes' % region, ]
            for region in _regions
        }
        home_urls['fr'] = [
            'http://www.sergiorossi.com/fr/femme/shoponline/chaussures',
            'http://www.sergiorossi.com/fr/femme/shoponline/sacs-femme',
            'http://www.sergiorossi.com/fr/homme/shoponline/chaussures',
        ]
        home_urls['it'] = [
            'http://www.sergiorossi.com/it/donna/shoponline/calzature',
            'http://www.sergiorossi.com/it/donna/shoponline/borse-donna',
            'http://www.sergiorossi.com/it/uomo/shoponline/calzature',
        ]
        home_urls['jp'] = [
            'http://www.sergiorossi.com/jp/%E3%83%AC%E3%83%87%E3%82%A3%E3%83%BC%E3%82%B9/shoponline/%E3%82%B7%E3%83%A5%E3%83%BC%E3%82%BA',
            'http://www.sergiorossi.com/jp/%E3%83%AC%E3%83%87%E3%82%A3%E3%83%BC%E3%82%B9/shoponline/%E3%83%AC%E3%83%87%E3%82%A3%E3%83%BC%E3%82%B9%E3%83%90%E3%83%83%E3%82%B0',
            'http://www.sergiorossi.com/jp/%E3%83%A1%E3%83%B3%E3%82%BA/shoponline/%E3%82%B7%E3%83%A5%E3%83%BC%E3%82%BA',
        ]
        home_urls['ru'] = [
            'http://www.sergiorossi.com/ru/%D0%B4%D0%BB%D1%8F-%D0%B6%D0%B5%D0%BD%D1%89%D0%B8%D0%BD/shoponline/%D1%82%D1%83%D1%84%D0%BB%D0%B8',
            'http://www.sergiorossi.com/ru/%D0%B4%D0%BB%D1%8F-%D0%B6%D0%B5%D0%BD%D1%89%D0%B8%D0%BD/shoponline/%D0%B6%D0%B5%D0%BD%D1%81%D0%BA%D0%B8%D0%B5-%D1%81%D1%83%D0%BC%D0%BA%D0%B8',
            'http://www.sergiorossi.com/ru/%D0%B4%D0%BB%D1%8F-%D0%BC%D1%83%D0%B6%D1%87%D0%B8%D0%BD/shoponline/%D1%82%D1%83%D1%84%D0%BB%D0%B8',
        ]
        self.spider_data['home_urls'] = home_urls
        super(SergiorossiSpider, self).__init__('sergiorossi', region)


    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    #def __init__(self, region):
    #    super(SergiorossiSpider, self).__init__('sergiorossi', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        sel = Selector(response)
        cat_title = ''.join(''.join(sel.xpath('//div[@id="wrapperOuter"]/nav/h2//text()').extract()))
        cat_name = cat_title.lower()
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//section[@id="main"]'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        for link in links:
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            url = link.url
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)
        name = ''.join(sel.xpath('//span[@id="itemStyle"]//text()').extract())
        metadata['name'] = self.reformat(name)
        details = ''.join(sel.xpath('//span[@class="itemNameTitle"]//text()').extract())
        metadata['details'] = self.reformat(details)
        model = ''.join(sel.xpath('//div[@id="productCode"]//span[@class="content"]//text()').extract())
        metadata['model'] = self.reformat(model)
        description = ''.join(sel.xpath('//span[@class="itemMicroAndDescription"]//text()').extract())
        metadata['description'] = self.reformat(description)
        old_price = ''.join(sel.xpath('//div[@class="oldprice"]//text()').extract())
        new_price = ''.join(sel.xpath('//div[@class="newprice"]//text()').extract())
        if not old_price and not new_price:
            old_price = new_price = ''.join(sel.xpath('//div[@class="itemBoxPrice"]//text()').extract())
        if 'price' not in metadata:
            price = self.reformat(old_price)
            price_discount = self.reformat(new_price)
            metadata['price'] = price
            metadata['price_discount'] = price_discount
        color = ''.join(sel.xpath('//div[@id="colors"]//text()').extract())
        metadata['color'] = color
        image_urls = sel.xpath('//div[@id="itemContent"]//img/@src').extract()
        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

