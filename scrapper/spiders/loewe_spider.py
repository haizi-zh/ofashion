# coding=utf-8
import json
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class LoeweSpider(MFashionSpider):
    spider_data = {'brand_id': 10220,
                   'model_term': {'cn': ur'型号\s*:\s*', 'us': r'Model ID\s*:\s*', 'jp': r'Model ID\s*:\s*'},
                   'home_urls': {'cn': 'http://www.loewe.com/cn_zh_hans',
                                 'jp': 'http://www.loewe.com/jp_ja/',
                                 'us': 'http://www.loewe.com/us_en'}}

    @classmethod
    def get_supported_regions(cls):
        return LoeweSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LoeweSpider, self).__init__('loewe', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul/li[@class="menu_justified"]/a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@class="dispatchIndex"]/li/a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        menu_sels = sel.xpath('//div[@id="menuSelects"]/ul/li')
        if not menu_sels:
            return
        for node in menu_sels[0].xpath('.//ul/li/a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url) + '&context=carousel'
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-2'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(url=url, callback=self.parse_ajax, errback=self.onerr, meta={'userdata': m},
                          headers={'X-Requested-With': 'XMLHttpRequest'})

    def parse_ajax(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@id="carouselWrapper"]/li[@class="slide"]/a[@href]'):
            m = copy.deepcopy(metadata)
            tmp = node.xpath('..//*[@class="product-name"]/text()').extract()
            if tmp:
                m['name'] = self.reformat(tmp[0])
            url = self.process_href(node.xpath('@href').extract()[0], response.url)
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        try:
            sel = Selector(response)
        except AttributeError:
            return

        tmp = sel.xpath('//div[@id="aside"]//*[@class="product-name"]/text()').extract()
        if tmp:
            metadata['name'] = self.reformat(tmp[0])
        tmp = sel.xpath('//div[@id="tab1"]/descendant-or-self::text()').extract()
        if tmp:
            metadata['description'] = '\r'.join(filter(lambda x: x, [self.reformat(val) for val in tmp]))
        tmp = sel.xpath('//div[@id="tab2"]/descendant-or-self::text()').extract()
        if tmp:
            metadata['details'] = '\r'.join(filter(lambda x: x, [self.reformat(val) for val in tmp]))

        model_term = self.spider_data['model_term'][self.region_list[0]]
        model_list = filter(lambda val: re.search(model_term, val, flags=re.IGNORECASE),
                            sel.xpath('//*/text()').extract())
        if not model_list:
            return
        metadata['model'] = self.reformat(re.sub(model_term, '', model_list[0], flags=re.IGNORECASE))

        tmp = sel.xpath('//*[@class="regular-price"]/*[@class="price"]/text()').extract()
        price = self.reformat(tmp[0]) if tmp else None
        if price:
            metadata['price'] = price

        image_urls = [self.process_href(val, response.url) for val in
                      sel.xpath('//div[@id="product"]//ul[@class="views"]/li/a[@href]/@href').extract()]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item
