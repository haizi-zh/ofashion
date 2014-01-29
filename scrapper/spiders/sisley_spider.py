# coding=utf-8
import hashlib
import json
import urlparse
import copy
import re
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class SisleySpider(MFashionSpider):
    spider_data = {'brand_id': 10322,
                   'home_urls': {'us': 'http://www.sisley-cosmetics.com/us-en',
                                 'fr': 'http://www.sisley-paris.com/fr-FR',
                                 # 'cn': 'http://www.sisley.com.cn/',
                                 'hk': 'http://www.sisley-cosmetics.com/hk-hk/',
                                 'jp': 'http://www.sisley-cosmetics.com/jp-jp/',
                                 'kr': 'http://www.sisley-cosmetics.co.kr/',
                                 'my': 'http://www.sisley-cosmetics.com/my-my/',
                                 'sg': 'http://www.sisley-cosmetics.com/sg-sg/',
                                 'tw': 'http://www.sisley-cosmetics.com/tw-cn/',
                                 'th': 'http://www.sisley.co.th/',
                                 'be': 'http://www.sisley-cosmetics.com/be-fr/',
                                 'cz': 'http://www.sisley.cz/',
                                 'de': 'http://www.sisley-cosmetics.com/de-de/',
                                 'it': 'http://www.sisley-cosmetics.com/it-it/',
                                 'nl': 'http://www.sisley-cosmetics.com/nl-nl/',
                                 'pt': 'http://www.sisley-cosmetics.com/pt-pt/',
                                 'ru': 'http://www.sisley-cosmetics.com/ru-ru/',
                                 'es': 'http://www.sisley-cosmetics.com/sp-sp/',
                                 'ch': 'http://www.sisley-cosmetics.com/ch-fr/',
                                 'uk': 'http://www.sisley-cosmetics.com/gb-en/',
                                 'au': 'http://www.sisley-cosmetics.com/au-au/'
                   }}

    @classmethod
    def get_supported_regions(cls):
        return SisleySpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(SisleySpider, self).__init__('sisley', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//li/a[contains(@class,"header_menu")]'):
            try:
                tmp = node1.xpath('./*/text()').extract()
                cat_title = self.reformat(tmp[0])
                cat_name = cat_title.lower()
            except (IndexError, TypeError):
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m1['gender'] = [gender]

            for node2 in node1.xpath(
                    '../div[contains(@class,"submenu")]/ul/li[contains(@class,"title_column")]/a[@href]'):
                try:
                    tmp = node2.xpath('./*/text()').extract()
                    cat_title = self.reformat(tmp[0])
                    cat_name = cat_title.lower()
                except (IndexError, TypeError):
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
                gender = cm.guess_gender(cat_name)
                if gender:
                    if 'gender' in m2 and m2['gender']:
                        tmp = set(m2['gender'])
                        tmp.add(gender)
                        m2['gender'] = list(tmp)
                    else:
                        m2['gender'] = [gender]
                yield Request(url=self.process_href(node2.xpath('@href').extract()[0], response),
                              callback=self.parse_grid, errback=self.onerr, meta={'userdata': m2})

    def parse_grid(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # 其它页面
        for href in sel.xpath(
                '//ul/li[contains(@class,"active_page") or contains(@class,"number_page")]/a[@href]/@href').extract():
            url = self.process_href(href, response)
            m = copy.deepcopy(metadata)
            yield Request(url=url, callback=self.parse_grid, errback=self.onerr, meta={'userdata': m})

        # 解析该页面
        for node in sel.xpath('//p[contains(@class,"product") and @itemprop="name"]'):
            try:
                tmp = node.xpath('./a[@href]/@href').extract()
                url = self.process_href(tmp[0], response)
                tmp = node.xpath('./a[@href]/text()').extract()
                name = self.reformat(' '.join(tmp))
                tmp = node.xpath('..//span[@class="price"]/text()').extract()
                price = self.reformat(' '.join(tmp)) if tmp else None
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            if price:
                m['price'] = price
            m['name'] = name
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        metadata['model'] = hashlib.md5(response.url).hexdigest()
        sel = Selector(response)

        tmp = sel.xpath('//div[contains(@id,"tab") and @itemprop="description"]/*/text()').extract()
        desc = '\r'.join([self.reformat(val) for val in tmp])
        if desc:
            metadata['description'] = self.reformat(desc)

        image_urls = [self.process_href(val, response) for val in
                      sel.xpath('//a[@id="zoomLnk" and @class="product_zoom" and @href]/@href').extract()]
        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item


