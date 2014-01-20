# coding=utf-8
import json
import os
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class KenzoSpider(MFashionSpider):
    spider_data = {'brand_id': 10192,
                   'image_data': 'https://www.kenzo.com/en/services/product/',
                   'currency': {'us', 'EUR'},
                   'home_urls': {'us': 'https://www.kenzo.com/en'}}

    @classmethod
    def get_supported_regions(cls):
        return KenzoSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(KenzoSpider, self).__init__('kenzo', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"header-categories")]/ul/li//a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
                gender = cm.guess_gender(cat_name)
                if gender:
                    m['gender'] = [gender]
                yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})
            except (IndexError, TypeError):
                continue

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"header-level-3-inner")]/a[@href]'):
            href = node.xpath('@href').extract()[0]
            if href not in response.url:
                continue
            for node1 in node.xpath('../ul/li/a[@href]'):
                try:
                    cat_title = self.reformat(node1.xpath('text()').extract()[0])
                    cat_name = cat_title.lower()
                    m = copy.deepcopy(metadata)
                    m['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
                    url = self.process_href(node1.xpath('@href').extract()[0], response.url)
                    yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m})
                except (IndexError, TypeError):
                    continue

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[contains(@class,"line")]/li[contains(@class,"product-list-unit")]'
                              '//a[@href and @class="history"]'):
            m = copy.deepcopy(metadata)
            url = self.process_href(node.xpath('@href').extract()[0], response.url)
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        # 获取model
        mt = re.search(r'_(\d+)/?$', response.url)
        if not mt:
            return
        model = mt.group(1)

        tmp = sel.xpath('//div[contains(@class,"product-detail")]/*[@class="font-title-product"]/text()').extract()
        if tmp:
            metadata['name'] = self.reformat(tmp[0])
            # tmp = sel.xpath('//div[contains(@class,"product-detail")]//*[contains(@class,"JS_price")]/text()').extract()
        # if tmp:
        #     metadata['price'] = self.reformat(tmp[0])
        # tmp = sel.xpath(
        #     '//div[contains(@class,"product-detail")]//*[contains(@class,"JS_specialprice")]/text()').extract()
        # if tmp:
        #     metadata['price_discount'] = self.reformat(tmp[0])

        tmp = sel.xpath('//div[contains(@class,"in-desc")]/descendant-or-self::text()').extract()
        if tmp:
            metadata['description'] = '\r'.join(filter(lambda x: x, [self.reformat(val) for val in tmp]))

        url = self.spider_data['image_data'] + str(model)
        yield Request(url=url, callback=self.parse_image, errback=self.onerr, meta={'userdata': metadata})

    def parse_image(self, response):
        metadata = response.meta['userdata']
        try:
            data = json.loads(response.body)
        except ValueError:
            return

        for clr_item in data['data']['colors_list']:
            m = copy.deepcopy(metadata)
            try:
                image_urls = [val['image_src'] for val in clr_item['images']]
                model = clr_item['id']
                color = self.reformat(clr_item['name'])

                prod_item = filter(lambda val: val['color_id'] == model, data['data']['products'])
                tmp = prod_item[0]
                try:
                    price = float(tmp['price']) / 100 if 'price' in tmp else None
                except (TypeError, ValueError):
                    price = None
                try:
                    price_discount = float(tmp['price_sale']) / 100 if 'price_sale' in tmp else None
                except (TypeError, ValueError):
                    price_discount = None
            except (IndexError, KeyError):
                continue

            m['model'] = model
            m['color'] = [color]
            if price:
                m['price'] = price
            if price_discount:
                m['price_discount'] = price_discount

            item = ProductItem()
            item['url'] = m['url']
            item['model'] = model
            item['image_urls'] = image_urls
            item['metadata'] = m
            yield item







