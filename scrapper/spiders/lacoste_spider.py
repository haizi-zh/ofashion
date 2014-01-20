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


class LacosteSpider(MFashionSpider):
    spider_data = {'brand_id': 10204,
                   'home_urls': {'cn': 'http://shop.lacoste.com.cn'}}

    @classmethod
    def get_supported_regions(cls):
        return LacosteSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LacosteSpider, self).__init__('lacoste', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//ul[@class="mainNavi"]//li[contains(@class,"mainNavi_item")]'):
            try:
                tmp = node1.xpath('.//a[@href and contains(@class,"mainNavi_link")]/span/text()').extract()
                cat_title = self.reformat(tmp[0])
                cat_name = cat_title.lower()
            except (IndexError, TypeError):
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m1['gender'] = [gender]

            for node2 in node1.xpath('.//ul[@class="nav_category_list"]/li/a[@href]'):
                url = self.process_href(node2.xpath('@href').extract()[0], response.url)
                try:
                    tmp = node2.xpath('./span/text()').extract()
                    cat_title = self.reformat(tmp[0])
                    cat_name = cat_title.lower()
                except (IndexError, TypeError):
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
                yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m2})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"grid-tile") and contains(@class,"product")]'):
            try:
                tmp = node.xpath('.//div[@class="product-name"]/a[@class="name" and @title]/@title').extract()
                name = self.reformat(tmp[0])
                tmp = node.xpath('.//div[@class="product-name"]/a[@class="name" and @title and @href]/@href').extract()
                url = self.process_href(tmp[0], response.url)

                tmp = node.xpath('.//div[@class="salesprice"]/del/text()').extract()
                if tmp:
                    # 打折
                    price = self.reformat(tmp[0])
                    tmp = node.xpath('.//div[@class="salesprice"]/span[@class="red"]/text()').extract()
                    price_discount = self.reformat(tmp[0])
                else:
                    # 未打折
                    tmp = node.xpath('.//div[@class="salesprice"]/text()').extract()
                    price = self.reformat(tmp[0])
                    price_discount = None
            except (IndexError, TypeError):
                continue

            m = copy.deepcopy(metadata)
            m['name'] = name
            m['price'] = price
            if price_discount:
                m['price_discount'] = price_discount

            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m},
                          dont_filter=True)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        tmp = sel.xpath('//div[contains(@class,"itemNo")]//span[@itemprop="productID"]/text()').extract()
        if not tmp:
            return
        metadata['model'] = self.reformat(tmp[0])

        tmp = sel.xpath('//div[@id="tab1"]/text()').extract()
        if tmp:
            metadata['description'] = '\r'.join(self.reformat(val) for val in tmp)

        image_urls = []
        for tmp in sel.xpath('//a[@href and @class="switchACss" and @rel]/@rel').extract():
            try:
                idx = tmp.find('largeimage')
                if idx == -1:
                    continue
                image_url = self.process_href(cm.extract_closure(tmp[idx:], "'", "'")[0][1:-1], response.url)
                if image_url not in image_urls:
                    image_urls.append(image_url)
            except (KeyError, ValueError, IndexError):
                continue

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item



