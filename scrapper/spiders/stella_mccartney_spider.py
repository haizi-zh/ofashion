# coding=utf-8
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class StellaMcCartneySpider(MFashionSpider):
    spider_data = {'brand_id': 10333, 'home_urls': {
        region: str.format('http://www.stellamccartney.com/{0}', region if region != 'uk' else 'gb') for region in
        {'us', 'it', 'uk', 'fr', 'de', 'ca', 'au', 'ad', 'be', 'cz', 'dk', 'eg', 'fi', 'gr', 'hk', 'ie', 'jp', 'mo',
         'my', 'mc', 'nl', 'nz', 'no', 'ru', 'sg', 'kr', 'es', 'se', 'ch', 'tw', 'th', }}}

    @classmethod
    def get_supported_regions(cls):
        return StellaMcCartneySpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(StellaMcCartneySpider, self).__init__('stella_mccartney', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//nav[@id="mainMenu"]/ul[contains(@class,"level1")]/li/a[@href]'):
            try:
                cat_title = self.reformat(node1.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
            except (IndexError, TypeError):
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'name': cat_name, 'title': cat_title}]

            for node2 in node1.xpath('..//ul[contains(@class,"level3")]/li/a[@href]'):
                try:
                    cat_title = self.reformat(node2.xpath('text()').extract()[0])
                    cat_name = cat_title.lower()
                except (IndexError, TypeError):
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-1'] = [{'name': cat_name, 'title': cat_title}]
                url = self.process_href(node2.xpath('@href').extract()[0], response.url)
                yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m2})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//li[@data-position and contains(@class,"product") and @data-code8]'):
            model = self.reformat(node.xpath('@data-code8').extract()[0])
            tmp_node = node.xpath('.//div[@class="productInfo"]/a[@class="modelName" and @href]')
            try:
                url = self.process_href(tmp_node[0].xpath('@href').extract()[0], response.url)
                name = self.reformat(tmp_node[0].xpath('text()').extract()[0])
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['model'] = model
            m['name'] = name

            price = None
            price_discount = None
            tmp = node.xpath(
                './/div[@class="productInfo"]//div[@data-fullprice]/span[@class="currency" or '
                '@class="priceValue"]/text()').extract()
            if tmp:
                price = ''.join(self.reformat(val) for val in tmp)
            tmp = node.xpath(
                './/div[@class="productInfo"]//div[@data-discountedprice]/span[@class="currency" '
                'or @class="priceValue"]/text()').extract()
            if tmp:
                price_discount = ''.join(self.reformat(val) for val in tmp)
            if price:
                m['price'] = price
            if price_discount:
                m['price_discount'] = price_discount

            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m},
                          dont_filter=True)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        tmp = sel.xpath('//div[@id="descriptionContent"]/p/text()').extract()
        if tmp:
            metadata['description'] = '\r'.join(self.reformat(val) for val in tmp)

        tmp = sel.xpath('//div[@id="detailsContent"]/p/text()').extract()
        if tmp:
            metadata['details'] = '\r'.join(self.reformat(val) for val in tmp)

        image_urls = []
        for href in sel.xpath('//div[@id="altImages"]//img[@data-retinasrc]/@data-retinasrc').extract():
            mt = re.search(r'_(\d+)[^\d]+$', href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            for idx in xrange(start_idx, 14):
                new_href = re.sub(r'_\d+([^\d]+)$', str.format(r'_{0}\1', idx), href)
                image_urls.append(self.process_href(new_href, response.url))

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item






