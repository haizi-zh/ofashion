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


class RobertoCavalliSpider(MFashionSpider):
    spider_data = {'brand_id': 10305,
                   'home_urls': {region: str.format('http://store.robertocavalli.com/{0}/robertocavalli',
                                                    'gb' if region == 'uk' else region) for region in
                                 {'cn', 'us', 'fr', 'it', 'uk', 'au', 'at', 'be', 'ca', 'cz', 'dk', 'fi', 'de', 'gr',
                                  'hk', 'ie', 'il', 'jp', 'my', 'nl', 'nz', 'no', 'pt', 'ru', 'sg', 'es', 'se', 'ch',
                                  'tw', 'ae'}}}

    @classmethod
    def get_supported_regions(cls):
        return RobertoCavalliSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(RobertoCavalliSpider, self).__init__('roberto_cavalli', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for gender in {'donna', 'uomo'}:
            for node1 in sel.xpath(
                    str.format('//div[@class="menuContainer {0}"]//div[@class="menuCategoryContainer"]', gender)):
                try:
                    cat_title = self.reformat(node1.xpath('./h3/text()').extract()[0])
                    cat_name = cat_title.lower()
                except (IndexError, TypeError):
                    continue
                m1 = copy.deepcopy(metadata)
                g = cm.guess_gender(gender)
                if g:
                    m1['gender'] = [g]
                m1['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]

                for node2 in node1.xpath('./ul/li/a[@href]'):
                    try:
                        cat_title = self.reformat(node2.xpath('text()').extract()[0])
                        cat_name = cat_title.lower()
                        url = self.process_href(node2.xpath('@href').extract()[0], response.url)
                    except (IndexError, TypeError):
                        continue
                    m2 = copy.deepcopy(m1)
                    m2['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]

                    yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m2})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@id="productList"]/li[@class="product" and @data-code8]'):
            model = node.xpath('@data-code8').extract()[0]
            try:
                url = self.process_href(
                    node.xpath('.//a[(@data-itemlink or @data-itemLink) and @href]/@href').extract()[0], response.url)
                name = self.reformat(node.xpath(
                    './/div[@class="productDetailsContainer"]//div[@class="productMicro"]/a[@href]/text()').extract()[
                    0])
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['model'] = model
            m['name'] = name

            tmp = node.xpath(
                './/div[contains(@class,"productDescriptionContainer")]//div[@data-item-prop="price"]/'
                '*[@class="currency" or @class="priceValue"]/text()').extract()
            price_new = ''.join(self.reformat(val) for val in tmp if val)
            tmp = node.xpath(
                './/div[contains(@class,"productDescriptionContainer")]//div[@data-item-prop="priceWithoutPromotion"]/'
                '*[@class="currency" or @class="priceValue"]/text()').extract()
            price_old = ''.join(self.reformat(val) for val in tmp if val)

            if price_old and price_new:
                m['price'] = price_old
                m['price_discount'] = price_new
            elif price_new and not price_old:
                m['price'] = price_new

            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        desc = '\r'.join(filter(lambda x: x, [self.reformat(val) for val in sel.xpath(
            '//div[@id="tabs"]//span[@itemprop="description"]/*/text()').extract()]))
        if desc:
            metadata['description'] = desc

        image_urls = []
        for href in sel.xpath('//ul[@id="alternateList"]/li/img[@src]/@src').extract():
            mt = re.search(r'(\d+)\w?_\w\.jpg$', href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            tmp = [
                self.process_href(re.sub(r'(.+)_\d+(\w?_\w\.jpg)$', str.format(r'\1_{0}\2', idx), href), response.url)
                for idx in xrange(start_idx, 16)]
            image_urls.extend(tmp)

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item