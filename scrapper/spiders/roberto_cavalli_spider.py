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
            # model = node.xpath('@data-code8').extract()[0]
            try:
                url = self.process_href(
                    node.xpath('.//a[(@data-itemlink or @data-itemLink) and @href]/@href').extract()[0], response.url)
                # name = self.reformat(node.xpath(
                #     './/div[@class="productDetailsContainer"]//div[@class="productMicro"]/a[@href]/text()').extract()[
                #     0])
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            # m['model'] = model
            # m['name'] = name

            # tmp = node.xpath(
            #     './/div[contains(@class,"productDescriptionContainer")]//div[@data-item-prop="price"]/'
            #     '*[@class="currency" or @class="priceValue"]/text()').extract()
            # price_new = ''.join(self.reformat(val) for val in tmp if val)
            # tmp = node.xpath(
            #     './/div[contains(@class,"productDescriptionContainer")]//div[@data-item-prop="priceWithoutPromotion"]/'
            #     '*[@class="currency" or @class="priceValue"]/text()').extract()
            # price_old = ''.join(self.reformat(val) for val in tmp if val)
            #
            # if price_old and price_new:
            #     m['price'] = price_old
            #     m['price_discount'] = price_new
            # elif price_new and not price_old:
            #     m['price'] = price_new

            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

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

    @classmethod
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        model = None
        mt = re.search(ur'cod(\d+)', response.url)
        if mt:
            try:
                model = mt.group(1)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        origin_node = sel.xpath('//div[@id="shopCnt"]//div[@data-item-prop="priceWithoutPromotion"][text()]')
        if origin_node:
            try:
                old_price = ''.join(cls.reformat(val)
                                    for val in origin_node.xpath('.//text()').extract())
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

            discount_node = sel.xpath('//div[@id="shopCnt"]//div[@data-item-prop="price"][text()]')
            if discount_node:
                try:
                    new_price = ''.join(cls.reformat(val)
                                        for val in discount_node.xpath('.//text()').extract())
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
        else:
            old_node = sel.xpath('//div[@id="shopCnt"]//div[@data-item-prop="price"][text()]')
            if old_node:
                try:
                    old_price = ''.join(cls.reformat(val) for val in old_node.xpath('.//text()').extract())
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="itemDetailsCnt"]/h1[text()]')
        if name_node:
            try:
                name = ''.join(name_node.xpath('./text()').extract())
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            desc = '\r'.join(filter(lambda x: x, [cls.reformat(val) for val in sel.xpath(
                '//div[@id="tabs"]//span[@itemprop="description"]/*/text()').extract()]))
            if desc:
                description = desc
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)

        colors = []
        color_nodes = sel.xpath('//div[@id="shopCnt"]//div[@id="ColorsDiv"]/ul/li[@data-title]')
        if color_nodes:
            try:
                colors = [cls.reformat(val)
                          for val in color_nodes.xpath('./@data-title').extract()]
            except(TypeError, IndexError):
                pass

        return colors
