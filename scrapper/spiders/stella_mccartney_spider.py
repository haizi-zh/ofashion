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
    spider_data = {'brand_id': 10333,
                   'currency': {'ch': 'EUR'},
                   'home_urls': {
                       region: str.format('http://www.stellamccartney.com/{0}', region if region != 'uk' else 'gb') for
                       region in
                       {'us', 'it', 'uk', 'fr', 'de', 'ca', 'au', 'be', 'cz', 'dk', 'eg', 'fi', 'gr', 'hk', 'ie', 'jp',
                        'mo',
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
            # model = self.reformat(node.xpath('@data-code8').extract()[0])
            tmp_node = node.xpath('.//div[@class="productInfo"]/a[@class="modelName" and @href]')
            try:
                url = self.process_href(tmp_node[0].xpath('@href').extract()[0], response.url)
                # name = self.reformat(tmp_node[0].xpath('text()').extract()[0])
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            # m['model'] = model
            # m['name'] = name

            # price = None
            # price_discount = None
            # tmp = node.xpath(
            #     './/div[@class="productInfo"]//div[@data-fullprice]/span[@class="currency" or '
            #     '@class="priceValue"]/text()').extract()
            # if tmp:
            #     price = ''.join(self.reformat(val) for val in tmp)
            # tmp = node.xpath(
            #     './/div[@class="productInfo"]//div[@data-discountedprice]/span[@class="currency" '
            #     'or @class="priceValue"]/text()').extract()
            # if tmp:
            #     price_discount = ''.join(self.reformat(val) for val in tmp)
            # if price:
            #     m['price'] = price
            # if price_discount:
            #     m['price_discount'] = price_discount

            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m},
                          dont_filter=True)

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

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

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
        try:
            mt = re.search(ur'cod(\d+)', response.url)
            if mt:
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
        old_price_node = sel.xpath('//div[@id="itemDetails"]//div[@id="itemPrice"]//div[@class="oldprice"][text()]')
        if old_price_node:  # 打折
            try:
                old_price = ''.join(old_price_node.xpath('.//text()').extract())
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

            new_price_node = sel.xpath('//div[@id="itemDetails"]//div[@id="itemPrice"]//div[@class="newprice"][text()]')
            if new_price_node:
                try:
                    new_price = ''.join(new_price_node.xpath('.//text()').extract())
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
        else:   # 不打折
            try:
                price_node = sel.xpath('//div[@id="itemDetails"]//div[@id="itemPrice"]//div[@class="newprice"][text()]')
                old_price = ''.join(price_node.xpath('.//text()').extract())
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
        try:
            name_node = sel.xpath('//div[@id="itemDetails"]//div[@id="itemInfo"]/h1[text()]')
            if name_node:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            tmp = sel.xpath('//div[@id="descriptionContent"]/p/text()').extract()
            if tmp:
                description = '\r'.join(cls.reformat(val) for val in tmp)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        details = None
        try:
            tmp = sel.xpath('//div[@id="detailsContent"]/p/text()').extract()
            if tmp:
                details = '\r'.join(cls.reformat(val) for val in tmp)
        except(TypeError, IndexError):
            pass

        return details
