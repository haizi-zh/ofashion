# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.eshop_spider import EShopSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import re


class AmazonSpider(EShopSpider):
    spider_data = {
        'brand_id': 8001,
        'home_urls': {
            'us': 'http://www.amazon.com/b/ref=clo_h_fb_headseeall?node=2479929011&pf_rd_p=1712192822&pf_rd_s=merchandised-search-10&pf_rd_t=101&pf_rd_i=1036592&pf_rd_m=ATVPDKIKX0DER&pf_rd_r=1AAXXW54KQA2KHJY14QX',
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(AmazonSpider, self).__init__('amazon', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="brand-directory"]/div/ul/li/a[@href][text()]')
        for node in nav_nodes:
            try:
                brand_text = node.xpath('./text()').extract()[0]
                brand_text = self.reformat(brand_text)
                brand_name = brand_text.lower()
            except(TypeError, IndexError):
                continue

            if brand_text and brand_name:
                brand_id = self.match_known_brand(brand_name)
                if brand_id:
                    m = copy.deepcopy(metadata)
                    m['brand_id'] = brand_id

                    try:
                        href = node.xpath('./@href').extract()[0]
                        href = self.process_href(href, response.url)
                    except(TypeError, IndexError):
                        continue

                    yield Request(url=href,
                                  callback=self.parse_product_nav,
                                  errback=self.onerr,
                                  meta={'userdata': m})

    def parse_product_nav(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="centerSlots"]//div[@class="action"]/a[@href]')
        for node in nav_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="resultsCol"]//div[contains(@id, "result_")]')
        for node in product_nodes:

            see_all_node = node.xpath('.//li[@class="seeAll"]/a[@href]')
            if see_all_node:
                try:
                    see_all_href = see_all_node.xpath('./@href').extract()[0]
                    see_all_href = self.process_href(see_all_href, response.url)
                    if see_all_href:
                        ms = copy.deepcopy(metadata)

                        yield Request(url=see_all_href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': ms})
                except(TypeError, IndexError):
                    pass

            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            m = copy.deepcopy(metadata)

            category_node = node.xpath('.//span[@class="bold orng"][text()]')
            if category_node:
                try:
                    category_text = category_node.xpath('./text()').extract()[0]
                    category_text = self.reformat(category_text)
                    # 去掉:
                    mt = re.search(ur'([^:]+)', category_text)
                    if mt:
                        category_text = mt.group(1)
                    category_name = category_text.lower()

                    if category_name and category_text:
                        m['tags_mapping']['category-0'] = [
                            {'name': category_name, 'title': category_text}
                        ]
                except(TypeError, IndexError):
                    pass

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        next_node = sel.xpath('//div[@id="centerBelowMinus"]//div[@id="pagn"]//a[@id="pagnNextLink"][@href]')
        if next_node:
            try:
                next_href = next_node.xpath('./@href').extract()[0]
                next_href = self.process_href(next_href, response.url)

                yield Request(url=next_href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': metadata})
            except(TypeError, IndexError):
                pass

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        image_urls = []
        image_nodes = sel.xpath('//div[@id="altImages"]//img[@src]')
        for node in image_nodes:
            origin_src = node.xpath('./@src').extract()[0]
            # 去掉了gif图
            mt = re.search(ur'\.gif$', origin_src)
            if mt:
                continue
            src = re.sub(ur'\._\S+?_\.', ur'.', origin_src)
            src = self.process_href(src, response.url)
            if src:
                image_urls += [src]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//input[@id="ftSelectAsin"][@value]')
        if model_node:
            try:
                model = model_node.xpath('./@value').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="centerCol"]//*[@id="productTitle"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath(
            '//div[@id="rightCol"]//div[@id="buybox"]//div[@id="unqualifiedBuyBox"]//span[@class="a-color-price"][text()]')
        if price_node:
            try:
                old_price = price_node.xpath('./text()').extract()[0]
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        description_nodes = sel.xpath('//div[@id="centerCol"]//div[@id="feature-bullets"]/ul/li/span[text()]')
        if description_nodes:
            try:
                description = '\r'.join(cls.reformat(val) for val in description_nodes.xpath('./text()').extract())
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description
