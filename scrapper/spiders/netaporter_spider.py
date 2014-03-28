# coding=utf-8
import copy
import re
from scrapy.http import Request
from scrapy.selector import Selector
import common
from scrapper.items import ProductItem
from scrapper.spiders.eshop_spider import EShopSpider

__author__ = 'Ryan'

class NetaporterSpider(EShopSpider):
    spider_data = {
        'brand_id': 8002,
        'home_urls': {
            'us': 'http://www.net-a-porter.com/Shop/AZDesigners?cm_sp=topnav-_-designers-_-designeraz'
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(NetaporterSpider, self).__init__('netaporter', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        brand_nodes = sel.xpath('//div[@id="atoz-page-container"]/div[@class="designer_list_col"]/ul/li/a[@href][text()]')
        for node in brand_nodes:
            try:
                brand_text = node.xpath('./text()').extract()[0]
                brand_text = self.reformat(brand_text)
                brand_name = brand_text.lower()
            except(TypeError, IndexError):
                continue

            brand_id = self.match_known_brand(brand_name)
            if brand_id:
                try:
                    m = copy.deepcopy(metadata)
                    m['brand_id'] = brand_id

                    try:
                        href = node.xpath('./@href').extract()[0]
                        href = self.process_href(href, response.url)
                        href = self.process_href(href, response.url)
                    except(TypeError, IndexError):
                        continue

                    yield Request(url=href,
                                  callback=self.parse_cat,
                                  errback=self.onerr,
                                  meta={'userdata': m})
                except(TypeError, IndexError):
                    pass

    def parse_cat(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//ul[@id="main-nav"]/li[position()>1]/a[@href][text()]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="product-list"]/div/div/a[@href]')
        for node in product_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

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

        details = self.fetch_details(response)
        if details:
            metadata['details'] = details

        image_urls = []
        image_nodes = sel.xpath('//div[@id="thumbnails-container"]/meta[@content]')
        if image_nodes:
            image_urls = [self.process_href(val, response.url) for val in image_nodes.xpath('./@content').extract()]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response, spider)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//meta[@itemprop="sku"][@content]')
        if model_node:
            try:
                model = model_node.xpath('./@content').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//h1[@itemprop="name"][text()]')
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
        price_node = sel.xpath('//span[@itemprop="price"][text()]')
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
        description_node = sel.xpath('//div[@itemprop="description"]//span[@class="en-desc"][text()]')
        if description_node:
            try:
                description = ''.join(description_node.xpath('./text()').extract())
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        details = None
        detail_nodes = sel.xpath('//div[@id="product-details-container"]//li[@class="product-detail"]//span//ul/li[text()]')
        if detail_nodes:
            try:
                details = '\r'.join(cls.reformat(val) for val in detail_nodes.xpath('./text()').extract())
                details = cls.reformat(details)
            except(TypeError, IndexError):
                pass

        return details
