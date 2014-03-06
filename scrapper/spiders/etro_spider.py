# coding=utf-8
import copy
from urlparse import urljoin
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'wuya'
#brand_id	brand_name	url
#10127	 Etro	 http://www.etro.com/en_uk/?___store=en_uk


class EtroSpider(MFashionSpider):
    spider_data = {'brand_id': 10127, }
    home_urls = {
        'uk': 'http://www.etro.com/en_uk/',
        'it': 'http://www.etro.com/it_it/',
        'fr': 'http://www.etro.com/en_fr/',
        'de': 'http://www.etro.com/en_de/',
        'nl': 'http://www.etro.com/en_nl/',
        'es': 'http://www.etro.com/en_es/',
        'cn': 'http://www.etro.com/zh_cn/',
        'jp': 'http://www.etro.com/ja_jp/',
        'kr': 'http://www.etro.com/ko_kr/',
        'us': 'http://www.etro.com/en_us/',
    }
    spider_data['home_urls'] = home_urls


    def __init__(self, region):
        super(EtroSpider, self).__init__('etro', region)

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//ul[@class="category-nav"]//a'))
        links = link_extractor.extract_links(response)
        for link in links:
            m = copy.deepcopy(metadata)
            url = link.url
            cat_title = link.text
            cat_name = cat_title.lower()
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@class="category-products"]//a'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        for link in links:
            m = copy.deepcopy(metadata)
            url = link.url
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

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        # detail = self.fetch_details(response)
        # if detail:
        #     metadata['details'] = detail

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        image_urls = sel.xpath('//ul[@class="more-views product-more-images"]//a/@data-big').extract()

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        try:
            model = ''.join(sel.xpath('//span[@class="product-code generic-title"]//text()').extract())
            model = model.split(':')[1]
            model = cls.reformat(model)
        except(TypeError, IndexError):
            model = None
            pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        try:
            name = ''.join(sel.xpath('//div[@class="wrap-titles"]//h2[@class="product-name"]//text()').extract())
            name = cls.reformat(name)
        except(TypeError, IndexError):
            name = None
            pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            price = ''.join(
                sel.xpath('//div[@class="product-main-info"]//span[@class="regular-price"]//text()').extract())
            price_discount = None
            if not price:
                price = ''.join(
                    sel.xpath('//div[@class="product-main-info"]//p[@class="old-price"]//span//text()').extract())
                price_discount = ''.join(
                    sel.xpath('//div[@class="product-main-info"]//p[@class="special-price"]//span//text()').extract())
            if price:
                old_price = classmethod.reformat(price)
                if price_discount:
                    new_price = cls.reformat(price_discount)
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
        try:
            desc = '\r'.join(cls.reformat(val) for val in sel.xpath('//div[@class="generic-info"]//text()').extract())
            desc = cls.reformat(desc)
            if not desc:
                description = '\r'.join(
                    cls.reformat(val) for val in sel.xpath('//p[@class="short-description text"]//text()').extract())
                description = cls.reformat(description)
            else:
                description = desc
        except(TypeError, IndexError):
            description = None
            pass

        return description

        # @classmethod
        # def fetch_details(cls, response, spider=None):
        #     sel = Selector(response)
        #
        #     details = None
        #     try:
        #         details = ''.join(sel.xpath('//div[@class="generic-info"]//text()').extract())
        #         details = cls.reformat(details)
        #     except(TypeError, IndexError):
        #         details = None
        #         pass
        #
        #     return details
