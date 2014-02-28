# coding=utf-8
import copy
from urlparse import urljoin
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import re


__author__ = 'wuya'
#brand_id	brand_name	url
#10100	De Beers	http://www.debeers.com/?region=true



class DebeersSpider(MFashionSpider):
    spider_data = {'brand_id': 10100, }
    home_urls = {
        'uk': 'http://www.debeers.co.uk/?region=true',
        'us': 'http://www.debeers.com/?region=true',
        'fr': 'http://www.debeers.fr/?region=true',
        'jp': 'http://www.debeers.co.jp/?region=true',
        'cn': 'http://www.debeers.com.cn/?region=true',
        'hk': 'http://www.debeers.hk/?region=true',
    }
    spider_data['home_urls'] = home_urls

    def __init__(self, region):
        super(DebeersSpider, self).__init__('debeers', region)

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//ul[@class="nav-link-set"]/li[position()<3]/div//a'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        for link in links:
            m = copy.deepcopy(metadata)
            cat_title = link.text
            cat_name = cat_title.lower()
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            url = link.url
            yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//li[contains(@class,"product")]//a'))
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

        image_urls = []
        image_srcs = sel.xpath('//li[@class="product-image"]//img/@src').extract()
        for src in image_srcs:
            large_src = re.sub(ur'/\d+/\d+/', '/0/0/', src)
            if large_src:
                image_urls += [large_src]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

    @classmethod
    def is_offlie(cls, response):
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
            model = ''.join(sel.xpath('//div[@class="product-id"]//text()').extract())
            model = model.split('#')[1]
            model = cls.reformat(model)
        except(TypeError, IndexError):
            model = None
            pass

        return model

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        try:
            name = ''.join(sel.xpath('//h1[@class="product-title"]//text()').extract())
            name = cls.reformat(name)
        except(TypeError, IndexError):
            name = None
            pass

        return name

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            price = ''.join(sel.xpath('//p[@class="product-price"]//text()').extract())
            price_discount = None
            # if not price:
            #     price = ''.join(sel.xpath('//div[@class="product-main-info"]//p[@class="old-price"]//span[2]//text()').extract())
            #     price_discount = ''.join(sel.xpath('//div[@class="product-main-info"]//p[@class="special-price"]//span[2]//text()').extract())
            if price:
                old_price = cls.reformat(price)
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
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            desc = ''.join(sel.xpath('//div[@class="product-description-complete"]//text()').extract())
            desc = cls.reformat(desc)
            if not desc:
                description = ''.join(sel.xpath('//div[@class="product-description-truncated"]//text()').extract())
                description = cls.reformat(description)
            else:
                description = desc
        except(TypeError, IndexError):
            description = None
            pass

        return description

    # @classmethod
    # def fetch_details(cls, response):
    #     sel = Selector(response)
    #
    #     details = None
    #     try:
    #         details = ''.join(sel.xpath('//div[@class="product-description-complete"]//text()').extract())
    #         details = cls.reformat(details)
    #     except(TypeError, IndexError):
    #         details = None
    #         pass
    #
    #     return details
