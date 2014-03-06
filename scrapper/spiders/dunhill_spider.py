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
#10113	Dunhill	http://www.dunhill.co.uk/the-shop/


#备注：
#部分区域金做展示，没有价格。


class DunhillSpider(MFashionSpider):
    spider_data = {'brand_id': 10113, }
    home_urls = {
        'uk': ['http://www.dunhill.co.uk/'],
        'us': ['http://www.dunhill.com/'],
        'de': ['http://en-de.dunhill.com/'],
        'fr': ['http://en-fr.dunhill.com/'],
        'it': ['http://en-it.dunhill.com/'],
        'nl': ['http://en-nl.dunhill.com/'],
        'es': ['http://en-es.dunhill.com/'],
        'cn': ['http://zh-cn.dunhill.com/'],
        'jp': ['http://ja-jp.dunhill.com/'],
        'hk': ['http://zh-hk.dunhill.com/'],
        'sg': ['http://en-sg.dunhill.com/'],
        'my': ['http://en-my.dunhill.com/'],
        'kr': ['http://ko-kr.dunhill.com/'],
    }
    spider_data['home_urls'] = home_urls


    def __init__(self, region):
        super(DunhillSpider, self).__init__('dunhill', region)

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        m = metadata
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@class="shared_header"]//li/a'))
        links = link_extractor.extract_links(response)
        enter_url = links[0].url
        yield Request(url=enter_url, callback=self.parse_type, errback=self.onerr, meta={'userdata': m})
        # sel = Selector(response)
        # sel.xpath('/@href')[0].extract()


    def parse_type(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@class="inner-nav-content"]//a'))
        links = link_extractor.extract_links(response)
        if links:
            results = self.parse_cat(response)
        else:
            results = self.parse_list(response)
        for result in results:
            yield result


    def parse_cat(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@class="inner-nav-content"]//a'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        for link in links:
            m = copy.deepcopy(metadata)
            url = link.url
            cat_title = link.text
            cat_name = cat_title.lower()
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            yield Request(url=url, callback=self.parse_type, errback=self.onerr, meta={'userdata': m})

    def parse_list(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@class="product_grid"]//a'))
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

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        # 无简要描述
        # description = ''.join(sel.xpath('//div[@id="tab-content_1"]//text()').extract())
        # metadata['description'] = self.reformat(description)

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        image_urls = sel.xpath('//ul[@class="product-image-set"]//li//a[@data-flash-zoom]/@data-flash-zoom').extract()

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
    def fetch_model(cls, response, spider):
        sel = Selector(response)

        model = None
        try:
            model = ''.join(sel.xpath('//div[@class="product-id"]//text()').extract())
            model = cls.reformat(model)
        except(TypeError, IndexError):
            model = None
            pass

        return model

    @classmethod
    def fetch_name(cls, response, spider):
        sel = Selector(response)

        name = None
        try:
            name = ''.join(sel.xpath('//h1//text()').extract())
            name = cls.reformat(name)
        except(TypeError, IndexError):
            name = None
            pass

        return name

    @classmethod
    def fetch_price(cls, response, spider):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            price = ''.join(sel.xpath('//div[contains(@class,"product-price")]//text()').extract())
            price_discount = None
            if not price:
                price = ''.join(sel.xpath('//span[contains(@class,"product-price-old")]//text()').extract())
                price_discount = ''.join(
                    sel.xpath('//span[contains(@class,"product-price-markdown")]//text()').extract())
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
    def fetch_details(cls, response, spider):
        sel = Selector(response)

        details = None
        try:
            details_sel = sel.xpath('//ul[@class="ctg-accordion-set"]//li')[:2]
            # 去掉有js代码的
            details = '\r'.join(
                cls.reformat(val) for val in details_sel.xpath('./*[not(@type="text/javascript")]//text()').extract())
            details = cls.reformat(details)
        except(TypeError, IndexError):
            details = None
            pass

        return details
