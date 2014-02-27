# coding=utf-8
import copy
import re

from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


from utils.utils import unicodify, iterable
__author__ = 'wuya'
#brand_id	brand_name	url
#10299	 Paul Smith	 http://www.paulsmith.co.uk/uk-en/shop/


#备注：

class PaulsmithSpider(MFashionSpider):
    spider_data = {'brand_id': 10299, }
    home_urls = {'uk': 'http://www.paulsmith.co.uk/uk-en/shop/',
                 'us': 'http://www.paulsmith.co.uk/us-en/shop/',
                 'au': 'http://www.paulsmith.co.uk/au-en/shop/',
                 'jp': 'http://www.paulsmith.co.jp/shop'}
    spider_data['home_urls'] = home_urls

    #强制允许一次重复抓取.否则会被dupefilter过滤掉一次重定向导致爬虫无法运行
    def start_requests(self):
        for request in super(PaulsmithSpider, self).start_requests():
            request.dont_filter=True
            yield request

    def __init__(self, region):
        super(PaulsmithSpider, self).__init__('paulsmith', region)

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        #处理常规部分
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@class="linksList"]//a'))
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
        #处理区域特别部分
        region = metadata['region']
        if region == 'jp':
            extra_urls = [
                'http://www.paulsmith.co.jp/shop/gifts/products',
                'http://www.paulsmith.co.jp/shop/reserve/products',
                'http://www.paulsmith.co.jp/shop/sales/products',
                'http://www.paulsmith.co.jp/shop/paulsmithcollection/products'
            ]
            for url in extra_urls:
                m = copy.deepcopy(metadata)
                yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})
        else:
            extra_urls = [
                'http://www.paulsmith.co.uk/%s-en/shop/valentines-day-gifts/valentines-day-gifts-for-her' % region,
                'http://www.paulsmith.co.uk/%s-en/shop/valentines-day-gifts/valentines-day-gifts-for-him' % region,
            ]
            for url in extra_urls:
                m = copy.deepcopy(metadata)
                yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        #先处理本页商品
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@class="category-products"]//h2//a'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        for link in links:
            m = copy.deepcopy(metadata)
            url = link.url
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})
        #再处理翻页
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//li[@class="next"]//a'))
        links = link_extractor.extract_links(response)
        if links:
            next_page = links[0]
            next_page_url = next_page.url
            m = copy.deepcopy(metadata)
            yield Request(url=next_page_url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

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

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        image_urls = self._get_image_urls(response)

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

    def _get_image_urls(self, response):
        urls = []
        sel = Selector(response)
        rels = sel.xpath('//a//@rel').extract()
        pattern = re.compile(r"largeimage: '(\S*)'}")
        for rel in rels:
            try:
                groups = pattern.findall(rel)
                img_url = groups[0]
                urls.append(img_url)
            except Exception as e:
                continue
        return urls

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
            model = ''.join(cls.reformat(val) for val in sel.xpath('//p[@class="product-ids"]//strong//text()').extract())
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
            name = ''.join(cls.reformat(val) for val in sel.xpath('//div[@class="product-name"]//text()').extract())
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
            price = ''.join(sel.xpath('//div[@class="product-main-info"]//span[@class="regular-price"]//text()').extract())
            price_discount = None
            if not price:
                price = ''.join(sel.xpath('//div[@class="product-main-info"]//p[@class="old-price"]//span[2]//text()').extract())
                price_discount = ''.join(sel.xpath('//div[@class="product-main-info"]//p[@class="special-price"]//span[2]//text()').extract())
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
            description = ''.join(cls.reformat(val) for val in sel.xpath('//div[@id="product-details"]//div[1]//text()').extract())
            description = cls.reformat(description)
        except(TypeError, IndexError):
            description = None
            pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        details = None
        try:
            details = ''.join(cls.reformat(val) for val in sel.xpath('//div[@id="product-details"]//text()').extract())
            details = cls.reformat(details)
        except(TypeError, IndexError):
            details = None
            pass

        return details
