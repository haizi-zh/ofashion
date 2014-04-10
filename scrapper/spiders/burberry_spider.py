# coding=utf-8
import re
import copy

from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
import common as cm
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils_core import unicodify


__author__ = 'Zephyre'


# TODO 检查burberry的update数据时，出现过 一个url 301 到 另一个url 再被 301 到 原来url 然后 200 的现象，还不清楚出现这个现象的原因
# 在update_spider的start_requests中产生的request加入dont_filter=True后，解决了上边的问题

# 还会有循环重定向 http://au.burberry.com/cashmere-hooded-top-p39057921


class BurberrySpider(MFashionSpider):
    handle_httpstatus_list = [403]
    spider_data = {'brand_id': 10057,
                   'currency': {'sg': 'SGD'}}
    supported_regions = {'cn', 'us', 'fr', 'uk', 'hk', 'jp', 'it', 'sg', 'tw', 'mo', 'au', 'ae', 'de', 'ca', 'es',
                         'ru', 'br', 'kr', 'my'}

    @classmethod
    def get_supported_regions(cls):
        return cls.supported_regions

    def __init__(self, region):
        self.spider_data['hosts'] = {k: str.format('http://{0}.burberry.com', k) for k in self.supported_regions}
        self.spider_data['home_urls'] = self.spider_data['hosts']
        super(BurberrySpider, self).__init__('burberry', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        m = re.search(r'([a-zA-Z]{2})\.burberry\.com', response.url)
        if m:
            hxs = Selector(response)
            for item in hxs.xpath(
                    "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link "
                    "l-1-link-open']//li/a[@href and @title]"):
                href = item.xpath('@href').extract()[0]
                # TODO What is cat?
                cat = self.reformat(re.sub(r'/', '', href)).lower()
                title = self.reformat(item.xpath('@title').extract()[0])
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-1'] = [{'name': cat, 'title': title}]
                gender = cm.guess_gender(cat)
                if gender:
                    m['gender'] = [gender]
                yield Request(url=self.process_href(href, response.url), meta={'userdata': m}, dont_filter=True,
                              callback=self.parse_category_1, errback=self.onerr)

    def parse_category_1(self, response):
        metadata = response.meta['userdata']
        hxs = Selector(response)
        for item in hxs.xpath(
                "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
                "//li[@class='l-2-link']/a[@href and @title]"):
            href = item.xpath('@href').extract()[0]
            # TODO What is cat?
            cat = self.reformat(re.sub(r'/', '', href)).lower()
            title = self.reformat(item.xpath('@title').extract()[0])
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
            yield Request(url=self.process_href(href, response.url), meta={'userdata': m}, dont_filter=True,
                          callback=self.parse_category_2, errback=self.onerr)

    def parse_category_2(self, response):
        metadata = response.meta['userdata']

        hxs = Selector(response)
        temp = hxs.xpath(
            "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
            "//li[@class='l-2-link']//li[@class='l-3-link']/a[@href and @title]")
        if not temp:
            ret = self.parse_category_3(response)
            for item in ret:
                yield item
        else:
            for item in temp:
                href = item.xpath('@href').extract()[0]
                # TODO What is cat?
                cat = self.reformat(re.sub(r'/', '', href)).lower()
                title = self.reformat(item.xpath('@title').extract()[0])
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-3'] = [{'name': cat, 'title': title}]
                yield Request(url=self.process_href(href, response.url), meta={'userdata': m}, dont_filter=True,
                              callback=self.parse_category_3, errback=self.onerr)

    def parse_category_3(self, response):
        self.log(unicode.format(u'PARSE_CAT_3: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']
        hxs = Selector(response)
        for item in hxs.xpath("//div[@id='product_split' or @id='product_list']//div[contains(@class,'products')]/"
                              "ul[contains(@class,'product-set')]/li[contains(@class, 'product')]"
                              "/a[@href and @data-product-id]"):
            model = item.xpath('@data-product-id').extract()[0]
            m = copy.deepcopy(metadata)
            m['model'] = model
            url = self.process_href(item.xpath('@href').extract()[0], response.url)
            m['url'] = url
            yield Request(url=url, meta={'userdata': m}, dont_filter=True, callback=self.parse_details,
                          errback=self.onerr)

    def parse_details(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']

        hxs = Selector(response)
        # 访问商品的其它颜色版本
        ret = hxs.xpath("//div[contains(@class,'colors')]/ul[contains(@class,'color-set')]"
                        "/li[contains(@class,'color') and not(contains(@class,'color-selected'))]"
                        "/a[@title and @data-color-link]")
        for node in ret:
            m = copy.deepcopy(metadata)
            m['color'] = [self.reformat(unicodify(node.xpath('@title').extract()[0])).lower()]
            url = self.process_href(node.xpath('@data-color-link').extract()[0], response.url)
            m['url'] = url
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

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

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        if 'name' in metadata and 'details' in metadata and 'description' in metadata:
            ret = hxs.xpath("//div[@class='product_detail_container']/div[@class='product_viewer']"
                            "//ul[@class='product-media-set']/li[@class='product-image']/img[@src]/@src").extract()
            image_urls = [self.process_href(val, response.url) for val in ret]
            item = ProductItem()
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            yield item
        else:
            self.log(unicode.format(u'INVALID ITEM: {0}', metadata['url']).encode('utf-8'), log.ERROR)

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
        try:
            tmp = sel.xpath('//p[contains(@class,"product-id")]/text()').extract()
            if tmp and tmp[0]:
                mt = re.search(r'(\d+)', cls.reformat(tmp[0]))
                if mt:
                    model = mt.group(1)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            tmp = sel.xpath("//div[@class='price']//span[@class='price-amount']/text()").extract()
            if tmp and tmp[0]:
                old_price = cls.reformat(tmp[0])
            tmp = sel.xpath("//div[@class='price']//span[@class='price-sale']/text()").extract()
            if tmp and tmp[0]:
                new_price = cls.reformat(tmp[0])
        except(TypeError, IndexError):
            pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        try:
            tmp = sel.xpath("//div[@class='product-title-container']/h1/text()").extract()
            if tmp:
                name = cls.reformat(tmp[0])
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        try:
            tmp = sel.xpath("//li[@id='description-panel']//ul//li/text()").extract()
            if tmp:
                description = ', '.join(cls.reformat(val) for val in tmp if val)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        details = None
        try:
            tmp = sel.xpath("//li[@id='feature-care-panel']//ul//li/text()").extract()
            if tmp:
                details = ', '.join(cls.reformat(val) for val in tmp if val)
        except(TypeError, IndexError):
            pass

        return details

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = None
        # 当前选择地商品
        ret = sel.xpath("//div[contains(@class,'colors')]/ul[contains(@class,'color-set')]"
                        "/li[contains(@class,'color') and contains(@class,'color-selected')]"
                        "/a[@title and @data-color-link]/@title").extract()
        # 本页面商品的颜色
        try:
            colors = [cls.reformat(val).lower() for val in ret]
        except (IndexError, TypeError):
            pass

        return colors
