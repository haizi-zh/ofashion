# coding=utf-8
import re
import datetime
from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper import utils
from scrapper.items import ProductItem
import global_settings as glob
import common as cm
import copy
from scrapper.spiders.mfashion_spider import MFashionSpider

__author__ = 'Zephyre'


class BurberrySpider(MFashionSpider):
    handle_httpstatus_list = [403]
    spider_data = {'brand_id': 10057}

    @classmethod
    def get_supported_regions(cls):
        return BurberrySpider.spider_data['hosts'].keys()

    def __init__(self, region):
        supported_regions = {'cn', 'us', 'fr', 'uk', 'hk', 'jp', 'it', 'sg', 'tw', 'mo', 'au', 'ae', 'de', 'ca', 'es',
                             'ru', 'br', 'kr', 'my'}
        self.spider_data['hosts'] = {k: str.format('http://{0}.burberry.com', k) for k in supported_regions}
        self.spider_data['home_urls'] = self.spider_data['hosts']
        super(BurberrySpider, self).__init__('burberry', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']
        m = re.search(r'([a-zA-Z]{2})\.burberry\.com', response.url)
        if m:
            region = metadata['region']

            hxs = Selector(response)
            for item in hxs.xpath(
                    "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link "
                    "l-1-link-open']//li/a[@href]"):
                href = item._root.attrib['href']
                cat = utils.unicodify(re.sub(r'/', '', href)).lower()
                title = utils.unicodify(item._root.attrib['title'])
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-1'] = [{'name': cat, 'title': title}]
                if cat in {'women', 'femme', 'donna'}:
                    m['gender'] = [u'female']
                elif cat in {'men', 'homme', 'uomo'}:
                    m['gender'] = [u'male']
                yield Request(url=self.process_href(href, region),
                              meta={'userdata': m}, dont_filter=True, callback=self.parse_category_1,
                              errback=self.onerr)

    def parse_category_1(self, response):
        self.log(unicode.format(u'PARSE_CAT_1: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']
        region = metadata['region']
        hxs = Selector(response)
        for item in hxs.xpath(
                "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
                "//li[@class='l-2-link']/a[@href]"):
            href = item._root.attrib['href']
            cat = utils.unicodify(re.sub(r'/', '', href)).lower()
            title = utils.unicodify(item._root.attrib['title'])
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
            m['category'] = [cat]
            yield Request(url=self.process_href(href, region),
                          meta={'userdata': m}, dont_filter=True, callback=self.parse_category_2,
                          errback=self.onerr)

    def parse_category_2(self, response):
        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        # metadata = self.extract_metadata(response.meta)
        metadata = response.meta['userdata']
        region = metadata['region']

        hxs = Selector(response)
        temp = hxs.xpath(
            "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
            "//li[@class='l-2-link']//li[@class='l-3-link']/a[@href]")
        if not temp:
            ret = self.parse_category_3(response)
            for item in ret:
                yield item
        else:
            for item in temp:
                href = item._root.attrib['href']
                cat = utils.unicodify(re.sub(r'/', '', href)).lower()
                title = utils.unicodify(item._root.attrib['title'])
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-3'] = [{'name': cat, 'title': title}]
                yield Request(url=self.process_href(href, region),
                              meta={'userdata': m}, dont_filter=True, callback=self.parse_category_3,
                              errback=self.onerr)

    def parse_category_3(self, response):
        self.log(unicode.format(u'PARSE_CAT_3: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']
        region = metadata['region']

        hxs = Selector(response)
        for item in hxs.xpath("//div[@id='product_split' or @id='product_list']//div[contains(@class,'products')]/"
                              "ul[contains(@class,'product-set')]/li[contains(@class, 'product')]/a[@href]"):
            if 'data-product-id' not in item._root.attrib:
                continue
            model = item._root.attrib['data-product-id']
            m = copy.deepcopy(metadata)
            m['model'] = model
            url = self.process_href(item._root.attrib['href'], region)
            m['url'] = url
            yield Request(url=url, meta={'userdata': m}, dont_filter=True, callback=self.parse_details,
                          errback=self.onerr)

    def parse_details(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url).encode('utf-8'), level=log.DEBUG)
        metadata = response.meta['userdata']

        hxs = Selector(response)
        ret = hxs.xpath("//div[contains(@class,'colors')]/ul[contains(@class,'color-set')]/"
                        "li[contains(@class,'color')]/a[@title and @data-color-link]")
        # 访问商品的其它颜色版本
        for node in (val for val in ret if val._root.attrib['data-color-link'] not in metadata['url']):
            m = copy.deepcopy(metadata)
            m['color'] = [self.reformat(cm.unicodify(node._root.attrib['title'])).lower()]
            url = self.process_href(node._root.attrib['data-color-link'], metadata['region'])
            m['url'] = url
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

        # 本页面商品的颜色
        tmp = [val for val in ret if val._root.attrib['data-color-link'] in metadata['url']]
        if tmp:
            metadata['color'] = [self.reformat(cm.unicodify(tmp[0]._root.attrib['title'])).lower()]

        ret = hxs.xpath('//p[contains(@class,"product-id")]')
        if ret:
            mt = re.search(r'(\d+)', self.reformat(cm.unicodify(ret[0]._root.text)))
            if mt:
                metadata['model'] = mt.group(1)

        ret = hxs.xpath("//div[@class='price']//span[@class='price-amount']")
        if ret:
            metadata['price'] = ret[0]._root.text

        ret = hxs.xpath("//li[@id='description-panel']//ul//li")
        if ret:
            metadata['description'] = u', '.join(filter(lambda x: x, (val._root.text for val in ret)))
        ret = hxs.xpath("//li[@id='feature-care-panel']//ul//li")
        if ret:
            metadata['details'] = u', '.join(filter(lambda x: x, (val._root.text for val in ret)))
        ret = hxs.xpath("//div[@class='product-title-container']/h1")
        if ret:
            metadata['name'] = utils.unicodify(ret[0]._root.text.strip() if ret[0]._root.text is not None else '')

        if 'name' in metadata and 'details' in metadata and 'description' in metadata:
            ret = hxs.xpath(
                "//div[@class='product_detail_container']/div[@class='product_viewer']//ul[@class='product-media-set']/"
                "li[@class='product-image']/img[@src]")
            image_urls = [val._root.attrib['src'] for val in ret]
            item = ProductItem()
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            yield item
        else:
            self.log(unicode.format(u'INVALID ITEM: {0}', metadata['url']).encode('utf-8'), log.ERROR)
