# coding=utf-8
from itertools import ifilter
import os

import re
import datetime
from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.mail import MailSender
from scrapy.selector import HtmlXPathSelector
from scrapper import utils
from scrapper.items import ProductItem
import global_settings as glob
import copy

__author__ = 'Zephyre'

brand_id = 10057


def create_spider():
    return BurberrySpider()


def supported_regions():
    return BurberrySpider.spider_data['supported_regions']


class BurberrySpider(CrawlSpider):
    name = 'burberry'
    # allowed_domains = ['burberry.com']

    spider_data = {'host': {'cn': 'http://cn.burberry.com',
                            'us': 'http://us.burberry.com',
                            'fr': 'http://fr.burberry.com',
                            'uk': 'http://uk.burberry.com',
                            'hk': 'http://hk.burberry.com',
                            'jp': 'http://jp.burberry.com',
                            'it': 'http://it.burberry.com',
                            'sg': 'http://sg.burberry.com',
                            'tw': 'http://sg.burberry.com',
                            'mo': 'http://mo.burberry.com',
                            'au': 'http://au.burberry.com',
                            'ae': 'http://ae.burberry.com',
                            'de': 'http://de.burberry.com',
                            'ca': 'http://ca.burberry.com',
                            'es': 'http://es.burberry.com',
                            'ru': 'http://ru.burberry.com',
                            'br': 'http://br.burberry.com',
                            'kr': 'http://kr.burberry.com',
                            'my': 'http://my.burberry.com', }}
    spider_data['supported_regions'] = spider_data['host'].keys()

    def __init__(self, *a, **kw):
        super(BurberrySpider, self).__init__(*a, **kw)
        self.spider_data = copy.deepcopy(BurberrySpider.spider_data)
        self.spider_data['brand_id'] = brand_id
        for k, v in glob.BRAND_NAMES[self.spider_data['brand_id']].items():
            self.spider_data[k] = v

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.log(str.format('Fetching data for {0}', region), log.INFO)
        if region in self.spider_data['host']:
            return [Request(url=self.spider_data['host'][region], callback=self.parse, errback=self.errback)]
            # return [Request(url='http://cn.burberry.com/psfas/2.html', callback=self.parse, errback=self.errback)]
        else:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []

    def errback(self, reason):
        log.msg(str.format('ERROR on {0}', reason.request._url), log.ERROR)

    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url), level=log.DEBUG)
        m = re.search(r'([a-zA-Z]{2})\.burberry\.com', response.url)
        if m:
            metadata = {'region': m.group(1)}

            metadata['tags_mapping'] = {}
            metadata['extra'] = {}

            region = metadata['region']

            hxs = HtmlXPathSelector(response)
            for item in hxs.select(
                    "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
                    "//li/a[@href]"):
                href = item._root.attrib['href']
                cat = utils.unicodify(re.sub(r'/', '', href)).lower()
                title = utils.unicodify(item._root.attrib['title'])
                if title:
                    title = title.lower()
                m = copy.deepcopy(metadata)
                m['extra']['category-1'] = [cat]
                m['tags_mapping']['category-1'] = [{'name': cat, 'title': title}]
                if cat in {'women', 'femme', 'donna'}:
                    m['gender'] = [u'female']
                elif cat in {'men', 'homme', 'uomo'}:
                    m['gender'] = [u'male']
                else:
                    m['gender'] = []
                url = self.spider_data['host'][region] + href
                yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_1)

    def parse_category_1(self, response):
        self.log(unicode.format(u'PARSE_CAT_1: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        region = metadata['region']

        hxs = HtmlXPathSelector(response)
        for item in hxs.select(
                "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
                "//li[@class='l-2-link']/a[@href]"):
            href = item._root.attrib['href']
            cat = utils.unicodify(re.sub(r'/', '', href)).lower()
            title = utils.unicodify(item._root.attrib['title'])
            if title:
                title = title.lower()
            m = copy.deepcopy(metadata)
            m['extra']['category-2'] = [cat]
            m['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
            m['category'] = [cat]
            url = self.spider_data['host'][region] + href
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_2)

    def parse_category_2(self, response):
        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url), level=log.DEBUG)
        # metadata = self.extract_metadata(response.meta)
        metadata = response.meta['userdata']
        region = metadata['region']

        hxs = HtmlXPathSelector(response)
        for item in hxs.select(
                "//div[@id='shared_sidebar']//div[@id='nav']//ul[@class='l-1-set']//li[@class='l-1-link l-1-link-open']"
                "//li[@class='l-2-link']//li[@class='l-3-link']/a[@href]"):
            href = item._root.attrib['href']
            cat = utils.unicodify(re.sub(r'/', '', href)).lower()
            title = utils.unicodify(item._root.attrib['title'])
            if title:
                title = title.lower()
            m = copy.deepcopy(metadata)
            m['extra']['category-3'] = [cat]
            m['tags_mapping']['category-3'] = [{'name': cat, 'title': title}]
            url = self.spider_data['host'][region] + href
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_3)

    def parse_category_3(self, response):
        self.log(unicode.format(u'PARSE_CAT_3: URL={0}', response.url), level=log.DEBUG)
        # metadata = self.extract_metadata(response.meta)
        metadata = response.meta['userdata']
        region = metadata['region']

        hxs = HtmlXPathSelector(response)
        for item in hxs.select("//div[@id='product_split']//div[contains(@class,'products')]/"
                               "ul[contains(@class,'product-set')]/li[contains(@class, 'product')]/a[@href]"):
            href = item._root.attrib['href']
            url = self.spider_data['host'][region] + href
            if 'data-product-id' not in item._root.attrib:
                continue
            model = item._root.attrib['data-product-id']
            m = copy.deepcopy(metadata)
            m['model'] = model
            m['url'] = url
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_details)

    def parse_details(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url), level=log.DEBUG)
        # metadata = self.extract_metadata(response.meta)
        metadata = response.meta['userdata']
        item = ProductItem()

        hxs = HtmlXPathSelector(response)
        ret = hxs.select("//div[@class='price']//span[@class='price-amount']")
        if len(ret) > 0:
            metadata['price'] = ret[0]._root.text
        ret = hxs.select("//div[contains(@class,'colors')]/ul[contains(@class,'color-set')]/"
                         "li[contains(@class,'color')]/a[@title]/@title")
        if len(ret) > 0:
            clrs = filter(lambda x: x, (utils.unicodify(val.extract()) for val in ret))
            metadata['color'] = [c.lower() for sublist in [re.split(u'[|/]', v) for v in clrs] for c in sublist]
            metadata['tags_mapping']['color'] = [{'name': c, 'title': c} for c in metadata['color']]
        ret = hxs.select("//div[contains(@class,'sizes')]/ul[contains(@class,'size-set')]/"
                         "li[contains(@class,'size')]/label[@class='-radio-label']")
        if len(ret) > 0:
            metadata['extra']['size'] = filter(lambda x: x, (utils.unicodify(val._root.text) for val in ret))
        ret = hxs.select("//li[@id='description-panel']//ul//li")
        if len(ret) > 0:
            metadata['description'] = u', '.join(filter(lambda x: x, (val._root.text for val in ret)))
        ret = hxs.select("//li[@id='feature-care-panel']//ul//li")
        if len(ret) > 0:
            metadata['details'] = u', '.join(filter(lambda x: x, (val._root.text for val in ret)))
        for k in {'brand_id', 'brandname_e', 'brandname_c'}:
            metadata[k] = self.spider_data[k]
        ret = hxs.select("//div[@class='product-title-container']/h1")
        if len(ret) > 0:
            metadata['name'] = utils.unicodify(ret[0]._root.text.strip() if ret[0]._root.text is not None else '')
        metadata['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if 'name' in metadata and 'details' in metadata and 'description' in metadata:
            ret = hxs.select(
                "//div[@class='product_detail_container']/div[@class='product_viewer']//ul[@class='product-media-set']/"
                "li[@class='product-image']/img[@src]")
            image_urls = [val._root.attrib['src'] for val in ret]
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            return item
        else:
            return None
