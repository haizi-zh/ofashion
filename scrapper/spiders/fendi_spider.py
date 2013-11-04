# coding=utf-8
import copy
import os
import datetime
import re
from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
from scrapper import utils
from scrapper.items import ProductItem
import global_settings

__author__ = 'Zephyre'

fendi_data = {'base_url': {'cn': 'http://www.fendi.com/cn/zh/collections/woman',
                           'us': 'http://www.fendi.com/us/en/collections/woman',
                           'fr': 'http://www.fendi.com/fr/fr/collections/femme',
                           'it': 'http://www.fendi.com/it/it/collezioni/donna',
                           'kr': 'http://www.fendi.com/kr/ko/collections/woman',
                           'jp': 'http://www.fendi.com/jp/ja/collections/woman',
                           'ii': 'http://www.fendi.com/ii/en/collections/woman',
                           'es': 'http://www.fendi.com/ii/es/colecciones/mujer'},
              'host': 'http://www.fendi.com',
              # 'cat-1-reject': {'cn': ['fashion-show', 'ready-to-wear-and-furwear', 'activewear', 'accessories'],
              #                  'us': ['fashion-show', 'ready-to-wear-and-furwear', 'activewear', 'accessories'], },
              'brand_id': 10135, 'brandname_e': 'Fendi', 'brandname_c': u'芬迪', 'bn_short': 'fendi'}


def create_spider():
    return FendiSpider()


def get_image_path():
    return os.path.normpath(os.path.join(global_settings.STORAGE_PATH, u'products/images'))


def get_job_path():
    return os.path.normpath(
        os.path.join(global_settings.STORAGE_PATH, unicode.format(u'products/crawl/{0}', fendi_data['bn_short'])))



class FendiSpider(CrawlSpider):
    name = 'fendi'

    def __init__(self, region=None):
        self.region = region

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.log(str.format('Fetching data for {0}', region), log.INFO)
        if region in fendi_data['base_url']:
            return [Request(url=fendi_data['base_url'][region], dont_filter=True)]
        else:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []


    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url), level=log.DEBUG)
        if 'www.fendi.com/cn' in response.url:
            metadata = {'region': 'cn'}
        elif 'www.fendi.com/us' in response.url:
            metadata = {'region': 'us'}
        elif 'www.fendi.com/fr' in response.url:
            metadata = {'region': 'fr'}
        elif 'www.fendi.com/it' in response.url:
            metadata = {'region': 'it'}
        elif 'www.fendi.com/kr' in response.url:
            metadata = {'region': 'kr'}
        elif 'www.fendi.com/jp' in response.url:
            metadata = {'region': 'jp'}
        elif 'www.fendi.com/ii/en' in response.url:
            metadata = {'region': 'ii'}
        elif 'www.fendi.com/ii/es' in response.url:
            metadata = {'region': 'es'}
        else:
            metadata = {'region': None}
        metadata['tags_mapping'] = {}
        metadata['extra'] = {}

        hxs = HtmlXPathSelector(response)
        for item in hxs.select("//header[@id='main-header']//ul[@class='links']/li/ul/li/a[@href]"):
            href = utils.unicodify(item._root.attrib['href'])
            title = utils.unicodify(item._root.text)

            if not title:
                continue
            temp = re.search(ur'/([^/]+)/?$', href)
            if not temp:
                continue
            cat = temp.group(1)
            m = copy.deepcopy(metadata)
            m['extra']['category-1'] = [cat]
            m['tags_mapping']['category-1'] = [{'name': cat, 'title': title}]
            if cat in {'woman', 'women', 'femme', 'donna', 'mujer'}:
                m['gender'] = [u'female']
            elif cat in {'man', 'men', 'homme', 'uomo', 'hombre'}:
                m['gender'] = [u'male']
            else:
                m['gender'] = []
            url = fendi_data['host'] + href
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_1, dont_filter=True)


    def parse_category_1(self, response):
        self.log(unicode.format(u'PARSE_CAT_1: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        region = metadata['region']
        hxs = HtmlXPathSelector(response)
        for item in hxs.select("//div[@id='page']//ul[@class='links']//li/a[@href]"):
            href = re.sub(ur'/cover/?', u'', utils.unicodify(item._root.attrib['href']))
            title = utils.unicodify(item._root.text)
            if not title:
                continue
            m = re.search(ur'/([^/]+)/?$', href)
            if not m:
                continue
            cat = m.group(1)
            # if cat in fendi_data['cat-1-reject'][region]:
            #     continue

            m = copy.deepcopy(metadata)
            m['extra']['category-2'] = [cat]
            m['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
            m['category'] = [cat]
            url = fendi_data['host'] + href
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_2)

    def parse_category_2(self, response):
        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        hxs = HtmlXPathSelector(response)

        # 是否有filter？
        ret = hxs.select("//aside[@class='sidebar-actions']//div[@class='filter']//ul/li/a[@href]")
        if len(ret) > 0 and 'filter' not in metadata['extra']:
            for item in ret:
                href = utils.unicodify(item._root.attrib['href'])
                title = utils.unicodify(item._root.text)
                if not title:
                    continue
                m = re.search(ur'/([^/]+)/?$', href)
                if not m:
                    continue
                cat = m.group(1).strip()
                if cat.lower() == u'all':
                    continue
                m = copy.deepcopy(metadata)
                m['extra']['filter'] = [cat]
                m['tags_mapping'][unicode.format(u'filter:{0}', metadata['extra']['category-2'][0])] = [
                    {'name': cat, 'title': title}]
                url = fendi_data['host'] + href
                yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_2)
        else:
            for item in hxs.select(
                    "//div[@id='page']/div[@class='view-all']/ul[@id='slider']/li/a[@href and @data-id]"):
                href = utils.unicodify(item._root.attrib['href'])
                m = copy.deepcopy(metadata)
                url = fendi_data['host'] + href
                yield Request(url=url, meta={'userdata': m}, callback=self.parse_details)

    def parse_details(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url), level=log.DEBUG)
        metadata = copy.deepcopy(response.meta['userdata'])
        region = metadata['region']
        hxs = HtmlXPathSelector(response)
        ret = hxs.select("//aside[@class='sidebar-actions']//div[@class='price']")
        if len(ret) > 0:
            temp = utils.unicodify(ret[0]._root.text)
            metadata['price'] = temp.strip() if temp else None
        ret = hxs.select("//aside[@class='sidebar-actions']//div[@class='desc']")
        if len(ret) > 0:
            temp = utils.unicodify(ret[0]._root.text)
            metadata['description'] = temp.strip() if temp else None
        m = re.search(ur'/([^/]+)/?$', response.url)
        if m:
            metadata['model'] = utils.unicodify(m.group(1))
        else:
            metadata['model'] = None
        metadata['url'] = response.url
        for k in {'brand_id', 'brandname_e', 'brandname_c'}:
            metadata[k] = fendi_data[k]

        if 'price' not in metadata:
            return None

        metadata['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        item = ProductItem()
        item['image_urls'] = []
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        ret = hxs.select("//div[@id='page']/div[@class='fullscreen-image']/a[@href]")
        if len(ret) > 0:
        #     获得图片
            href = utils.unicodify(ret[0]._root.attrib['href'])
            url = fendi_data['host'] + href
            return Request(url=url, meta={'userdata': metadata, 'item': item}, callback=self.parse_image)
        else:
        #     没有图片
            return item

    def parse_image(self, response):
        self.log(unicode.format(u'PARSE_IMAGE: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        item = response.meta['item']
        hxs = HtmlXPathSelector(response)
        image_urls = []
        for node in hxs.select("//div[@id='zoom']/ul[@class='thumbs']/li"):
            temp = {}
            for lv in node.select("./ul[@class='levels']/li/a[@href]"):
                ret = lv.select("./span")
                if len(ret) == 0:
                    continue
                else:
                    ret = ret[0]
                lv_val = ret._root.text
                if not lv_val:
                    continue
                url = lv._root.attrib['href']
                temp[lv_val] = url
            image_urls.append(temp[max(temp.keys())])
        item['image_urls'] = image_urls
        return item
