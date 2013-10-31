# coding=utf-8
import copy
import os
import datetime
import re
from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
import global_settings
import common as cm

__author__ = 'Zephyre'

gucci_data = {'base_url': {'cn': 'http://www.gucci.com/cn/home',
                           'us': 'http://www.fendi.com/us/en/collections/woman',
                           'fr': 'http://www.fendi.com/fr/fr/collections/femme',
                           'it': 'http://www.fendi.com/it/it/collezioni/donna',
                           'kr': 'http://www.fendi.com/kr/ko/collections/woman',
                           'jp': 'http://www.fendi.com/jp/ja/collections/woman'},
              'host': 'http://www.gucci.com',
              'brand_id': 10152, 'brandname_e': 'Gucci', 'brandname_c': u'古驰'}


def creat_spider():
    return GucciSpider()


def get_image_path():
    return os.path.normpath(os.path.join(global_settings.HOME_PATH, u'products/images'))


def get_job_path():
    return os.path.normpath(os.path.join(global_settings.HOME_PATH, u'products/crawl/gucci'))


def get_log_path():
    dir_path = os.path.join(settings.HOME_PATH, u'products/log')
    log_path = os.path.normpath(os.path.join(dir_path,
                                             unicode.format(u'{0}_{1}_{2}.log', gucci_data['brand_id'],
                                                            gucci_data['brandname_e'],
                                                            datetime.datetime.now().strftime('%Y%m%d'))))
    return log_path


class GucciSpider(CrawlSpider):
    name = 'gucci'

    def __init__(self, region=None):
        self.region = region

    def start_requests(self):
        if self.region:
            return Request(url=gucci_data['base_url'][self.region])
        else:
            return [Request(url=gucci_data['base_url'][r]) for r in self.crawler.settings.get('REGION_LIST')]

    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url), level=log.INFO)
        if 'www.gucci.com/cn' in response.url:
            metadata = {'region': 'cn'}
        elif 'www.gucci.com/us' in response.url:
            metadata = {'region': 'us'}
        elif 'www.gucci.com/fr' in response.url:
            metadata = {'region': 'fr'}
        else:
            metadata = {'region': None}
        metadata['tags_mapping'] = {}
        metadata['extra'] = {}

        hxs = HtmlXPathSelector(response)
        for node1 in hxs.select("//ul[@id='header_main']/li[@class='mega_menu']"):
            ret = node1.select("./span[@class='mega_link']")
            if len(ret) == 0:
                continue
            else:
                ret = ret[0]
            cat = cm.unicodify(ret._root.text)
            if not cat:
                continue
            m = copy.deepcopy(metadata)
            m['extra']['category-1'] = cat
            m['tags_mapping']['category-1'] = [{'name': cat, 'title': cat}]
            if cat in {u'woman', u'women', u'femme', u'donna', u'女士系列'}:
                m['gender'] = [u'female']
            elif cat in {u'man', u'men', u'homme', u'uomo', u'男士系列'}:
                m['gender'] = [u'male']
            else:
                m['gender'] = []

            for node2 in node1.select("./div/ul/li[not(@class='mega_promo')]/a[@href]"):
                href = cm.unicodify(node2._root.attrib['href'])
                mt = re.search(ur'/([^/]+)/?$', href)
                if not mt:
                    continue
                cat = mt.group(1)
                title = cm.unicodify(node2._root.text)
                if not title:
                    continue
                m2 = copy.deepcopy(m)
                m2['extra']['category-2'] = cat
                m2['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
                m2['category'] = [cat]
                yield Request(url=href, meta={'userdata': m2}, callback=self.parse_category_2)

    def parse_category_2(self, response):
        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url), level=log.INFO)
        metadata = response.meta['userdata']
        hxs = HtmlXPathSelector(response)

        for node in hxs.select("//div[@id='content_slider']/div[@id='panel_wrapper']/div[contains(@class, 'ggpanel')]"
                               "/ul/li[@class='odd' or @class='even']/img/a[@href]"):
            m=copy.deepcopy(metadata)

            pass




