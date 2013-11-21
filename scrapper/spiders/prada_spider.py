# coding=utf-8
import copy
import json
import re
from scrapy import log
import scrapy.contrib.spiders
from scrapy.http import Request
from scrapy.selector import Selector
import global_settings as glob
import common as cm
from scrapper.items import ProductItem

__author__ = 'Zephyre'

brand_id = 10300


# 实例化
def create_spider():
    return PradaSpider()


def supported_regions():
    return PradaSpider.spider_data['supported_regions']


class PradaSpider(scrapy.contrib.spiders.CrawlSpider):
    name = 'prada'
    spider_data = {'hosts': 'http://store.prada.com',
                   'home_urls': {'cn': 'http://store.prada.com/hans/CN/',
                                 'us': 'http://store.prada.com/en/US/',
                                 'ap': 'http://store.prada.com/hant/AP/',
                                 'au': 'http://store.prada.com/en/AT/',
                                 'be': 'http://store.prada.com/en/BE/',
                                 'dk': 'http://store.prada.com/en/DK/',
                                 'fi': 'http://store.prada.com/en/FI/',
                                 'fr': 'http://store.prada.com/en/FR/',
                                 'de': 'http://store.prada.com/en/DE/',
                                 'gr': 'http://store.prada.com/en/GR/',
                                 'ie': 'http://store.prada.com/en/IE/',
                                 'it': 'http://store.prada.com/en/IT/',
                                 'jp': 'http://store.prada.com/en/JP/',
                                 'lu': 'http://store.prada.com/en/LU/',
                                 'mc': 'http://store.prada.com/en/MC/',
                                 'nl': 'http://store.prada.com/en/NL/',
                                 'pt': 'http://store.prada.com/en/PT/',
                                 'es': 'http://store.prada.com/en/ES/',
                                 'uk': 'http://store.prada.com/en/GB/',
                                 'se': 'http://store.prada.com/en/SE/',
                                 'ch': 'http://store.prada.com/en/CH/',
                   },
    }
    spider_data['supported_regions'] = spider_data['home_urls'].keys()

    def process_href(self, href, host=None):
        if not href or not href.strip():
            return None
        else:
            href = href.strip()

        if re.search('^(http|https)://', href):
            return href
        elif re.search('^//', href):
            return 'http:' + href
        elif re.search('^/', href):
            if not host:
                host = self.spider_data['hosts']
            return host + href

    def onerr(self, reason):
        url_main = None
        response = reason.value.response if hasattr(reason.value, 'response') else None
        if not response:
            self.log(str.format('ERROR ON PROCESSING {0}', reason.request.url), log.ERROR)
            return

        url = response.url

        temp = reason.request.meta
        if 'userdata' in temp:
            metadata = temp['userdata']
            if 'url' in metadata:
                url_main = metadata['url']

        if url_main and url_main != url:
            msg = str.format('ERROR ON PROCESSING {0}, REFERER: {1}, CODE: {2}', url, url_main, response.status)
        else:
            msg = str.format('ERROR ON PROCESSING {1}, CODE: {0}', response.status, url)

        self.log(msg, log.ERROR)

    def __init__(self, *a, **kw):
        super(PradaSpider, self).__init__(*a, **kw)
        self.spider_data = copy.deepcopy(PradaSpider.spider_data)
        self.spider_data['brand_id'] = brand_id
        for k, v in glob.BRAND_NAMES[self.spider_data['brand_id']].items():
            self.spider_data[k] = v

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.name = str.format('{0}-{1}', PradaSpider.name, region)
        if region in self.spider_data['supported_regions']:
            metadata = {'region': region, 'brand_id': brand_id,
                        'brandname_e': glob.BRAND_NAMES[brand_id]['brandname_e'],
                        'brandname_c': glob.BRAND_NAMES[brand_id]['brandname_c'], 'tags_mapping': {}, 'extra': {}}
            return [Request(url=self.spider_data['home_urls'][region], meta={'userdata': metadata})]
        else:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"menu")]/ul[contains(@class,"collections")]/li[contains(@class,'
                              '"collection")]/div/a[@href]'):
            m = copy.deepcopy(metadata)
            href = self.process_href(node._root.attrib['href'])
            mt = re.search('/([^/]+)$', href)
            if mt:
                tag_name = cm.unicodify(mt.group(1)).lower()
                tag_type = 'category-0'
                tag_text = cm.unicodify(node._root.text).lower() if node._root.text else tag_name
                m['extra'][tag_type] = [tag_name]
                m['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]

            yield Request(url=href, callback=self.parse_cat_0, meta={'userdata': m}, errback=self.onerr)

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//section[@id="contents"]/article[contains(@class,"products")]/'
                              'div[contains(@class,"product")]/a[@href]'):
            m = copy.deepcopy(metadata)
            href = self.process_href(node._root.attrib['href'])
            temp = node.xpath('./figcaption/div[@class="name"]')
            if not temp:
                continue
            m['name'] = cm.unicodify(temp[0]._root.text)
            yield Request(url=href, callback=self.parse_details, meta={'userdata': m}, errback=self.onerr)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        temp = sel.xpath('//section[@class="summary"]/div[@class="code"]')
        if temp and temp[0]._root.text:
            metadata['model'] = cm.unicodify(temp[0]._root.text)
        else:
            return None

        temp = sel.xpath('//section[@class="summary"]/div[@class="price"]/span[@class="value"]')
        if temp:
            metadata['price'] = cm.unicodify(temp[0]._root.text)

        temp = sel.xpath('//section[@class="summary"]/div[@class="color"]/div[@class="name"]')
        if temp:
            temp = [val.strip() for val in cm.unicodify(temp[0]._root.text).split('+')]
            metadata['extra']['color'] = temp
            metadata['tags_mapping']['color'] = [{'name': val, 'title': val} for val in temp]
            metadata['color'] = temp

        temp = sel.xpath('//section[@class="details"]/figcaption[@class="description"]/ul/li')
        metadata['description'] = '\n'.join(cm.unicodify(val._root.text) for val in temp if val._root.text)

        temp = sel.xpath('//article[@class="product"]/figure[@class="slider"]/img[@data-zoom-url]')
        image_urls = [self.process_href(val._root.attrib['data-zoom-url']) for val in temp]

        metadata['category'] = metadata['extra']['category-1'] if 'category-1' in metadata['extra'] else \
            metadata['extra']['category-0']

        if metadata['extra']['category-0'] in ('women', 'woman', 'femme', 'donna', 'damen', 'mujer', 'demes',
                                               'vrouw', 'frauen'):
            metadata['gender'] = ['female']
        elif metadata['extra']['category-0'] in ('man', 'men', 'homme', 'uomo', 'herren', 'hombre', 'heren',
                                                 'mann', 'signore'):
            metadata['gender'] = ['male']

        metadata['url'] = response._url
        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        return item

    def parse_cat_0(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # MINI-BAG
        temp = sel.xpath(
            '//article[contains(@class,"sliding-backgrounds")]//a[@href and contains(@class,"background")]')
        if temp:
            return Request(url=self.process_href(temp[0]._root.attrib['href']), callback=self.parse_list,
                           meta={'userdata': metadata}, errback=self.onerr)

        node = None
        temp = sel.xpath('//div[@class="menu"]/ul[@class="collections"]/li[contains(@class,"collection")]/'
                         'div[contains(@class,"name")]/a[@href]')
        if temp:
            for temp1 in temp:
                if self.process_href(temp1._root.attrib['href']) == response._url:
                    node = temp1
                    break
        if not node:
            return None

        ret = []
        for node1 in node.xpath(
                '../../ul[contains(@class,"departments")]/li[contains(@class,"department")]/div/a[@href]'):
            m1 = copy.deepcopy(metadata)
            href = node1._root.attrib['href']
            mt = re.search('/([^/]+)$', href)
            if mt:
                tag_name = cm.unicodify(mt.group(1)).lower()
                tag_type = 'category-1'
                tag_text = cm.unicodify(node1._root.text).lower() if node1._root.text else tag_name
                m1['extra'][tag_type] = [tag_name]
                m1['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]

            # 是否有子分类级别
            for node2 in node1.xpath(
                    '../../ul[contains(@class,"categories")]/li[contains(@class,"category")]//a[@href]'):
                m2 = copy.deepcopy(m1)
                href = node2._root.attrib['href']
                mt = re.search('/([^/]+)$', href)
                if mt:
                    tag_name = cm.unicodify(mt.group(1))
                    tag_type = 'category-2'
                    tag_text = cm.unicodify(node2._root.text) if node2._root.text else tag_name
                    m2['extra'][tag_type] = [tag_name]
                    m2['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]
                ret.append(Request(url=self.process_href(href), meta={'userdata': m2}, callback=self.parse_list,
                                   errback=self.onerr))

        return ret
