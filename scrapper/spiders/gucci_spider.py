# coding=utf-8
import copy
import json
import os
import datetime
import re
from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
import global_settings as glob
import common as cm
from scrapper.items import ProductItem

__author__ = 'Zephyre'

gucci_data = {'base_url': 'http://www.gucci.com/{0}/home',
              # 'details_base': 'http://www.gucci.com/{0}/styles{1}',
              'host': 'http://www.gucci.com',
              'supported_regions': {'cn', 'us', 'fr', 'de', 'es', 'it', 'nl', 'ae', 'jp', 'kr', 'au', 'bg', 'cz', 'dk',
                                    'fi', 'hu', 'ie', 'no', 'pl', 'pt', 'ro', 'si', 'se', 'ch', 'tr', 'uk', 'at', 'ca',
                                    'be', },
              'brand_id': 10152, 'brandname_e': 'Gucci', 'brandname_c': u'古驰', 'brandname_s': 'gucci'}


def region_term(region):
    return 'ca-en' if region == 'ca' else region


def create_spider():
    return GucciSpider()


def get_spider_data():
    return dict((k, gucci_data[k]) for k in gucci_data if
                k in ('host', 'supported_regions', 'brand_id', 'brandname_e', 'brandname_c', 'brandname_s'))


class GucciSpider(CrawlSpider):
    name = 'gucci'
    allowed_domains = ['gucci.com']

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.name = str.format('{0}-{1}', self.name, region)
        if region in gucci_data['supported_regions']:
            return [Request(url=str.format(gucci_data['base_url'], region_term(region)))]
        else:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []

    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url), level=log.DEBUG)
        mt = re.search(r'www\.gucci\.com/([a-z]{2})', response.url)
        if mt:
            region = mt.group(1)
            metadata = {'region': region, 'brand_id': gucci_data['brand_id'], 'brandname_e': gucci_data['brandname_e'],
                        'brandname_c': gucci_data['brandname_c'], 'tags_mapping': {}, 'extra': {}}
            hxs = HtmlXPathSelector(response)
            for node1 in hxs.select("//ul[@id='header_main']/li[contains(@class, 'mega_menu')]"):
                span = node1.select("./span[@class='mega_link']")
                if len(span) == 0:
                    continue
                span = span[0]
                inner = span.select('.//cufontext')
                if len(inner) > 0:
                    cat = cm.unicodify(inner[0]._root.text)
                else:
                    cat = cm.unicodify(span._root.text)
                if not cat:
                    continue
                else:
                    cat = cat.strip()

                m = copy.deepcopy(metadata)
                m['extra']['category-1'] = [cat]
                m['tags_mapping']['category-1'] = [{'name': cat, 'title': cat}]
                if cat in {u'woman', u'women', u'femme', u'donna', u'女士系列', u'shop women', u'여성 쇼핑', u'damen',
                           u'mujer'}:
                    m['gender'] = [u'female']
                elif cat in {u'man', u'men', u'homme', u'uomo', u'男士系列', u'shop men', u'남성 쇼핑', u'herren', u'hombre'}:
                    m['gender'] = [u'male']
                else:
                    m['gender'] = []

                for node2 in node1.select("./div/ul/li[not(@class='mega_promo')]/a[@href]"):
                    href = cm.unicodify(node2._root.attrib['href'])
                    inner = node2.select('.//cufontext')
                    if len(inner) > 0:
                        title = cm.unicodify(inner[0]._root.text)
                    else:
                        title = cm.unicodify(node2._root.text)
                    if not title:
                        continue
                    else:
                        title = title.strip()
                    mt = re.search(ur'/([^/]+)/?$', href)
                    if not mt:
                        continue
                    cat = cm.unicodify(mt.group(1))
                    if not cat:
                        continue
                    else:
                        cat = cat.strip()

                    m2 = copy.deepcopy(m)
                    m2['extra']['category-2'] = [cat]
                    m2['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
                    m2['category'] = [cat]
                    if href.find('http://') == -1:
                        continue
                    yield Request(url=href, meta={'userdata': m2}, callback=self.parse_category_2)

    def parse_category_2(self, response):
        def func(node, m, with_tag=True):
            href = node._root.attrib['href']
            if with_tag:
                mt = re.search(ur'/([^/]+)/?$', href)
                if not mt:
                    return None
                cat = cm.unicodify(mt.group(1))
                if not cat:
                    return None
                else:
                    cat = cat.strip()
                title = cm.unicodify(node._root.text)
                if not title:
                    return None
                else:
                    title = title.strip()
                m['extra']['category-3'] = [cat]
                m['tags_mapping']['category-3'] = [{'name': cat, 'title': title}]
            return Request(url=href, meta={'userdata': m}, callback=self.parse_category_3, dont_filter=True)

        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        hxs = HtmlXPathSelector(response)

        for node in hxs.select(
                '//section[@id="sub_nav"]/div[@class="content"]/ul[@id="topsort"]/li[@class="full_row"]/a[@href]'):
            ret = func(node, copy.deepcopy(metadata), with_tag=False)
            if ret:
                yield ret
        for node in hxs.select(
                '//section[@id="sub_nav"]/div[@class="content"]/ul[@id="topsort"]/li[not(@class="full_row")]/a[@href]'):
            ret = func(node, copy.deepcopy(metadata))
            if ret:
                yield ret


    def parse_category_3(self, response):
        self.log(unicode.format(u'PARSE_CAT_3: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        hxs = HtmlXPathSelector(response)
        for node in hxs.select(
                '//div[contains(@class,"ggpanel")]//li[contains(@class,"odd") or contains(@class,"even")]/img[@rel]'):
            rel = node._root.attrib['rel']
            mt = re.search(r'href="([^"]+)"', rel)
            if not mt:
                continue
            url = mt.group(1)
            m = copy.deepcopy(metadata)
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_details, dont_filter=True)

    def parse_variations(self, response):
        self.log(unicode.format(u'PARSE_VARIATIONS: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        hxs = HtmlXPathSelector(response)
        for node in hxs.select(
                '//div[@id="variations"]/div[@id="container_variations"]//ul[@class="items"]/li/a[@href]'):
            href = node._root.attrib['href'][1:]
            url = gucci_data['host'] + '/' + href
            m = copy.deepcopy(metadata)
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_details, dont_filter=True)

    def parse_details(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        hxs = HtmlXPathSelector(response)

        title = None
        node = hxs.select('//section[@id="column_description"]//span[@class="container_title"]/h1/span')
        if len(node) > 0:
            node = node[0]
            inner = node.select('.//cufontext')
            if len(inner) == 0:
                title = cm.unicodify(node._root.text)
            else:
                title = u''.join(val._root.text for val in inner if val._root.text)

        node = hxs.select('//div[@id="accordion_left"]//div[@id="description"]//ul/li')
        desc = u'\n'.join(cm.unicodify(val._root.text) for val in node if val._root.text)

        node = hxs.select('//div[@id="zoom_in_window"]/div[@class="zoom_in"]/img[@src]')
        if len(node) > 0:
            href = node[0]._root.attrib['src']
            image_base = os.path.split(href)[0]
            node_list = hxs.select('//div[@id="zoom_tools"]/ul[@id="view_thumbs_list"]/li/img[@src]')
            image_list = set([])
            for node in node_list:
                href = node._root.attrib['src']
                pic_name = os.path.split(href)[1]
                idx = pic_name.find('web_variation')
                if idx == -1:
                    continue
                pic_name = pic_name.replace('web_variation.', 'web_zoomin.')
                image_list.add(str.format('{0}/{1}', image_base, pic_name))
            metadata['image_urls'] = image_list

        if title:
            metadata['name'] = title
        if desc:
            metadata['description'] = desc
        metadata['url'] = response.url

        style_id = os.path.split(response.url)[1]
        url = str.format('{0}/{1}/styles/{2}/load_style.js', gucci_data['host'], region_term(metadata['region']),
                         style_id)
        metadata['dynamic_url'] = response.url + '/2/populate_dynamic_content'

        return Request(url=url, meta={'userdata': metadata}, callback=self.parse_style, dont_filter=True,
                       headers={'Accept': 'application/json, text/javascript, */*'})

    def parse_style(self, response):
        metadata = response.meta['userdata']
        try:
            data = json.loads(response._body)['images']['web_zoomin']
            metadata['image_urls'] = data
        except KeyError:
            return None

        url = metadata.pop('dynamic_url')
        return Request(url=url, meta={'userdata': metadata}, callback=self.parse_dynamic, dont_filter=True,
                       headers={'Accept': 'application/json, text/javascript, */*'})


    def parse_dynamic(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        data = json.loads(response._body)['style_wrappers']
        k = data.keys()[0]
        data = data[k]

        if 'price' in data and data['price']:
            metadata['price'] = data['price']
        if 'style_code' in data:
            metadata['model'] = data['style_code']

        metadata['color'] = []
        metadata['texture'] = []
        metadata['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if 'model' in metadata:
            item = ProductItem()
            if 'image_urls' in metadata:
                image_urls = metadata.pop('image_urls')
                item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            return item
        else:
            return None











