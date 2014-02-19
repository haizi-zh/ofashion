# coding=utf-8
import copy
import json
import os
import datetime
import re
from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils import unicodify

__author__ = 'Zephyre'


class GucciSpider(MFashionSpider):
    spider_data = {'brand_id': 10152,
                   'currency': {k: 'EUR' for k in ('hu', 'bg', 'cz', 'dk', 'no', 'pl', 'ro', 'se')},
                   'home_urls': {k: str.format('http://www.gucci.com/{0}/home', k) for k in
                                 ['cn', 'us', 'fr', 'de', 'es', 'it', 'nl', 'ae', 'jp', 'kr', 'au',
                                  'bg', 'cz', 'dk',
                                  'fi', 'hu', 'ie', 'no', 'pl', 'pt', 'ro', 'si', 'se', 'ch', 'tr',
                                  'uk', 'at', 'ca',
                                  'be']}}
    spider_data['hosts'] = {k: 'http://www.gucci.com' for k in spider_data['home_urls'].keys()}

    @classmethod
    def get_supported_regions(cls):
        return GucciSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        super(GucciSpider, self).__init__('gucci', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    allowed_domains = ['gucci.com']

    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        mt = re.search(r'www\.gucci\.com/([a-z]{2})', response.url)
        if mt:
            region = mt.group(1)
            sel = Selector(response)
            for node1 in sel.xpath("//ul[@id='header_main']/li[contains(@class, 'mega_menu')]"):
                span = node1.xpath("./span[@class='mega_link']")
                if len(span) == 0:
                    continue
                span = span[0]
                inner = span.xpath('.//cufontext')
                if len(inner) > 0:
                    cat = unicodify(inner[0]._root.text)
                else:
                    cat = unicodify(span._root.text)
                if not cat:
                    continue

                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-1'] = [{'name': cat.lower(), 'title': cat}]
                gender = cm.guess_gender(cat)
                if gender:
                    m['gender'] = [gender]

                for node2 in node1.xpath("./div/ul/li[not(@class='mega_promo')]/a[@href]"):
                    href = unicodify(node2._root.attrib['href'])
                    inner = node2.xpath('.//cufontext')
                    if len(inner) > 0:
                        title = unicodify(inner[0]._root.text)
                    else:
                        title = unicodify(node2._root.text)
                    if not title:
                        continue
                    else:
                        title = title.strip()
                    mt = re.search(ur'/([^/]+)/?$', href)
                    if not mt:
                        continue
                    cat = unicodify(mt.group(1))
                    if not cat:
                        continue
                    else:
                        cat = cat.lower()

                    m2 = copy.deepcopy(m)
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
                cat = unicodify(mt.group(1))
                if not cat:
                    return None
                else:
                    cat = cat.lower()
                title = unicodify(node._root.text)
                if not title:
                    return None
                m['tags_mapping']['category-3'] = [{'name': cat, 'title': title}]
            return Request(url=href, meta={'userdata': m}, callback=self.parse_category_3, dont_filter=True)

        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath(
                '//section[@id="sub_nav"]/div[@class="content"]/ul[@id="topsort"]/li[@class="full_row"]/a[@href]'):
            ret = func(node, copy.deepcopy(metadata), with_tag=False)
            if ret:
                yield ret
        for node in sel.xpath(
                '//section[@id="sub_nav"]/div[@class="content"]/ul[@id="topsort"]/li[not(@class="full_row")]/a[@href]'):
            ret = func(node, copy.deepcopy(metadata))
            if ret:
                yield ret


    def parse_category_3(self, response):
        self.log(unicode.format(u'PARSE_CAT_3: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        sel = Selector(response)
        for node in sel.xpath(
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
        sel = Selector(response)
        for node in sel.xpath(
                '//div[@id="variations"]/div[@id="container_variations"]//ul[@class="items"]/li/a[@href]'):
            href = node._root.attrib['href'][1:]
            url = self.spider_data['hosts'][metadata['region']] + '/' + href
            m = copy.deepcopy(metadata)
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_details, dont_filter=True)

    def parse_details(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        sel = Selector(response)

        title = None
        node = sel.xpath('//section[@id="column_description"]//span[@class="container_title"]/h1/span')
        if len(node) > 0:
            node = node[0]
            inner = node.xpath('.//cufontext')
            if len(inner) == 0:
                title = unicodify(node._root.text)
            else:
                title = u''.join(val._root.text for val in inner if val._root.text)

        node = sel.xpath('//div[@id="accordion_left"]//div[@id="description"]//ul/li')
        desc = u'\n'.join(unicodify(val._root.text) for val in node if val._root.text)

        node = sel.xpath('//div[@id="zoom_in_window"]/div[@class="zoom_in"]/img[@src]')
        if len(node) > 0:
            href = node[0]._root.attrib['src']
            image_base = os.path.split(href)[0]
            node_list = sel.xpath('//div[@id="zoom_tools"]/ul[@id="view_thumbs_list"]/li/img[@src]')
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
        url = str.format('{0}/{1}/styles/{2}/load_style.js', self.spider_data['hosts'][metadata['region']],
                         'ca-en' if metadata['region'] == 'ca' else metadata['region'], style_id)
        metadata['dynamic_url'] = response.url + '/2/populate_dynamic_content'

        return Request(url=url, meta={'userdata': metadata}, callback=self.parse_style, dont_filter=True,
                       headers={'Accept': 'application/json, text/javascript, */*'})

    def parse_style(self, response):
        metadata = response.meta['userdata']
        try:
            data = json.loads(response.body)['images']['web_zoomin']
            metadata['image_urls'] = data
        except KeyError:
            return None

        url = metadata.pop('dynamic_url')
        return Request(url=url, meta={'userdata': metadata}, callback=self.parse_dynamic, dont_filter=True,
                       headers={'Accept': 'application/json, text/javascript, */*'})


    def parse_dynamic(self, response):
        self.log(unicode.format(u'PARSE_DETAILS: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        data = json.loads(response.body)['style_wrappers']
        k = data.keys()[0]
        data = data[k]

        if 'price' in data and data['price']:
            metadata['price'] = data['price']
        if 'style_code' in data:
            metadata['model'] = data['style_code']

        metadata['color'] = []
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
        model_node = sel.xpath('//div[@id="content"]//div[@id="product_card"]//p[@id="stylenum"][text()]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="content"]//div[@id="container_title_description"]//h1[@itemprop="url"]')
        if name_node:
            try:
                name_text = ''.join(cls.reformat(val) for val in name_node.xpath('.//text()').extract())
                name_text = cls.reformat(name_text)
                if name_text:
                    name = cls.reformat(name_text)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@id="content"]//div[@id="product_card"]//p[@id="price"][text()]')
        if price_node:
            try:
                old_price = price_node.xpath('./text()').extract()[0]
                old_price = cls.reformat(old_price)
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
        description_node = sel.xpath('//div[@id="content"]//div[@id="container_title_description"]//div[@id="description"]//li[text()]')
        if description_node:
            description = '\r'.join(cls.reformat(val) for val in description_node.xpath('./text()').extract())
            description = cls.reformat(description)

        return description
