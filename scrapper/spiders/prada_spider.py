# coding=utf-8
import copy
import re
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils import unicodify

__author__ = 'Zephyre'


class PradaSpider(MFashionSpider):
    spider_data = {'brand_id': 10300,
                   'currency': {'dk': 'EUR'},
                   'home_urls': {'cn': 'http://store.prada.com/hans/CN/',
                                 'us': 'http://store.prada.com/en/US/',
                                 'ap': 'http://store.prada.com/hant/AP/',
                                 'at': 'http://store.prada.com/en/AT/',
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
    spider_data['hosts'] = {k: 'http://store.prada.com' for k in spider_data['home_urls']}

    @classmethod
    def get_supported_regions(cls):
        return PradaSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        super(PradaSpider, self).__init__('prada', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"menu")]/ul[contains(@class,"collections")]/li[contains(@class,'
                              '"collection")]/div/a[@href]'):
            m = copy.deepcopy(metadata)
            href = self.process_href(node._root.attrib['href'], response.url)
            mt = re.search('/([^/]+)$', href)
            if mt:
                tag_name = unicodify(mt.group(1)).lower()
                tag_type = 'category-0'
                tag_text = unicodify(node._root.text).lower() if node._root.text else tag_name
                m['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]

            yield Request(url=href, callback=self.parse_cat_0, meta={'userdata': m}, errback=self.onerr)

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//section[@id="contents"]/article[contains(@class,"products")]/'
                              'div[contains(@class,"product")]/a[@href]'):
            m = copy.deepcopy(metadata)
            href = self.process_href(node._root.attrib['href'], response.url)
            # temp = node.xpath('./figcaption/div[@class="name"]')
            # if not temp:
                # continue
            # m['name'] = unicodify(temp[0]._root.text)
            yield Request(url=href, callback=self.parse_details, meta={'userdata': m}, errback=self.onerr)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

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

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        temp = sel.xpath('//article[@class="product"]/figure[@class="slider"]/img[@data-zoom-url]')
        image_urls = [self.process_href(val._root.attrib['data-zoom-url'], response.url) for val in temp]

        metadata['category'] = [val['name'] for val in
                                metadata['tags_mapping'][
                                    'category-1' if 'category-1' in metadata['tags_mapping'] else 'category-0']
                                if val]

        gender = cm.guess_gender(metadata['tags_mapping']['category-0'][0]['name'])
        if gender:
            metadata['gender'] = [gender]

        metadata['url'] = response.url
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
            return Request(url=self.process_href(temp[0]._root.attrib['href'], response.url),
                           callback=self.parse_list,
                           meta={'userdata': metadata}, errback=self.onerr)

        node = None
        temp = sel.xpath('//div[@class="menu"]/ul[@class="collections"]/li[contains(@class,"collection")]/'
                         'div[contains(@class,"name")]/a[@href]')
        if temp:
            for temp1 in temp:
                if self.process_href(temp1._root.attrib['href'], response.url) == response.url:
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
                tag_name = unicodify(mt.group(1)).lower()
                tag_text = unicodify(node1._root.text).lower() if node1._root.text else tag_name
                m1['tags_mapping']['category-1'] = [{'name': tag_name, 'title': tag_text}]

            # 是否有子分类级别
            for node2 in node1.xpath(
                    '../../ul[contains(@class,"categories")]/li[contains(@class,"category")]//a[@href]'):
                m2 = copy.deepcopy(m1)
                href = node2._root.attrib['href']
                mt = re.search('/([^/]+)$', href)
                if mt:
                    tag_name = unicodify(mt.group(1))
                    tag_text = unicodify(node2._root.text) if node2._root.text else tag_name
                    m2['tags_mapping']['category-2'] = [{'name': tag_name, 'title': tag_text}]
                ret.append(Request(url=self.process_href(href, response.url), meta={'userdata': m2},
                                   callback=self.parse_list,
                                   errback=self.onerr))

        return ret

    @classmethod
    def is_offline(cls, response):
        return not cls.fetch_model(response)

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        model = None
        try:
            temp = sel.xpath('//section[@class="summary"]/div[@class="code"]')
            if temp and temp[0]._root.text:
                model = unicodify(temp[0]._root.text)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            temp = sel.xpath('//section[@class="summary"]/div[@class="price"]/span[@class="value"]')
            if temp:
                old_price = unicodify(temp[0]._root.text)
        except(TypeError, IndexError):
            pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//article[@class="product"]//div[@class="title"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            temp = sel.xpath('//section[@class="details"]/figcaption[@class="description"]/ul/li')
            description = '\n'.join(unicodify(val._root.text) for val in temp if val._root.text)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)

        colors = []
        try:
            temp = sel.xpath('//section[@class="summary"]/div[@class="color"]/div[@class="name"]')
            if temp:
                colors = [val.strip() for val in unicodify(temp[0]._root.text).split('+')]
        except(TypeError, IndexError):
            pass

        return colors
