# coding=utf-8
import re
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
from utils.utils import unicodify


__author__ = 'Zephyre'


class MichaelKorsSpider(MFashionSpider):
    spider_data = {'brand_id': 10259,
                   'ref_notation': {'cn': u'款号',
                                    'kr': u'스타일 번호',
                                    'br': u'Número do modelo',
                                    'jp': u'スタイルナンバー'},
                   'home_urls': {'cn': 'http://www.michaelkors.cn/catalog/',
                                 'jp': 'http://www.michaelkors.jp/catalog/',
                                 'kr': 'http://kr.michaelkors.com/catalog/',
                                 'br': 'http://br.michaelkors.com/catalog/'}}

    @classmethod
    def get_supported_regions(cls):
        return MichaelKorsSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MichaelKorsSpider, self).__init__('michael_kors', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//nav/ul/li[@class="category-parent"]/a[@href]'):
            tag_text = self.reformat(unicodify(node1._root.text))
            if not tag_text:
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m1['category'] = [tag_text]

            for node2 in node1.xpath('../ul/li/a[@href]'):
                tag_text = self.reformat(unicodify(node2._root.text))
                if not tag_text:
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
                yield Request(url=self.process_href(node2._root.attrib['href'], response.url),
                              callback=self.parse_cat, errback=self.onerr, dont_filter=True,
                              meta={'userdata': m2, 'cat-level': 0})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        cat_level = response.meta['cat-level']
        sel = Selector(response)

        node_list = []
        if cat_level == 0:
            node_list = sel.xpath('//ul[@class="product-categories"]/ul/li/a[@href]')
            for node in node_list:
                # 还有下级目录
                tag_text = self.reformat(unicodify(node._root.text))
                if not tag_text:
                    continue
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-2'] = [{'name': tag_text.lower(), 'title': tag_text}]
                yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                              callback=self.parse_cat, errback=self.onerr, dont_filter=True,
                              meta={'userdata': m, 'cat-level': 1})

        if not node_list:
            # 没有下级目录的情况，返回所有单品
            for node in sel.xpath('//ul[@id="list-content"]/li[contains(@class,"item")]/a[@href]'):
                m = copy.deepcopy(metadata)
                # tmp = node.xpath('./span[@class="product-name"]')
                # if tmp:
                #     m['name'] = self.reformat(unicodify(tmp[0]._root.text))
                # tmp = node.xpath('.//span[@class="price"]')
                # if tmp:
                #     m['price'] = self.reformat(unicodify(tmp[0]._root.text))
                yield Request(url=self.process_href(node._root.attrib['href'], response.url), dont_filter=True,
                              callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
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

        tmp = sel.xpath('//div[@class="product-info"]/ul[@class="product"]/li/a[@href and @data-zoom-width and '
                        '@data-zoom-height]')
        image_urls = [self.process_href(val._root.attrib['href'], response.url) for val in tmp]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

    @classmethod
    def is_offline(cls, response):
        return not cls.fetch_model(response)

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        try:
            tmp = sel.xpath('//div[@class="product-info"]//span[@class="style-no"]/text()').extract()
            if tmp:
                model = cls.reformat(re.sub(cls.spider_data['ref_notation'][region], '', tmp[0]))
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@class="product-info"]//span[@class="price"][text()]')
        if price_node:
            old_price = price_node.xpath('./text()').extract()[0]
            old_price = cls.reformat(old_price)

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@class="product-info"]/div[@class="product-info-content-top"]/h1[text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = cls.reformat(name)

        return name
