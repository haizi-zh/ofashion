# coding=utf-8
import json
import re
from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import copy

__author__ = 'Zephyre'


class MichaelKorsSpider(MFashionSpider):
    spider_data = {'brand_id': 10259,
                   'home_urls': {'cn': 'http://www.michaelkors.cn/catalog/',
                                 'jp': 'http://www.michaelkors.jp/catalog/'}}

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
            tag_text = self.reformat(cm.unicodify(node1._root.text))
            if not tag_text:
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m1['category'] = [tag_text]

            for node2 in node1.xpath('../ul/li/a[@href]'):
                tag_text = self.reformat(cm.unicodify(node2._root.text))
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
                tag_text = self.reformat(cm.unicodify(node._root.text))
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
                tmp = node.xpath('./span[@class="product-name"]')
                if tmp:
                    m['name'] = self.reformat(cm.unicodify(tmp[0]._root.text))
                tmp = node.xpath('.//span[@class="price"]')
                if tmp:
                    m['price'] = self.reformat(cm.unicodify(tmp[0]._root.text))
                yield Request(url=self.process_href(node._root.attrib['href'], response.url), dont_filter=True,
                              callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        tmp = sel.xpath('//div[@class="product-info"]//span[@class="style-no"]')
        if tmp:
            mt = re.search(r'[\s\d\-\._a-zA-Z]+', self.reformat(cm.unicodify(tmp[0]._root.text)), flags=re.U)
            metadata['model'] = mt.group().strip() if mt else None
        if 'model' not in metadata or not metadata['model']:
            return

        tmp = sel.xpath('//div[@class="product-info"]/ul[@class="product"]/li/a[@href and @data-zoom-width and '
                        '@data-zoom-height]')
        image_urls = [self.process_href(val._root.attrib['href'], response.url) for val in tmp]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item







