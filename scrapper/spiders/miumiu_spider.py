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


class MiumiuSpider(MFashionSpider):
    spider_data = {'brand_id': 10264,
                   'home_urls': {'cn': 'http://store.miumiu.com/hans/CN/'}}
    # 'us': 'http://www.miumiu.com/en'}}}

    @classmethod
    def get_supported_regions(cls):
        return MiumiuSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MiumiuSpider, self).__init__('miumiu', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="departments"]/a[@href]'):
            tag_text = self.reformat(unicodify(node._root.text))
            if not tag_text:
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m['category'] = [tag_text]
            yield Request(url=self.process_href(node._root.attrib['href'], response.url), dont_filter=True,
                          callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="categories"]/a[@href]'):
            tag_text = self.reformat(unicodify(node._root.text))
            if not tag_text:
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
            yield Request(url=self.process_href(node._root.attrib['href'], response.url), dont_filter=True,
                          callback=self.parse_list, errback=self.onerr, meta={'userdata': m})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        subtitles = sel.xpath('//div[@id="products"]//h5')
        if subtitles:
            for node1 in subtitles:
                m1 = copy.deepcopy(metadata)
                tag_text = self.reformat(unicodify(node1._root.text))
                if not tag_text:
                    continue
                m1['tags_mapping']['category-2'] = [{'name': tag_text.lower(), 'title': tag_text}]

                for node2 in node1.xpath('../div[@class="products_list"]/div[contains(@class,"product")]/'
                                         'div[contains(@class,"wrapper")]/a[@href]'):
                    m2 = copy.deepcopy(m1)
                    tmp = node2.xpath('../div[contains(@class,"desc")]/*[@class="item-desc"]')
                    if tmp:
                        m2['name'] = self.reformat(unicodify(tmp[0]._root.text))
                    yield Request(url=self.process_href(node2._root.attrib['href'], response.url),
                                  callback=self.parse_details, errback=self.onerr, meta={'userdata': m2})

        else:
            for node2 in sel.xpath('//div[@class="products_list"]/div[contains(@class,"product")]/'
                                   'div[contains(@class,"wrapper")]/a[@href]'):
                m = copy.deepcopy(metadata)
                tmp = node2.xpath('../div[contains(@class,"desc")]')
                if tmp:
                    m['name'] = self.reformat(unicodify(tmp[0]._root.text))
                yield Request(url=self.process_href(node2._root.attrib['href'], response.url),
                              callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="color-variants"]//ul/li[contains(@class,"single_item")]/a[@href]'):
            m = copy.deepcopy(metadata)
            yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                          callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

        tmp = sel.xpath('//div[@id="selection"]//*[@id="selected-code"]')
        if tmp:
            mt = re.search(r'[\s\d\-\._a-zA-Z]+', self.reformat(unicodify(tmp[0]._root.text)), flags=re.U)
            metadata['model'] = re.sub(r'cod\.', '', mt.group(), flags=re.IGNORECASE).strip() if mt else None
        if 'model' not in metadata or not metadata['model']:
            return
        metadata['url'] = response.url

        tmp = sel.xpath('//div[@id="selection"]//*[@id="selected-color"]')
        if tmp:
            metadata['color'] = [self.reformat(unicodify(tmp[0]._root.text))]

        if 'name' not in metadata or not metadata['name']:
            tmp = sel.xpath('//div[@id="description"]/h2')
            if tmp:
                metadata['name'] = self.reformat(unicodify(tmp[0]._root.text))

        node_list = sel.xpath('//div[@id="description"]/*[@class="desc"]')
        node_list.extend(sel.xpath('//div[@id="description"]/*[@class="desc"]/*'))
        node_list.extend(sel.xpath('//div[@id="description"]/*[@class="dimensions"]'))
        node_list.extend(sel.xpath('//div[@id="description"]/*[@class="dimensions"]/*'))
        metadata['details'] = '\r'.join(
            ','.join(filter(lambda val: val, (self.reformat(unicodify(val)) for val in
                                              (node._root.prefix, node._root.text, node._root.tail)))) for node in
            node_list)

        image_urls = [self.process_href(val._root.attrib['data-zoom'], response.url) for val in
                      sel.xpath('//div[@id="detail_image"]//ul[@id="views"]/li/a[@data-zoom]')]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item




