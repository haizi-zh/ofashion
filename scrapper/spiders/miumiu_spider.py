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
                   'currency': {'se': 'EUR'},
                   'home_urls': {k: str.format('http://store.miumiu.com/en/{0}', k.upper() if k != 'uk' else 'GB')
                                 for k in
                                 {'at', 'be', 'dk', 'fi', 'fr', 'gr', 'de', 'ie', 'it', 'lu', 'mc', 'nl', 'pt', 'es',
                                  'se', 'ch', 'uk', 'us', 'jp'}}}
    spider_data['home_urls']['cn'] = 'http://store.miumiu.com/hans/CN'

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
            try:
                tag_text = self.reformat(node.xpath('text()').extract()[0])
                tag_name = tag_text.lower()
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
            m['category'] = [tag_text]
            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url), dont_filter=True,
                          callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="categories"]/a[@href]'):
            try:
                tag_text = self.reformat(node.xpath('text()').extract()[0])
                tag_name = tag_text.lower()
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'name': tag_name, 'title': tag_text}]
            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url), dont_filter=True,
                          callback=self.parse_list, errback=self.onerr, meta={'userdata': m})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        subtitles = sel.xpath('//div[@id="products"]//h5')
        if subtitles:
            for node1 in subtitles:
                try:
                    tag_text = self.reformat(node1.xpath('text()').extract()[0])
                    tag_name = tag_text.lower()
                except (IndexError, TypeError):
                    continue
                m1 = copy.deepcopy(metadata)
                m1['tags_mapping']['category-2'] = [{'name': tag_name, 'title': tag_text}]

                for node2 in node1.xpath('../div[@class="products_list"]/div[contains(@class,"product")]/'
                                         'div[contains(@class,"wrapper")]/a[@href]'):
                    m2 = copy.deepcopy(m1)
                    try:
                        tmp = self.reformat(node2.xpath('../div[contains(@class,"desc")]/*[@class="item-desc"]'
                                                        '/text()').extract()[0])
                        m2['name'] = tmp
                    except (IndexError, TypeError):
                        pass
                    yield Request(url=self.process_href(node2.xpath('@href').extract()[0], response.url),
                                  dont_filter=True, callback=self.parse_details, errback=self.onerr,
                                  meta={'userdata': m2})

        else:
            for node2 in sel.xpath('//div[@class="products_list"]/div[contains(@class,"product")]/'
                                   'div[contains(@class,"wrapper")]/a[@href]'):
                m = copy.deepcopy(metadata)
                try:
                    tmp = self.reformat(node2.xpath('../div[contains(@class,"desc")]/text()').extract()[0])
                    m['name'] = tmp
                except (IndexError, TypeError):
                    pass
                yield Request(url=self.process_href(node2.xpath('@href').extract()[0], response.url), dont_filter=True,
                              callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="color-variants"]//ul/li[contains(@class,"single_item")]/a[@href]'):
            m = copy.deepcopy(metadata)
            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url), dont_filter=True,
                          callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

        try:
            tmp = self.reformat(sel.xpath('//div[@id="selection"]//*[@id="selected-code"]/text()').extract()[0])
            mt = re.search(r'[\s\d\-\._a-zA-Z]+', tmp, flags=re.U)
            metadata['model'] = re.sub(r'cod\.', '', mt.group(), flags=re.IGNORECASE).strip() if mt else None
        except (IndexError, TypeError):
            pass
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

        tmp = self.reformat(''.join(sel.xpath('//div[@id="item-price"]/descendant-or-self::text()').extract()))
        if tmp:
            metadata['price'] = tmp

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




