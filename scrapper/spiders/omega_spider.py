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


class OmegaSpider(MFashionSpider):
    spider_data = {'brand_id': 10288,
                   'catalogue': {'cn': u'产品目录'},
                   'home_urls': {'cn': {'collection': 'http://www.omegawatches.cn/cn/collection',
                                        'accessories': 'http://www.omegawatches.cn/cn/accessories'}}}
    # 'us': 'http://www.miumiu.com/en'}}}

    @classmethod
    def get_supported_regions(cls):
        return OmegaSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(OmegaSpider, self).__init__('omega', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def start_requests(self):
        for region in self.region_list:
            if region in self.get_supported_regions():
                metadata = {'region': region, 'brand_id': self.spider_data['brand_id'],
                            'tags_mapping': {}, 'category': []}

                m = copy.deepcopy(metadata)
                yield Request(url=self.spider_data['home_urls'][region]['collection'], meta={'userdata': m},
                              callback=self.parse_col, errback=self.onerr)
                m = copy.deepcopy(metadata)
                yield Request(url=self.spider_data['home_urls'][region]['collection'], meta={'userdata': m},
                              callback=self.parse_acc, errback=self.onerr)
            else:
                self.log(str.format('No data for {0}', region), log.WARNING)

    def parse_col(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath(
                '//div[@id="collection-hub"]/div[contains(@id,"collection_")]/div[@class="collection-title"]/h2'):
            tag_text = self.reformat(cm.unicodify(node1._root.text))
            if not tag_text:
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['collection-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m1['tags_mapping']['category-0'] = [{'name': 'watches', 'title': 'watches'}]
            m1['category'] = ['watches']

            for node2 in node1.xpath(
                    '../../div[@class="collection-detail"]//ul/li/div[@class="container-text"]//a[@href]'):
                tag_text = self.reformat(cm.unicodify(node2._root.text))
                if not tag_text:
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['collection-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
                yield Request(url=self.process_href(node2._root.attrib['href'], response.url),
                              meta={'userdata': m2, 'got-catalogue': False},
                              callback=self.parse_col_detail, errback=self.onerr)

    def parse_col_detail(self, response):
        """
        解析详细的系列页面
        @param response:
        """
        metadata = response.meta['userdata']
        sel = Selector(response)

        if response.meta['get-catalogue']:
            # 已进入产品目录的页面
            for node in sel.xpath('//div[@id="product-hub"]//ul[@class="list"]/li//a[@href]'):
                m = copy.deepcopy(metadata)
                tmp = node.xpath('../../h2')
                if tmp:
                    m['name'] = self.reformat(cm.unicodify(tmp[0]._root.text))
                yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                              meta={'userdata': m}, callback=self.parse_details, errback=self.onerr)
        else:
            # 根据关键词，找到产品目录的链接
            catalogue_key = self.spider_data['catalogue'][metadata['region']]
            tmp = filter(lambda node: self.reformat(cm.unicodify(node._root.text)) == catalogue_key,
                         sel.xpath('//div[@id="subcollection-tabs-area"]/ur/li/a[@href]'))
            if tmp:
                yield Request(url=self.process_href(tmp[0]._root.attrib['href'], response.url),
                              meta={'userdata': metadata, 'got-catalogue': True}, callback=self.parse_col_detail,
                              errback=self.onerr)

    def parse_details(self, response):
        """
        解析“系列”下面的单品
        @param response:
        """
        metadata = response.meta['userdata']
        sel = Selector(response)

        tmp = sel.xpath('//div[@id="product-detail"]/div[@class="inner-detail"]//*[@class="reference-number"]')
        if tmp:
            metadata['model'] = self.reformat(cm.unicodify(tmp[0]._root.text))
        if 'model' not in metadata or not metadata['model']:
            return
        metadata['url'] = response.url

        tmp = sel.xpath('//div[@id="tabs-product-detail-overview"]/div[@class="product-detail-tab-content"]/'
                        'p[@class="slide-paragraph"]')
        if tmp:
            metadata['description'] = self.reformat(cm.unicodify(tmp[0]._root.text))

        details_terms = sel.xpath('//div[@id="tabs-product-detail-specification"]/'
                                  'div[@class="product-detail-tab-content"]//span[@class="title"]')
        details_terms.extend(sel.xpath(
            '//div[@id="tabs-product-detail-movement"]/div[@class="product-detail-tab-content"]//span[@class="title"'))
        details_terms.extend(
            sel.xpath('//div[@id="tabs-product-detail-movement"]/div[@class="product-detail-tab-content"]/p'))
        if details_terms:
            metadata['details'] = '\r'.join(self.reformat(cm.unicodify(val._root.text)) for val in details_terms)

        image_urls = [self.process_href(val._root.attrib['src'], response.url) for val in sel.xpath(
            '//div[@id="product-gallery"]/div[@class="product-gallery-part"]/div[contains(@class,"positioned-product")]'
            '/img[@src]')]
        image_urls.extend([self.process_href(val._root.attrib['src'], response.url) for val in
                           sel.xpath('//div[@id="product-gallery"]/div[@class="product-gallery-part"]/img[@src]')])

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item


    def parse_acc(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)