# coding=utf-8
import copy

from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
from utils.text import unicodify


__author__ = 'Zephyre'


class OmegaSpider(MFashionSpider):
    spider_data = {'brand_id': 10288,
                   'catalogue': {'cn': u'产品目录', 'us': 'catalogue', 'fr': 'catalogue', 'de': 'katalog',
                                 'es': u'catálogo', 'it': 'catalogo', 'pt': u'catálogo', 'ru': u'каталог',
                                 'jp': 'catalogue', 'kr': u'카탈로그'},
                   'home_urls': {'cn': {'collection': 'http://www.omegawatches.cn/cn/collection',
                                        'accessories': 'http://www.omegawatches.cn/cn/accessories'},
                                 'de': {'collection': 'http://www.omegawatches.com/de/collection',
                                        'accessories': 'http://www.omegawatches.com/de/accessories'},
                                 'es': {'collection': 'http://www.omegawatches.com/es/collection',
                                        'accessories': 'http://www.omegawatches.com/es/accessories'},
                                 'it': {'collection': 'http://www.omegawatches.com/it/collection',
                                        'accessories': 'http://www.omegawatches.com/it/accessories'},
                                 'pt': {'collection': 'http://www.omegawatches.com/pt/collection',
                                        'accessories': 'http://www.omegawatches.com/pt/accessories'},
                                 'ru': {'collection': 'http://www.omegawatches.com/ru/collection',
                                        'accessories': 'http://www.omegawatches.com/ru/accessories'},
                                 'jp': {'collection': 'http://www.omegawatches.jp/jp/collection',
                                        'accessories': 'http://www.omegawatches.jp/jp/accessories'},
                                 'kr': {'collection': 'http://www.omegawatches.co.kr/ko/collection',
                                        'accessories': 'http://www.omegawatches.co.kr/ko/accessories'},
                                 'fr': {'collection': 'http://www.omegawatches.com/fr/collection',
                                        'accessories': 'http://www.omegawatches.com/fr/accessories'},
                                 'us': {'collection': 'http://www.omegawatches.com/collection',
                                        'accessories': 'http://www.omegawatches.com/accessories'}}}

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
                yield Request(url=self.spider_data['home_urls'][region]['accessories'], meta={'userdata': m},
                              callback=self.parse_acc, errback=self.onerr)

                m = copy.deepcopy(metadata)
                yield Request(url=self.spider_data['home_urls'][region]['collection'], meta={'userdata': m},
                              callback=self.parse_col, errback=self.onerr)
            else:
                self.log(str.format('No data for {0}', region), log.WARNING)

    def parse_col(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath(
                '//div[@id="collection-hub"]/div[contains(@id,"collection_")]/div[@class="collection-title"]/h2'):
            tag_text = self.reformat(unicodify(node1.xpath('text()').extract()[0]))
            if not tag_text:
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['collection-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m1['tags_mapping']['category-0'] = [{'name': 'watches', 'title': 'watches'}]
            m1['category'] = ['watches']

            for node2 in node1.xpath(
                    '../../div[@class="collection-detail"]//ul/li/div[@class="container-text"]//a[@href]'):
                tag_text = self.reformat(unicodify(node2.xpath('text()').extract()[0]))
                if not tag_text:
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['collection-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
                yield Request(url=self.process_href(node2.xpath('@href').extract()[0], response.url),
                              meta={'userdata': m2, 'got-catalogue': False},
                              callback=self.parse_col_detail, errback=self.onerr)

    def parse_col_detail(self, response):
        """
        解析详细的系列页面
        @param response:
        """
        metadata = response.meta['userdata']
        sel = Selector(response)

        if response.meta['got-catalogue']:
            # 已进入产品目录的页面
            for node in sel.xpath('//div[@id="product-hub"]//ul[@class="list"]/li//a[@href and @class="hub-thumb"]'):
                m = copy.deepcopy(metadata)
                try:
                    m['name'] = self.reformat(unicodify(node.xpath('../../h2/text()').extract()[0]))
                except IndexError:
                    pass
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              meta={'userdata': m}, callback=self.parse_details, errback=self.onerr)
        else:
            # 根据关键词，找到产品目录的链接
            catalogue_key = self.spider_data['catalogue'][metadata['region']]

            def func(val):
                try:
                    return self.reformat(unicodify(val.xpath('text()').extract()[0])).lower() == catalogue_key
                except (TypeError, IndexError):
                    return False

            tmp = filter(func, sel.xpath('//div[@id="subcollection-tabs-area"]/ul/li/a[@href]'))
            if tmp:
                yield Request(url=self.process_href(tmp[0].xpath('@href').extract()[0], response.url),
                              meta={'userdata': metadata, 'got-catalogue': True}, callback=self.parse_col_detail,
                              errback=self.onerr)

    def parse_details(self, response):
        """
        解析“系列”下面的单品
        @param response:
        """
        metadata = response.meta['userdata']
        sel = Selector(response)

        try:
            model = sel.xpath('//div[@id="product-detail"]/div[@class="inner-detail"]//*[@class="reference-number"]/'
                              'text()').extract()[0]
            if not model:
                return
            metadata['model'] = model
        except IndexError:
            return
        metadata['url'] = unicodify(response.url)

        if 'name' not in metadata or not metadata['name']:
            tmp = sel.xpath('//div[@id="product-detail"]/div[@class="inner-detail"]//*[@class="format"]'
                            '/text()').extract()
            if tmp:
                metadata['name'] = self.reformat(unicodify(tmp[0]))

        # 颜色
        sub_products = sel.xpath('//div[@id="product-detail"]/div[@class="inner-detail"]//ul[@class="color-list"]'
                                 '/li/a[@href]/@href').extract()
        for href in sub_products:
            if href in response.url:
                continue
            yield Request(url=self.process_href(href, response.url), callback=self.parse_details, errback=self.onerr,
                          meta={'userdata': copy.deepcopy(metadata)})

        try:
            metadata['description'] = self.reformat(unicodify(sel.xpath('//div[@id="tabs-product-detail-overview"]'
                                                                        '/div[@class="product-detail-tab-content"]'
                                                                        '/p[@class="slide-paragraph"]/text()').extract()[
                0]))
        except IndexError:
            pass

        details_nodes = sel.xpath('//div[@id="tabs-product-detail-specification"]/'
                                  'div[@class="product-detail-tab-content"]//li/span[@class="tooltip" or '
                                  '@class="title"]/..')
        details = self.reformat(
            unicodify('\r'.join(': '.join(node.xpath('*/text()').extract()) for node in details_nodes)))
        if details:
            metadata['details'] = details

        image_urls = [self.process_href(val, response.url) for val in
                      sel.xpath('//div[@id="product-gallery"]/div[@class="product-gallery-part"]'
                                '/div[contains(@class,"positioned-product")]/img[@src]/@src').extract()]
        image_urls.extend([self.process_href(val, response.url) for val in
                           sel.xpath('//div[@id="product-gallery"]/div[@class="product-gallery-part"]'
                                     '/img[@src]/@src').extract()])

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item

    def parse_acc(self, response):
        """
        解析配饰的主页面
        @param response:
        """
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="product-hub"]/ul[@class="list"]/li/a[@href and @title]'):
            tag_text = self.reformat(unicodify(node.xpath('@title').extract()[0]))
            if not tag_text:
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m['category'] = [tag_text]
            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                          callback=self.acc_list, errback=self.onerr, meta={'userdata': m})

    def acc_list(self, response):
        """
        解析配饰的商品列表
        @param response:
        """
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath(
                '//div[@id="product-hub"]/ul[@class="list"]/li/a[@href and @title and @class="hub-thumb"]'):
            tag_text = self.reformat(unicodify(node.xpath('@title').extract()[0]))
            if not tag_text:
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                          meta={'userdata': m}, callback=self.acc_list2, errback=self.onerr)

    def acc_list2(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # 判断是否已经是单品页面
        tmp = sel.xpath(
            '//div[@id="product-detail"]/div[@class="inner-detail"]//*[@class="reference-number"]/text()').extract()
        if not tmp or not self.reformat(unicodify(tmp[0])):
            # 这是一个列表页面
            for node in sel.xpath('//div[@id="product-hub"]/ul[@class="list"]/li/h2[@class="group"]'):
                try:
                    tag_text = self.reformat(unicodify(node.xpath('text()').extract()[0]))
                    if not tag_text:
                        continue
                except IndexError:
                    continue

                m1 = copy.deepcopy(metadata)
                m1['tags_mapping']['category-2'] = [{'name': tag_text.lower(), 'title': tag_text}]

                for href in node.xpath('../ul/li/a[@href and @class="cross-reference"]/@href').extract():
                    m2 = copy.deepcopy(m1)
                    yield Request(url=self.process_href(href, response.url), callback=self.parse_details,
                                  errback=self.onerr, meta={'userdata': m2})
        else:
            # 这已经是一个单品页面
            for val in self.parse_details(response):
                yield val

