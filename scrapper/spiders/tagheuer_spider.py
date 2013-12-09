# coding=utf-8
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class TagheuerSpider(MFashionSpider):
    spider_data = {'brand_id': 10345,
                   'home_urls': {'cn': 'http://www.tagheuer.cn/zh-hans/%E4%B8%BB%E9%A1%B5',
                                 'uk': 'http://www.tagheuer.co.uk/',
                                 'au': 'http://au.tagheuer.com/',
                   }}

    @classmethod
    def get_supported_regions(cls):
        return TagheuerSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(TagheuerSpider, self).__init__('tagheuer', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # for node in sel.xpath(u'//ul[@class="main"]/li/div/a[@href and text()="系列"]/../../ul/li/a[@href]'):
        for node in sel.xpath('//ul[@class="main"]/li/ul/li/a[@href]'):
            try:
                tag_text = self.reformat(node.xpath('text()').extract()[0])
                tag_name = tag_text.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)

                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
                yield Request(url=url, callback=self.parse_col, errback=self.onerr, meta={'userdata': m},
                              dont_filter=True)
            except (IndexError, TypeError):
                pass

    def parse_col(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        ret = urlparse.urlparse(response.request.url)
        # 根据fragment判断当前在哪个分类中
        if not ret[5]:
            return
        for node in sel.xpath(str.format('//div[@id="{0}"]//ul[contains(@class,"links")]/li/a[@href]', ret[5])):
            try:
                tag_text = self.reformat('|'.join(node.xpath('./descendant-or-self::text()').extract()))
                tag_name = tag_text.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-1'] = [{'name': tag_name, 'title': tag_text}]
                yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m})
            except (IndexError, TypeError):
                pass

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath(
            '//div[contains(@class,"discover")]/a[@href and contains(@class,"button")]/@href').extract()
        if node_list:
            for href in node_list:
                url = self.process_href(href, response.url)
                m = copy.deepcopy(metadata)
                yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m})
        else:
            # 已经到了list2列表中
            for item in self.parse_list2(response):
                yield item

    def parse_list2(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = list(set(
            sel.xpath('//li[contains(@class,"variant")]//a[@href and contains(@class,"navigate")]/@href').extract()))
        for href in node_list:
            url = self.process_href(href, response.url)
            m = copy.deepcopy(metadata)
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for item in self.parse_list2(response):
            yield item

        tmp = sel.xpath('//p[@class="ref"]/text()').extract()
        if not tmp or not self.reformat(tmp[0]):
            return
        tmp = self.reformat(tmp[0])
        mt = re.search('([\.a-zA-Z\d\-_]+)$', tmp)
        if not mt:
            return
        model = mt.group(1)
        metadata['model'] = model
        metadata['url'] = response.url

        tmp = sel.xpath('//div[@class="summary"]/p[@class="full"]/text()').extract()
        if tmp:
            tmp = self.reformat(tmp[0])
            metadata['description'] = tmp

        tmp = sel.xpath('//div[@class="infos"]/h1/text()').extract()
        if tmp:
            tmp = self.reformat(tmp[0])
            metadata['name'] = tmp

        image_urls = [self.process_href(tmp, response.url) for tmp in
                      sel.xpath('//ul[@class="actions"]/li[@class="zoom-button"]/a[@href]/@href').extract()]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item



