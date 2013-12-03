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


class FerragamoSpider(MFashionSpider):
    spider_data = {'brand_id': 10308,
                   # 提取型号
                   'model_template': {'cn': ur'型号代码([\s\da-zA-Z]+)'},
                   'home_urls': {'cn': 'http://www.ferragamo.cn'}}
    # TODO 多国家支持

    @classmethod
    def get_supported_regions(cls):
        return FerragamoSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(FerragamoSpider, self).__init__('ferragamo', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//div[@class="nav"]/ul/li/a[@href]'):
            try:
                tag_title = self.reformat(node1.xpath('text()').extract()[0])
                tag_name = tag_title.lower()
            except (TypeError, IndexError):
                continue
            m1 = copy.deepcopy(metadata)
            gender = cm.guess_gender(tag_name)
            if gender:
                m1['gender'] = [gender]

            for node2 in node1.xpath('../ul/li/a[@href]'):
                try:
                    tag_title = self.reformat(node2.xpath('text()').extract()[0])
                    tag_name = tag_title.lower()
                except (TypeError, IndexError):
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_title}]
                m2['category'] = [tag_name]
                yield Request(url=self.process_href(node2.xpath('@href').extract()[0], response.url),
                              callback=self.parse_cat, errback=self.onerr, meta={'userdata': m2})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@class="view-content"]/div[contains(@class,"page-wrapper-product")]/div'
                              '/a[@href]/*[@class="prodcaption"]'):
            yield Request(url=self.process_href(node.xpath('../@href').extract()[0], response.url),
                          callback=self.parse_details, errback=self.onerr, meta={'userdata': copy.deepcopy(metadata)})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)
        metadata['url'] = response.url

        tmp = sel.xpath('//div[@class="product-title"]/text()').extract()
        if tmp:
            metadata['name'] = self.reformat(tmp[0])

        tmp = sel.xpath('//div[@class="product-code"]/text()').extract()
        desc = None
        if tmp:
            desc = self.reformat(tmp[0])
            metadata['description'] = desc

        # 试图找出产品编号
        model = None
        if desc:
            mt = re.search(self.spider_data['model_template'][metadata['region']], desc)
            if mt and mt.group(1).strip():
                model = mt.group(1).strip()
        if not model:
            mt = re.search(r'/([0-9a-zA-Z]+)$', response.url)
            if mt:
                model = mt.group(1)
        if not model:
            return
        metadata['model'] = model

        tmp = sel.xpath('//div[@class="product-price"]/text()').extract()
        if tmp:
            metadata['price'] = self.reformat(tmp[0])

        tmp = '\r'.join(self.reformat(val) for val in sel.xpath('//div[@class="product-desc"]'
                                                                '/div[@class="field-content"]/text()').extract())
        if tmp:
            metadata['details'] = tmp

        tmp = sel.xpath('//div[@class="product-collection"]/text()').extract()
        if tmp and tmp[0]:
            tag_text = self.reformat(tmp[0])
            metadata['tags_mapping']['collection'] = [{'name': tag_text.lower(), 'title': tag_text}]

        tmp = sel.xpath('//select[@class="select-color"]/option//a[@href]/text()').extract()
        if tmp:
            metadata['color'] = [self.reformat(val) for val in tmp]

        image_urls = [self.process_href(val, response.url) for val in
                      sel.xpath('//div[@class="item-list"]/ul[contains(@class,"field-slideshow-pager")]/li'
                                '/a[@href]/img[@src]/@src').extract()]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item



