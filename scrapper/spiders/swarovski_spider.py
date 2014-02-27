# coding=utf-8
import hashlib
import json
import urlparse
import copy
import re
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class SwarovskiSpider(MFashionSpider):
    spider_data = {'brand_id': 10339,
                   'home_urls': {'cn': 'http://www.swarovski.com.cn/Web_CN/zh/index',
                   }}

    @classmethod
    def get_supported_regions(cls):
        return SwarovskiSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(SwarovskiSpider, self).__init__('swarovsky', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul/li/a[@href and @data-cat]'):
            try:
                cat_title = self.reformat(node.xpath('@data-cat').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]

            yield Request(callback=self.parse_cat, errback=self.onerr, meta={'userdata': m}, url=url)

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@class="facette"]/ul/li/a[@href and @data-tracking-filtername]'):
            try:
                cat_title = self.reformat(node.xpath('@data-tracking-filtername').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(callback=self.parse_grid, errback=self.onerr, meta={'userdata': m}, url=url)

    def parse_grid(self, response, parse_grid=True):
        """
        @param response:
        @param parse_grid: 是否对其它分页进行解析？
        """
        metadata = response.meta['userdata']
        sel = Selector(response)

        if parse_grid:
            # 其它页面
            page_list = set({})
            page_template = None
            for page_url in list(set(tmp for tmp in self.parse_pages(response))):
                mt = re.search(r'PageNumber=(\d+)', page_url, re.IGNORECASE)
                if not mt:
                    continue
                page_list.add(int(mt.group(1)))
                page_template = page_url

            if page_list:
                start_idx = min(page_list)
                end_idx = max(page_list) + 1
                for idx in xrange(start_idx, end_idx):
                    page_url = re.sub(r'PageNumber=\d+', str.format('PageNumber={0}', idx), page_template)
                    page_url = re.sub(r'CurrentPageView=[^&]*', 'CurrentPageView=M', page_url)
                    m = copy.deepcopy(metadata)
                    yield Request(callback=(lambda r: self.parse_grid(r, parse_grid=False)),
                                  errback=self.onerr, meta={'userdata': m}, url=page_url)

        for node in sel.xpath('//div[@id="listpage"]/div[@class="listproduct"]'):
            tmp = node.xpath('./a[@href and @data-ajaxurl-bubble]')
            if not tmp:
                continue
            bubble = self.process_href(tmp.xpath('@data-ajaxurl-bubble').extract()[0], response.url)
            url = self.process_href(tmp.xpath('@href').extract()[0], response.url)

            tmp = node.xpath('.//span[@class="price"]/text()').extract()
            price = self.reformat(tmp[0]) if tmp else None

            m = copy.deepcopy(metadata)
            # 寻找model
            tmp = re.search(r'sku=(.+)$', bubble, re.IGNORECASE)
            model = self.reformat(tmp.group(1)) if tmp else None
            if model:
                m['model'] = model
            if price:
                m['price'] = price

                # yield Request(callback=self.parse_details, errback=self.onerr, meta={'userdata': m}, url=url)

    def parse_pages(self, response, skip_first=False):
        """
        解析分页列表，返回相应的url
        @param skip_first: 返回的url列表中，是否包含第一页？
        @param response:
        """
        sel = Selector(response)

        for node in sel.xpath('//div[@class="paging c"]/ul[@class="linkList"]/li/a[@href]'):
            yield self.process_href(node.xpath('@href').extract()[0], response.url)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)



