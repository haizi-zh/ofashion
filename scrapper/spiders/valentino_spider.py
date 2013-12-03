# coding=utf-8
import json
import re
import urlparse
from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import copy

__author__ = 'Zephyre'


class ValentinoSpider(MFashionSpider):
    spider_data = {'brand_id': 10367,
                   'currency': {'cn': 'EUR', 'hk': 'EUR', 'tw': 'EUR'},
                   'home_urls': {'cn': 'http://store.valentino.com/VALENTINO/home/tskay/5A81B803/mm/112',
                                 'us': 'http://store.valentino.com/VALENTINO/home/tskay/B60ACEA7/mm/112',
                                 'fr': 'http://store.valentino.com/VALENTINO/home/tskay/D5C4AA66/mm/112',
                                 'it': 'http://store.valentino.com/VALENTINO/home/tskay/CD784FB3/mm/112',
                                 'uk': 'http://store.valentino.com/VALENTINO/home/tskay/112439D7/mm/112',
                                 'jp': 'http://store.valentino.com/VALENTINO/home/tskay/7D74C94E/mm/112',
                                 'hk': 'http://store.valentino.com/VALENTINO/home/tskay/3DC16A52/mm/112',
                                 'tw': 'http://store.valentino.com/VALENTINO/home/tskay/928128F6/mm/112'
                   }}

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(ValentinoSpider, self).__init__('valentino', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"switchSeason")]/ul/li/*[contains(@class,"Season") and @href '
                              'and (name()="span" or name()="a")]'):
            try:
                tag_text = self.reformat(node.xpath('text()').extract()[0])
                tag_name = tag_text.lower()
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                          callback=self.parse_gender, errback=self.onerr, meta={'userdata': m})

        try:
            tag_text = self.reformat(sel.xpath('//div[contains(@class,"switchSeason")]/ul/li'
                                               '/span[@class="mainSeason"]/text()').extract()[0])
            tag_name = tag_text.lower()
        except (IndexError, TypeError):
            return
        metadata['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
        for val in self.parse_gender(response):
            yield val

    def parse_gender(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//div[contains(@class,"switchGender")]')
        if node_list:
            for node in node_list[0].xpath('./ul/li/a[@href and @class="notSelGender"]'):
                try:
                    tmp = self.reformat(node.xpath('text()').extract()[0]).lower()
                except (TypeError, IndexError):
                    continue
                m = copy.deepcopy(metadata)
                gender = cm.guess_gender(tmp)
                if gender:
                    m['gender'] = [gender]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              callback=self.parse_cat1, errback=self.onerr, meta={'userdata': m})
            try:
                tmp = self.reformat(node_list[0].xpath('./ul/li/span[@class="selGender"]/text()').extract()[0]).lower()
                gender = cm.guess_gender(tmp)
                if gender:
                    metadata['gender'] = [gender]
            except (TypeError, IndexError):
                pass

        for val in self.parse_cat1(response):
            yield val

    def parse_cat1(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//div[@id="subMenu"]/ul[contains(@class,"menuNavigation")]/li/a[@title and @href]'):
            try:
                tag_text = self.reformat(node1.xpath('@title').extract()[0])
                tag_name = tag_text.lower()
            except (TypeError, IndexError):
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-1'] = [{'name': tag_name, 'title': tag_text}]

            node_list = node1.xpath('../ul/li/a[@href and @title]')
            if node_list:
                for node2 in node_list:
                    try:
                        tag_text = self.reformat(node2.xpath('@title').extract()[0])
                        tag_name = tag_text.lower()
                    except (IndexError, TypeError):
                        continue
                    m2 = copy.deepcopy(m1)
                    m2['tags_mapping']['category-2'] = [{'name': tag_name, 'title': tag_text}]
                    yield Request(url=self.process_href(node2.xpath('@href').extract()[0], response.url),
                                  callback=self.parse_cat2, errback=self.onerr, meta={'userdata': m2})
            else:
                yield Request(url=self.process_href(node1.xpath('@href').extract()[0], response.url),
                              callback=self.parse_cat2, errback=self.onerr, meta={'userdata': m1})

    def parse_cat2(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//ul[@id="micro"]/li/a[@href and @title]')
        if node_list:
            for node in node_list:
                try:
                    tag_text = self.reformat(node.xpath('@title').extract()[0])
                    tag_name = tag_text.lower()
                except (IndexError, TypeError):
                    continue
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-3'] = [{'name': tag_name, 'title': tag_name}]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              callback=self.parse_filter, errback=self.onerr, meta={'userdata': m})
        else:
            for val in self.parse_filter(response):
                yield val

    def parse_filter(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//ul[@id="filterColor"]/li//a[@href]')
        if node_list:
            for node in node_list:
                try:
                    color = self.reformat(node.xpath('@title').extract()[0]).lower()
                except (IndexError, TypeError):
                    color = None
                m = copy.deepcopy(metadata)
                if color:
                    m['color'] = [color]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              callback=self.parse_list, errback=self.onerr, meta={'userdata': m})
        else:
            for val in self.parse_list(response):
                yield val

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="elementsContainer"]/div[contains(@id,"item") and @class="productimage"]'):
            tmp = node.xpath('.//a[@class="itemContainer" and @href and @title]')
            if not tmp:
                continue
            tmp = tmp[0]
            model = self.reformat(tmp.xpath('@title').extract()[0])
            url = self.process_href(tmp.xpath('@href').extract()[0], response.url)

            name = None
            try:
                name = self.reformat(
                    node.xpath('.//div[@class="descCont"]/span[@class="prodInfoViewAll"]/text()').extract()[0])
            except IndexError:
                pass

            price = None
            try:
                price = self.reformat(
                    node.xpath('.//div[@class="priceCont"]/span[@class="prodPrice"]/text()').extract()[0])
            except IndexError:
                pass

            m = copy.deepcopy(metadata)
            m['model'] = model
            if name:
                m['name'] = name
            if price:
                m['price'] = price
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m},
                          dont_filter=True)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        tmp = sel.xpath('//div[@id="descr"]/span').extract()
        tmp = '\r'.join(self.reformat(val) for val in tmp)
        if tmp:
            metadata['description'] = tmp

        tmp = sel.xpath('//div[@id="details"]/span').extract()
        tmp = '\r'.join(self.reformat(val) for val in tmp)
        if tmp:
            metadata['details'] = tmp

        image_urls = []
        for href in sel.xpath('//div[@id="innerThumbs"]//img[@src and contains(@class,"thumb")]/@src').extract():
            mt = re.search(r'_(\d)+_[a-zA-Z]\.[^/]+$', href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            image_urls.extend(re.sub(r'(?<=_)\d+(?=_[a-zA-Z]\.[^/]+)', str(val), href)
                              for val in xrange(start_idx, 15))

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item