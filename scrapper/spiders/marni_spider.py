# coding=utf-8
import json
import urllib
import urlparse
import copy
import re
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'wuya'


class MarniSpider(MFashionSpider):
    spider_data = {'brand_id': 10241,
                   'home_urls': {
                       'cn': 'http://www.marni.cn/areas/corners/sh.asp?tskay=A444F5AB&sts=SHOPWOMAN&season=main',
                       'us': 'http://www.marni.com/home.asp?tskay=3FD17CD7',
                       'fr': 'http://www.marni.com/home.asp?tskay=2FA4E164',
                       'it': 'http://www.marni.com/home.asp?tskay=6383154F',
                       'uk': 'http://www.marni.com/home.asp?tskay=B84CE7A2',
                       'au': 'http://www.marni.com/home.asp?tskay=8B954900',
                       'be': 'http://www.marni.com/home.asp?tskay=6556E1E7',
                       'ca': 'http://www.marni.com/home.asp?tskay=0A0ECED9',
                       'cz': 'http://www.marni.com/home.asp?tskay=0784B54C',
                       'dk': 'http://www.marni.com/home.asp?tskay=5AD08E3B',
                       'fi': 'http://www.marni.com/home.asp?tskay=3B47AB92',
                       'de': 'http://www.marni.com/home.asp?tskay=B490DC11',
                       'gr': 'http://www.marni.com/home.asp?tskay=361A2971',
                       'ie': 'http://www.marni.com/home.asp?tskay=0C8AA578',
                       'il': 'http://www.marni.com/home.asp?tskay=8E260395',
                       'jp': 'http://www.marni.com/home.asp?tskay=050D2086',
                       'nl': 'http://www.marni.com/home.asp?tskay=2A5DD3A9',
                       'no': 'http://www.marni.com/home.asp?tskay=B9BB7D25',
                       'pl': 'http://www.marni.com/home.asp?tskay=6558F54B',
                       'pt': 'http://www.marni.com/home.asp?tskay=F0FDB994',
                       'ru': 'http://www.marni.com/home.asp?tskay=244A367F',
                       'es': 'http://www.marni.com/home.asp?tskay=768BABE2',
                       'ch': 'http://www.marni.com/home.asp?tskay=2BA68C8E',
                       'se': 'http://www.marni.com/home.asp?tskay=4FA1ECD0',
                       'ae': 'http://www.marni.com/home.asp?tskay=320DC484'
                   }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MarniSpider, self).__init__('marni', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@id="banners"]'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        for link in links:
            m = copy.deepcopy(metadata)
            url = link.url
            yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m})

    def parse_cat(self, response):
        link_extractor = SgmlLinkExtractor(restrict_xpaths=('//div[@id="itemsSlider"]'))
        links = link_extractor.extract_links(response)
        metadata = response.meta['userdata']
        sel = Selector(response)
        cat_title = ''.join(sel.xpath('//title/text()').extract()).split()[0]
        cat_name = cat_title.lower()
        for link in links:
            m = copy.deepcopy(metadata)
            url = link.url
            url = urllib.unquote(url)
            url = url.replace('\n', '').replace('\t', '')
            m['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_disount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        image_urls = []
        for href in sel.xpath('//div[@id="thumbs"]//img/@src').extract():
            mt = re.search(r'_(\d+)P_\w\.[a-z]+$', href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            tmp = [re.sub(r'(.+)_\d+P_(\w)\.[a-z]+$', str.format(r'\1_{0}_\2.jpg', idx), href) for idx in
                   xrange(start_idx, 15)]
            image_urls.extend(tmp)

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
        item['metadata'] = metadata
        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model = ''.join(sel.xpath('//div[@id="mfc"]//text()').extract())
        if model:
            model = model.replace('MARNI', '').replace(u'代码', '')
            model = cls.reformat(model)

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price = sel.xpath('//div[@class="itemBoxPrice"]//text()').extract()
        price_str = ''.join(price)
        tmp = sel.xpath('//div[@class="descr"]/text()').extract()
        if price_str.find('%') != -1:
            old_price = price[0]
            old_price = cls.reformat(old_price)
            new_price = price[1]
            new_price = cls.reformat(new_price)
        else:
            old_price = price_str
            old_price = cls.reformat(old_price)
            new_price = None
            new_price = cls.reformat(new_price)

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        try:
            name = ''.join(sel.xpath('//title/text()').extract()).split('-')[0]
            if name:
                name = cls.reformat(name)
        except (IndexError, TypeError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        tmp = sel.xpath('//div[@class="descr"]/text()').extract()
        if tmp:
            description = '\r'.join(filter(lambda x: x, [cls.reformat(val) for val in tmp]))

        return description
