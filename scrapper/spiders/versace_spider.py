# coding=utf-8
import urlparse
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import re


__author__ = 'Zephyre'


class VersaceSpider(MFashionSpider):
    spider_data = {'brand_id': 10373,
                   'home_urls': {'us': 'http://us.versace.com',
                                 'uk': 'http://uk.versace.com',
                                 'fr': 'http://fr.versace.com',
                                 'it': 'http://it.versace.com',
                                 'es': 'http://eu.versace.com/page/home,en_ES,pg.html',
                                 'be': 'http://eu.versace.com/page/home,en_BE,pg.html',
                                 'nl': 'http://eu.versace.com/page/home,en_NL,pg.html',
                                 'at': 'http://eu.versace.com/page/home,de_AT,pg.html'}}

    @classmethod
    def get_supported_regions(cls):
        return VersaceSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(VersaceSpider, self).__init__('versace', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        metadata['url'] = response.url

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = [self.process_href(href, response.url) for href in
                      sel.xpath('//div[@class="productthumbnails"]/ul[contains(@class,"productthumbnails-list")]'
                                '/li[@data-zoomurl]/@data-zoomurl').extract()]
        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[contains(@class,"productlisting")]/li[contains(@class,"product-item")]'
                              '/a[@href and @title and @name]'):
            m = copy.deepcopy(metadata)
            # tmp = self.reformat(node.xpath('@title').extract()[0])
            # if tmp:
            #     m['name'] = tmp
            #
            # tmp = node.xpath('.//div[@class="salesprice"]/text()').extract()
            # if tmp:
            #     tmp = self.reformat(tmp[0])
            #     if tmp:
            #         m['price'] = tmp
            # tmp = node.xpath('.//div[@class="standardprice"]/text()').extract()
            # if tmp:
            #     tmp = self.reformat(tmp[0])
            #     if tmp:
            #         m['price_discount'] = tmp

            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                          callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        def func(m, node, level):
            """
            根据subCategories，深度优先搜索节点
            @param node:
            @param level:
            """
            for ns in node.xpath('./ul[@class="subCategories"]/li/a[@name and @href]'):
                tmp = ns.xpath('text()').extract()
                if not tmp:
                    continue
                tag_title = self.reformat(tmp[0])
                if not tag_title:
                    continue
                m2 = copy.deepcopy(m)
                m2['tags_mapping'][str.format('category-{0}', level)] = [{'name': tag_title.lower(),
                                                                          'title': tag_title}]
                gender = cm.guess_gender(tag_title)
                if gender:
                    if 'gender' in m2:
                        m2['gender'].append(gender)
                    else:
                        m2['gender'] = [gender]

                # 检查是否为叶节点
                ns_list = ns.xpath('../../ul[@class="subCategories"]/li/a[@name and @href]')
                if ns_list:
                    # 还有子类别
                    for val in func(m2, ns.xpath('../..')[0], level + 1):
                        yield val
                else:
                    # 到达叶节点。使用?sz=9999&productsPerRow=5这样的形式，使得网页不分页
                    query_terms = {}
                    ret = urlparse.urlparse(self.process_href(ns.xpath('@href').extract()[0], response.url))
                    if ret.query.strip():
                        tmp = ret.query.split('&')
                        if tmp:
                            query_terms = {val[0]: val[1] for val in (val2.split('=') for val2 in tmp)}

                    query_terms['sz'] = '9999'
                    query_terms['productsPerRow'] = '5'
                    query_str = '&'.join('='.join((val[0], val[1])) for val in query_terms.items())
                    url = urlparse.urlunparse((ret.scheme, ret.netloc, ret.path, ret.params, query_str, ret.fragment))

                    yield Request(url=url,
                                  callback=self.parse_list, errback=self.onerr, meta={'userdata': m2})

        for node1 in sel.xpath('//ul[contains(@class,"mainMenu")]/li[contains(@class,"mainItem")]/a[@name and @href]'):
            tmp = node1.xpath('text()').extract()
            if not tmp:
                continue
            tag_title = self.reformat(tmp[0])
            if not tag_title:
                continue
            m1 = copy.deepcopy(metadata)
            gender = cm.guess_gender(tag_title)
            if gender:
                m1['gender'] = [gender]

            for node2 in node1.xpath('..//div[@class="section_menu"]'):
                for val in func(copy.deepcopy(m1), node2, 0):
                    yield val

    @classmethod
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider):
        sel = Selector(response)

        model = None
        tmp = sel.xpath('//*[@class="sku" and @itemprop="identifier"]/text()').extract()
        if tmp:
            try:
                model = cls.reformat(tmp[0])
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        origin_price_node = sel.xpath(
            '//div[@class="productinfo"]//div[@itemprop="offerDetails"]//div[@class="standard-price-old standardprice"][text()]')
        if origin_price_node:  # 打折
            try:
                old_price = origin_price_node.xpath('./text()').extract()[0]
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

            discount_price_node = sel.xpath(
                '//div[@class="productinfo"]//div[@itemprop="offerDetails"]//div[@class="salesprice hasStandardPrice"][text()]')
            if discount_price_node:
                try:
                    new_price = discount_price_node.xpath('./text()').extract()[0]
                    new_price = cls.reformat(new_price)
                    new_price = re.sub(ur'\r', '', new_price)
                except(TypeError, IndexError):
                    pass
        else:  # 未打折
            price_node = sel.xpath(
                '//div[@class="productinfo"]//div[@itemprop="offerDetails"]//div[@class="price singleprice"][text()]')
            if price_node:
                try:
                    old_price = ''.join(cls.reformat(val) for val in price_node.xpath('.//text()').extract())
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response, spider):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@class="productinfo"]//*[@class="productname"]/*[@itemprop="category"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider):
        sel = Selector(response)

        description = None
        tmp = filter(lambda val: val and val.strip(),
                     sel.xpath('//*[@class="descText"]/descendant-or-self::text()').extract())
        if tmp:
            try:
                tmp = cls.reformat(tmp[0])
                if tmp:
                    description = tmp
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider):
        sel = Selector(response)

        colors = []
        tmp = sel.xpath('//div[contains(@class,"colorVariations")]/ul/li/a[@title]/@title').extract()
        if tmp:
            try:
                colors = [cls.reformat(val).lower() for val in tmp]
            except(TypeError, IndexError):
                pass

        return colors
