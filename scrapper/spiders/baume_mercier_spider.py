# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import common
import re


class BaumeMercierSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10032,
        'home_urls': {
            'us': 'http://www.baume-et-mercier.com/en-us/home.html',
            'uk': 'http://www.baume-et-mercier.co.uk/home.html',
            'ie': 'http://www.baume-et-mercier.co.ie/home.html',

            'cn': 'http://www.baume-et-mercier.com/zh-hans/home.html',
            'jp': 'http://www.baume5-et-mercier.com/ja/home.html',
            'de': 'http://www.baume-et-mercier.com/de/home.html',
            'it': 'http://www.baume-et-mercier.com/it/home.html',
            'pt': 'http://www.baume-et-mercier.com/pt/home.html',
            'fr': 'http://www.baume-et-mercier.com/fr/home.html',
            'es': 'http://www.baume-et-mercier.com/es/home.html',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(BaumeMercierSpider, self).__init__('baume&mercier', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@class="l-watches-navigation"]/div/div[@class="navigation-element"]')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = ''.join(
                    self.reformat(val)
                    for val in node.xpath('./h3//text()').extract()
                )
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_collection,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_collection(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        collection_nodes = sel.xpath('//table[@id="top-watches-list"]//tr[@class="top-list-buttons"]/td/a')
        for node in collection_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = node.xpath('./@title').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_list_collection,
                          errback=self.onerr,
                          meta={'userdata': m})

        # 这里既可能是男女的collection页，可能是watch finder页
        view_all_node = sel.xpath('//div[@id="l-gender-teaser"]//a[@href]')
        if view_all_node:
            try:
                href = view_all_node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list_collection,
                              errback=self.onerr,
                              meta={'userdata': metadata})
            except(TypeError, IndexError):
                pass
        else:
            for val in self.parse_product_list_watchesfinder(response):
                yield val

    def parse_product_list_collection(self, response):
        """
        解析从男女分类进入系列之后的单品列表
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[contains(@class, "clearfix")]/div')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            # # 这里这个应该既是单品名字，有是单品货号
            # model_node = node.xpath('./h2[@class="watch-tile-title"]/a')
            # if model_node:
            #     try:
            #         model = model_node.xpath('./text()').extract()[0]
            #         model = self.reformat(model)
            #         if model:
            #             m['model'] = model.upper()
            #             m['name'] = model.lower()
            #     except(TypeError, IndexError):
            #         pass
            #
            # price_node = node.xpath('.//span[@class="price"]/a')
            # if price_node:
            #     try:
            #         price = price_node.xpath('./text()').extract()[0]
            #         price = self.reformat(price)
            #         if price:
            #             m['price'] = price
            #     except(TypeError, IndexError):
            #         pass

            # 这里边很多链接，随便一个都可以进入单品页
            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product,
                              errback=self.onerr,
                              meta={'userdata': m})
            except(TypeError, IndexError):
                pass

    def parse_product_list_watchesfinder(self, response):
        """
        解析从watchesfinder进入的单品列表
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath(
            '//div[@class="clearfix"]/div[contains(@class, "row")]/div[contains(@class, "column")]/div')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            # model_node = node.xpath('./h5/a')
            # if model_node:
            #     try:
            #         model = model_node.xpath('./text()').extract()[0]
            #         model = self.reformat(model)
            #         if model:
            #             m['model'] = model.upper()
            #             m['name'] = model.lower()
            #     except(TypeError, IndexError):
            #         pass
            # try:
            #     price_node = node.xpath('.//span[@class="price"]')
            #     if price_node:
            #         price = price_node.xpath('./text()').extract()[0]
            #         price = self.reformat(price)
            #         if price:
            #             m['price'] = price
            # except(TypeError, IndexError):
            #     pass

            try:
                href = node.xpath('./a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product(self, response):
        """
        解析单品页面
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        # colors = self.fetch_color(response)
        # if colors:
        #     metadata['color'] = colors

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        # if not metadata.get('model'):
        #     model_node = sel.xpath('//div[@class="l-info-container"]/div[@class="l-info-title"]/h1')
        #     if model_node:
        #         try:
        #             model = model_node.xpath('./text()').extract()[0]
        #             model = self.reformat(model)
        #             if model:
        #                 metadata['model'] = model.upper()
        #                 metadata['name'] = model.lower()
        #         except(TypeError, IndexError):
        #             pass
        #
        # if not metadata.get('model'):
        #     return
        #
        # if not metadata.get('price'):
        #     price_node = sel.xpath('//div[@class="l-info-container"]/div[@class="l-info-title"]/h2')
        #     if price_node:
        #         try:
        #             price = price_node.xpath('./text()').extract()[0]
        #             price = self.reformat(price)
        #             if price:
        #                 metadata['price'] = price
        #         except(TypeError, IndexError):
        #             pass
        #
        # # 有两个部分都应该是description
        # # 这是图片右边的部分
        # description1 = None
        # description_node1 = sel.xpath('//div[@class="l-info-description"]/div/div[contains(@class, "description")]')
        # if description_node1:
        #     try:
        #         description1 = description_node1.xpath('./text()').extract()[0]
        #         description1 = self.reformat(description1)
        #     except(TypeError, IndexError):
        #         pass
        # # 这是图片左下的部分
        # description2 = None
        # description_node2 = sel.xpath(
        #     '//div[@class="l-details"]/div[contains(@class, "information")]/div[contains(@class, "description")]/div[@style]')
        # if description_node2:
        #     try:
        #         description2 = description_node2.xpath('./text()').extract()[0]
        #         description2 = self.reformat(description2)
        #     except(TypeError, IndexError):
        #         pass
        # # 组合两部分
        # description = '\r'.join(
        #     filter(None, [description1, description2])
        # )
        # description = self.reformat(description)
        # if description:
        #     metadata['description'] = description
        #
        # detail_nodes = sel.xpath(
        #     '//div[@class="l-details"]/div[contains(@class, "technical")]/*[not(@id="technicaldetails")][not(contains(@class, "button"))]')
        # if detail_nodes:
        #
        #     def func(node):
        #         try:
        #             node_name = node._root.tag
        #             allText = ''.join(self.reformat(val) for val in node.xpath('./text()').extract())
        #             # h5标签说明他是一行的开头
        #             if node_name == 'h5':
        #                 return '\r' + allText
        #             else:
        #                 return allText
        #         except(TypeError, IndexError):
        #             return ''
        #
        #     try:
        #         detail = ''.join(func(node) for node in detail_nodes)
        #         detail = self.reformat(detail)
        #         if detail:
        #             metadata['details'] = detail
        #     except(TypeError, IndexError):
        #         pass

        image_urls = []
        image_nodes = sel.xpath('//div[@id="scroll"]/ul/li[@data-hdimage]')
        for image_node in image_nodes:
            try:
                url = image_node.xpath('./@data-hdimage').extract()[0]
                url = self.reformat(url)
                if url:
                    url = self.process_href(url, response.url)
                    if url:
                        image_urls += [url]
            except(TypeError, IndexError):
                continue
        # if image_nodes:
        #     try:
        #         image_urls = [
        #             self.process_href(val, response.url)
        #             for val in image_nodes.xpath('./@data-hdimage').extract()
        #         ]
        #     except(TypeError, IndexError):
        #         pass

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
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
        model_node = sel.xpath('//div[@class="l-info-container"]/div[@class="l-info-title"]/h1')
        if model_node:
            try:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = cls.reformat(model_text)
                mt = re.search(ur'\w+$', model_text)
                if mt:
                    model = mt.group(0)
            except(TypeError, IndexError):
                    pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@class="l-info-container"]/div[@class="l-info-title"]/h1')
        if name_node:
            try:
                name_text = name_node.xpath('./text()').extract()[0]
                name_text = cls.reformat(name_text)
                mt = re.search(ur'^\w+', name_text)
                if mt:
                    name = mt.group(0)
            except(TypeError, IndexError):
                    pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@class="l-info-container"]/div[@class="l-info-title"]/h2')
        if price_node:
            try:
                price = price_node.xpath('./text()').extract()[0]
                price = cls.reformat(price)
                if price:
                    old_price = price
            except(TypeError, IndexError):
                pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        # 有两个部分都应该是description
        # 这是图片右边的部分
        description1 = None
        description_node1 = sel.xpath('//div[@class="l-info-description"]/div/div[contains(@class, "description")]')
        if description_node1:
            try:
                description1 = description_node1.xpath('./text()').extract()[0]
                description1 = cls.reformat(description1)
            except(TypeError, IndexError):
                pass
        # 这是图片左下的部分
        description2 = None
        description_node2 = sel.xpath(
            '//div[@class="l-details"]/div[contains(@class, "information")]/div[contains(@class, "description")]/div[@style]')
        if description_node2:
            try:
                description2 = description_node2.xpath('./text()').extract()[0]
                description2 = cls.reformat(description2)
            except(TypeError, IndexError):
                pass
        # 组合两部分
        description = '\r'.join(
            filter(None, [description1, description2])
        )
        description = cls.reformat(description)

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        detail = None
        detail_nodes = sel.xpath(
            '//div[@class="l-details"]/div[contains(@class, "technical")]/*[not(@id="technicaldetails")][not(contains(@class, "button"))]')
        if detail_nodes:

            def func(node):
                try:
                    node_name = node._root.tag
                    allText = ''.join(cls.reformat(val) for val in node.xpath('./text()').extract())
                    # h5标签说明他是一行的开头
                    if node_name == 'h5':
                        return '\r' + allText
                    else:
                        return allText
                except(TypeError, IndexError):
                    return ''

            try:
                detail = ''.join(func(node) for node in detail_nodes)
                detail = cls.reformat(detail)
            except(TypeError, IndexError):
                pass

        return detail
