# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class JilSanderSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10183,
        'home_urls': {
            'us': 'http://store.jilsander.com/us',
            'au': 'http://store.jilsander.com/au',
            'at': 'http://store.jilsander.com/at',
            'be': 'http://store.jilsander.com/be',
            'bg': 'http://store.jilsander.com/bg',
            'ca': 'http://store.jilsander.com/ca',
            'cz': 'http://store.jilsander.com/cz',
            'dk': 'http://store.jilsander.com/dk',
            'fi': 'http://store.jilsander.com/fi',
            'fr': 'http://store.jilsander.com/fr',
            'de': 'http://store.jilsander.com/de',
            'gr': 'http://store.jilsander.com/gr',
            'hk': 'http://store.jilsander.com/hk',
            'hu': 'http://store.jilsander.com/hu',
            'ie': 'http://store.jilsander.com/ie',
            'it': 'http://store.jilsander.com/it',
            'jp': 'http://store.jilsander.com/jp',
            'lv': 'http://store.jilsander.com/lv',
            'lt': 'http://store.jilsander.com/lt',
            'ru': 'http://store.jilsander.com/ru',
            'sk': 'http://store.jilsander.com/sk',
            'kr': 'http://store.jilsander.com/kr',
            'es': 'http://store.jilsander.com/es',
            'se': 'http://store.jilsander.com/se',
            'ch': 'http://store.jilsander.com/ch',
            'uk': 'http://store.jilsander.com/gb',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(JilSanderSpider, self).__init__('jil_sander', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="sideBarMenu"]/div[@id="categoriesMenu"]/div[child::span[child::a]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./span/a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,}
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./div/div[child::span]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./span/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,}
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        third_nodes = sub_node.xpath('./div/ul/li/a[text()][@href]')
                        for third_node in third_nodes:
                            try:
                                tag_text = third_node.xpath('./text()').extract()[0]
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()
                            except(TypeError, IndexError):
                                continue

                            if tag_text and tag_name:
                                mcc = copy.deepcopy(mc)

                                mcc['tags_mapping']['category-2'] = [
                                    {'name': tag_name, 'title': tag_text,}
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mcc['gender'] = [gender]

                                try:
                                    href = third_node.xpath('./@href').extract()[0]
                                    href = self.process_href(href, response.url)
                                except(TypeError, IndexError):
                                    continue

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

                # try:
                #     href = node.xpath('./span/a/@href').extract()[0]
                #     href = self.process_href(href, response.url)
                # except(TypeError, IndexError):
                #     continue
                #
                # yield Request(url=href,
                #               callback=self.parse_product_nav,
                #               errback=self.onerr,
                #               meta={'userdata': m})
    # def parse_product_nav(self, response):
    #
    #     metadata = response.meta['userdata']
    #     sel = Selector(response)
    #
    #     cat_nodes = sel.xpath('//div[@id="sideBarMenu"]/div[@id="categoriesMenu"]/div[contains(@class, "opened")]/div/div[contains(@class, "MenuContainer")][child::span]')
    #     for node in cat_nodes:
    #         try:
    #             # 这里text有可能在span下属a里边
    #             tag_text = node.xpath('./span//text()').extract()[0]
    #             tag_text = self.reformat(tag_text)
    #             tag_name = tag_text.lower()
    #         except(TypeError, IndexError):
    #             continue
    #
    #         if tag_text and tag_name:
    #             m = copy.deepcopy(metadata)
    #
    #             m['tags_mapping']['category-1'] = [
    #                 {'name': tag_name, 'title': tag_text,}
    #             ]
    #
    #             gender = common.guess_gender(tag_name)
    #             if gender:
    #                 m['gender'] = [gender]
    #
    #             sub_nodes = node.xpath('./div/ul/li/a[@href][text()]')
    #             for sub_node in sub_nodes:
    #                 try:
    #                     tag_text = sub_node.xpath('./text()').extract()[0]
    #                     tag_text = self.reformat(tag_text)
    #                     tag_name = tag_text.lower()
    #                 except(TypeError, IndexError):
    #                     continue
    #
    #                 if tag_text and tag_name:
    #                     mc = copy.deepcopy(m)
    #
    #                     mc['tags_mapping']['category-2'] = [
    #                         {'name': tag_name, 'title': tag_text,}
    #                     ]
    #
    #                     gender = common.guess_gender(tag_name)
    #                     if gender:
    #                         mc['gender'] = [gender]
    #
    #                     try:
    #                         href = sub_node.xpath('./@href').extract()[0]
    #                         href = self.process_href(href, response.url)
    #                     except(TypeError, IndexError):
    #                         continue
    #
    #                     yield Request(url=href,
    #                                   callback=self.parse_product_list,
    #                                   errback=self.onerr,
    #                                   meta={'userdata': mc})
    #
    #             if not sub_nodes:
    #                 try:
    #                     href = node.xpath('./span/a/@href').extract()[0]
    #                     href = self.process_href(href, response.url)
    #                 except(TypeError, IndexError):
    #                     continue
    #
    #                 yield Request(url=href,
    #                               callback=self.parse_product_list,
    #                               errback=self.onerr,
    #                               meta={'userdata': m})


    def parse_product_list(self, response):

        # TODO 每个页面的单品都不多，但是没找到可以下一页的按钮

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="pageContentInner"]/ul/li/a[@href]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

    def parse_product(self, response):

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

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

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

        image_urls = []
        image_nodes = sel.xpath('//div[@id="wrapper"]//ul[@id="imageList"]/li/img[@src]')
        for image_node in image_nodes:
            try:
                src = image_node.xpath('./@src').extract()[0]
                src = self.process_href(src, response.url)
                if src:
                    image_urls += [src]
            except(TypeError, IndexError):
                pass

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def fetch_other_offline_identifier(cls, response, spider=None):
        sel = Selector(response)

        soldout_node = sel.xpath('//div[@id="itemDetails"]//div[@id="soldout"][text()]')

        if soldout_node:
            return True
        else:
            return False

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        other_offline_identifier = cls.fetch_other_offline_identifier(response, spider)

        if model and not other_offline_identifier:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//div[@id="itemDetails"]//div[@id="modelName"][text()]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@id="itemDetails"]//div[@id="itemPrice"]/div[@class="newprice"]')
        if price_node:
            try:
                price = ''.join(price_node.xpath('.//text()').extract())
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
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="itemDetails"]//div[@id="itemTitle"]/h1[text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@id="itemDetails"]//div[@id="tabs"]/ul[@id="panels"]/li[@class="active"][text()]')
        if description_node:
            try:
                description = description_node.xpath('./text()').extract()[0]
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        detail = None
        detail_node = sel.xpath('//div[@id="itemDetails"]//div[@id="descriptionWrapper"]/*[not(@id="tabs")][text()]')
        if detail_node:
            try:
                detail = '\r'.join(detail_node.xpath('.//text()').extract())
                detail = cls.reformat(detail)
            except(TypeError, IndexError):
                pass

        return detail

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        color_nodes = sel.xpath('//div[@id="itemDetails"]//ul[@id="itemColors"]/li/span[text()]')
        for color_node in color_nodes:
            try:
                color_name = color_node.xpath('./text()').extract()[0]
                colors += [color_name]
            except(TypeError, IndexError):
                continue

        return colors
