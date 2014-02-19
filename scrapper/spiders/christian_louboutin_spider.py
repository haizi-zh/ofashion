# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class ChristianLouboutinSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10084,
        'currency': {
            'ca': 'USD',
        },
        'home_urls': {
            'us': 'http://us.christianlouboutin.com/us_en/',
            'ca': 'http://us.christianlouboutin.com/ca_en/',
            'fr': 'http://eu.christianlouboutin.com/fr_fr/',
            'uk': 'http://eu.christianlouboutin.com/uk_en/',
            'it': 'http://eu.christianlouboutin.com/it_en/',
            'de': 'http://eu.christianlouboutin.com/de_en/',
            'es': 'http://eu.christianlouboutin.com/es_en/',
            'ch': 'http://eu.christianlouboutin.com/ch_en/',
            'at': 'http://eu.christianlouboutin.com/at_en/',
            'be': 'http://eu.christianlouboutin.com/be_fr/',
            'gr': 'http://eu.christianlouboutin.com/gr_en/',
            'ie': 'http://eu.christianlouboutin.com/ie_en/',
            'lu': 'http://eu.christianlouboutin.com/lu_fr/',
            'mc': 'http://eu.christianlouboutin.com/mc_fr/',
            'nl': 'http://eu.christianlouboutin.com/nl_en/',
            'pt': 'http://eu.christianlouboutin.com/pt_en/',
            'hk': 'http://asia.christianlouboutin.com/hk_en/',
            # 'mo': 'http://asia.christianlouboutin.com/hk_en/',  # 它这个和香港用了一个地址
            'my': 'http://asia.christianlouboutin.com/my_en/',
            'sg': 'http://asia.christianlouboutin.com/sg_en/',
            'tw': 'http://asia.christianlouboutin.com/tw_tc/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(ChristianLouboutinSpider, self).__init__('christian loubouin', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="container"]/div/ul/li/ul/li/a[text()]')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name, extra={'male': [], 'female': ['lady']})
                if gender:
                    m['gender'] = [gender]

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_sub_nav,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_sub_nav(self, response):
        """
        处理二级分类
        有些分类有二级分类，比如men
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        sub_nav_nodes = sel.xpath('//div[@id="main"]/div/div[contains(@class, "navigation")]/ul/li/ul/li/ul/li/a[text()]')
        for sub_node in sub_nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = sub_node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name, extra={'male': [], 'female': ['lady']})
                if gender:
                    m['gender'] = [gender]

            href = sub_node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_third_nav,
                          errback=self.onerr,
                          meta={'userdata': m})

        for val in self.parse_procut_list(response):
            yield val

    def parse_third_nav(self, response):
        """
        有些二级分类有三级
        比如：http://us.christianlouboutin.com/us_en/shop-online-3/women/platforms.html
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        third_nav_nodes = sel.xpath('//div[@id="main"]/div/div[contains(@class, "navigation")]/ul/li/ul/li/ul/li/ul/li/a[text()]')
        for sub_node in third_nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = sub_node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name, extra={'male': [], 'female': ['lady']})
                if gender:
                    m['gender'] = [gender]

            href = sub_node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_procut_list,
                          errback=self.onerr,
                          meta={'userdata': m})

        for val in self.parse_procut_list(response):
            yield val

    def parse_procut_list(self, response):
        """
        处理单品列表
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@class="category-view"]/div/a')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name_node = node.xpath('.//h3[text()]')
            if name_node:
                name = name_node.xpath('./text()').extract()[0]
                name = self.reformat(name)

                m['name'] = name

                gender = common.guess_gender(name, extra={'male': [], 'female': ['lady']})
                if gender:
                    m['gender'] = [gender]

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 进入不同颜色的单品页，它给了不同的单品号
        other_nodes = sel.xpath('//dl[@id="media-tabs"]/dd[2]//a')
        for other_node in other_nodes:
            m = copy.deepcopy(metadata)

            href = other_node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

            gender = common.guess_gender(name, extra={'male': [], 'female': ['lady']})
            if gender:
                metadata['gender'] = [gender]

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        image_urls = None
        image_nodes = sel.xpath('//dl[@id="media-tabs"]/dd/div[@class="more-views"]/ul/li/a[@href]')
        if image_nodes:
            image_urls = [
                self.process_href(val, response.url)
                for val in image_nodes.xpath('./@href').extract()
            ]

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        model = None
        try:
            model_node = sel.xpath('//dl[@id="collateral-tabs"]/dd//ul/li[1]/strong[text()]')
            if model_node:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            price_node = sel.xpath('//div[@class="product-shop"]/div[@class="price-box"]//span[@class="price"][text()]')
            if price_node:
                old_price = price_node.xpath('./text()').extract()[0]
                old_price = cls.reformat(old_price)
        except(TypeError, IndexError):
            pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        try:
            name_node = sel.xpath('//div[@class="product-view"]/form/hgroup/h1[text()]')
            if name_node:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            description_node = sel.xpath('//div[@id="product-description"][text()]')
            if description_node:
                description = description_node.xpath('./text()').extract()[0]
                description = cls.reformat(description)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        details = None
        try:
            detail_nodes = sel.xpath('//dl[@id="collateral-tabs"]/dd//ul/li[preceding-sibling::li[1]]')
            if detail_nodes:
                detail = '\r'.join(
                    cls.reformat(val)
                    for val in (
                        ''.join(cls.reformat(node_text))
                        for node_text in detail_nodes.xpath('.//text()').extract()
                    )
                )
                detail = cls.reformat(detail)
        except(TypeError, IndexError):
            pass

        return details

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)

        colors = []
        # 它这个颜色只写一个，多种颜色会写multi
        color_node = sel.xpath('//dl[@id="collateral-tabs"]/dd//ul/li[2]/strong[text()]')
        if color_node:
            try:
                color = color_node.xpath('./text()').extract()[0]
                color = cls.reformat(color)
                if color:
                    colors = [color]
            except(TypeError, IndexError):
                pass

        return colors
