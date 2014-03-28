# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.eshop_spider import EShopSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re
from utils.utils_core import process_price


class WangfujingSpider(EShopSpider):
    spider_data = {
        'brand_id': 8000,
        'home_urls': {
            'cn': 'http://www.wangfujing.com/',
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(WangfujingSpider, self).__init__('wangfujing', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # nav_nodes = sel.xpath('//div[@id="nav"]/ul[@id="nav-list"]/li/a[@href][text()]')
        nav_nodes = sel.xpath('//nav[@id="navigation"]//div[@id="j_cats"]/div/dl[contains(@id, "cat")][child::dt[child::a[text()]]]')
        for node in nav_nodes:
            try:
                # tag_text = node.xpath('./text()').extract()[0]
                # tag_text = self.reformat(tag_text)
                # tag_name = tag_text.lower()
                tag_text = node.xpath('./dt/a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                # sub_nodes = node.xpath('..//div[@class="nav-main"]//div[@class="item-name"]/a[@href][text()]')
                sub_nodes = node.xpath('./div/textarea/div/dl[child::dt[child::a[text()]]]')
                for sub_node in sub_nodes:
                    try:
                        # tag_text = sub_node.xpath('./text()').extract()[0]
                        # tag_text = self.reformat(tag_text)
                        # tag_name = tag_text.lower()
                        tag_text = sub_node.xpath('./dt/a/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        # third_nodes = sub_node.xpath('../../div[@class="item"]/a[@href][text()]')
                        third_nodes = sub_node.xpath('./dd/span/a[@href][text()]')
                        for third_node in third_nodes:
                            try:
                                tag_text = third_node.xpath('./text()').extract()[0]
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()
                            except(TypeError, IndexError):
                                continue

                            mcc = copy.deepcopy(mc)

                            mcc['tags_mapping']['category-2'] = [
                                {'name': tag_name, 'title': tag_text},
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
                                          callback=self.parse_product_brand,
                                          errback=self.onerr,
                                          meta={'userdata': mcc})

    def parse_product_brand(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        brand_nodes = sel.xpath('//div[@id="filters"]/dl[1]/dd//a[@href][child::span[text()]]')
        for node in brand_nodes:
            try:
                brand_text = node.xpath('./span/text()').extract()[0]
                brand_text = self.reformat(brand_text)
                brand_name = brand_text.lower()
            except(TypeError, IndexError):
                continue

            brand_id = self.match_known_brand(brand_name)
            if brand_id:
                try:
                    m = copy.deepcopy(metadata)
                    m['brand_id'] = brand_id

                    try:
                        href = node.xpath('./@href').extract()[0]
                        href = self.process_href(href, response.url)
                        href = self.process_href(href, response.url)
                    except(TypeError, IndexError):
                        continue

                    yield Request(url=href,
                                  callback=self.parse_product_list,
                                  errback=self.onerr,
                                  meta={'userdata': m})
                except(TypeError, IndexError):
                    pass

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="box"]//ul/li//a[@class="itemhover"][@href]')
        for node in product_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        if product_nodes:
            mt = re.search(ur'(\d+)$', response.url)
            if mt:
                try:
                    current_page = (int)(mt.group(1))
                    next_page = current_page + 1
                    href = re.sub(ur'\d+$', str(next_page), response.url)
                    yield Request(url=href,
                                  callback=self.parse_product_list,
                                  errback=self.onerr,
                                  meta={'userdata', metadata})
                except(TypeError, IndexError):
                    pass

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        metadata['url'] = response.url

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = []
        origin_image_nodes = sel.xpath('//div[@id="thumb-list"]//ul/li/a/img[@src]')
        for image_node in origin_image_nodes:
            src = image_node.xpath('./@src').extract()[0]
            src = re.sub(ur'_\d+x\d+', '', src)
            if src:
                image_urls += [src]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response, spider)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//div[@class="prop-wrap"]/div[@id="pro-code"][text()]')
        if model_node:
            try:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = cls.reformat(model_text)
                if model_text:
                    mt = re.search(ur"(\w+)$", model_text)
                    if mt:
                        model = mt.group(1)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//span[@class="breadcrumb_current"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        old_price = None
        new_price = None

        info_node = sel.xpath('//div[@id="WFJ_Item"][text()]')
        if info_node:
            try:
                product_info = info_node.xpath('./text()').extract()[0]
                mt = re.search(ur"\smarket_price\s*:\s*'(\S+)',", product_info)
                if mt:
                    old_price = mt.group(1)
                    old_price = cls.reformat(old_price)
                    mt = re.search(ur"\sprice\s*:\s*'(\S+)',", product_info)
                    if mt:
                        discount_price = mt.group(1)
                        discount_price = cls.reformat(discount_price)
                        if process_price(discount_price, region) != process_price(old_price, region):
                            new_price = discount_price
                        else:
                            new_price = None
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

        description = None
        description_node = sel.xpath('//div[@class="main-wrap"]//ul[@id="attr-list"]/li[text()]')
        if description_node:
            try:
                description = ' '.join(cls.reformat(val) for val in description_node.xpath('.//text()').extract())
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        color_nodes = sel.xpath('//div[@class="prop-wrap"]//dl[@id="color-prop"]//ul/li/a/img[@alt]')
        if color_nodes:
            try:
                colors = [cls.reformat(val) for val in color_nodes.xpath('./@alt').extract()]
            except(TypeError, IndexError):
                pass

        return colors
