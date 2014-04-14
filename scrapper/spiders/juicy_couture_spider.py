# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class JuicyCoutureSpider(MFashionSpider):

    # TODO 网页似乎没有切换国家选项，价格写的是$，网址也是美国的，所以这里写了美国

    spider_data = {
        'brand_id': 10186,
        'home_urls': {
            'us': 'http://www.juicycouture.com/'
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(JuicyCoutureSpider, self).__init__('juicy_couture', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//nav/ul[contains(@class, "menu-category")]/li')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a/span[text()]/text()').extract()[0]
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

                sub_nodes = node.xpath('./div/div/ul/li')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
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

                        third_nodes = sub_node.xpath('./div/ul/li')
                        for third_node in third_nodes:
                            try:
                                tag_text = third_node.xpath('./a/text()').extract()[0]
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
                                    href = third_node.xpath('./a/@href').extract()[0]
                                    href = self.process_href(href, response.url)
                                except(TypeError, IndexError):
                                    continue

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

                        try:
                            href = sub_node.xpath('./a/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//ul[@id="search-result-items"]/li/div/a[@href]')
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

        # 页面下拉到底部会自动加载更多，需要模拟请求，解析返回的json
        # 测试发现，在原有url后边添加 ?start=20 也可以取到第二页内容
        # 如果当前页有内容，再考虑请求下一页
        if product_nodes:
            # 取的当前页数
            current_start = 0
            mt = re.search(r'start=(\d+)', response.url)
            if mt:
                current_page = (int)(mt.group(1))

            next_start = current_start + len(product_nodes)
            # 拼下一页的url
            if mt:
                next_url = re.sub(r'start=\d+', str.format('start={0}', next_start), response.url)
            else:
                next_url = str.format('{0}?start={1}', response.url, next_start)

            # 请求下一页
            yield Request(url=next_url,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': metadata})

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

        image_urls = []
        image_nodes = sel.xpath('//div[@id="pdpMain"]//div[@class="product-thumbnails"]/ul/li/a/img[@class][@src]')
        for image_node in image_nodes:
            try:
                src_text = image_node.xpath('./@src').extract()[0]
                src_text = self.process_href(src_text, response.url)
                if src_text:
                    src = re.sub(ur'\?.*', '?scl=1&fmt=jpg', src_text)
                    if src:
                        image_urls += [src]
            except(TypeError, IndexError):
                continue

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
        model_node = sel.xpath('//div[@id="pdpMain"]//span[@itemprop="productID"][text()]')
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
        origin_node = sel.xpath('//div[@id="pdpMain"]//div[@class="product-price"]/span[contains(@class, "price-standard")][text()]')
        if origin_node:
            try:
                old_price_text = origin_node.xpath('./text()').extract()[0]
                old_price_text = cls.reformat(old_price_text)
                if old_price_text:
                    old_price = old_price_text
            except(TypeError, IndexError):
                pass

            try:
                discount_node = sel.xpath('//div[@id="pdpMain"]//div[@class="product-price"]/span[contains(@class, "price-sales")][text()]')
                if discount_node:
                    new_price_text = discount_node.xpath('./text()').extract()[0]
                    new_price_text = cls.reformat(new_price_text)
                    if new_price_text:
                        new_price = new_price_text
            except(TypeError, IndexError):
                pass
        else:
            old_price_node = sel.xpath('//div[@id="pdpMain"]//div[@class="product-price"]/span[contains(@class, "price-sales")][text()]')
            if old_price_node:
                try:
                    old_price_text = old_price_node.xpath('./text()').extract()[0]
                    old_price_text = cls.reformat(old_price_text)
                    if old_price_text:
                        old_price = old_price_text
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
        name_node = sel.xpath('//div[@id="pdpMain"]//h1[@class="product-name"][text()]')
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
        description_node = sel.xpath('//div[@id="pdpMain"]//div[@class="cnt product-description"]')
        if description_node:
            try:
                description_text = '\r'.join(cls.reformat(val) for val in description_node.xpath('.//text()').extract())
                description_text = cls.reformat(description_text)
                if description_text:
                    description = description_text
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        color_nodes = sel.xpath('//div[@id="pdpMain"]//li[@class="attribute color"]//a[@title]')
        for color_node in color_nodes:
            try:
                color_name = color_node.xpath('./@title').extract()[0]
                color_name = color_name.lower()
                if color_name:
                    colors += [color_name]
            except(TypeError, IndexError):
                continue

        return colors
