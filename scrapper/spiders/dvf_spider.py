# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re


class DVFSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10617,
        'home_urls': {
            'us': 'http://www.dvf.com/',
            'uk': 'http://uk.dvf.com/on/demandware.store/Sites-DvF_UK-Site/default/Home-Page',
            'fr': 'http://eu.dvf.com/on/demandware.store/Sites-DvF_EU-Site/default/Home-Page',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(DVFSpider, self).__init__('dvf', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath(
            '//div[@id="container"]/div[@id="header"]/div[@class="cont-main-menu"]/div[@class="inner-menu"]/ul/li[child::a[@href][text()]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./ul/li[child::a[@href][text()]]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text, },
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

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

        product_nodes = sel.xpath('//div[@id="main"]//div[@id="primary"]/div[@class="search-result-content"]/ul/li')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('.//a/@href').extract()[0]
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
        # 如果当前页有内容，再考虑请求下一页，每页有20个
        if product_nodes:
            # 取的当前页数
            current_start = 0
            mt = re.search(r'start=(\d+)', response.url)
            if mt:
                current_page = (int)(mt.group(1))

            next_start = current_start + 20
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

        other_product_nodes = sel.xpath(
            '//div[@id="content"]//div[@id="product-content"]//div[@class="product-variations"]/ul/li/div/ul[@class="swatches Color"]/li/a[@href]')
        for node in other_product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

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
        origin_image_node = sel.xpath('//div[@id="content"]//div[@id="pdp-pinterest-container"]/img[@src]')
        if origin_image_node:
            try:
                origin_image_url = origin_image_node.xpath('./@src').extract()[0]
                origin_image_url = self.process_href(origin_image_url, response.url)
                origin_image_url = re.sub(ur'\?.*$', ur'_A1?$Demandware%20Large%20Rectangle$', origin_image_url)
                if origin_image_url:
                    image_urls += [origin_image_url]
                    image_urls += [
                        re.sub(ur'_A\d\?', str.format(r'_A{0}?', val), origin_image_url)
                        for val in xrange(2, 5)
                    ]
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
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        # TODO 有价格是一个区间的 ：http://uk.dvf.com/on/demandware.store/Sites-DvF_UK-Site/default/Product-Variation?pid=D5873607T13B&dwvar_D5873607T13B_color=&dwvar_D5873607T13B_size=L
        old_price = None
        new_price = None
        discount_node = sel.xpath(
            '//div[@id="content"]//div[@id="product-content"]//div[@class="product-price"]/span[@class="price-standard"][text()]')
        if discount_node:
            try:
                old_price = sel.xpath(
                    '//div[@id="content"]//div[@id="product-content"]//div[@class="product-price"]/span[@class="price-standard"][text()]/text()').extract()[
                    0]
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

            try:
                new_price = sel.xpath(
                    '//div[@id="content"]//div[@id="product-content"]//div[@class="product-price"]/span[@class="price-sales"][text()]/text()').extract()[
                    0]
                new_price = cls.reformat(new_price)
            except(TypeError, IndexError):
                pass
        else:
            old_price_node = sel.xpath(
                '//div[@id="content"]//div[@id="product-content"]//div[@class="product-price"]/span[@class="price-sales"][text()]')
            if old_price_node:
                try:
                    old_price = sel.xpath(
                        '//div[@id="content"]//div[@id="product-content"]//div[@class="product-price"]/span[@class="price-sales"][text()]/text()').extract()[
                        0]
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//div[@id="container"][@data-pid]')
        if model_node:
            try:
                model = model_node.xpath('./@data-pid').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="content"]//div[@id="product-content"]//h1[@class="product-name"][text()]')
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
        description_node = sel.xpath('//div[@id="content"]//div[@itemprop="description"][text()]')
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
        detail_node = sel.xpath('//div[@id="content"]//p[@class="tab-fabric-description"][text()]')
        if detail_node:
            try:
                detail = detail_node.xpath('./text()').extract()[0]
                detail = cls.reformat(detail)
            except(TypeError, IndexError):
                pass

        return detail

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        color_nodes = sel.xpath(
            '//div[@id="content"]//div[@id="product-content"]//div[@class="product-variations"]/ul/li/div/ul[@class="swatches Color"]/li/a[@data-colorname]')
        for color_node in color_nodes:
            try:
                color_name = color_node.xpath('./@data-colorname').extract()[0]
                colors += [color_name]
            except(TypeError, IndexError):
                continue

        return colors
