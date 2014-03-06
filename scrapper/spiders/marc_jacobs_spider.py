# coding=utf-8


__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re


class MarcJacobsSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10239,
        'home_urls': {
            'us': 'http://www.marcjacobs.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MarcJacobsSpider, self).__init__('marc_jacobs', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="nav-main"]/ul/li[child::a[@href][text()]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a[text()]/text()').extract()[0]
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

                sub_nodes = node.xpath('./div[@class="nav-dropdown"]/ul/li')
                for sub_node in sub_nodes:

                    third_nodes = sub_node.xpath('./ul/li[child::a[@href][text()]]')
                    if third_nodes:
                        try:
                            tag_text = sub_node.xpath('./span[text()]/text()').extract()[0]
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

                            for third_node in third_nodes:
                                try:
                                    tag_text = third_node.xpath('./a[text()]/text()').extract()[0]
                                    tag_text = self.reformat(tag_text)
                                    tag_name = tag_text.lower()
                                except(TypeError, IndexError):
                                    continue

                                if tag_text and tag_name:
                                    mcc = copy.deepcopy(mc)

                                    mcc['tags_mapping']['category-2'] = [
                                        {'name': tag_name, 'title': tag_text, },
                                    ]

                                    gender = common.guess_gender(tag_name)
                                    if gender:
                                        mcc['gender'] = [gender]

                                    try:
                                        href = third_node.xpath('./a[@href]/@href').extract()[0]
                                        href = self.process_href(href, response.url)
                                    except(TypeError, IndexError):
                                        continue

                                    yield Request(url=href,
                                                  callback=self.parse_product_list,
                                                  errback=self.onerr,
                                                  meta={'userdata': mcc})
                    else:
                        href_nodes = sub_node.xpath('./a[@href][text()]')
                        for href_node in href_nodes:
                            try:
                                tag_text = href_node.xpath('./text()').extract()[0]
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
                                    href = href_node.xpath('./@href').extract()[0]
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

        product_nodes = sel.xpath(
            '//div[@class="page-container"]/div[@class="partial-product_listpage"]/ul/li//a[@href][not(contains(text(),"Quick View"))]')
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
        # 测试发现，在原有url后边添加 ?p=2 也可以取到第二页内容
        # 如果当前页有内容，再考虑请求下一页
        if product_nodes:
            # 取的当前页数
            current_page = 1
            mt = re.search(r'p=(\d+)', response.url)
            if mt:
                current_page = (int)(mt.group(1))

            next_page = current_page + 1
            # 拼下一页的url
            if mt:
                next_url = re.sub(r'page=\d+', str.format('p={0}', next_page), response.url)
            else:
                next_url = str.format('{0}?p={1}', response.url, next_page)

            # 请求下一页
            yield Request(url=next_url,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        other_nodes = sel.xpath(
            '//div[@class="product-detail-container"]//ul[@class="swatch-set clearfix"]/li/a[@href][@title]')
        for node in other_nodes:
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
        image_nodes = sel.xpath(
            '//div[@class="product-detail-container"]//ul[@class="variant-thumbnail-set"]/li/a/img[@src]')
        for node in image_nodes:
            try:
                url = node.xpath('./@src').extract()[0]
                url = self.process_href(url, response.url)
                if url:
                    image_url = re.sub(ur'/\d+/\d+/', u'/2000/2000/', url)
                    if image_url:
                        image_urls += [image_url]
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
        model_node = sel.xpath('//meta[@itemprop="productID"][@content]')
        if model_node:
            try:
                model = model_node.xpath('./@content').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider):
        sel = Selector(response)
        ret = {}

        price = None
        price_node = sel.xpath('//div[@class="product-detail-container"]//span[@itemprop="price"][text()]')
        if price_node:
            try:
                price = price_node.xpath('./text()').extract()[0]
                price = cls.reformat(price)
            except(TypeError, IndexError):
                pass

        if price:
            ret['price'] = price

        return ret

    @classmethod
    def fetch_name(cls, response, spider):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@class="product-detail-container"]//h1[@itemprop="name"][text()]')
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
        description_node = sel.xpath('//meta[@name="twitter:description"][@content]')
        if description_node:
            try:
                description = description_node.xpath('./@content').extract()[0]
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider):
        sel = Selector(response)

        colors = None
        color_nodes = sel.xpath(
            '//div[@class="product-detail-container"]//ul[@class="swatch-set clearfix"]/li/a[@href][@title]')
        if color_nodes:
            try:
                colors = [
                    cls.reformat(val).lower()
                    for val in color_nodes.xpath('./@title').extract()
                ]
            except(TypeError, IndexError):
                pass

        return colors
