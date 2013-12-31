# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class EccoSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10105,
        'home_urls': {
            'uk': 'http://shopeu.ecco.com/uk/en',
            'de': 'http://shopeu.ecco.com/de/de',
            'be': 'http://shopeu.ecco.com/be/nl-be',
            'fr': 'http://shopeu.ecco.com/fr/fr',
            'ie': 'http://shopeu.ecco.com/ie/en-ie',
            'se': 'http://shopeu.ecco.com/se/sv',
            'no': 'http://shopeu.ecco.com/no/no',
            'fi': 'http://shopeu.ecco.com/fi/fi',
            'nl': 'http://shopeu.ecco.com/nl/nl',
            'pl': 'http://shopeu.ecco.com/pl/pl',
            # 美国，加拿大，中国，各不相同
            # 'cn': 'http://ecco.tmall.com/',
            # 'us': 'http://www.eccocanada.com/',
            # 'ca': 'http://www.eccocanada.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(EccoSpider, self).__init__('ecco', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@class="navbar"]/div[@class="menu-wrapper"]/ul/li')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = nav_node.xpath('./a/span[text()]/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = nav_node.xpath('./ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    # 有些直接span，有些下属a再span
                    tag_node = sub_node.xpath('./span[text()]')
                    if not tag_node:
                        tag_node = sub_node.xpath('./a/span[text()]')
                    if tag_node:
                        tag_text = tag_node.xpath('./text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()

                        if tag_text and tag_name:
                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text,},
                            ]

                            gender = common.guess_gender(tag_name)
                            if gender:
                                mc['gender'] = [gender]

                            third_nodes = sub_node.xpath('./ul/li[child::a]')
                            for third_node in third_nodes:
                                mcc = copy.deepcopy(mc)

                                tag_text = third_node.xpath('./a/span[text()]/text()').extract()[0]
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()

                                if tag_text and tag_name:
                                    mcc['tags_mapping']['category-2'] = [
                                        {'name': tag_name, 'title': tag_text,},
                                    ]

                                    gender = common.guess_gender(tag_name)
                                    if gender:
                                        mcc['gender'] = [gender]

                                    href = third_node.xpath('./a/@href').extract()[0]
                                    href = self.process_href(href, response.url)

                                    yield Request(url=href,
                                                  callback=self.parse_product_list,
                                                  errback=self.onerr,
                                                  meta={'userdata': mcc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        #??? 这里会不会有些链接取不到呢？
        href_list = re.findall(r'href=\\"([^"]+)"', response.body)
        for href_text in href_list:
            m = copy.deepcopy(metadata)

            href = re.sub(r'\\', '', href_text)
            href = self.process_href(href, response.url)

            # 检查是不是去向下一页的链接
            call_back = self.parse_product
            mt = re.search(r'\?page=', href)
            if mt:
                call_back = self.parse_product_list

            yield Request(url=href,
                          callback=call_back,
                          errback=self.onerr,
                          meta={'userdata': m})

        # product_nodes = sel.xpath('//ul[@id="product-list-cont"]/li//a[@href]')
        # for node in product_nodes:
        #     m = copy.deepcopy(metadata)
        #
        #     href = node.xpath('.//a[@href]/@href').extract()[0]
        #     href = self.process_href(href, response.url)
        #
        #     yield Request(url=href,
        #                   callback=self.parse_product,
        #                   errback=self.onerr,
        #                   meta={'userdata': m},
        #                   dont_filter=True)

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = None
        model_node = sel.xpath('//div[@class="pdetail-cont-left"]/div/p[@class="art-number"]/span[@id="prd-item-number"][text()]')
        if model_node:
            model_text = model_node.xpath('./text()').extract()[0]
            model_text = self.reformat(model_text)
            if model_text:
                mt = re.search(r'\b([0-9\-]+)\b', model_text)
                if mt:
                    model = mt.group(1)
        if model:
            metadata['model'] = model
        else:
            return

        name_node = sel.xpath('//div[@class="pdetail-cont-left"]//p[@class="shoe-headline"][text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)
            if name:
                metadata['name'] = name

        colors = None
        color_nodes = sel.xpath('//div[@class="bx-color"]//ul/li[@title]')
        if color_nodes:
            colors = [
                self.reformat(val)
                for val in color_nodes.xpath('./@title').extract()
            ]
        if colors:
            metadata['color'] = colors

        price_node = sel.xpath('//div[@itemprop="offers"]/div[contains(@class, "prd-price") and not(contains(@class, "hidden"))]')
        if price_node:
            del_node = price_node.xpath('./del[text()]')
            if del_node:
                price = del_node.xpath('./text()').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price

                price_discount = price_node.xpath('./div[@itemprop="price"][text()]').extract()[0]
                price_discount = self.reformat(price_discount)
                if price_discount:
                    metadata['price_discount'] = price_discount
            else:
                price = price_node.xpath('./div/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price

        description_node = sel.xpath('//div[@itemprop="description"]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('.//text()').extract()
            )
            description = self.reformat(description)
            if description:
                metadata['description'] = description

        image_urls = []
        image_nodes = sel.xpath('//ul[@class="thumb-list"]/li/a[@src]')
        for node in image_nodes:
            href = node.xpath('./@src').extract()[0]
            href = self.process_href(href, response.url)

            image_urls += [href]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
