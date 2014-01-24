# coding=utf-8


__author__ = 'Ryan'


from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re
from utils.utils import unicodify, iterable


class MaxMaraSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10248,
        'home_urls': {
            'uk': 'http://gb.maxmara.com/',
            'at': 'http://at.maxmara.com/',
            'be': 'http://be.maxmara.com/',
            'cy': 'http://cy.maxmara.com/',
            'cz': 'http://cz.maxmara.com/',
            'dk': 'http://dk.maxmara.com/',
            'fi': 'http://fi.maxmara.com/',
            'fr': 'http://fr.maxmara.com/',
            'de': 'http://de.maxmara.com/',
            'gr': 'http://gr.maxmara.com/',
            'hu': 'http://hu.maxmara.com/',
            'ie': 'http://ie.maxmara.com/',
            'it': 'http://it.maxmara.com/',
            'lu': 'http://lu.maxmara.com/',
            'mt': 'http://mt.maxmara.com/',
            'nl': 'http://nl.maxmara.com/',
            'pl': 'http://pl.maxmara.com/',
            'pt': 'http://pt.maxmara.com/',
            'sk': 'http://sk.maxmara.com/',
            'si': 'http://si.maxmara.com/',
            'es': 'http://es.maxmara.com/',
            'se': 'http://se.maxmara.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MaxMaraSpider, self).__init__('max_mara', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="nav_main"]//nav[@id="menu"]/ul/ul/div/li[child::a[text()]]')
        for node in nav_nodes:
            tag_text = node.xpath('./a[text()]/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./ul/li[child::a[@href][text()]]')
                for sub_node in sub_nodes:
                    tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        href = sub_node.xpath('./a[@href]/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_collection,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_collection(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        collection_nodes = sel.xpath('//div[@id="main"]/ul//div[@class="row"]')
        for node in collection_nodes:
            tag_text = node.xpath('.//h2/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                product_nodes = node.xpath('.//div[@class="thumbnail"][child::a[@href]]')
                for product_node in product_nodes:
                    mc = copy.deepcopy(m)

                    href = product_node.xpath('./a[@href]/@href').extract()[0]
                    href = self.process_href(href, response.url)

                    yield Request(url=href,
                                  callback=self.parse_product,
                                  errback=self.onerr,
                                  meta={'userdata': mc},
                                  dont_filter=True)

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)


        other_nodes = sel.xpath('//aside[@id="sidebar"]//div[@class="colour switcher-box clearfix"]/ul/li/a[@value]')
        for node in other_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@value').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})


        metadata['url'] = response.url


        model = None
        model_node = sel.xpath('//input[@id="productCodePost"][@value]')
        if model_node:
            model = model_node.xpath('./@value').extract()[0]
            model = self.reformat(model)

        if model:
            metadata['model'] = model
        else:
            return


        name = None
        name_node = sel.xpath('//aside[@id="sidebar"]//div[@id="productDetailUpdateableTitle"]/h1[text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)

        if name:
            metadata['name'] = name


        old_price = None
        new_price = None
        old_price_node = sel.xpath('//aside[@id="sidebar"]//div[@id="big-price"]/span[@class="exSalesPrice"][text()]')
        if old_price_node:
            old_price = old_price_node.xpath('./text()').extract()[0]
            old_price = self.reformat(old_price)

            new_price = sel.xpath('//aside[@id="sidebar"]//div[@id="big-price"]/*[@class="big-price"][text()]/text()').extract()[0]
            new_price = self.reformat(new_price)
        else:
            old_price = sel.xpath('//aside[@id="sidebar"]//div[@id="big-price"]/*[@class="big-price"][text()]/text()').extract()[0]
            old_price = self.reformat(old_price)

        if old_price:
            metadata['price'] = old_price
        if new_price:
            metadata['price_discount'] = new_price


        colors = None
        color_nodes = sel.xpath('//aside[@id="sidebar"]//div[@class="colour switcher-box clearfix"]/ul/li/a/img[@title]')
        if color_nodes:
            colors = [
                self.reformat(val)
                for val in color_nodes.xpath('./@title').extract()
            ]

        if colors:
            metadata['color'] = colors


        description = None
        description_nodes = sel.xpath('//aside[@id="sidebar"]//ul[@id="productDetailsAndStyleDiv"]/*[text()]')
        if description_nodes:
            description = '\r'.join(
                self.reformat(val)
                for val in description_nodes.xpath('.//text()').extract()
            )
            description = self.reformat(description)

        if description:
            metadata['description'] = description


        details = None
        detail_nodes = sel.xpath('//aside[@id="sidebar"]//div[@id="tab-fitting"]//ul/li[position() < last()][text()]')
        if detail_nodes:
            details = '\r'.join(
                self.reformat(val)
                for val in detail_nodes.xpath('./text()').extract()
            )
            details = self.reformat(details)

        if details:
            metadata['details'] = details


        image_urls = None
        image_nodes = sel.xpath('//div[@id="product-thumbs"]/ul/li/a[@href]')
        if image_nodes:
            image_urls = [
                self.process_href(val, response.url)
                for val in image_nodes.xpath('./@href').extract()
            ]


        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
