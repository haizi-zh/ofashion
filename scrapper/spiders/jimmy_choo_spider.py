# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re
import json

class JimmyChooSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10184,
        'home_urls': {
            'us': 'http://us.jimmychoo.com/?geoip=geoip&siteid=jchus',
            'uk': 'http://www.jimmychoo.com/?geoip=geoip&siteid=jchgb',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(JimmyChooSpider, self).__init__('jimmy choo', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="header"]//div[@class="categorymenu"]/ul/li[child::a[text()]]')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

            # 二级标签
            sub_nodes = node.xpath('./ul/li[child::a[text()]]')
            for sub_node in sub_nodes:
                mc = copy.deepcopy(m)

                tag_text = sub_node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()

                if tag_text and tag_name:
                    mc['tags_mapping']['category-1'] = [
                        {'name': tag_name, 'title': tag_text,},
                    ]

                    gender = common.guess_gender(tag_name)
                    if gender:
                        mc['gender'] = [gender]

                # 三级标签，有些没有
                third_nodes = sub_node.xpath('./div/a[text()]')
                for third_node in third_nodes:
                    mcc = copy.deepcopy(mc)

                    tag_text = third_node.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mcc['tags_mapping']['category-2'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mcc['gender'] = [gender]

                    href = third_node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)

                    yield Request(url=href,
                                  callback=self.parse_product_list,
                                  errback=self.onerr,
                                  meta={'userdata': mcc})

                href = sub_node.xpath('./a/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': mc})

            # 这里只有二级和三级链接进入以后才有单品列表，这里进入没用。
            # href = node.xpath('./a/@href').extract()[0]
            # href = self.process_href(href, response.url)
            #
            # yield Request(url=href,
            #               callback=self.parse_left_nav,
            #               errback=self.onerr,
            #               meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="search"]//div[contains(@class, "producttile")]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name = node.xpath('./div[@class="name"]/a/text()').extract()[0]
            name = self.reformat(name)
            if name:
                m['name'] = name

            price_node = node.xpath('./div[@class="pricing"]/div[@class="price"]/div[@class="salesprice"]')
            # 非打折商品
            if price_node:
                price = price_node.xpath('./text()').extract()[0]
                price = self.reformat(price)
                if price:
                    m['price'] = price
            # 打折商品
            else:
                price = node.xpath('./div[@class="pricing"]/div[@class="price"]/div[@class="discountprice"]/div[@class="standardprice"]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    m['price'] = price

                discount_price = node.xpath('./div[@class="pricing"]/div[@class="price"]/div[@class="discountprice"]/div[@class="salesprice"]/text()').extract()[0]
                discount_price = self.reformat(discount_price)
                if discount_price:
                    m['price_discount'] = discount_price

            colors = None
            color_nodes = node.xpath('./div[@class="swatches"]/div[@class="palette"]/div[@class="innerpalette"]/a[@title]')
            if color_nodes:
                colors = [
                    self.reformat(val).lower()
                    for val in color_nodes.xpath('./@title').extract()
                ]
            if colors:
                m['color'] = colors

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这里不进入其他页面，因为后边找图片的方法，可以把所有颜色的图片找全
        # # 其他颜色页面
        # color_href_nodes = sel.xpath('//div[@class="variationattributes"]/div[@class="swatches color"]/ul/li/a[@href]')
        # for node in color_href_nodes:
        #     m = copy.deepcopy(metadata)
        #
        #     href = node.xpath('./@href').extract()[0]
        #     href = self.process_href(href, response.url)
        #
        #     Request(url=href,
        #             callback=self.parse_product,
        #             errback=self.onerr,
        #             meta={'userdata': m})

        metadata['url'] = response.url

        # model隐藏在源码的一个link里边
        model = None
        model_link_node = sel.xpath('//link[@rel="canonical"][@href]')
        if model_link_node:
            model_link = model_link_node.xpath('./@href').extract()[0]
            model_link = self.reformat(model_link)
            if model_link:
                mt = re.search(r'-(\w+)\.', model_link)
                if mt:
                    model = mt.group(1).upper()
        if model:
            metadata['model'] = model
        else:
            return

        # 有些时候，这个单品的颜色，在list里边不会展示
        # 这里在单品页再取一次
        # 比如：http://row.jimmychoo.com/en/women/handbags/cross-body-bags/rebel/black--grainy-calf-leather-cross-body-bag-247rebelgrc.html?start=5&dwvar_247rebelgrc_size=One%20Size&dwvar_247rebelgrc_color=Black
        colors = None
        color_nodes = sel.xpath('//div[@class="variationattributes"]/div[@class="swatches color"]/ul/li/a[@title]')
        if color_nodes:
            colors = [
                self.reformat(val)
                for val in color_nodes.xpath('./@title').extract()
            ]
        if (not metadata.get('color')) and colors:
            metadata['color'] = colors

        description_node = sel.xpath('//div[@id="descriptionAccordian"]/p[text()]')
        if description_node:
            description = description_node.xpath('./text()').extract()[0]
            description = self.reformat(description)
            if description:
                metadata['description'] = description

        # 这里包含了页面上 DELIVERY AND RETURNS 和 SIZE AND FIT 两部分
        # 感觉都有点儿用
        detail_node = sel.xpath('//div[@id="deliveryAccordian" or @id="sizeAccordian"]/p[text()]')
        if detail_node:
            detail = '\r'.join(
                self.reformat(val)
                for val in detail_node.xpath('./text()').extract()
            )
            detail = self.reformat(detail)
            if detail:
                metadata['details'] = detail

        image_urls = []
        start = 0
        while 1:
            mt = re.search(r'xlarge:', response.body[start:])
            if mt:
                result = common.extract_closure(response.body[mt.start():], '\[', '\]')
                content = result[0]
                start = result[2]
                if 0 == start:
                    break
                url_list = re.findall('"url":.*\'(.+)\?.*\'', content)
                for url in url_list:
                    image_urls += [self.process_href(url, response.url)]
            else:
                break

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
