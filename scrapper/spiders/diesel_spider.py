# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class DieselSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10105,
        'home_urls': {
            'us': 'http://shop.diesel.com/homepage?origin=NOUS',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(DieselSpider, self).__init__('diesel', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这里 ul[] 去掉它标明 moblie only 的内容，去掉最后一个sale标签，单独处理sale
        nav_nodes = sel.xpath('//div[@id="navigation"]/nav/div/ul[not(contains(@class, "mobile"))]/li[not(@id="sale")]')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = nav_node.xpath('./a/span/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                # 前四个标签的第二级
                sub_nodes = nav_node.xpath('./div/ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    # 前两个和第四个二级标签的标题是这种取法，第三个标签的处理在下边
                    tag_node = sub_node.xpath('./div/a')
                    if tag_node:
                        tag_text = sub_node.xpath('./div/a/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()

                        if tag_text and tag_name:
                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text,},
                            ]

                            gender = common.guess_gender(tag_name)
                            if gender:
                                mc['gender'] = [gender]

                            third_nodes = sub_node.xpath('./div/div/ul/li')
                            # 前两个二级标签，有下属，第四个的处理在下边
                            if third_nodes:
                                for third_node in third_nodes:
                                    mcc = copy.deepcopy(mc)

                                    tag_text = third_node.xpath('./a/text()').extract()[0]
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
                            else:   # 第四个标签的下属处理
                                href = sub_node.xpath('./div/a/@href').extract()[0]
                                href = self.process_href(href, response.url)

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mc})

                    else:   # 第三个标签的下属标签处理
                        tag_node = sub_node.xpath('./div/h2[text()]')
                        if tag_node:
                            tag_text = sub_node.xpath('./div/h2/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()
                        else:
                            continue

                        if tag_text and tag_name:
                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text,},
                            ]

                            gender = common.guess_gender(tag_name)
                            if gender:
                                mc['gender'] = [gender]

                            third_nodes = sub_node.xpath('./div/div/ul/li')
                            for third_node in third_nodes:
                                mcc = copy.deepcopy(mc)

                                tag_text = third_node.xpath('./a/text()').extract()[0]
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()

                                if tag_text and tag_name:
                                    mcc['tags_mapping']['category-2'] = [
                                        {'name': tag_name, 'title': tag_text,},
                                    ]

                                    fourth_nodes = third_node.xpath('./div/ul/li')
                                    for fourth_node in fourth_nodes:
                                        mccc = copy.deepcopy(mcc)

                                        tag_text = fourth_node.xpath('./a/text()').extract()[0]
                                        tag_text = self.reformat(tag_text)
                                        tag_name = tag_text.lower()

                                        if tag_text and tag_name:
                                            mccc['tags_mapping']['category-3'] = [
                                                {'name': tag_name, 'title': tag_text,},
                                            ]

                                            gender = common.guess_gender(tag_name)
                                            if gender:
                                                mccc['gender'] = [gender]

                                            href = fourth_node.xpath('./a/@href').extract()[0]
                                            href = self.process_href(href, response.url)

                                            yield Request(url=href,
                                                          callback=self.parse_product_list,
                                                          errback=self.onerr,
                                                          meta={'userdata': mccc})

                # 后几个标签，除了sale，不再区分级别
                sub_nodes = nav_node.xpath('./div/div//a')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    tag_text = sub_node.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        href = sub_node.xpath('./@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

        # 单独处理sale标签及其下属标签
        sale_node = sel.xpath('//div[@id="navigation"]/nav/div/ul[not(contains(@class, "mobile"))]/li[@id="sale"]')
        if sale_node:
            m = copy.deepcopy(metadata)

            tag_text = sale_node.xpath('./a/span/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = sale_node.xpath('./div/div/ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    tag_text = sub_node.xpath('./div/a/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            m['gender'] = [gender]

                        third_nodes = sub_node.xpath('./div/div/ul/li')
                        for third_node in third_nodes:
                            mcc = copy.deepcopy(mc)

                            tag_text = third_node.xpath('./a/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()

                            if tag_text and tag_name:
                                mcc['tags_mapping']['category-2'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                href = third_node.xpath('./a/@href').extract()[0]
                                href = self.process_href(href, response.url)

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[contains(@class, "content")]/div[contains(@class, "grid-tile")]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name = ''.join(
                self.reformat(val)
                for val in node.xpath('.//div[@class="product-name"]//text()').extract()
            )
            name = self.reformat(name)
            if name:
                m['name'] = name

            colors = [
                self.reformat(val)
                for val in node.xpath('.//div[@class="grid-swatch-list"]/ul/li/div[@title]/@title').extract()
            ]
            if colors:
                m['color'] = colors

            discount_node = node.xpath('.//div[@class="product-pricing"]/div[@class="product-discounted-price clearfix"]')
            # 有折扣
            if discount_node:
                discount_price = discount_node.xpath('./span[@class="product-sales-price"]/text()').extract()[0]
                discount_price = self.reformat(discount_price)
                if discount_price:
                    m['price_discount'] = discount_price

                price = discount_node.xpath('./span[@class="product-standard-price"]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    m['price'] = price
            else:
                price = node.xpath('.//div[@class="product-pricing"]/span[@class="product-sales-price"]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    m['price'] = price

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        color_nodes = sel.xpath('//div[@id="product-content-detail"]//div[@class="swatch-Slider"]/ul/li/a[@href]')
        for node in color_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        model = None
        mt = re.search(r'[^/]/(\w+)\.|pid=(\w+)&', response.url)
        if mt:
            model = mt.group(1).upper()
        if model:
            metadata['model'] = model
        else:
            return

        description_node = sel.xpath('//div[@id="pdpMain"]//div[@class="detail-content"]/p/span[@class="para-content"][text()]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('.//text()').extract()
            )
            description = self.reformat(description)
            if description:
                metadata['description'] = description

        detail = ''.join(
            self.reformat(val)
            for val in sel.xpath('//div[@id="product-content-detail"]//ul[@class="product-description-list"]/li[text()]/text()').extract()
        )
        detail = self.reformat(detail)
        if detail:
            metadata['details'] = detail

        image_urls = []
        image_nodes = sel.xpath('//div[@id="pdpMain"]//ul[@class="product-slides-list"]/li/a/img[@src]')
        for image_node in image_nodes:
            image_url = image_node.xpath('./@src').extract()[0]
            image_url = re.sub(r'\?.*', '', image_url)
            if image_url:
                image_urls += [image_url]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
