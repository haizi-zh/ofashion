# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class DknySpider(MFashionSpider):

    spider_data = {
        'brand_id': 10108,
        'home_urls': {
            'us': 'http://www.dkny.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(DknySpider, self).__init__('dkny', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@class="header"]/div[@class="fixer"]/div[contains(@class, "global-nav")]/ul/li')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = ''.join(
                self.reformat(val)
                for val in nav_node.xpath('./a//text()').extract()
            )
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = nav_node.xpath('./div/div/ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    # 这里有些有下一级，有些没有
                    # 无下一级的是这里是a标签下文字，有下一级的是span下文字
                    span_tag_node = sub_node.xpath('./span')
                    if span_tag_node:
                        tag_text = span_tag_node.xpath('./text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()

                        if tag_text and tag_name:
                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text,},
                            ]

                            gender = common.guess_gender(tag_name)
                            if gender:
                                mc['gender'] = [gender]

                            third_nodes = sub_node.xpath('./div/ul/li')
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
                    else:
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

                            href = sub_node.xpath('./a/@href').extract()[0]
                            href = self.process_href(href, response.url)

                            yield Request(url=href,
                                          callback=self.parse_product_list,
                                          errback=self.onerr,
                                          meta={'userdata': mc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="container"]/div[contains(@class, "view-product_list")]//ul/li[@class="product"]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name = node.xpath('.//a[@class="product-name"]/text()').extract()[0]
            name = self.reformat(name)
            if name:
                m['name'] = name

            price_node = node.xpath('.//div[@class="product-price"]')
            if price_node:
                # 这里检查是不是打折商品
                discount_price_node = price_node.xpath('./div[@class="product-price-markdown"]')
                if discount_price_node:
                    discount_price = ''.join(
                        self.reformat(val)
                        for val in discount_price_node.xpath('.//text()').extract()
                    )
                    discount_price = self.reformat(discount_price)
                    if discount_price:
                        m['price_discount'] = discount_price

                    price = ''.join(
                        self.reformat(val)
                        for val in price_node.xpath('./div[@class="product-price-was"]//text()').extract()
                    )
                    price = self.reformat(price)
                    if price:
                        m['price'] = price
                else:
                    price = ''.join(
                        self.reformat(val)
                        for val in price_node.xpath('./div[@class="product-price-retail"]//text()').extract()
                    )
                    price = self.reformat(price)
                    if price:
                        m['price'] = price

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            # 这里dont_filter保证不同路径进入单品页
            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        # 下一页
        next_node = sel.xpath('//ul[contains(@class, "page-set")]/li[contains(@class, "next-page")]/a[@href]')
        if next_node:
            m = copy.deepcopy(metadata)

            href = next_node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        # TODO 有没有货号的
        # 比如：http://www.dkny.com/bags/shop-by-shape/view-all/resort13bags145/dknypure-large-hobo?p=2&s=12
        model = None
        mt = re.search(r'.+/(\w+)/.+$', response.url)
        if mt:
            model = mt.group(1)
            if model:
                model = model.upper()
        if model:
            metadata['model'] = model
        else:
            return

        description_node = sel.xpath('//div[contains(@class, "view-product_detail")]//div[@class="product-description"]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('.//text()').extract()
            )
            description = self.reformat(description)

            if description:
                metadata['description'] = description

        colors = None
        color_nodes = sel.xpath('//div[@class="product-info-container"]//form/ul/li/ul/li/a/img[@alt]')
        if color_nodes:
            colors = [
                self.reformat(val).lower()
                for val in color_nodes.xpath('./@alt').extract()
            ]
        if colors:
            metadata['color'] = colors

        # TODO 这里其他颜色的图片怎么取的
        image_urls = []
        image_nodes = sel.xpath('//div[contains(@class, "view-product_detail")]//div[@class="partial-product_viewer"]/ul/li/a/img[@src]')
        for image_node in image_nodes:
            src = image_node.xpath('./@src').extract()[0]
            src = self.process_href(src, response.url)

            # 这里，把src里边的/60/80/替换为/0/0/即可得到全尺寸图片
            src = re.sub(r'/(\d+/\d+)/', '/0/0/', src)

            image_urls += [src]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
