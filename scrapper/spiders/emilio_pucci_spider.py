# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class EmilioPucciSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10117,
        'currency': {
            'cn': 'EUR',
        },
        'home_urls': {
            'cn': 'http://www.emiliopucci.com/home.asp?tskay=06B33963',
            'uk': 'http://www.emiliopucci.com/home.asp?tskay=229040E8',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(EmilioPucciSpider, self).__init__('emilio_pucci', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        seasons_nodes = sel.xpath('//div[@id="div_body_head"]//div[@class="seasonsBox"]/h2')
        for seasons_node in seasons_nodes:
            m = copy.deepcopy(metadata)

            # 检查是不是当前选中状态
            href_node = seasons_node.xpath('./a[@href]')
            if not href_node:
                tag_text = ''.join(
                    self.reformat(val)
                    for val in seasons_node.xpath('.//text()').extract()
                )
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()

                if tag_text and tag_name:
                    m['tags_mapping']['category-0'] = [
                        {'name': tag_name, 'title': tag_text,},
                    ]

                    left_nav_nodes = sel.xpath('//div[@class="page_content"]/div[@id="menu_sx"]/div[@class="left_menu_pad"]//div/ul/li')
                    for left_nav_node in left_nav_nodes:
                        mc = copy.deepcopy(m)

                        tag_node = left_nav_node.xpath('./h2[text()]')
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

                                sub_nodes = left_nav_node.xpath('./ul/li[child::a]')
                                for sub_node in sub_nodes:
                                    mcc = copy.deepcopy(mc)

                                    tag_text = sub_node.xpath('./a/text()').extract()[0]
                                    tag_text = self.reformat(tag_text)
                                    tag_name = tag_text.lower()

                                    if tag_text and tag_name:
                                        mcc['tags_mapping']['category-2'] = [
                                            {'name': tag_name, 'title': tag_text,},
                                        ]

                                        gender = common.guess_gender(tag_name)
                                        if gender:
                                            mcc['gender'] = [gender]

                                        href = sub_node.xpath('./a/@href').extract()[0]
                                        href = self.process_href(href, response.url)

                                        yield Request(url=href,
                                                      callback=self.parse_product_list,
                                                      errback=self.onerr,
                                                      meta={'userdata': mcc})
                        else:
                            tag_text = left_nav_node.xpath('./a/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()

                            if tag_text and tag_name:
                                mc['tags_mapping']['category-1'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mc['gender'] = [gender]

                                href = left_nav_node.xpath('./a/@href').extract()[0]
                                href = self.process_href(href, response.url)

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mc})
            else:
                href = href_node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="elementsContainer"]/div[contains(@id, "item")]')
        for product_node in product_nodes:
            m = copy.deepcopy(metadata)

            href = product_node.xpath('.//a[@href]/@href').extract()[0]
            href = re.sub(r'\r|\n|\t', '', href)
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = None
        mt = re.search(r'cod10=(\w+)', response.url)
        if not mt:
            mt = re.search(r'cod10/(\w+)/', response.url)
        if mt:
            model = mt.group(1)
        if model:
            metadata['model'] = model
        else:
            return

        name = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@class="itemTitle"]/h1/text()').extract()[0]
        name = self.reformat(name)
        if name:
            metadata['name'] = name

        old_price_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@id="itemPriceContainer"]//div[@class="oldprice"][text()]')
        if old_price_node:
            price = old_price_node.xpath('./text()').extract()[0]
            price = self.reformat(price)
            if price:
                metadata['price'] = price

            discount_price_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@id="itemPriceContainer"]//div[@class="newprice"][text()]')
            if discount_price_node:
                discount_price = discount_price_node.xpath('./text()').extract()[0]
                discount_price = self.reformat(discount_price)
                if discount_price:
                    metadata['price_discount'] = discount_price
        else:
            price_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@id="itemPriceContainer"]')
            price = ''.join(
                self.reformat(val)
                for val in price_node.xpath('.//text()').extract()
            )
            price = self.reformat(price)
            if price:
                metadata['price'] = price

        description_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]//div[@id="descr_content"][text()]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('.//text()').extract()
            )
            description = self.reformat(description)

            if description:
                metadata['description'] = description

        detail_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]//div[@id="details_content"][text()]')
        if detail_node:
            detail = '\r'.join(
                self.reformat(val)
                for val in detail_node.xpath('.//text()').extract()
            )
            detail = self.reformat(detail)

            if detail:
                metadata['details'] = detail

        image_urls = []
        image_nodes = sel.xpath('//div[@id="innerContentCol"]//div[@id="thumbContainer"]//div[@class="thumbElement"]/img[@src]')
        for node in image_nodes:
            src = node.xpath('./@src').extract()[0]
            src = self.process_href(src, response.url)

            image_urls += [
                re.sub(r'_\d+_', str.format('_{0}_', val), src)
                for val in xrange(10, 15)
            ]


        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
