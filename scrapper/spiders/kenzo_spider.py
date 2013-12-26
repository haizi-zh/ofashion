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
from scrapy import log

class KenzoSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10192,
        'currency': {
            'uk': 'EUR',
        },
        'home_urls': {
            'uk': 'https://www.kenzo.com/en/',
            'fr': 'https://www.kenzo.com/fr/',
        },
        'server_urls': {
            'uk': 'https://www.kenzo.com/en/services/product/',
            'fr': 'https://www.kenzo.com/fr/services/product/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(KenzoSpider, self).__init__('kenzo', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这里只有最左边shop下属的标签，因为别的标签没有单品
        nav_nodes = sel.xpath('//div[@id="header-level-2"]//div[contains(@class, "shop")]//ul/li//a[text()][@href]')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

            # 这里解析它的二级标签
            index = nav_nodes.index(node)
            xpath_string = str.format('//div[@class="header-level-3"]/div[{0}]/div/ul/li/a[text()][@href]', index+1)
            sub_nav_nodes = sel.xpath(xpath_string)
            for sub_node in sub_nav_nodes:
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

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="product-list"]/div/ul/li')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name = node.xpath('.//p/a/text()').extract()[0]
            name = self.reformat(name)
            if name:
                m['name'] = name

            # 这里的price是js填上的
            # price = node.xpath('.//p/a/span/text()').extract()[0]
            # price = self.reformat(price)
            # if price:
            #     m['price'] = price

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        # TODO 这是针对童装那个页面的处理
        if not product_nodes:
            pass

    def parse_product(self, response):
        """
        很多信息都在另外的一个请求里
        比如：https://www.kenzo.com/en/services/product/9198
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        # 这个网站没有搜索，也找不到明显的货号
        # 从额外的那个信息请求看来，url后边那部分数字，应该是一个唯一编号性质的
        model = None
        mt = re.search(r'_(\d+)/$', response.url)
        if mt:
            model = mt.group(1)
        if model:
            metadata['model'] = model
        else:
            return

        name = sel.xpath('//div[@class="product-detail JS_product_wrap"]/h2[text()]/text()').extract()[0]
        name = self.reformat(name)
        if name:
            metadata['name'] = name

        # 没看见有打折的商品
        price_node = sel.xpath('//div[@class="product-detail JS_product_wrap"]/p[@class="font-title-product look-price"]')
        if price_node:
            price = ''.join(
                self.reformat(val)
                for val in price_node.xpath('.//text()').extract()
            )
            price = self.reformat(price)
            if price:
                metadata['price'] = price

        # 发送额外信息的请求
        server_url = self.spider_data['server_urls'][metadata['region']]
        request_url = str.format('{0}{1}', server_url, model)

        m = copy.deepcopy(metadata)
        yield Request(url=request_url,
                      callback=self.parse_product_extra,
                      errback=self.onerr,
                      meta={'userdata': m})

        # item = ProductItem()
        # item['url'] = metadata['url']
        # item['model'] = metadata['model']
        # item['metadata'] = metadata
        #
        # yield item

    def parse_product_extra(self, response):
        """
        解析类似https://www.kenzo.com/en/services/product/9198请求返回的数据
        """

        metadata = response.meta['userdata']
        data = json.loads(response.body)

        status = data.get('status')
        data = data.get('data')
        if status == 'ok' and data:
            colors = []
            image_urls = []
            color_list = data.get('colors_list')
            for color_node in color_list:

                color = color_node.get('name').lower()
                if color:
                    colors += [color]

                image_nodes = color_node.get('images')
                for image_node in image_nodes:
                    image_url = image_node.get('image_src')
                    if image_url:
                        image_urls += [image_url]

            if colors:
                metadata['color'] = colors

            item = ProductItem()
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            if image_urls:
                item['image_urls'] = image_urls
            item['metadata'] = metadata

            yield item

        else:
            self.log(str.format('Request product extra failed: {0}', response.url), level=log.ERROR)
