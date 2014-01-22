# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class DiorSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10106,
        'home_urls': {
            'uk': 'http://www.dior.com/home/en_gb',
            'be': 'http://www.dior.com/home/en_be',
            'de': 'http://www.dior.com/home/de_de',
            'es': 'http://www.dior.com/home/es_es',
            'fr': 'http://www.dior.com/home/fr_fr',
            'it': 'http://www.dior.com/home/it_it',
            'ru': 'http://www.dior.com/home/ru_ru',
            'br': 'http://www.dior.com/home/pt_br',
            'us': 'http://www.dior.com/home/en_us',
            'cn': 'http://www.dior.com/home/zh_cn',
            'hk': 'http://www.dior.com/home/zh_hk',
            'jp': 'http://www.dior.com/home/ja_jp',
            'kr': 'http://www.dior.com/home/ko_kr',
            'tw': 'http://www.dior.com/home/zh_tw',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(DiorSpider, self).__init__('dior', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="pre-footer"]/ul[@class="nav"]/li[child::h4[text()]]')
        for node in nav_nodes:
            tag_text = ''.join(
                self.reformat(val)
                for val in node.xpath('./h4//text()').extract()
            )
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
                    tag_text = sub_node.xpath('./a/text()').extract()[0]
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

                        href = sub_node.xpath('./a/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_filter,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_filter(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        filter_nodes = sel.xpath('//div[@id="main-nav"]/ul/li[child::a[@href][text()]]')
        for node in filter_nodes:
            tag_text = node.xpath('./a/text()').extract()[0]
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

                xpath_str = str.format('//div[@id="sub-nav"]/div[{0}]/div/ul/li[child::h3[text()]][child::a[@href][text()]]',
                                       filter_nodes.index(node)+1)
                sub_nodes = node.xpath(xpath_str)
                for sub_node in sub_nodes:
                    tag_text = ''.join(
                        self.reformat(val)
                        for val in sub_node.xpath('./h3/text()').extract()
                    )
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-3'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        href_nodes = sub_node.xpath('./a[@href][text()]')
                        for href_node in href_nodes:
                            tag_text = href_node.xpath('./text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()

                            if tag_text and tag_name:
                                mcc = copy.deepcopy(mc)

                                mcc['tags_mapping']['category-4'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mcc['gender'] = [gender]

                                href = href_node.xpath('./@href').extract()[0]
                                href = self.process_href(href, response.url)

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

                href = node.xpath('./a/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//ul[@class="slides anythingBase horizontal"]/li[descendant::a[@href]]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.reformat(href)  # 这里这个url前边有一些空白字符
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        product_nodes = sel.xpath('//div[@class="mods modCrossSell"]/ul[@class="crossList png-bg"]/li[child::a[@href]] | //ul[@id="productList"]/li[child::a[@href]]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.reformat(href)  # 这里这个url前边有一些空白字符
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
        model_node = sel.xpath('//div[@id="content"]//ul[@class="ref"]/li[1][text()]')
        if model_node:
            model_text = ''.join(
                self.reformat(val)
                for val in model_node.xpath('./text()').extract()
            )
            model_text = self.reformat(model_text)
            if model_text:
                mt = re.search(ur'— ([\w ]+)', model_text)
                if mt:
                    model = mt.group(1)
                    model = self.reformat(model)

        if model:
            metadata['model'] = model
        else:
            return


        name = None
        name_node = sel.xpath('//title[text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)

        if name:
            metadata['name'] = name


        description = None
        description_node = sel.xpath('//meta[@name="description"][@content]')
        if description_node:
            description = description_node.xpath('./@content').extract()[0]
            description = self.reformat(description)

        if description:
            metadata['description'] = description


        price = None
        price_node = sel.xpath('//div[@class="modEcommerce"]//span[@class="hoverPrice"][text()]')
        if price_node:
            price = price_node.xpath('./text()').extract()[0]
            price = self.reformat(price)

        if price:
            metadata['price'] = price


        image_urls = None
        image_nodes = sel.xpath('//div[@class="modEcommerce"]//ul[@class="thumbsProduct"]/li/a[@data-zoom]')
        if image_nodes:
            image_urls = [
                self.process_href(self.reformat(val), response.url)
                for val in image_nodes.xpath('./@data-zoom').extract()
            ]


        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item