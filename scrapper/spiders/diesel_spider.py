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

        # 这里 ul[] 去掉它标明 moblie only 的内容
        nav_nodes = sel.xpath('//div[@id="navigation"]/nav/div/ul[not(contains(@class, "mobile"))]/li')
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
                        tag_text = sub_node.xpath('./div/h2/text()').extract()[0]
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

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)
