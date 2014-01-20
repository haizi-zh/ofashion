# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class MulberrySpider(MFashionSpider):

    spider_data = {
        'brand_id': 10270,
        'home_urls': {
            'uk': 'http://www.mulberry.com/shop',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MulberrySpider, self).__init__('mulberry', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="content"]/nav[@class="categories-nav"]/ul/li[child::a[@href][text()]]')
        for node in nav_nodes:
            tag_text = node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name,
                                             extra={'male': [],
                                                    'female': ['womenswear'],
                                                    })
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./div[@class="dropdown"]/ul/li/a[@href][text()]')
                for sub_node in sub_nodes:
                    tag_text = sub_node.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name,
                                                     extra={'male': [],
                                                            'female': ['womenswear'],
                                                     })
                        if gender:
                            mc['gender'] = [gender]

                        href = sub_node.xpath('./@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="content"]/section[contains(@class,"products")]/div[contains(@class,"product")]/a[@href]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True,)

        # 页面下拉到底部会自动加载更多，需要模拟请求，解析返回的json
        # 测试发现，在原有url后边添加 ?page=2 也可以取到第二页内容
        # 如果当前页有内容，再考虑请求下一页
        if product_nodes:
            # 取的当前页数
            current_page = 1
            mt = re.search(r'page=(\d+)', response.url)
            if mt:
                current_page = (int)(mt.group(1))

            next_page = current_page + 1
            # 拼下一页的url
            if mt:
                next_url = re.sub(r'page=\d+', str.format('page={0}', next_page), response.url)
            else:
                next_url = str.format('{0}?page={1}', response.url, next_page)

            # 请求下一页
            yield Request(url=next_url,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)


        metadata['url'] = response.url


        model = None
        model_node = sel.xpath('//div[@id="content"]/section[@class="more-details"]/div[@class="row"]/div[contains(@class,"baseline")]/div/p/strong[text()]')
        if model_node:
            model = model_node.xpath('./text()').extract()[0]
            model = self.reformat(model)

        if model:
            metadata['model'] = model
        else:
            return


        # 有些有折扣价 ：http://www.mulberry.com/shop/sale/sale-womenswear/pleated-dress-black-mini-meadow-triple-georgette
        name = None
        old_price = None
        new_price = None
        name_price_node = sel.xpath('//div[@id="content"]/section[@class="section-hero"]/div[@class="row"]/div[@class="fourcol last"]/div[@class="product-info"]/h1[@id="prodEssentials"]')
        if name_price_node:

            was_price_node = name_price_node.xpath('.//div[@class="wasPrice"]')
            if was_price_node:  # 有折扣
                name = ' '.join(
                    self.reformat(val)
                    for val in name_price_node.xpath('./text() | ./span/text()').extract()
                )

                old_price = name_price_node.xpath('.//div[@class="wasPrice"]/text()').extract()[0]
                old_price = self.reformat(old_price)

                new_price = name_price_node.xpath('.//div[@class="nowPrice"]/text()').extract()[0]
                new_price = self.reformat(new_price)
            else:   # 无折扣
                name = ' '.join(
                    self.reformat(val)
                    for val in name_price_node.xpath('.//text()[position() < last()-1]').extract()
                )

                old_price = self.reformat(name_price_node.xpath('.//text()[last()-1]').extract()[0])

        if name:
            metadata['name'] = name
        if old_price:
            metadata['price'] = old_price
        if new_price:
            metadata['price_discount'] = new_price


        description = None
        description_node = sel.xpath('//div[@id="content"]/section[@class="section-hero"]//div[@class="product-description"]/p[text()]')
        if description_node:
            description = self.reformat(description_node.xpath('./text()').extract()[0])

        if description:
            metadata['description'] = description


        color = None
        color_node = sel.xpath('//div[@id="content"]/section[@class="more-details"]//div[@class="additional-product"][1]//div[@class="sixcol"]/h3[text()]')
        if color_node:
            color = self.reformat(color_node.xpath('./text()').extract()[0])

        if color:
            metadata['color'] = [color]


        detail = None
        detail_node = sel.xpath('//div[@id="content"]/section[@class="more-details"]/div[@class="row"]/div[contains(@class,"baseline")]/div[@class="detailed-info"]/*[1 < position()][position() < last()-1]')
        if detail_node:
            detail = '\r'.join(
                self.reformat(val)
                for val in detail_node.xpath('.//text()').extract()
            )

        if detail:
            metadata['details'] = detail


        image_urls = []
        image_nodes = sel.xpath('//div[@id="content"]/section[@class="section-hero"]//div[@id="carousel-hero"]/ul/li/a[@data-zoom]')
        for image_node in image_nodes:
            image_urls += [
                self.process_href(val, response.url)
                for val in image_node.xpath('./@data-zoom').extract()
            ]


        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
