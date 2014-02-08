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

class DknySpider(MFashionSpider):

    spider_data = {
        'brand_id': 10108,
        'curreny': {
            'us': 'USD',
            'uk': 'GBP',
            'au': 'AUD',
            'at': 'EUR',
            'be': 'EUR',
            'br': 'BRL',
            'ca': 'CAD',
            'cy': 'EUR',
            'dk': 'EUR',
            'fi': 'EUR',
            'fr': 'EUR',
            'de': 'EUR',
            'gr': 'EUR',
            'in': 'INR',
            'ie': 'EUR',
            'it': 'EUR',
            'li': 'EUR',
            'mx': 'MXN',
            'mc': 'EUR',
            'nl': 'EUR',
            'nz': 'NZD',
            'no': 'NOK',
            'pt': 'EUR',
            'ro': 'RON',
            'si': 'EUR',
            'es': 'EUR',
            'se': 'SEK',
            'ch': 'CHF',
        },
        'home_urls': {
            'common': 'http://www.dkny.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['curreny'].keys()

    def __init__(self, region):
        super(DknySpider, self).__init__('dkny', region)

    def start_requests(self):
        for region in self.region_list:
            metadata = {'region': region, 'brand_id': self.spider_data['brand_id'],
                        'tags_mapping': {}, 'category': []}

            tmp = self.spider_data['home_urls']['common']
            cookie = {
                'DKI_FiftyOneInternationalCookie': str.format('{0}-{1}', region.upper(), self.spider_data['curreny'][region])
            }
            start_urls = tmp if iterable(tmp) else [tmp]
            for url in start_urls:
                m = copy.deepcopy(metadata)
                yield Request(url=url,
                              meta={'userdata': m},
                              callback=self.parse,
                              errback=self.onerr,
                              cookies=cookie,
                              dont_filter=True)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # country_node = sel.xpath('//div[@class="ecommerce-nav"]/ul/li/span[2][text()]')
        # if country_node:
        #     try:
        #         country = country_node.xpath('./text()').extract()[0]
        #         self.log(str.format('region: {0}    country : {1}', metadata['region'], country))
        #     except(TypeError, IndexError):
        #         pass

        nav_nodes = sel.xpath('//div[@class="header"]/div[@class="fixer"]/div[contains(@class, "global-nav")]/ul/li')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = ''.join(
                    self.reformat(val)
                    for val in nav_node.xpath('./a//text()').extract()
                )
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

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
                        try:
                            tag_text = span_tag_node.xpath('./text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()
                        except(TypeError, IndexError):
                            continue

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

                                try:
                                    tag_text = third_node.xpath('./a/text()').extract()[0]
                                    tag_text = self.reformat(tag_text)
                                    tag_name = tag_text.lower()
                                except(TypeError, IndexError):
                                    continue

                                if tag_text and tag_name:
                                    mcc['tags_mapping']['category-2'] = [
                                        {'name': tag_name, 'title': tag_text,},
                                    ]

                                    gender = common.guess_gender(tag_name)
                                    if gender:
                                        mcc['gender'] = [gender]

                                    try:
                                        href = third_node.xpath('./a/@href').extract()[0]
                                        href = self.process_href(href, response.url)
                                    except(TypeError, IndexError):
                                        continue

                                    yield Request(url=href,
                                                  callback=self.parse_product_list,
                                                  errback=self.onerr,
                                                  meta={'userdata': mcc},
                                                  dont_filter=True)
                    else:
                        try:
                            tag_text = sub_node.xpath('./a/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()
                        except(TypeError, IndexError):
                            continue

                        if tag_text and tag_name:
                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text,},
                            ]

                            gender = common.guess_gender(tag_name)
                            if gender:
                                mc['gender'] = [gender]

                            try:
                                href = sub_node.xpath('./a/@href').extract()[0]
                                href = self.process_href(href, response.url)
                            except(TypeError, IndexError):
                                continue

                            yield Request(url=href,
                                          callback=self.parse_product_list,
                                          errback=self.onerr,
                                          meta={'userdata': mc},
                                          dont_filter=True)

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # country_node = sel.xpath('//div[@class="ecommerce-nav"]/ul/li/span[2][text()]')
        # if country_node:
        #     try:
        #         country = country_node.xpath('./text()').extract()[0]
        #         self.log(str.format('region: {0}    country : {1}', metadata['region'], country))
        #     except(TypeError, IndexError):
        #         pass

        product_nodes = sel.xpath('//div[@id="container"]/div[contains(@class, "view-product_list")]//ul/li[@class="product"]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                name = node.xpath('.//a[@class="product-name"]/text()').extract()[0]
                name = self.reformat(name)
                if name:
                    m['name'] = name
            except(TypeError, IndexError):
                pass

            price_node = node.xpath('.//div[@class="product-price"]')
            if price_node:
                # 这里检查是不是打折商品
                discount_price_node = price_node.xpath('./div[@class="product-price-markdown"]')
                if discount_price_node:
                    try:
                        discount_price = ''.join(
                            self.reformat(val)
                            for val in discount_price_node.xpath('.//text()').extract()
                        )
                        discount_price = self.reformat(discount_price)
                        if discount_price:
                            m['price_discount'] = discount_price
                    except(TypeError, IndexError):
                        pass

                    try:
                        price = ''.join(
                            self.reformat(val)
                            for val in price_node.xpath('./div[@class="product-price-was"]//text()').extract()
                        )
                        price = self.reformat(price)
                        if price:
                            m['price'] = price
                    except(TypeError, IndexError):
                        pass
                else:
                    try:
                        price = ''.join(
                            self.reformat(val)
                            for val in price_node.xpath('./div[@class="product-price-retail"]//text()').extract()
                        )
                        price = self.reformat(price)
                    except(TypeError, IndexError):
                        continue
                    if price:
                        # 有些东西价格是一个区间，这种是多个商品组合起来的套装
                        # 比如：http://www.dkny.com/-notyourordinary/holiday/shine/
                        mt = re.search(ur'—', price)
                        if mt:
                            # TODO 套装解析
                            continue
                        else:   # 说明它不是一个套装
                            m['price'] = price

            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

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

            try:
                href = next_node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product,
                              errback=self.onerr,
                              meta={'userdata': m},
                              dont_filter=True)
            except(TypeError, IndexError):
                pass

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # country_node = sel.xpath('//div[@class="ecommerce-nav"]/ul/li/span[2][text()]')
        # if country_node:
        #     try:
        #         country = country_node.xpath('./text()').extract()[0]
        #         self.log(str.format('region: {0}    country : {1}', metadata['region'], country))
        #     except(TypeError, IndexError):
        #         pass

        # TODO dkny的爬虫通过cookie切换国家，这里的url是无意义的，需要尝试用url切换到指定
        metadata['url'] = response.url

        # 有货号不在URL中的
        # 比如：http://www.dkny.com/bags/shop-by-shape/view-all/resort13bags145/dknypure-large-hobo?p=2&s=12
        # 也有不在那个li的node中的
        # 比如：http://www.dkny.com/sale/womens-sale/dresses/n43731afa/dknypure-dress-with-sleek-jersey-yoke-and-sleeves
        model = None
        model_node = sel.xpath('//li[@class="product"][@id]')
        if model_node:
            try:
                model_text = model_node.xpath('./@id').extract()[0]
                mt = re.search(r'-(\w+)$', model_text)
                if mt:
                    model = mt.group(1)
            except(TypeError, IndexError):
                pass
        if not model:
            try:
                mt = re.search(r'.+/(\w+)/.+$', response.url)
                if mt:
                    model = mt.group(1)
                    if model:
                        model = model.upper()
            except(TypeError, IndexError):
                pass
        if model:
            metadata['model'] = model
        else:
            return

        description_node = sel.xpath('//div[contains(@class, "view-product_detail")]//div[@class="product-description"]')
        if description_node:
            try:
                description = '\r'.join(
                    self.reformat(val)
                    for val in description_node.xpath('.//text()').extract()
                )
                description = self.reformat(description)

                if description:
                    metadata['description'] = description
            except(TypeError, IndexError):
                pass

        colors = None
        color_nodes = sel.xpath('//div[@class="product-info-container"]//form/ul/li/ul/li/a/img[@alt]')
        if color_nodes:
            try:
                colors = [
                    self.reformat(val).lower()
                    for val in color_nodes.xpath('./@alt').extract()
                ]
            except(TypeError, IndexError):
                pass
        if colors:
            metadata['color'] = colors

        image_urls = []
        image_nodes = sel.xpath('//div[contains(@class, "view-product_detail")]//div[@class="partial-product_viewer"]/ul/li/a/img[@src]')
        for image_node in image_nodes:
            try:
                src = image_node.xpath('./@src').extract()[0]
                src = self.process_href(src, response.url)

                # 这里，把src里边的/60/80/替换为/0/0/即可得到全尺寸图片
                src = re.sub(r'/(\d+/\d+)/', '/0/0/', src)

                image_urls += [src]
            except(TypeError, IndexError):
                continue

        # # TODO 这里其他颜色的图片怎么取的
        # # 这里发送请求，找到其他颜色图片
        # # 这里好像有两种请求，一种用了link_id，model，value_id三个参数，一种用了model，value_id两个参数
        # link_id = None
        # link_node = sel.xpath('//link[@rel="canonical"][@href]')
        # if link_node:
        #     link_text = link_node.xpath('./@href').extract()[0]
        #     if link_text:
        #         mt = re.search(r'.+/(\w+)/.+$', link_text)
        #         if mt:
        #             link_id = mt.group(1).upper()
        # if link_id:
        #     other_color_node = sel.xpath('//ul[@class="product-set"]//ul[@class="option-set"]//ul[@class="option-value-set"]/li[@id][child::a[child::img]]')
        #     for node in other_color_node:
        #         value_id = None
        #         value_id_text = node.xpath('./@id').extract()[0]
        #         if value_id_text:
        #             mt = re.search(r'.+/(\w+)/.+$', value_id_text)
        #             if mt:
        #                 value_id = mt.group(1)
        #         if value_id:
        #             m = copy.deepcopy(metadata)
        #
        #             href = str.format('http://www.dkny.com/product/detailpartial?id={0}&variantId={1}', model, value_id)
        #
        #             yield Request(url=href,
        #                           callback=self.parse_other_color,
        #                           errback=self.onerr,
        #                           meta={'meta': m})

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    # def parse_other_color(self, response):
    #
    #     metadata = response.meta['userdata']
    #     sel = Selector(response)
