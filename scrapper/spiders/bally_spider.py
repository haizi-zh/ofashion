# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import common
import re


class BallySpider(MFashionSpider):
    spider_data = {
        'brand_id': 10030,
        'home_urls': {
            k: str.format('http://www.bally.com/index.aspx?sitecode=BALLY_{0}', k.upper() if k != 'uk' else 'GB')
            for k in [
            'cn', 'us', 'it',
            'at', 'be', 'bg', 'cz', 'dk',
            'fi', 'fr', 'de', 'gr', 'hu',
            'ie', 'lv', 'lt', 'lu', 'nl',
            'pl', 'pt', 'ro', 'sk', 'si',
            'es', 'se', 'ch',
        ]
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(BallySpider, self).__init__('bally', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这里把三级分类都解析全，直接进入单品列表页
        # 因为中间页的差别较大
        # 这里包含很多没有单品的标签，也被进入了
        nav_nodes = sel.xpath('//div[@id="header"]/ul/li')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

            # 第二级分类
            sub_nodes = node.xpath('./ul/li/ul')
            for sub_node in sub_nodes:
                mc = copy.deepcopy(m)

                try:
                    tag_text = sub_node.xpath('../h3/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()
                except(TypeError, IndexError):
                    continue

                if tag_text and tag_name:
                    mc['tags_mapping']['category-1'] = [
                        {'name': tag_name, 'title': tag_text, },
                    ]

                    gender = common.guess_gender(tag_name)
                    if gender:
                        mc['gender'] = [gender]

                # 第三级分类
                third_nodes = sub_node.xpath('./li')
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
                            {'name': tag_name, 'title': tag_text, },
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
                                  meta={'userdata': mcc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="content"]/div[@id="elementsContainer"]/div/div[contains(@id, "item")]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            # try:
            #     name = node.xpath('.//div[@class="infoItem"]//div[@class="macroBmode"]/text()').extract()[0]
            #     name = self.reformat(name)
            #     if name:
            #         m['name'] = name
            # except(TypeError, IndexError):
            #     pass
            #
            # # 这里区分是否打折，price_node不打折的下一级是span，打折的是div
            # price_node = node.xpath('.//div[@class="infoItem"]//div[@class="price"]/div[@class="prodPrice"]')
            # if price_node:
            #     old_price_node = price_node.xpath('./div[@class="oldprice"]')
            #     # 有old_price_node说明在打折
            #     if old_price_node:
            #
            #         new_price = None
            #         new_price_node = price_node.xpath('/div[@class="newprice"]')
            #         if new_price_node:
            #             try:
            #                 new_price = ''.join(
            #                     self.reformat(val)
            #                     for val in new_price_node.xpath('.//text()').extract()
            #                 )
            #                 new_price = self.reformat(new_price)
            #             except(TypeError, IndexError):
            #                 pass
            #
            #         old_price = None
            #         try:
            #             old_price = ''.join(
            #                 self.reformat(val)
            #                 for val in old_price_node.xpath('.//text()').extract()
            #             )
            #             old_price = self.reformat(old_price)
            #         except(TypeError, IndexError):
            #             pass
            #
            #         if old_price:
            #             m['price'] = old_price
            #         if new_price:
            #             m['price_discount'] = new_price
            #     else:
            #         price = None
            #         try:
            #             price = ''.join(
            #                 self.reformat(val)
            #                 for val in price_node.xpath('.//text()').extract()
            #             )
            #             price = self.reformat(price)
            #         except(TypeError, IndexError):
            #             pass
            #
            #         if price:
            #             m['price'] = price

            # 这个取到的链接里边，居然包含\t啥的
            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = re.sub(r'\r|\n|\t', '', href)
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = None
        image_nodes = sel.xpath('//div[@id="col2"]//div[@class="innerCol"]//div[@id="innerThumbs"]/div/img[@src]')
        if image_nodes:
            try:
                thumb_srcs = [
                    val for val in image_nodes.xpath('./@src').extract()
                ]

                image_urls = [
                    re.sub(r'_\d+_', str.format('_{0}_', val), src)
                    for val in xrange(17, 20)
                    for src in thumb_srcs
                ]
            except(TypeError, IndexError):
                pass

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        # 从url中找到model
        model = None
        try:
            mt = re.search(r'cod10/(\w+)/', response.url)
            if mt:
                model = mt.group(1)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            tmp = sel.xpath(
                '//div[@class="itemBoxPrice"]//span[@class="priceValue"]/span[@class="oldprice"]/text()').extract()
            price = cls.reformat(' '.join(tmp)) if tmp else None
            tmp = sel.xpath(
                '//div[@class="itemBoxPrice"]//span[@class="priceValue"]/span[@class="newprice"]/text()').extract()
            price_discount = cls.reformat(' '.join(tmp)) if tmp else None
            if not price:
                tmp = sel.xpath(
                    '//div[@class="itemBoxPrice"]/span[@class="priceValue"]/span[@class="currency" or @class="priceValue"]/text()').extract()
                price = cls.reformat(' '.join(tmp)) if tmp else None
                price_discount = None
            if price:
                old_price = price
                if price_discount:
                    new_price = price_discount
        except(TypeError, IndexError):
            pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="wrapper"]//h2[@id="topModelName"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        descripton = None
        try:
            descripton_node = sel.xpath('//div[@id="descr_content"]/div[@id="EditorialDescription"]')
            if descripton_node:
                descripton = descripton_node.xpath('./text()').extract()[0]
                descripton = cls.reformat(descripton)
        except(TypeError, IndexError):
            pass

        return descripton

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)

        colors = []
        # TODO 这个看起来好像应该有不同颜色的单品，没找到例子
        color_node = sel.xpath('//div[@id="colorsBoxContainer"]//ul[@id="colorsContainer"]/li/div[@title]')
        if color_node:
            try:
                colors = [
                    cls.reformat(val)
                    for val in color_node.xpath('./@title').extract()
                ]
            except(TypeError, IndexError):
                pass

        return colors
