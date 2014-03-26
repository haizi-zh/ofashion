# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class YslSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10388,
        'currency': {
            'ca': 'USD',
            'se': 'EUR',
            'ch': 'EUR',
            'cz': 'EUR',
            'dk': 'EUR',
        },
        'home_urls': {
            'us': 'http://www.ysl.com/us',
            'jp': 'http://www.ysl.com/jp',
            'ca': 'http://www.ysl.com/ca',
            'uk': 'http://www.ysl.com/gb',
            'se': 'http://www.ysl.com/se',
            'ch': 'http://www.ysl.com/ch',
            'es': 'http://www.ysl.com/es',
            'si': 'http://www.ysl.com/si',
            'sk': 'http://www.ysl.com/sk',
            'pt': 'http://www.ysl.com/pt',
            'it': 'http://www.ysl.com/it',
            'de': 'http://www.ysl.com/de',
            'cz': 'http://www.ysl.com/cz',
            'dk': 'http://www.ysl.com/dk',
            'be': 'http://www.ysl.com/be',
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(YslSpider, self).__init__('YSL', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="menu"]/ul/li[child::a[@href][text()]][child::div[@class="submenuMask"]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a[@href][text()]/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text}
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./div[@class="submenuMask"]/ul/li/a[@href][text()]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text}
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        no_result_node = sel.xpath('//div[@id="noResult"]')
        if not no_result_node:
            product_nodes = sel.xpath('//div[@class="productsFromGrid clearfix"]/article[child::a[@href]]')
            if not product_nodes:
                product_nodes = sel.xpath('//div[@id="pageContent"]//div[@data-position][child::a[@href]]')
            for node in product_nodes:
                try:
                    href = node.xpath('./a/@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                m = copy.deepcopy(metadata)

                yield Request(url=href,
                              callback=self.parse_product,
                              errback=self.onerr,
                              meta={'userdata': m},
                              dont_filter=True)

            if product_nodes:
                # 取的当前页数
                current_page = 1
                mt = re.search(r'\?page=(\d+)', response.url)
                if mt:
                    current_page = (int)(mt.group(1))

                next_page = current_page + 1
                # 拼下一页的url
                if mt:
                    next_url = re.sub(r'\?page=\d+', str.format('?page={0}', next_page), response.url)
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

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        image_urls = []
        origin_image_nodes = sel.xpath('//div[@id="itemContent"]//ul[@id="alternateList"]/li/div/img[@src]')
        for image_node in origin_image_nodes:
            try:
                origin_image_url = image_node.xpath('./@src').extract()[0]
                urls = [re.sub(ur'_\d+_', str.format('_{0}_', val), origin_image_url)
                        for val in xrange(13, 15)]
            except(TypeError, IndexError):
                continue

            image_urls += urls

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_node = sel.xpath('//div[@id="itemContent"][@data-default-code10]')
        if model_node:
            try:
                model = model_node.xpath('./@data-default-code10').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="itemContent"]/div[@id="itemDetails"]//span[@class="customItemDescription"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@id="itemContent"]/div[@id="itemDetails"]/div[@id="itemInfo"]/div[@id="itemPrice"]')
        if price_node:

            old_price_node = price_node.xpath('./div[@data-item-prop="priceWithoutPromotion"]')
            if old_price_node:
                try:
                    old_price = ''.join(old_price_node.xpath('.//text()').extract())
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass

            new_price_node = price_node.xpath('./div[@data-item-prop="price"]')
            if new_price_node:
                try:
                    if old_price:
                        new_price = ''.join(new_price_node.xpath('.//text()').extract())
                        new_price = cls.reformat(new_price)
                    else:
                        old_price = ''.join(new_price_node.xpath('.//text()').extract())
                        old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass

            if new_price == old_price:
                new_price = None

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@id="itemContent"]/div[@id="itemDetails"]/div[@id="descriptionWrapper"]/div[@class="container"]')
        if description_node:
            try:
                description = '\r'.join(cls.reformat(val) for val in description_node.xpath('.//text()').extract())
            except(TypeError, IndexError):
                pass

        return description

    # @classmethod
    # def fetch_color(cls, response, spider=None):
    #     sel = Selector(response)
    #
    #     color = []
    #
