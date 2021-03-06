# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re


class VanCleffArpelsSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10369,
        'home_urls': {
            'us': 'http://www.vancleefarpels.com/us/en/',
            'jp': 'http://www.vancleefarpels.com/jp/ja/',
            'uk': 'http://www.vancleefarpels.com/eu/en/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(VanCleffArpelsSpider, self).__init__('van_cleff_arpels', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//nav[@id="left-nav"]/ul[@id="left-ul"]/li[child::h4[text()]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./h4/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath(
                    '//nav[@id="left-nav"]/ul[@id="left-ul"]/li[child::h4[text()]]/div[@class="sub-nav"]/div/ul/li[child::a[@href][text()]]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text, },
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./a[@href]/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_collection,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_collection(self, response):
        """
        有些是系列页面，有些是进入了单品列表页面
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        collection_ndoes = sel.xpath('//div[@id="collection-menu-content"]/ul/li/a[@href][text()]')
        for node in collection_ndoes:
            try:
                tag_text = node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

        for val in self.parse_product_list(response):
            yield val

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="purchase-grid"]/ul/li[descendant::a[@href]]')
        if not product_nodes:
            product_nodes = sel.xpath('//div[@id="collection-slider"]/ul/li[descendant::a[@href]]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        # 这里感觉没必要非找下一页的node，重复url反正会被去掉的
        page_nodes = sel.xpath('//ul[@id="search-control"]/li/p/a[@href]')
        for node in page_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_list,
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

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        image_urls = []
        image_nodes = sel.xpath('//div[@id="product-left-part"]//ul[@class="caroussel-ul"]/li/img[@src]')
        for image_node in image_nodes:
            try:
                image_src = image_node.xpath('./@src').extract()[0]
                image_src = self.process_href(image_src, response.url)
                if image_src:
                    image_url = re.sub(ur'/[/\w]+/\d+x\d+/', ur'/', image_src)
                    if image_url:
                        image_urls += [image_url]
            except(TypeError, IndexError):
                continue

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
        model_node = sel.xpath('//div[@id="details"]//p[@class="ref"][text()]')
        if model_node:
            try:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = cls.reformat(model_text)
                if model_text:
                    mt = re.search(ur'(\w+)$', model_text)
                    if mt:
                        model = mt.group(1)
                        model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        price = None
        price_node = sel.xpath('//div[@id="details"]/div[@class="price png_bg"]//span[@class="price-details"][text()]')
        if price_node:
            try:
                price = price_node.xpath('./text()').extract()[0]
                price = cls.reformat(price)
            except(TypeError, IndexError):
                pass

        if price:
            ret['price'] = price

        return ret

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="product-right-part"]/h1[text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@id="product-right-part"]/div[@class="scroll-pane"]//p[text()]')
        if description_node:
            try:
                description = '\r'.join(
                    cls.reformat(val)
                    for val in description_node.xpath('./text()').extract()
                )
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = None
        color_node = sel.xpath('//div[@id="product-right-part"]/p[@class="short-resume"][text()]')
        if color_node:
            try:
                colors = [cls.reformat(val)
                          for val in color_node.xpath('./text()').extract()]
            except(TypeError, IndexError):
                pass

        return colors

