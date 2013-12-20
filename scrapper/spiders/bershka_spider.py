# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import common
import re
import json

class BershkaSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10040,        'currency': {
            'hk': 'USD',
            'sg': 'USD',
        },
        'home_urls': {
            'cn': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkacn/zh/40109502',
            'fr': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkafr/fr/40109502',
            'uk': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkagb/en/40109502',
            'hk': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkahk/en/40109502',
            'jp': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkajp/en/40109502',
            'it': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkait/it/40109502',
            'ae': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkaae/en/40109502',
            'sg': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkasg/en/40109502',
            'de': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkade/de/40109502',
            'es': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkaes/es/40109502',
            'ch': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkach/en/40109502',
            'ru': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkaru/ru/40109502',
            'th': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkath/en/40109502',
            'kr': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkakr/en/40109502',
            'my': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkamy/en/40109502',
            'nl': 'http://www.bershka.com/webapp/wcs/stores/servlet/home/bershkanl/nl/40109502',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(BershkaSpider, self).__init__('bershka', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="main_menu_menu"]/ul/li')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = ''.join(
                    self.reformat(val)
                    for val in node.xpath('.//text()').extract()
                )
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name, {'male': [], 'female': [u'少女']})
                if gender:
                    m['gender'] = [gender]

            href = node.xpath('./a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_left_nav,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_left_nav(self, response):
        """
        这个左侧当前进入的类别会被展开
        展开的类别下属虽然还可以再展开，但是只有所有和bershka两个选项，没用
        当前类别右侧的系列，会根据当前类别变，应该算是当前类别的下属
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 当前类别的下属第二级类别
        left_nav_nodes = sel.xpath('//div[@id="col_colizq"]/div[@id="col_list"]/ul/li[child::a[text()]]')
        for node in left_nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = node.xpath('./a[text()]/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name, {'male': [], 'female': [u'少女']})
                if gender:
                    m['gender'] = [gender]

            href = node.xpath('./a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': m})

        # 当前类别的下属系列页面链接
        collection_node = sel.xpath('//div[@id="col_colizq"]/div[@id="subhome_pest1"]/a[@href]')
        if collection_node:
            href = collection_node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_left_nav_collection,
                          errback=self.onerr,
                          meta={'userdata': metadata})

        # 解析当前页面的单品列表
        for val in self.parse_product_list(response):
            yield val

    def parse_left_nav_collection(self, response):
        """
        这里解析左边当前类别右侧的系列，看起来是当前类别的下属
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        sub_nodes = sel.xpath('//div[@id="col_colizq"]/div[@id="col_list"]/ul/li[child::a[text()]]')
        for sub_node in sub_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = sub_node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name, {'male': [], 'female': [u'少女']})
                if gender:
                    m['gender'] = [gender]

            href = sub_node.xpath('./a/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product_list(self, response):
        """
        解析单品列表

        注意：这里所有的单品信息都在js的一个json里边，直接解析response，啥都得不到
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这里取它的json内容loads的话，总是有错误，所以用这种正则表达式的办法
        url_list = re.findall(r'url: "(.+)/_COLOR_"', response.body)
        for url in url_list:
            m = copy.deepcopy(metadata)

            href = self.process_href(url, response.url)

            # dont_filter保证从不同路径进来的url都进入单品页，生成不同的item标签
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
        model_node = sel.xpath('//div[@id="info1"]/div[@class="padding15"]/table//tr[2]/td[not(child::*)][2]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = self.reformat(model)
            except(TypeError, IndexError):
                pass

        if model:
            metadata['model'] = model
        else:
            return

        name_node = sel.xpath('//div[@id="info1"]//h1')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = self.reformat(name)
                if name:
                    metadata['name'] = name
            except(TypeError, IndexError):
                pass

        # 价格是用js后加载的
        default_price = None
        default_price_re = re.search(r'defaultPrice: "(.*)"', response.body)
        if default_price_re:
            try:
                default_price = default_price_re.group(1)
                default_price = self.reformat(default_price)
                default_price = re.sub(ur'&nbsp', ur' ', default_price)
            except(TypeError, IndexError):
                pass
        # 这里这个defaultComparePrice是原价
        # 如果没有，就是没有打折
        old_price = None
        old_price_re = re.search(r'defaultComparePrice: "(.*)"', response.body)
        if old_price_re:
            try:
                old_price = old_price_re.group(1)
                old_price = self.reformat(old_price)
                old_price = re.sub(ur'&nbsp', ur' ', old_price)
            except(TypeError, IndexError):
                pass

        if old_price:
            # 有打折
            metadata['price'] = old_price
            if default_price:
                metadata['price_discount'] = default_price
        elif default_price:
            # 没打折
            metadata['price'] = default_price

        # 颜色标签
        colors = None
        color_nodes = sel.xpath('//div[@id="tallasdiv"]/div[@class="colors_detail"]/div[@title]')
        if color_nodes:
            try:
                colors = [
                    self.reformat(val)
                    for val in color_nodes.xpath('./@title').extract()
                ]
            except(TypeError, IndexError):
                pass
        if colors:
            metadata['color'] = colors

        # 这个所有放大图片的地址，实在源码中找到的
        image_urls = None
        image_nodes = sel.xpath('//div[contains(@id, "superzoom_")]/div[@rel]')
        if image_nodes:
            try:
                image_urls = [
                    self.process_href(val, response.url)
                    for val in image_nodes.xpath('./@rel').extract()
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
