# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import common
import re

class TodsSpider(MFashionSpider):
    """
    这个品牌有些国家有网店，有些国家没网店，网页样式应该有这两种
    有网店：
        http://store.tods.com/Tods/GB/categories/Shop-Man/Shoes/Gommino-Driving-Shoes/c/217-Tods
    无网店：
        http://www.tods.com/jp/woman/bags
    """

    allowed_domains = ['tods.com']

    spider_data = {
        'brand_id': 10354,
        'home_urls': {
            'uk': 'http://www.tods.com/uk/',
            'us': 'http://www.tods.com/us/',
            'it': 'http://www.tods.com/it/',
            'fr': 'http://www.tods.com/fr/',
            'de': 'http://www.tods.com/de/',
            'es': 'http://www.tods.com/es/',

            'cn': 'http://www.tods.com/cn/',
            'jp': 'http://www.tods.com/jp/',
            'kr': 'http://www.tods.com/rok/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        self.spider_data['callbacks'] = {
            'uk': self.parse_left_nav_withshop,
            'us': self.parse_left_nav_withshop,
            'it': self.parse_left_nav_withshop,
            'fr': self.parse_left_nav_withshop,
            'de': self.parse_left_nav_withshop,
            'es': self.parse_left_nav_withshop,

            'cn': self.parse_left_nav_withoutshop,
            'jp': self.parse_left_nav_withoutshop,
            'kr': self.parse_left_nav_withoutshop,
        }

        super(TodsSpider, self).__init__('tod\'s', region)

    def parse(self, response):
        """
        不管有没有网店，主页面上边这个导航栏，都是一样的
        有网店：
            http://www.tods.com/uk
        无网店：
            http://www.tods.com/cn
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 菜单标题，作为第一级tag
        sub_node_number = 1
        nav_nodes = sel.xpath('//div[@class="nav-container"]/ul[@id="mega-dropdown-menu"]//li')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                # 针对这个node，生成xpath，找到第二级菜单，生成第二级tag
                sub_xpath = str.format('//div[@id="maga-dropdown-inner"]/ul[{0}]/li//li', sub_node_number)
                sub_nodes = sel.xpath(sub_xpath)
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./a/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        callback_func = self.spider_data['callbacks'][metadata['region']]

                        yield Request(url=href,
                                      callback=callback_func,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

                sub_node_number += 1

                try:
                    href = node.xpath('./a/@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                callback_func = self.spider_data['callbacks'][metadata['region']]

                yield Request(url=href,
                              callback=callback_func,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_left_nav_withshop(self, response):
        """
        有网店和无网店，左边导航栏结构不同；
        针对有网店的国家，解析左边导航栏。

        有网店：
            http://store.tods.com/Tods/GB/categories/Shop-Man/Bags/All/c/221-Tods
        无网点：
            http://www.tods.com/cn/woman
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 解析当前打开的标签的下一级标签，作为第三级tag
        current_open_nodes = sel.xpath('//div[@class="leftNavBarfashionList"]//div[contains(@class, "item")]/div[contains(@class, "select")]//a')
        for node in current_open_nodes:
            try:
                tag_text = ''.join(self.reformat(val) for val in node.xpath('.//text()').extract())
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list_withshop,
                              errback=self.onerr,
                              meta={'userdata': m})

        # 解析当前页的单品列表
        for val in self.parse_product_list_withshop(response):
            yield val

    def parse_product_list_withshop(self, response):
        """
        针对有网店国家，解析单品列表
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@class="prodotti"]//li')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                name = node.xpath('./h2/a/text()').extract()[0]
                name = self.reformat(name)
                if name:
                    m['name'] = name
            except(TypeError, IndexError):
                pass

            # 这里有两个涉及价格的标签，listPrice和listPrice_discount，
            # 但是我看见的几个，只有listPrice有值
            try:
                price = node.xpath('.//a[@class="listPrice"]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    m['price'] = price
            except(TypeError, IndexError):
                pass

            # 这里有很多链接，都一样，都是指向单品页的
            try:
                href = node.xpath('.//a/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            # 这里dont_filter保证从不同路径进入单品，他们可能标签不同
            yield Request(url=href,
                          callback=self.parse_product_withshop,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        # 解析右下角下一页，和showall
        page_nodes = sel.xpath('//div[@class="bottomOrder"]//a')
        for node in page_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_list_withshop,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_product_withshop(self, response):
        """
        针对有网店国家，解析单品页面
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 每个颜色的单品它都给了单独的货号，这里从页面上找到各个颜色的链接，递归
        color_nodes = sel.xpath('//div[@id="colorVariant"]//ul/li/a')
        for node in color_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_withshop,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        # 尝试从url中取得model
        model = None
        mt = re.search(r'/(\w+)$', response.url)
        if mt:
            model = mt.group(1)

        if model:
            metadata['model'] = model
        else:
            return

        # 如果metadata中没有name，尝试从页面中找到name
        if not metadata['name']:
            try:
                name = ''.join(self.reformat(val) for val in sel.xpath('//div[@id="productName"]/h1/text()').extract())
                name = self.reformat(name)
                if name:
                    metadata['name'] = name
            except(TypeError, IndexError):
                pass

        # 如果metadata里边没有price，尝试从页面找到price，
        # 这里的价格，源码里既有final-price也有full-price，
        # 我看的几个，都是final-price有价格，full-price没有东西，
        # 这里抓这个显示出来的final-price先
        if not metadata['price']:
            try:
                price = sel.xpath('//div[contains(@class, "right_container")]//span[@class="final-price"]').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price
            except(TypeError, IndexError):
                pass


        description_node = sel.xpath('//div[@id="body1"]//div[@class="text"]')
        if description_node:
            try:
                description = description_node.xpath('./text()').extract()[0]
                description = self.reformat(description)
                if description:
                    metadata['description'] = description
            except(TypeError, IndexError):
                pass

        detail_node = sel.xpath('//div[@id="body2"]//div[@class="text"]')
        if detail_node:
            try:
                detail = '\r'.join(self.reformat(val) for val in detail_node.xpath('.//text()').extract())
                detail = self.reformat(detail)
                if detail:
                    metadata['details'] = detail
            except(TypeError, IndexError):
                pass

        # 取的imageURL，每个缩略图的标签中，有一个放大图的链接
        image_urls = None
        image_nodes = sel.xpath('//div[@id="zoomProductDetail"]//div[@id="thumbImage"]//a[@data-imagezoomedurl]')
        if image_nodes:
            try:
                image_urls = list(
                    self.process_href(val, response.url)
                    for val in image_nodes.xpath('./@data-imagezoomedurl').extract()
                )
            except(TypeError, IndexError):
                pass

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    def parse_left_nav_withoutshop(self, response):
        """
        有网店和无网店，左边导航栏结构不同；
        针对无网店的国家，解析左边导航栏。

        有网店：
            http://store.tods.com/Tods/GB/categories/Shop-Man/Bags/All/c/221-Tods
        无网点：
            http://www.tods.com/cn/woman
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 解析当前打开的标签的下一级标签，作为第三级tag
        current_open_nodes = sel.xpath('//ul[@id="vert-nav"]/li[@class="second-level-group"][contains(@style, "display")]//a[@title]')
        for node in current_open_nodes:
            try:
                tag_text = node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list_withoutshop,
                              errback=self.onerr,
                              meta={'userdata': m})

        for val in self.parse_product_list_withoutshop(response):
            yield val

    def parse_product_list_withoutshop(self, response):
        """
        针对无网店国家，解析单品列表
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@class="category-products"]//li/a')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                name = node.xpath('./@title').extract()[0]
                name = self.reformat(name)
                if name:
                    m['name'] = name

                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_withoutshop,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        # 解析右下角的页数链接，包括显示全部
        page_nodes = sel.xpath('//div[@class="toolbar-bottom"]//li[@class="pager"]//a')
        for node in page_nodes:
            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_list_withoutshop,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_product_withoutshop(self, response):
        """
        针对无网店国家，解析单品页面
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 每个颜色的单品它都给了单独的货号，这里从页面上找到各个颜色的链接，递归
        color_nodes = sel.xpath('//div[contains(@class, "colour_preview")]//a')
        for node in color_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_withoutshop,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        # 尝试从页面中取得model
        model = ''.join(self.reformat(val) for val in sel.xpath('//div[@class="product-main-info"]/p//text()').extract())
        model = self.reformat(model)
        if model:
            metadata['model'] = model
        else:
            return

        # 如果metadata中没有name，尝试从页面中找到name
        try:
            if not metadata['name']:
                name = ''.join(self.reformat(val) for val in sel.xpath('//div[@class="product-name"]/h1/text()').extract())
                name = self.reformat(name)
                if name:
                    metadata['name'] = name
        except(TypeError, IndexError):
            pass

        try:
            description = '\r'.join(self.reformat(val) for val in sel.xpath('//div[contains(@class, "description")]/div[@class="std"]//text()').extract())
            description = self.reformat(description)
            if description:
                metadata['description'] = description
        except(TypeError, IndexError):
            pass

        image_urls = None
        try:
            image_nodes = sel.xpath('//div[@class="more-views"]/ul[@class="thumnail-images"]/li/a/img[@data-zoom-image]')
            if image_nodes:
                image_urls = list(
                    self.process_href(val, response.url)
                    for val in image_nodes.xpath('./@data-zoom-image').extract()
                )
        except(TypeError, IndexError):
            pass

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

