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

class DieselSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10105,
        'home_urls': {
            'us': 'http://shop.diesel.com/homepage?origin=NOUS',    # 这里不加origin会被重定向走
            'uk': 'http://store.diesel.com/gb',
            'fr': 'http://store.diesel.com/fr',
            'de': 'http://store.diesel.com/de',
            'it': 'http://store.diesel.com/it',
            'jp': 'http://www.store.diesel.co.jp/jp',
            'ru': 'http://store.diesel.com/ru',
            'es': 'http://store.diesel.com/es',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(DieselSpider, self).__init__('diesel', region)

    def parse(self, response):
        """
        针对美国的处理，其他国家转到parse_other
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        if (metadata['region'] != 'us'):
            for val in self.parse_other(response):
                yield val

        # 这里 ul[] 去掉它标明 moblie only 的内容，去掉最后一个sale标签，单独处理sale
        nav_nodes = sel.xpath('//div[@id="navigation"]/nav/div/ul[not(contains(@class, "mobile"))]/li[not(@id="sale")]')
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
                        tag_node = sub_node.xpath('./div/h2[text()]')
                        if tag_node:
                            tag_text = sub_node.xpath('./div/h2/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()
                        else:
                            continue

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

                # 后几个标签，除了sale，不再区分级别
                sub_nodes = nav_node.xpath('./div/div//a')
                for sub_node in sub_nodes:
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

        # 单独处理sale标签及其下属标签
        sale_node = sel.xpath('//div[@id="navigation"]/nav/div/ul[not(contains(@class, "mobile"))]/li[@id="sale"]')
        if sale_node:
            m = copy.deepcopy(metadata)

            tag_text = sale_node.xpath('./a/span/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = sale_node.xpath('./div/div/ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    tag_text = sub_node.xpath('./div/a/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            m['gender'] = [gender]

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

                                href = third_node.xpath('./a/@href').extract()[0]
                                href = self.process_href(href, response.url)

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[contains(@class, "content")]/div[contains(@class, "grid-tile")]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name = ''.join(
                self.reformat(val)
                for val in node.xpath('.//div[@class="product-name"]//text()').extract()
            )
            name = self.reformat(name)
            if name:
                m['name'] = name

            colors = [
                self.reformat(val)
                for val in node.xpath('.//div[@class="grid-swatch-list"]/ul/li/div[@title]/@title').extract()
            ]
            if colors:
                m['color'] = colors

            discount_node = node.xpath('.//div[@class="product-pricing"]/div[@class="product-discounted-price clearfix"]')
            # 有折扣
            if discount_node:
                discount_price = discount_node.xpath('./span[@class="product-sales-price"]/text()').extract()[0]
                discount_price = self.reformat(discount_price)
                if discount_price:
                    m['price_discount'] = discount_price

                price = discount_node.xpath('./span[@class="product-standard-price"]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    m['price'] = price
            else:
                price = node.xpath('.//div[@class="product-pricing"]/span[@class="product-sales-price"]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    m['price'] = price

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        # 美国的这个看起来是没有下拉加载更多的

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        color_nodes = sel.xpath('//div[@id="product-content-detail"]//div[@class="swatch-Slider"]/ul/li/a[@href]')
        for node in color_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        model = None
        mt = re.search(r'[^/]/(\w+)\.|pid=(\w+)&', response.url)
        if mt:
            model = mt.group(1)
            if not model:
                model = mt.group(2)
        if model:
            metadata['model'] = model
        else:
            return

        description_node = sel.xpath('//div[@id="pdpMain"]//div[@class="detail-content"]/p/span[@class="para-content"][text()]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('.//text()').extract()
            )
            description = self.reformat(description)
            if description:
                metadata['description'] = description

        detail = ''.join(
            self.reformat(val)
            for val in sel.xpath('//div[@id="product-content-detail"]//ul[@class="product-description-list"]/li[text()]/text()').extract()
        )
        detail = self.reformat(detail)
        if detail:
            metadata['details'] = detail

        image_urls = []
        image_nodes = sel.xpath('//div[@id="pdpMain"]//ul[@class="product-slides-list"]/li/a/img[@src]')
        for image_node in image_nodes:
            image_url = image_node.xpath('./@src').extract()[0]
            image_url = re.sub(r'\?.*', '', image_url)
            if image_url:
                image_urls += [image_url]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    def parse_other(self, response):
        """
        针对其他国家的处理
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="lowerHeader"]/ul[@id="navMenu"]/li[not(@class="navSALES")]')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = node.xpath('./span/a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                # 第 1，2，6个顶级标签的下属符合这个规则
                xpath_string = str.format(
                    '//div[@id="dropDownMenuWrapper"]/div[{0}]/div[@class="rightSpacer"]/div[@class="column3"]',
                    nav_nodes.index(node)+1)
                sub_nodes = node.xpath(xpath_string)
                if sub_nodes:
                    for sub_node in sub_nodes:
                        mc = copy.deepcopy(m)

                        # 第六个顶级标签没有下属
                        tag_node = sub_node.xpath('./div[@class="titleColumn"]')
                        if tag_node:
                            tag_text = tag_node.xpath('./text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()

                            if tag_text and tag_name:
                                mc['tags_mapping']['category-1'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mc['gender'] = [gender]

                                third_nodes = sub_node.xpath('./div[@class="column2"]/ul/li/a[text()][@href]')
                                for third_node in third_nodes:
                                    mcc = copy.deepcopy(mc)

                                    tag_text = third_node.xpath('./text()').extract()[0]
                                    tag_text = self.reformat(tag_text)
                                    tag_name = tag_text.lower()

                                    if tag_text and tag_name:
                                        mcc['tags_mapping']['category-2'] = [
                                            {'name': tag_name, 'title': tag_text,},
                                        ]

                                        gender = common.guess_gender(tag_name)
                                        if gender:
                                            mcc['gender'] = [gender]

                                        href = third_node.xpath('./@href').extract()[0]
                                        href = self.process_href(href, response.url)

                                        yield Request(url=href,
                                                      callback=self.parse_other_product_list,
                                                      errback=self.onerr,
                                                      meta={'userdata': mcc})
                        else:
                            tag_text = sub_node.xpath('./a/div[@class="labelMacroLifestyle"]/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()

                            if tag_text and tag_name:
                                mc['tags_mapping']['category-1'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mc['gender'] = [gender]

                                href = sub_node.xpath('./a/@href').extract()[0]
                                href = self.process_href(href, response.url)

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mc})
                else:
                    # 第3个顶级标签的下属符合这个规则
                    xpath_string = str.format(
                        '//div[@id="dropDownMenuWrapper"]/div[{0}]/div[@class="rightSpacer"]/div[contains(@class, "title")]',
                        nav_nodes.index(node)+1)
                    sub_nodes = node.xpath(xpath_string)
                    if sub_nodes:
                        for sub_node in sub_nodes:
                            mc = copy.deepcopy(m)

                            tag_text = sub_node.xpath('./span/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()

                            if tag_text and tag_name:
                                mc['tags_mapping']['category-1'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mc['gender'] = [gender]

                                # 再向下，不区分级别了
                                xpath_string = str.format(
                                    '//div[@id="dropDownMenuWrapper"]/div[{0}]/div[@class="rightSpacer"]/div[contains(@class, "column2")]/div[contains(@class, "column2")][{1}]',
                                    nav_nodes.index(node)+1, sub_nodes.index(sub_node)+1)
                                third_node = sel.xpath(xpath_string)
                                if third_node:
                                    href_nodes = third_node.xpath('.//a[@href][text()]')
                                    for href_node in href_nodes:
                                        mcc  = copy.deepcopy(mc)

                                        tag_text = href_node.xpath('./text()').extract()[0]
                                        tag_text = self.reformat(tag_text)
                                        tag_name = tag_text.lower()

                                        if tag_text and tag_name:
                                            mcc['tags_mapping']['category-2'] = [
                                                {'name': tag_name, 'title': tag_text,},
                                            ]

                                            gender = common.guess_gender(tag_name)
                                            if gender:
                                                mcc['gender'] = [gender]

                                            href = href_node.xpath('./@href').extract()[0]
                                            href = self.process_href(href, response.url)

                                            yield Request(url=href,
                                                          callback=self.parse_other_product_list,
                                                          errback=self.onerr,
                                                          meta={'userdata': mcc})
                    else:   # TODO 第4，5个顶级标签
                        pass

        # 第7个sale标签
        sale_nav_node = sel.xpath('//div[@id="lowerHeader"]/ul[@id="navMenu"]/li[@class="navSALES"]')
        if sale_nav_node:
            m = copy.deepcopy(metadata)

            tag_text = sale_nav_node.xpath('./span[text()]/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sale_node = sel.xpath('//div[@id="dropDownMenuWrapper"]/div[@id="DdMenu-sale"]/div[@class="rightSpacer"]')
                if sale_node:
                    sub_nodes = sale_node.xpath('./div[contains(@class, "column")][child::div[@class="titleColumn"]]')
                    for sub_node in sub_nodes:
                        mc = copy.deepcopy(m)

                        tag_text = sub_node.xpath('./div[@class="titleColumn"]/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()

                        if tag_text and tag_name:
                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text,},
                            ]

                            gender = common.guess_gender(tag_name)
                            if gender:
                                mc['gender'] = [gender]

                            third_nodes = sub_node.xpath('.//a[@href]')
                            for third_node in third_nodes:
                                mcc = copy.deepcopy(mc)

                                tag_text = ''.join(
                                    self.reformat(val)
                                    for val in third_node.xpath('.//text()').extract()
                                )
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()

                                if tag_text and tag_name:
                                    mcc['tags_mapping']['category-2'] = [
                                        {'name': tag_name, 'title': tag_text,},
                                    ]

                                    gender = common.guess_gender(tag_name)
                                    if gender:
                                        mcc['gender'] = [gender]

                                    href = third_node.xpath('./@href').extract()[0]
                                    href = self.process_href(href, response.url)

                                    yield Request(url=href,
                                                  callback=self.parse_other_product_list,
                                                  errback=self.onerr,
                                                  meta={'userdata': mcc})

    def parse_other_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="results"]/div[contains(@class, "slot")]/div[@class="item"]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            # 有些商品没有名字居然
            # 比如：http://store.diesel.com/gb/men/jewellery
            name_node = node.xpath('./a[@class="prodInfo"]//strong[@class="itemName"][text()]')
            if name_node:
                name = name_node.xpath('./text()').extract()[0]
                name = self.reformat(name)
                if name:
                    m['name'] = name

            price = node.xpath('./a[@class="prodInfo"]//em[contains(@class, "itemPrice")]/text()').extract()[0]
            price = self.reformat(price)
            if price:
                m['price'] = price

            # 判断是否打折
            discount_node = node.xpath('./a[@class="prodInfo"]//span[@class="itemDiscountedPrice"][text()]')
            if discount_node:
                discount_price = discount_node.xpath('./text()').extract()[0]
                discount_price = self.reformat(discount_price)
                if discount_price:
                    m['price_discount'] = discount_price

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_other_procut,
                          errback=self.onerr,
                          meta={'userdata': m})

        # TODO 有下拉加载更多，至少英国有，get请求

    def parse_other_procut(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = None
        mt = re.search(r'_cod(\w+)\.', response.url)
        if mt:
            model = mt.group(1)
        if model:
            metadata['model'] = model
        else:
            return

        name_node = sel.xpath('//aside[@class="itemSidebar"]/h1[text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)
            if name:
                metadata['name'] = name

        description_node = sel.xpath('//div[@id="tabs"]/ul/li/div/p')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('.//text()').extract()
            )
            description = self.reformat(description)
            if description:
                metadata['description'] = description

        detail = ''.join(
            self.reformat(val)
            for val in sel.xpath('//aside[@class="itemSidebar"]/ul/li//text()').extract()
        )
        detail = self.reformat(detail)
        if detail:
            metadata['details'] = detail

        colors = None
        color_nodes = sel.xpath('//aside[@class="itemSidebar"]//div[@class="colorMask"]/ul/li//span[text()]')
        if color_nodes:
            colors = [
                self.reformat(val)
                for val in color_nodes.xpath('./text()').extract()
            ]
        if colors:
            metadata['color'] = colors

        # 这里会有比需要的图片多
        image_fix_list = re.findall(r'"(\d{2}_[a-z])"', response.body)
        # 这里去掉一下没用的后缀
        max_fix = '0'
        for fix in image_fix_list:
            if fix > max_fix:
                max_fix = fix[:2]
        def func(item):
            mt = re.search(str.format('{0}_[a-z]', max_fix), item)
            if mt:
                return True
            else:
                return False
        image_fix_list = filter(func, image_fix_list)

        # 用页面中图片的地址取的他们图片服务器的地址
        # 顺便用它里边已经写好的单品的id和颜色的id
        image_urls = None
        image_node = sel.xpath('//aside[@class="itemSidebar"]//div[@class="colors"]/div[@class="colorSizeContent colorSlider"]/div[@class="colorMask"]//img[@src]')
        if image_node:
            image_urls = [
                re.sub('\d{2}_[a-z]', val, src)
                for val in image_fix_list
                for src in image_node.xpath('./@src').extract()
            ]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
