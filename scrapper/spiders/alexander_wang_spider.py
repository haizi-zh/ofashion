# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re


class AlexanderWangSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10009,
        'currency': {
            'cn': 'CNY',
            'it': 'EUR',
            'us': 'USD',
            'fr': 'EUR',
            'uk': 'GBP',
            'hk': 'USD',
            'jp': 'JPY',
            'au': 'USD',
            'sg': 'USD',
            'de': 'EUR',
            'ca': 'USD',
            'es': 'EUR',
            'ch': 'EUR',
            'ru': 'EUR',
            'my': 'USD',
            'nl': 'EUR',
            'kr': 'USD',

            'at': 'EUR',
            'be': 'EUR',
            'bg': 'EUR',
            'cz': 'EUR',
            'dk': 'EUR',
            'eg': 'EUR',
            'fi': 'EUR',
            'hu': 'EUR',
            'in': 'USD',
            'ie': 'EUR',
            'il': 'EUR',
            'lv': 'EUR',
            'lt': 'EUR',
            'lu': 'EUR',
            'nz': 'USD',
            'no': 'EUR',
            'pl': 'EUR',
            'ro': 'EUR',
            'sk': 'EUR',
            'si': 'EUR',
            'se': 'EUR',
            'tw': 'USD',
            'th': 'USD',
            'tr': 'EUR',
        },
        'home_urls': {
            'cn': 'http://www.alexanderwang.cn/',
            'it': 'http://store.alexanderwang.com/it',
            'us': 'http://store.alexanderwang.com/us',  # 虽然此页不存在，但可以避免被重定向到中国官网
            'fr': 'http://store.alexanderwang.com/fr',
            'uk': 'http://store.alexanderwang.com/gb',
            'hk': 'http://store.alexanderwang.com/hk',
            'jp': 'http://store.alexanderwang.com/jp',
            'au': 'http://store.alexanderwang.com/au',
            'sg': 'http://store.alexanderwang.com/sg',
            'de': 'http://store.alexanderwang.com/de',
            'ca': 'http://store.alexanderwang.com/ca',
            'es': 'http://store.alexanderwang.com/es',
            'ch': 'http://store.alexanderwang.com/ch',
            'ru': 'http://store.alexanderwang.com/ru',
            'my': 'http://store.alexanderwang.com/my',
            'nl': 'http://store.alexanderwang.com/nl',
            'kr': 'http://store.alexanderwang.com/kr',

            # 'ar': 'http://store.alexanderwang.com/ar',
            'at': 'http://store.alexanderwang.com/at',
            'be': 'http://store.alexanderwang.com/be',
            'bg': 'http://store.alexanderwang.com/bg',
            # 'cl': 'http://store.alexanderwang.com/cl',
            # 'co': 'http://store.alexanderwang.com/co',
            # 'hr': 'http://store.alexanderwang.com/hr',
            'cz': 'http://store.alexanderwang.com/cz',
            'dk': 'http://store.alexanderwang.com/dk',
            'eg': 'http://store.alexanderwang.com/eg',
            # 'ee': 'http://store.alexanderwang.com/ee',
            'fi': 'http://store.alexanderwang.com/fi',
            'hu': 'http://store.alexanderwang.com/hu',
            'in': 'http://store.alexanderwang.com/in',
            # 'id': 'http://store.alexanderwang.com/id',
            'ie': 'http://store.alexanderwang.com/ie',
            'il': 'http://store.alexanderwang.com/il',
            'lv': 'http://store.alexanderwang.com/lv',
            'lt': 'http://store.alexanderwang.com/lt',
            'lu': 'http://store.alexanderwang.com/lu',
            'nz': 'http://store.alexanderwang.com/nz',
            'no': 'http://store.alexanderwang.com/no',
            # 'ph': 'http://store.alexanderwang.com/ph',
            'pl': 'http://store.alexanderwang.com/pl',
            'ro': 'http://store.alexanderwang.com/ro',
            'sk': 'http://store.alexanderwang.com/sk',
            'si': 'http://store.alexanderwang.com/si',
            # 'za': 'http://store.alexanderwang.com/za',
            'se': 'http://store.alexanderwang.com/se',
            'tw': 'http://store.alexanderwang.com/tw',
            'th': 'http://store.alexanderwang.com/th',
            # 'tn': 'http://store.alexanderwang.com/tn',
            'tr': 'http://store.alexanderwang.com/tr',
            # 'ua': 'http://store.alexanderwang.com/ua',
            # 'vn': 'http://store.alexanderwang.com/vn',
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(AlexanderWangSpider, self).__init__('alexander wang', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//nav[@id="sitenav"]/ul/li[child::a[@href]]')
        if not nav_nodes:  # 针对美国官网
            nav_nodes = sel.xpath('//div[@class="global-nav"]/ul/li')
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

                sub_nodes = node.xpath('.//li[child::a[@href]]')
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

                        href = sub_node.xpath('./a/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_left_filter,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

                href = node.xpath('./a/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_left_filter,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_left_filter(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 有些类别有第三级展开，比如中国，促销，女装
        nav_nodes = sel.xpath('//nav[@id="navMenu"]//ul//ul//ul//li//a[@href]')
        if not nav_nodes:  # 针对美国官网
            nav_nodes = sel.xpath('//div[@class="left-navigation"]//ul/li/ul/li/a[@href]')
        for node in nav_nodes:
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

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = gender

                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

        for val in self.parse_product_list(response):
            yield val

    def parse_product_list(self, response):
        """
        解析单品列表，发送加载更多的请求
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[contains(@class, "content")]//ul[@class="productsContainer"]//li')
        if not product_nodes:  # 针对美国官网
            product_nodes = sel.xpath('//div[@class="fixer products-grid"]/ul/li')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            # name = None
            # try:
            #     name = node.xpath('.//div[@class="description"]/a/div[@class="title"]/text()').extract()[0]
            #     name = self.reformat(name)
            #     if name:
            #         m['name'] = name
            # except(TypeError, IndexError):
            #     pass
            # if not name:    # 针对美国官网
            #     try:
            #         name = node.xpath('./div[@class="product-info"]//a[@class="product-name"]/text()').extract()[0]
            #         name = self.reformat(name)
            #         if name:
            #             m['name'] = name
            #     except(TypeError, IndexError):
            #         pass

            # try:
            #     price_node = node.xpath('.//div[@class="productPrice"]/div[@class="oldprice"]')
            #     if price_node:
            #
            #         new_price_node = node.xpath('.//div[@class="productPrice"]/div[@class="newprice"]')
            #         new_price = None
            #         if new_price_node:
            #             new_price = ''.join(self.reformat(val) for val in new_price_node.xpath('.//text()').extract())
            #             new_price = self.reformat(new_price)
            #
            #         price = ''.join(self.reformat(val) for val in price_node.xpath('.//text()').extract())
            #         price = self.reformat(price)
            #         # 这里，用这个price是否有值来判断是不是在打折
            #         # new_price应该是总能取到的，否则说明xpath有问题
            #         # 有price说明它在打折
            #         if price:
            #             m['price'] = price
            #             m['price_discount'] = new_price
            #         elif new_price:
            #             m['price'] = new_price
            #     else:   # 针对美国官网
            #         price_node = node.xpath('.//li[@class="product-price"]/cite[text()]')
            #         if price_node:  # 这是无折扣的
            #             price = price_node.xpath('./text()').extract()[0]
            #             price = self.reformat(price)
            #             if price:
            #                 m['price'] = price
            #         else:   # 这是有折扣的
            #             price_node = node.xpath('.//li[@class="product-price"]//li[contains(@class, "retail")][text()]')
            #             price = price_node.xpath('./text()').extract()[0]
            #             price = self.reformat(price)
            #             if price:
            #                 m['price'] = price
            #             price_discount_node = node.xpath('.//li[@class="product-price"]//li[contains(@class, "markdown")][text()]')
            #             price_disount = price_discount_node.xpath('./text()').extract()[0]
            #             price_disount = self.reformat(price_disount)
            #             if price_disount:
            #                 m['price_discount'] = price_disount
            # except(TypeError, IndexError):
            #     pass

            # # 这里只有非美国的，美国官网的那个，这里列表的颜色，没有一个描述
            # try:
            #     color_nodes = node.xpath('.//div[@class="colorsList"]//div[@class="color"]//img[@title]')
            #     if color_nodes:
            #         colors = [
            #             self.reformat(val)
            #             for val in color_nodes.xpath('./@title').extract()
            #         ]
            #         if colors:
            #             m['color'] = colors
            # except(TypeError, IndexError):
            #     pass

            # 这个li的node里边，随便一个a标签，都可以到单品页面
            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

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
        """
        解析单品页面
        """

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

        # # 这里主要是针对有些商品打折，有些没打折
        # # 如果没打折，那么，在parse_product_list中的那个price_node会为None
        # # 此处针对没打折商品，找到价格
        # # 美国的价格已经在上一层找到，这里不找一次了再
        # try:
        #     if not metadata.get('price'):
        #         price_node = sel.xpath('//div[@id="mainContent"]//div[@id="itemPrice"]/div[@class="oldprice"]')
        #         if price_node:
        #
        #             new_price_node = sel.xpath('//div[@id="mainContent"]//div[@id="itemPrice"]/div[@class="newprice"]')
        #             new_price = None
        #             if new_price_node:
        #                 new_price = ''.join(self.reformat(val) for val in new_price_node.xpath('.//text()').extract())
        #                 new_price = self.reformat(new_price)
        #
        #             price = ''.join(self.reformat(val) for val in price_node.xpath('.//text()').extract())
        #             price = self.reformat(price)
        #             # 这里，同样用这个price是否有值来判断是不是在打折
        #             # new_price应该是总能取到的，否则说明xpath有问题
        #             # 有price说明它在打折
        #             if price:
        #                 metadata['price'] = price
        #                 metadata['price_discount'] = new_price
        #             elif new_price:
        #                 metadata['price'] = new_price
        # except(TypeError, IndexError):
        #     pass

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        # 下边是取的图片url
        image_urls = None

        # 另一些颜色的url，与当前node的url区别在于一个叫data-cod10的东西
        # 根据从颜色标签中取的的data-cod10，生成另一种颜色的图片url
        color_codes = [
            self.reformat(val)
            for val in
            sel.xpath('//div[@class="itemColorsContainer"]/ul[@id="itemColors"]/li[@data-cod10]/@data-cod10').extract()
        ]

        # 这里只取到了当前显示颜色的node，
        # 这里经过测试，即使当前颜色没有一些角度的图片，这里也能取到url，页面上会有1px的一些占位
        # 例如：http://www.alexanderwang.cn/cn/%E7%9F%AD%E6%AC%BE%E8%BF%9E%E8%A1%A3%E8%A3%99_cod34283023ab.html
        image_nodes = sel.xpath('//div[@class="itemImages"]/ul[@id="imageList"]/li/img[@src]')
        for node in image_nodes:
            try:
                origin_src = node.xpath('./@src').extract()[0]
                origin_src = self.process_href(origin_src, response.url)
            except(TypeError, IndexError):
                continue

            # 其他颜色的图片src
            all_color_srcs = [
                re.sub(r'/[0-9A-Za-z]+_', str.format('/{0}_', val), origin_src)
                for val in color_codes
            ]

            # 不同尺寸的图片
            image_urls = [
                re.sub(r'_\d+_', str.format('_{0}_', val), src)
                for val in xrange(12, 17)
                for src in all_color_srcs
            ]

        # 针对美国官网，取图片url
        image_nodes = sel.xpath('//div[@id="product-right"]//div[@class="image-pdp-wrapper"]/img[@data-zoomed]')
        if image_nodes:
            image_urls = [
                self.process_href(val, response.url)
                for val in image_nodes.xpath('./@data-zoomed').extract()
            ]

            # 这里进入链接，取不同颜色图片
            other_nodes = sel.xpath('//div[@id="product_options"]//div[@class="swatchWrapper"]//a[@href]')
            for node in other_nodes:
                m = copy.deepcopy(metadata)

                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product,
                              errback=self.onerr,
                              meta={'userdata': m})

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
    def fetch_model(cls, response, spider):
        sel = Selector(response)

        model = None
        # 页面中的货号栏，注意前边会有没用的字符（比如 货号：,style：等）
        try:
            model_node = sel.xpath('//li[@id="description_container"]/div[@id="description_pane"]/div[@class="style"]')
            if not model_node:  # 针对美国官网
                model_node = sel.xpath('//div[@class="accordion"]//span[@class="product-style"]')
            if model_node:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = cls.reformat(model_text)
                if model_text:
                    mt = re.search(r'\b([0-9]+\w*)\b', model_text)
                    if mt:
                        model = mt.group(1)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response, spider):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None

        price_text_new = None
        price_text_old = None
        try:
            # Zephyre：处理价格信息
            tmp = sel.xpath(
                '//div[@id="itemPrice"]/div[@class="newprice"]/*[@class="currency" or @class="priceValue"]/text()').extract()
            price_text_new = cls.reformat(' '.join(tmp)) if tmp else None
            tmp = sel.xpath(
                '//div[@id="itemPrice"]/div[@class="oldprice"]/*[@class="currency" or @class="priceValue"]/text()').extract()
            price_text_old = cls.reformat(' '.join(tmp)) if tmp else None
        except(TypeError, IndexError):
            pass

        # 另外几种网页结构
        if not price_text_new:
            try:
                tmp = sel.xpath(
                    '//div[@id="detail_sku"]/div[contains(@class,"product-price")]//li[@class="product-price-retail"]/text()').extract()
                price_text_old = cls.reformat(' '.join(tmp)) if tmp else None
                tmp = sel.xpath(
                    '//div[@id="detail_sku"]/div[contains(@class,"product-price")]//li[@class="product-price-markdown"]/text()').extract()
                price_text_new = cls.reformat(' '.join(tmp)) if tmp else None
            except(TypeError, IndexError):
                pass
        if not price_text_new:
            try:
                tmp = sel.xpath('//div[@id="detail_sku"]//cite[contains(@class,"product-price")]/text()').extract()
                price_text_new = cls.reformat(' '.join(tmp)) if tmp else None
                price_text_old = None
            except(TypeError, IndexError):
                pass
        if price_text_new:
            if price_text_old:
                old_price = price_text_old
                new_price = price_text_new
            else:
                old_price = price_text_new

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response, spider):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="itemDetails"]//div[@id="itemInfo"]/h1[text()]')
        if not name_node:
            name_node = sel.xpath('//div[@id="product-left"]//div[@id="detail_sku"]/h1[text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider):
        sel = Selector(response)

        description = None
        try:
            description = '\r'.join(
                cls.reformat(val)
                for val in sel.xpath('//div[@id="description_pane"]/div[@class="itemDesc"]//text()').extract()
            )
            description = cls.reformat(description)
        except(TypeError, IndexError):
            pass
        # 这里针对美国官网
        if not description:
            try:
                description = '\r'.join(
                    cls.reformat(val)
                    for val in sel.xpath(
                        '//div[@class="product-information"]//div[contains(@class, "first")]//div[@class="accordion-inner"]/p/text()').extract()
                )
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response, spider):
        sel = Selector(response)

        details = None
        try:
            detail = '\r'.join(
                cls.reformat(val)
                for val in sel.xpath('//div[@id="description_pane"]/div[@class="details"]//text()').extract()
            )
            detail = cls.reformat(detail)
            if detail:
                details = detail
        except(TypeError, IndexError):
            pass
        if not details:
            try:
                detail = '\r'.join(
                    cls.reformat(val)
                    for val in sel.xpath(
                        '//div[@class="product-information"]//div[@class="accordion-group"][not(child::ul)]//div[@class="accordion-inner"]/p/text()').extract()
                )
                detail = cls.reformat(detail)
                if detail:
                    details = detail
            except(TypeError, IndexError):
                pass

        return details

    @classmethod
    def fetch_color(cls, response, spider):
        sel = Selector(response)

        colors = None
        try:
            colors = [
                cls.reformat(val)
                for val in
                sel.xpath('//div[@class="itemColorsContainer"]/ul[@id="itemColors"]/li[@title]/@title').extract()
            ]
        except(TypeError, IndexError):
            pass
        if not colors:
            try:
                colors = [
                    cls.reformat(val)
                    for val in sel.xpath('//div[@id="product_options"]//img[@alt]/@alt').extract()
                ]
            except(TypeError, IndexError):
                pass

        return colors
