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
            'hk': 'USD',
            'tw': 'USD',
        },
        'home_urls': {
            'cn': 'http://www.alexanderwang.cn/',
            'it': 'http://store.alexanderwang.com/it',
            'us': 'http://store.alexanderwang.com/',
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
        for node in nav_nodes:
            tag_text = node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

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
                    tag_text = sub_node.xpath('./a/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

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
        for node in nav_nodes:
            tag_text = node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

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
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            name = node.xpath('.//div[@class="description"]/a/div[@class="title"]/text()').extract()[0]
            name = self.reformat(name)

            if name:
                m['name'] = name

            price_node = node.xpath('.//div[@class="productPrice"]/div[@class="oldprice"]')
            if price_node:
                price = ''.join(self.reformat(val) for val in price_node.xpath('.//text()').extract())
                price = self.reformat(price)
                if price:
                    m['price'] = price

            color_nodes = node.xpath('.//div[@class="colorsList"]//div[@class="color"]//img[@title]')
            if color_nodes:
                colors = [
                    self.reformat(val)
                    for val in color_nodes.xpath('./@title').extract()
                ]
                if colors:
                    m['color'] = colors

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

        # 页面中的货号栏，注意前边会有没用的字符（比如 货号：,style：等）
        model = None
        model_node = sel.xpath('//li[@id="description_container"]/div[@id="description_pane"]/div[@class="style"]')
        if model_node:
            model_text = model_node.xpath('./text()').extract()[0]
            model_text = self.reformat(model_text)
            if model_text:
                mt = re.search(r'\b([0-9]+\w*)\b', model_text)
                if mt:
                    model = mt.group(1)

        if model:
            metadata['model'] = model
        else:
            return


        # 这里主要是针对有些商品打折，有些没打折
        # 如果没打折，那么，在parse_product_list中的那个price_node会为None
        # 此处针对没打折商品，找到价格
        if (not 'price' in metadata.keys()) or (not metadata['price']):
            price_node = sel.xpath('//div[@id="mainContent"]//span[@class="priceValue"]')
            if price_node:
                price = price_node.xpath('./text()').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price


        colors = [
            self.reformat(val)
            for val in sel.xpath('//div[@class="itemColorsContainer"]/ul[@id="itemColors"]/li[@title]/@title').extract()
        ]
        if colors:
            metadata['color'] = colors


        description = '\r'.join(
            self.reformat(val)
            for val in sel.xpath('//div[@id="description_pane"]/div[@class="itemDesc"]//text()').extract()
        )
        description = self.reformat(description)
        if description:
            metadata['description'] = description


        detail = '\r'.join(
            self.reformat(val)
            for val in sel.xpath('//div[@id="description_pane"]/div[@class="details"]//text()').extract()
        )
        detail = self.reformat(detail)
        if detail:
            metadata['details'] = detail


        # 下边是取的图片url
        image_urls = []

        # 另一些颜色的url，与当前node的url区别在于一个叫data-cod10的东西
        # 根据从颜色标签中取的的data-cod10，生成另一种颜色的图片url
        color_codes = [
            self.reformat(val)
            for val in sel.xpath('//div[@class="itemColorsContainer"]/ul[@id="itemColors"]/li[@data-cod10]/@data-cod10').extract()
        ]

        # 这里只取到了当前显示颜色的node，
        # 这里经过测试，即使当前颜色没有一些角度的图片，这里也能取到url，页面上会有1px的一些占位
        # 例如：http://www.alexanderwang.cn/cn/%E7%9F%AD%E6%AC%BE%E8%BF%9E%E8%A1%A3%E8%A3%99_cod34283023ab.html
        image_nodes = sel.xpath('//div[@class="itemImages"]/ul[@id="imageList"]/li/img[@src]')
        for node in image_nodes:
            origin_src = node.xpath('./@src').extract()[0]
            origin_src = self.process_href(origin_src, response.url)

            # 其他颜色的图片src
            all_color_srcs = [
                re.sub(r'/[0-9A-Za-z]+_', str.format('/{0}_', val), origin_src)
                for val in color_codes
            ]

            # 不同尺寸的图片
            image_urls += [
                re.sub(r'_\d+_', str.format('_{0}_', val), src)
                for val in xrange(12, 17)
                for src in all_color_srcs
            ]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

