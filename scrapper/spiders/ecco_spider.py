# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class EccoSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10105,
        'home_urls': {
            'uk': 'http://shopeu.ecco.com/uk/en',
            'de': 'http://shopeu.ecco.com/de/de',
            'be': 'http://shopeu.ecco.com/be/nl-be',
            'fr': 'http://shopeu.ecco.com/fr/fr',
            'ie': 'http://shopeu.ecco.com/ie/en-ie',
            'se': 'http://shopeu.ecco.com/se/sv',
            'no': 'http://shopeu.ecco.com/no/no',
            'fi': 'http://shopeu.ecco.com/fi/fi',
            'nl': 'http://shopeu.ecco.com/nl/nl',
            'pl': 'http://shopeu.ecco.com/pl/pl',
            # 美国，加拿大，中国，各不相同
            # 'cn': 'http://ecco.tmall.com/',
            'us': 'http://us.shop.ecco.com/',
            'ca': 'http://www.eccocanada.com/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(EccoSpider, self).__init__('ecco', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        if metadata['region'] == 'ca':
            for val in self.parse_ca(response):
                yield val
            return
        elif metadata['region'] == 'us':
            for val in self.parse_us(response):
                yield val
            return

        nav_nodes = sel.xpath('//div[@class="navbar"]/div[@class="menu-wrapper"]/ul/li')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = nav_node.xpath('./a/span[text()]/text()').extract()[0]
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

                sub_nodes = nav_node.xpath('./ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    # 有些直接span，有些下属a再span
                    tag_node = sub_node.xpath('./span[text()]')
                    if not tag_node:
                        tag_node = sub_node.xpath('./a/span[text()]')
                    if tag_node:
                        try:
                            tag_text = tag_node.xpath('./text()').extract()[0]
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

                            third_nodes = sub_node.xpath('./ul/li[child::a]')
                            for third_node in third_nodes:
                                mcc = copy.deepcopy(mc)

                                try:
                                    tag_text = third_node.xpath('./a/span[text()]/text()').extract()[0]
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
                                                  meta={'userdata': mcc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        #??? 这里会不会有些链接取不到呢？
        href_list = re.findall(r'href=\\"([^"]+)"', response.body)
        for href_text in href_list:
            m = copy.deepcopy(metadata)

            try:
                href = re.sub(r'\\', '', href_text)
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            # 检查是不是去向下一页的链接
            call_back = self.parse_product
            mt = re.search(r'\?page=', href)
            if mt:
                call_back = self.parse_product_list

            yield Request(url=href,
                          callback=call_back,
                          errback=self.onerr,
                          meta={'userdata': m})

        # product_nodes = sel.xpath('//ul[@id="product-list-cont"]/li//a[@href]')
        # for node in product_nodes:
        #     m = copy.deepcopy(metadata)
        #
        #     href = node.xpath('.//a[@href]/@href').extract()[0]
        #     href = self.process_href(href, response.url)
        #
        #     yield Request(url=href,
        #                   callback=self.parse_product,
        #                   errback=self.onerr,
        #                   meta={'userdata': m},
        #                   dont_filter=True)

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = None
        try:
            model_node = sel.xpath('//div[@class="pdetail-cont-left"]/div/p[@class="art-number"]/span[@id="prd-item-number"][text()]')
            if model_node:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = self.reformat(model_text)
                if model_text:
                    mt = re.search(r'\b([0-9\-]+)\b', model_text)
                    if mt:
                        model = mt.group(1)
        except(TypeError, IndexError):
            model = None
        if model:
            metadata['model'] = model
        else:
            return

        try:
            name_node = sel.xpath('//div[@class="pdetail-cont-left"]//p[@class="shoe-headline"][text()]')
            if name_node:
                name = name_node.xpath('./text()').extract()[0]
                name = self.reformat(name)
                if name:
                    metadata['name'] = name
        except(TypeError, IndexError):
            pass

        try:
            colors = None
            color_nodes = sel.xpath('//div[@class="bx-color"]//ul/li[@title]')
            if color_nodes:
                colors = [
                    self.reformat(val)
                    for val in color_nodes.xpath('./@title').extract()
                ]
            if colors:
                metadata['color'] = colors
        except(TypeError, IndexError):
            pass

        price_node = sel.xpath('//div[@itemprop="offers"]/div[contains(@class, "prd-price") and not(contains(@class, "hidden"))]')
        if price_node:
            del_node = price_node.xpath('./del[text()]')
            if del_node:
                try:
                    price = del_node.xpath('./text()').extract()[0]
                    price = self.reformat(price)
                    if price:
                        metadata['price'] = price
                except(TypeError, IndexError):
                    pass

                try:
                    price_discount = price_node.xpath('./div[@itemprop="price"][text()]').extract()[0]
                    price_discount = self.reformat(price_discount)
                    if price_discount:
                        metadata['price_discount'] = price_discount
                except(TypeError, IndexError):
                    pass
            else:
                try:
                    price = price_node.xpath('./div/text()').extract()[0]
                    price = self.reformat(price)
                    if price:
                        metadata['price'] = price
                except(TypeError, IndexError):
                    pass

        description_node = sel.xpath('//div[@itemprop="description"]')
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

        image_urls = []
        try:
            image_nodes = sel.xpath('//ul[@class="thumb-list"]/li/a[@src]')
            for node in image_nodes:
                href = node.xpath('./@src').extract()[0]
                href = self.process_href(href, response.url)

                image_urls += [href]
        except(TypeError, IndexError):
            image_urls = None

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    def parse_ca(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="top_menu"]/ul/li')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = nav_node.xpath('./span/a/text()').extract()[0]
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

                sub_nodes = nav_node.xpath('./ul/li[child::a]')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

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

                        third_nodes = sub_node.xpath('./ul/li[child::a]')
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
                                              callback=self.parse_product_list_ca,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

    def parse_product_list_ca(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="pagination_contents"]/table//form')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_ca,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        page_nodes = sel.xpath('//div[@id="pagination_contents"]/div[contains(@class, "pagination")]/a[@href]')
        for page_node in page_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = page_node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_list_ca,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product_ca(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        other_nodes = sel.xpath('//ul[@id="product_thumbnails"]/li/a[@href]')
        for node in other_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_ca,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        model = None
        name = None
        try:
            name_model_node = sel.xpath('//div[@class="product-main-info"]//div[@class="product-info"]//h1[@class="mainbox-title"][text()]')
            if name_model_node:
                name_model_text = name_model_node.xpath('./text()').extract()[0]
                name_model_text = self.reformat(name_model_text)
                if name_model_text:
                    name_mt = re.search(r'(.+) - ', name_model_text)
                    if name_mt:
                        name = name_mt.group(1)
                    model_mt = re.search(r' - (\w+)', name_model_text)
                    if model_mt:
                        model = model_mt.group(1)
        except(TypeError, IndexError):
            model = None
            pass
        if model:
            metadata['model'] = model
        else:
            return

        if name:
            metadata['name'] = name

        # 检查是不是打折
        discount_node = sel.xpath('//div[@class="product-main-info"]//div[@class="product-info"]//div[contains(@class, "product-prices")]')
        if discount_node:
            price_node = discount_node.xpath('.//span[contains(@id, "old_price")]//strike')
            if price_node:
                try:
                    price = ''.join(
                        self.reformat(val)
                        for val in price_node.xpath('.//text()').extract()
                    )
                    price = self.reformat(price)
                    if price:
                        metadata['price'] = price
                except(TypeError, IndexError):
                    pass

            try:
                discount_price = ''.join(
                    self.reformat(val)
                    for val in discount_node.xpath('.//span[@class="price discprice"]//text()').extract()
                )
                discount_price = self.reformat(discount_price)
                if discount_price:
                    metadata['price_discount'] = discount_price
            except(TypeError, IndexError):
                pass
        else:
            price_node = sel.xpath('//div[@class="product-main-info"]//div[@class="clear"]//span[contains(@id, "line_discounted_price")]')
            if price_node:
                try:
                    price = ''.join(
                        self.reformat(val)
                        for val in price_node.xpath('.//text()').extract()
                    )
                    price = self.reformat(price)
                    if price:
                        metadata['price'] = price
                except(TypeError, IndexError):
                    pass

        color = None
        color_node = sel.xpath('//div[@class="product-main-info"]//div[@class="product-info"]/p[@class="sku"]')
        if color_node:
            try:
                color_text = color_node.xpath('./text()').extract()[0]
                color_text = self.reformat(color_text)
                if color_text:
                    mt = re.search(r':(.+)', color_text)
                    if mt:
                        color = mt.group(1)
            except(TypeError, IndexError):
                pass
        if color:
            metadata['color'] = [color]

        description_node = sel.xpath('//div[@class="product-main-info"]//div[@class="special-descr"]/ul/li[text()]')
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

        image_urls = None
        image_node = sel.xpath('//div[@class="product-main-info"]//div[@class="float-left"]/div/a[child::img[@src]][@href]')
        if image_node:
            try:
                image_urls = [
                    self.process_href(val , response.url)
                    for val in image_node.xpath('./@href').extract()
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

    def parse_us(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="navigation"]/nav/ul/li')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = nav_node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = nav_node.xpath('./ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        third_nodes = sub_node.xpath('./ul/li')
                        for third_node in third_nodes:
                            mcc = copy.deepcopy(mc)

                            tag_text = third_node.xpath('./a[text()]/text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()

                            if tag_text and tag_name:
                                mcc['tags_mapping']['category-2'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mcc['gender'] = [gender]

                                href = third_node.xpath('./a[@href]/@href').extract()[0]
                                href = self.process_href(href, response.url)

                                yield Request(url=href,
                                              callback=self.parse_product_list_us,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

                        href = sub_node.xpath('./a[@href]/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_product_list_us,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_product_list_us(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="primary"]//div[@class="search-result-content"]/ul/li')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('.//a[@href]').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_us,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        page_nodes = sel.xpath('//div[@id="primary"]//div[@class="pagination"]/ul/li/a[@href]')
        for node in page_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_list_us,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product_us(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        other_nodes = sel.xpath('//div[contains(@class, "product-detail")]//ul[@class="swatches Color"]/li/a[@href]')
        for node in other_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product_us,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        model = None
        model_node = sel.xpath('//div[@id="product-content"]//span[@itemprop="productID"][text()]')
        if model_node:
            model_text = model_node.xpath('./text()').extract()[0]
            model_text = self.reformat(model_text)
            if model_text:
                mt = re.search(r'^(\d+)-?', model_text)
                if mt:
                    model = mt.group(1)
        if model:
            metadata['model'] = model
        else:
            return

        price_node = sel.xpath('//div[@id="product-content"]/div[contains(@class, "product-price")]')
        if price_node:
            discount_node = price_node.xpath('./span[@class="price-sales"][text()]')
            if discount_node:
                discount_price = discount_node.xpath('./text()').extract()[0]
                discount_price = self.reformat(discount_price)
                if discount_price:
                    metadata['price_discount'] = discount_price
                price = price_node.xpath('./span[@class="price-standard"][text()]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price
            else:
                price = price_node.xpath('./span[@class="price-normal"]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price

        name_node = sel.xpath('//div[@id="primary"]//h1[@class="product-name"][text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)
            if name:
                metadata['name'] = name

        colors = []
        color_nodes = sel.xpath('//div[contains(@class, "product-detail")]//ul[@class="swatches Color"]/li/a[@title]')
        for node in color_nodes:
            color_text = node.xpath('./@title').extract()[0]
            color = re.sub(r'\(\d+\)', '', color_text)
            if color:
                colors += [color]
        if colors:
            metadata['color'] = colors

        description_node = sel.xpath('//div[@id="tabDescription"]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('.//text()').extract()
            )
            description = self.reformat(description)
            if description:
                metadata['description'] = description

        image_urls = []
        image_nodes = sel.xpath('//div[@id="primary"]//div[@class="product-thumbnails"]/ul/li/a[@href]')
        for node in image_nodes:
            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            href = re.sub(r'\?.*', '', href)

            if href:
                image_urls += [href]
        if not image_urls:
            image_node = sel.xpath('//div[@id="primary-image"]/a[@href]')
            href = image_node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            href = re.sub(r'\?.*', '', href)

            mt = re.search(r'noimage', href)
            if not mt:
                if href:
                    image_urls += [href]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
