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
        'brand_id': 10114,
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
                    {'name': tag_name, 'title': tag_text, },
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
                                {'name': tag_name, 'title': tag_text, },
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

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

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
                    {'name': tag_name, 'title': tag_text, },
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
                            {'name': tag_name, 'title': tag_text, },
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

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        image_urls = None
        image_node = sel.xpath(
            '//div[@class="product-main-info"]//div[@class="float-left"]/div/a[child::img[@src]][@href]')
        if image_node:
            try:
                image_urls = [
                    self.process_href(val, response.url)
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

            try:
                tag_text = nav_node.xpath('./a/text()').extract()[0]
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

                sub_nodes = nav_node.xpath('./ul/li')
                for sub_node in sub_nodes:
                    mc = copy.deepcopy(m)

                    try:
                        tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
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

                        third_nodes = sub_node.xpath('./ul/li')
                        for third_node in third_nodes:
                            mcc = copy.deepcopy(mc)

                            try:
                                tag_text = third_node.xpath('./a[text()]/text()').extract()[0]
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
                                    href = third_node.xpath('./a[@href]/@href').extract()[0]
                                    href = self.process_href(href, response.url)
                                except(TypeError, IndexError):
                                    continue

                                yield Request(url=href,
                                              callback=self.parse_product_list_us,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

                        try:
                            href = sub_node.xpath('./a[@href]/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

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

            try:
                href = node.xpath('.//a[@href]').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_us,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        page_nodes = sel.xpath('//div[@id="primary"]//div[@class="pagination"]/ul/li/a[@href]')
        for node in page_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

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

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_us,
                          errback=self.onerr,
                          meta={'userdata': m})

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

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        image_urls = []
        image_nodes = sel.xpath('//div[@id="primary"]//div[@class="product-thumbnails"]/ul/li/a[@href]')
        for node in image_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                href = re.sub(r'\?.*', '', href)

                if href:
                    image_urls += [href]
            except(TypeError, IndexError):
                continue
        if not image_urls:
            image_node = sel.xpath('//div[@id="primary-image"]/a[@href]')
            try:
                href = image_node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                href = re.sub(r'\?.*', '', href)

                mt = re.search(r'noimage', href)
                if not mt:
                    if href:
                        image_urls += [href]
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
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        if region == 'us':
            try:
                model_node = sel.xpath('//div[@id="product-content"]//span[@itemprop="productID"][text()]')
                if model_node:
                    model_text = model_node.xpath('./text()').extract()[0]
                    model_text = cls.reformat(model_text)
                    if model_text:
                        mt = re.search(r'^(\d+)-?', model_text)
                        if mt:
                            model = mt.group(1)
            except(TypeError, IndexError):
                pass
        elif region == 'ca':
            try:
                name_model_node = sel.xpath(
                    '//div[@class="product-main-info"]//div[@class="product-info"]//h1[@class="mainbox-title"][text()]')
                if name_model_node:
                    name_model_text = name_model_node.xpath('./text()').extract()[0]
                    name_model_text = cls.reformat(name_model_text)
                    if name_model_text:
                        model_mt = re.search(r' - (\w+)', name_model_text)
                        if model_mt:
                            model = model_mt.group(1)
            except(TypeError, IndexError):
                pass
        else:
            try:
                model_node = sel.xpath(
                    '//div[@class="pdetail-cont-left"]/div/p[@class="art-number"]/span[@id="prd-item-number"][text()]')
                if model_node:
                    model_text = model_node.xpath('./text()').extract()[0]
                    model_text = cls.reformat(model_text)
                    if model_text:
                        mt = re.search(r'\b([0-9\-]+)\b', model_text)
                        if mt:
                            model = mt.group(1)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        old_price = None
        new_price = None
        if region == 'us':
            price_node = sel.xpath('//div[@id="product-content"]/div[contains(@class, "product-price")]')
            if price_node:
                discount_node = price_node.xpath('./span[@class="price-sales"][text()]')
                if discount_node:
                    try:
                        new_price = discount_node.xpath('./text()').extract()[0]
                        new_price = cls.reformat(new_price)
                    except(TypeError, IndexError):
                        pass
                    try:
                        old_price = price_node.xpath('./span[@class="price-standard"][text()]/text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass
                else:
                    try:
                        old_price = price_node.xpath('./span[@class="price-normal"]/text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass
        elif region == 'ca':
            # 检查是不是打折
            discount_node = sel.xpath(
                '//div[@class="product-main-info"]//div[@class="product-info"]//div[contains(@class, "product-prices")]')
            if discount_node:
                price_node = discount_node.xpath('.//span[contains(@id, "old_price")]//strike')
                if price_node:
                    try:
                        old_price = ''.join(
                            cls.reformat(val)
                            for val in price_node.xpath('.//text()').extract()
                        )
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass

                try:
                    new_price = ''.join(
                        cls.reformat(val)
                        for val in discount_node.xpath('.//span[@class="price discprice"]//text()').extract()
                    )
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
            else:
                price_node = sel.xpath(
                    '//div[@class="product-main-info"]//div[@class="clear"]//span[contains(@id, "line_discounted_price")]')
                if price_node:
                    try:
                        old_price = ''.join(
                            cls.reformat(val)
                            for val in price_node.xpath('.//text()').extract()
                        )
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass
        else:
            price_node = sel.xpath(
                '//div[@itemprop="offers"]/div[contains(@class, "prd-price") and not(contains(@class, "hidden"))]')
            if price_node:
                del_node = price_node.xpath('./del[text()]')
                if del_node:
                    try:
                        old_price = del_node.xpath('./text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass

                    try:
                        new_price = price_node.xpath('./div[@itemprop="price"][text()]').extract()[0]
                        new_price = cls.reformat(new_price)
                    except(TypeError, IndexError):
                        pass
                else:
                    try:
                        old_price = price_node.xpath('./div/text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        name = None
        if region == 'us':
            name_node = sel.xpath('//div[@id="primary"]//h1[@class="product-name"][text()]')
            if name_node:
                try:
                    name = name_node.xpath('./text()').extract()[0]
                    name = cls.reformat(name)
                except(TypeError, IndexError):
                    pass
        elif region == 'ca':
            try:
                name_model_node = sel.xpath(
                    '//div[@class="product-main-info"]//div[@class="product-info"]//h1[@class="mainbox-title"][text()]')
                if name_model_node:
                    name_model_text = name_model_node.xpath('./text()').extract()[0]
                    name_model_text = cls.reformat(name_model_text)
                    if name_model_text:
                        name_mt = re.search(r'(.+) - ', name_model_text)
                        if name_mt:
                            name = name_mt.group(1)
            except(TypeError, IndexError):
                pass
        else:
            try:
                name_node = sel.xpath('//div[@class="pdetail-cont-left"]//p[@class="shoe-headline"][text()]')
                if name_node:
                    name = name_node.xpath('./text()').extract()[0]
                    name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        description = None
        if region == 'us':
            description_node = sel.xpath('//div[@id="tabDescription"]')
            if description_node:
                try:
                    description = '\r'.join(
                        cls.reformat(val)
                        for val in description_node.xpath('.//text()').extract()
                    )
                    description = cls.reformat(description)
                except(TypeError, IndexError):
                    pass
        elif region == 'ca':
            description_node = sel.xpath('//div[@class="product-main-info"]//div[@class="special-descr"]/ul/li[text()]')
            if description_node:
                try:
                    description = '\r'.join(
                        cls.reformat(val)
                        for val in description_node.xpath('.//text()').extract()
                    )
                    description = cls.reformat(description)
                except(TypeError, IndexError):
                    pass
        else:
            description_node = sel.xpath('//div[@itemprop="description"]')
            if description_node:
                try:
                    description = '\r'.join(
                        cls.reformat(val)
                        for val in description_node.xpath('.//text()').extract()
                    )
                    description = cls.reformat(description)
                except(TypeError, IndexError):
                    pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        colors = []
        if region == 'us':
            color_nodes = sel.xpath(
                '//div[contains(@class, "product-detail")]//ul[@class="swatches Color"]/li/a[@title]')
            for node in color_nodes:
                try:
                    color_text = node.xpath('./@title').extract()[0]
                    color = re.sub(r'\(\d+\)', '', color_text)
                    if color:
                        colors += [color]
                except(TypeError, IndexError):
                    continue
        elif region == 'ca':
            color_node = sel.xpath('//div[@class="product-main-info"]//div[@class="product-info"]/p[@class="sku"]')
            if color_node:
                try:
                    color_text = color_node.xpath('./text()').extract()[0]
                    color_text = cls.reformat(color_text)
                    if color_text:
                        mt = re.search(r':(.+)', color_text)
                        if mt:
                            color = mt.group(1)
                except(TypeError, IndexError):
                    pass
        else:
            try:
                color_nodes = sel.xpath('//div[@class="bx-color"]//ul/li[@title]')
                if color_nodes:
                    colors = [
                        cls.reformat(val)
                        for val in color_nodes.xpath('./@title').extract()
                    ]
            except(TypeError, IndexError):
                pass

        return colors
