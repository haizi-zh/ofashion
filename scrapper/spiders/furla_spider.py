# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import re
import common

class FurlaSpider(MFashionSpider):
    """
    中国，
        产品系列：http://www.furla.com/cn/collections/slide/
    其他，(英国uk = en)
        e-shop：http://www.furla.com/us/eshop/
        （暂时不抓）collection（网站上没看见链接，但是存着网页）：http://www.furla.com/us/collections/bags/
    """

    spider_data = {
        'brand_id': 10142,
        'home_urls': {
            #'cn': 'http://www.furla.com/cn/collections/slide',
            #'uk': 'http://www.furla.com/en/eshop',
            k: str.format('http://www.furla.com/{0}/eshop', k if k != 'uk' else 'en')
            if k != 'cn' else 'http://www.furla.com/cn/collections/slide'
            for k in {
                'cn', 'uk', 'us', 'fr', 'jp',
                'it', 'oc', 'bg', 'ke', 'fi',
                'ie', 'lt', 'nl', 'cz', 'si',
                'hu', 'at', 'dk', 'lu', 'pl',
                'ro', 'es', 'be', 'cy', 'ee',
                'de', 'el', 'lv', 'mt', 'pt',
                'sk', 'se',
            }
        },
    }

    allow_domains = ['furla.com']

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def __init__(self, region):
        self.spider_data['callbacks'] = {
            'cn': self.parse_cn,
            'other': self.parse_other,
        }

        super(FurlaSpider, self).__init__('furla', region)

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def parse(self, response):
        """
        针对不同国家(中国和其他)，调用不同的callback解析
        """

        metadata = response.meta['userdata']
        key = metadata['region']
        if key in self.spider_data['callbacks'].keys():
            for val in self.spider_data['callbacks'][key](response):
                yield val
        else:
            for val in self.spider_data['callbacks']['other'](response):
                yield val

    def parse_cn(self, response):
        """
        针对中国，解析collection的左边导航栏
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 左边导航栏的第一级解析
        sidebar_nodes = sel.xpath('//div[@class="sidebar"]/ul/li')
        for node in sidebar_nodes:
            try:
                tag_text= node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                # 猜男女
                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                # 左边导航栏的第二级解析
                sub_nodes = node.xpath('.//li')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)
                        mc['tags_mapping']['category-1']= [
                            {'name': tag_name, 'title': tag_text},
                        ]

                        # 猜男女
                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./a/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_cn_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

                    try:
                        href = node.xpath('./a/@href').extract()[0]
                        href = self.process_href(href, response.url)
                    except(TypeError, IndexError):
                        continue

                    yield Request(url=href,
                                  callback=self.parse_cn_list,
                                  errback=self.onerr,
                                  meta={'userdata': m})

        for val in self.parse_cn_list(response):
            yield val

    def parse_cn_list(self, response):
        """
        针对中国，解析collection的单品列表
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这个xpath是去掉一些指向微博，facebook或它自己注册页面的链接
        product_nodes = sel.xpath('//ul[@class="collection_list"]//li[not(contains(@class, "big"))]//a[contains(@href, "furla.com")]')
        for node in product_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            if href:
                m = copy.deepcopy(metadata)

                yield Request(url=href,
                              callback=self.parse_cn_product,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_cn_product(self, response):
        """
        针对中国，解析单品页面信息
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        #解析不同颜色的单品
        color_nodes = sel.xpath('//ul[@class="colors_materials"]//ul//li//a')
        for node in color_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            if href:
                m = copy.deepcopy(metadata)

                yield Request(url=href,
                              callback=self.parse_cn_product,
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

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        #解析材质
        materials = []
        try:
            for val in sel.xpath('//ul[@class="colors_materials"]/li/text()').extract():
                if self.reformat(val):
                    materials += [
                        {'name': self.reformat(val).lower(), 'title': self.reformat(val)},
                    ]
        except(TypeError, IndexError):
            pass
        if materials:
            metadata['tags_mapping']['materials'] = materials

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        #解析图片
        image_urls = None
        try:
            image_urls = list(
                self.process_href(re.sub(r'350/', r'2048/', src), response.url)
                for src in sel.xpath('//div[@class="foto_product"]//img/@src').extract()
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

    def parse_other(self, response):
        """
        针对其他国家，解析nav各个标签
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # nav一级标签
        nav_nodes = sel.xpath('//ul[@class="nav long"]/li')
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

                # 猜男女
                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                # nav下拉的二级标签
                sub_nodes = node.xpath('./ul/li')
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

                        # 猜男女
                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./a/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_other_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

                try:
                    href = node.xpath('./a/@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_other_filter,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_other_filter(self, response):
        """
        针对其他国家，解析页面左边分类
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 左边分类标签
        sidebar_nodes = sel.xpath('//div[@class="sidebar"]//li')
        for node in sidebar_nodes:
            try:
                tag_text = node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                try:
                    href = node.xpath('./a/@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_other_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_other_product_list(self, response):
        """
        针对其他国家，解析单品列表
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="listing"]//li')
        for node in product_nodes:
            # try:
            #     name = node.xpath('//p[@class="title"]/a/text()').extract()[0]
            #     name = self.reformat(name)
            # except(TypeError, IndexError):
            #     continue
            #
            # price = node.xpath('//p[@class="price"]/text()').extract()[0]
            # price = self.reformat(price)

            m = copy.deepcopy(metadata)
            # if name:
            #     m['name'] = name
            # if price:
            #     m['price'] = price

            try:
                href = node.xpath('./a/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_other_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        #页面底部的页数链接
        page_nodes = sel.xpath('//div[@class="paginazione bottom"]//a')
        for node in page_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_other_product_list,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_other_product(self, response):
        """
        针对其他国家，解析单品页面
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这样不能得到颜色
        #colorImgSrc = sel.xpath('//div[@class="select"]/p[@class="colors"]//img[@class="selected"]/@src').extract()[0]
        #result = re.search(r'/\d*([a-zA-Z]+)\d*\.', colorImgSrc)
        #if result:
        #    color = result.group(1).lower()
        #    metadata['color'] = [color]
        # TODO 可以尝试在description中找到颜色

        # 解析其他颜色单品
        color_nodes = sel.xpath('//div[@class="select"]/p[@class="colors"]//a')
        for node in color_nodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_other_product,
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

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = None
        try:
            image_urls = list(
                self.process_href(re.sub(r'90/', r'2048/', val), response.url)
                for val in sel.xpath('//div[@class="prodotto_dettagli"]//img/@src').extract()
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

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        if region == 'cn':
            #尝试从url取得model
            try:
                mt = re.search(r'_(\d+)\.', response.url)
                if mt:
                    model = mt.group(1)
            except(TypeError, IndexError):
                pass
        else:
            #尝试从页面中取得model
            model_node = sel.xpath('//div[@class="select"]/p[contains(text(), "code")]')
            if model_node:
                mt = None
                try:
                    product_code_text = model_node.xpath('./text()').extract()[0]
                    mt = re.search(r'\b(\d+)\b', product_code_text)
                    if mt:
                        model = mt.group(1)
                except(TypeError, IndexError):
                    pass
            #尝试从url取得model
            if not model:
                try:
                    mt = re.search(r'_(\d+)\.', response.url)
                    if mt:
                        model = mt.group(1)
                except(TypeError, IndexError):
                    pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        old_price = None
        new_price = None
        if region == 'cn':
            pass
        else:
            price_node = sel.xpath('//div[@id="container"]//div[@id="prodotto_descrizione"]//p[@class="prezzo"]/span[text()]')
            if price_node:
                try:
                    old_price = price_node.xpath('./text()').extract()[0]
                    old_price = cls.reformat(old_price)
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

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        name = None
        if region == 'cn':
            #解析name，页面上叫description，但是看起来像name
            name = None
            try:
                name = ''.join(
                    cls.reformat(val)
                    for val in sel.xpath('//div[@class="description_product"]//h1/text()').extract()
                )
            except(TypeError, IndexError):
                pass
        else:
            name_node = sel.xpath('//div[@id="container"]//div[@id="prodotto_descrizione"]/h1[text()]')
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

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        description = None
        if region == 'cn':
            pass
        else:
            try:
                description = '\r'.join(cls.reformat(val) for val in sel.xpath('//p[contains(@id, "prod")]//text()').extract())
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        details = None
        if region == 'cn':
            #解析size，放在details里边
            size = None
            try:
                size = ''.join(
                    cls.reformat(val)
                    for val in sel.xpath('//div[@class="description_product"]//h2/text()').extract()
                )
                size = cls.reformat(size)
            except(TypeError, IndexError):
                pass
            if size:
                details = size
        else:
            detail = None
            try:
                detail = '\r'.join(cls.reformat(val) for val in sel.xpath('//div[@class="select"]/p[preceding-sibling::p[1]][following-sibling::p[contains(text(), "code")]]//text()').extract())
                detail = cls.reformat(detail)
            except(TypeError, IndexError):
                pass
            if detail:
                details = detail

        return details

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        colors = []
        if region == 'cn':
            #解析颜色
            try:
                colors = filter(None, list(
                    cls.reformat(val)
                    for val in sel.xpath('//ul[@class="colors_materials"]//ul//li[not(child::a) or descendant::img[@class="selected"]]//text()').extract()
                ))
            except(TypeError, IndexError):
                pass
        else:
            pass

        return colors
