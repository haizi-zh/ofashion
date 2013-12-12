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
        sidebarNodes = sel.xpath('//div[@class="sidebar"]/ul/li')
        for node in sidebarNodes:
            tag_text= node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [
                {'name': tag_name, 'title': tag_text},
            ]

            # 猜男女
            gender = common.guess_gender(tag_name)
            if gender:
                m['gender'] = [gender]

            # 左边导航栏的第二级解析
            subNodes = node.xpath('.//li')
            for subNode in subNodes:
                tag_text = subNode.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()

                mc = copy.deepcopy(m)
                mc['tags_mapping']['category-1']= [
                    {'name': tag_name, 'title': tag_text},
                ]

                # 猜男女
                gender = common.guess_gender(tag_name)
                if gender:
                    mc['gender'] = [gender]

                href = subNode.xpath('./a/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_cn_list,
                              errback=self.onerr,
                              meta={'userdata': mc})

            href = node.xpath('./a/@href').extract()[0]
            href = self.process_href(href, response.url)

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
        productNodes = sel.xpath('//ul[@class="collection_list"]//li[not(contains(@class, "big"))]//a[contains(@href, "furla.com")]')
        for node in productNodes:
            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

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

        metadata['url'] = response.url

        #解析model，页面上叫description，但是看起来像model，也像name
        model = ''.join(
            self.reformat(val)
            for val in sel.xpath('//div[@class="description_product"]//h1/text()').extract()
        )
        if model:
            metadata['model'] = model
            metadata['name'] = model
        else:
            return

        #解析size，放在details里边
        size = ''.join(
            self.reformat(val)
            for val in sel.xpath('//div[@class="description_product"]//h2/text()').extract()
        )
        size = self.reformat(size)
        if size:
            metadata['details'] = size

        #解析材质
        #materials = {
        #    self.reformat(val): self.reformat(val)
        #    for val in sel.xpath('//ul[@class="colors_materials"]/li/text()').extract()
        #}
        #materials = dict((k, v) for k, v in materials.iteritems() if k)
        #if materials:
        #    metadata['tags_mapping']['materials'] = [materials]

        #解析颜色
        colors = filter(None, list(
            self.reformat(val)
            for val in sel.xpath('//ul[@class="colors_materials"]//ul//li//text()').extract()
        ))
        if colors:
            metadata['color'] = colors

        #解析图片
        imageNodes = sel.xpath('//ul[@class="colors_materials"]//a[@href]')
        for node in imageNodes:
            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_cn_image,
                          errback=self.onerr,
                          meta={'userdata': m})

        for val in self.parse_cn_image(response):
            yield val

    def parse_cn_image(self, response):
        """
        针对中国，解析图片
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        imageUrls = list(
            self.process_href(re.sub(r'350/', r'2048/', src), response.url)
            for src in sel.xpath('//div[@class="foto_product"]//img/@src').extract()
        )

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = imageUrls
        item['metadata'] = metadata

        yield item

    def parse_other(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        navNodes = sel.xpath('//ul[@class="nav long"]/li')
        for node in navNodes:
            tag_text = node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [
                {'name': tag_name, 'title': tag_text},
            ]

            subNodes = node.xpath('./ul/li')
            for subNode in subNodes:
                tag_text = subNode.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()

                mc = copy.deepcopy(m)
                mc['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text},
                ]

                href = subNode.xpath('./a/@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_other_product_list,
                              errback=self.onerr,
                              meta={'userdata': mc})

            href = node.xpath('./a/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_other_filter,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_other_filter(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        sidebarNodes = sel.xpath('//div[@class="sidebar"]//li')
        for node in sidebarNodes:
            tag_text = node.xpath('./a/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [
                {'name': tag_name, 'title': tag_text},
            ]

            href = node.xpath('./a/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_other_product_list,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_other_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        productNodes = sel.xpath('//div[@id="listing"]//li')
        for node in productNodes:
            name = node.xpath('//p[@class="title"]/a/text()').extract()[0]
            name = self.reformat(name)

            price = node.xpath('//p[@class="price"]/text()').extract()[0]

            m = copy.deepcopy(metadata)
            if name:
                m['name'] = name
            if price:
                m['price'] = price

            href = node.xpath('./a/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_other_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        #页面底部的页数链接
        pageNodes = sel.xpath('//div[@class="paginazione bottom"]//a')
        for node in pageNodes:
            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_other_product_list,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_other_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 这样不能得到颜色
        #colorImgSrc = sel.xpath('//div[@class="select"]/p[@class="colors"]//img[@class="selected"]/@src').extract()[0]
        #result = re.search(r'/\d*([a-zA-Z]+)\d*\.', colorImgSrc)
        #if result:
        #    color = result.group(1).lower()
        #    metadata['color'] = [color]
        # TODO 怎样找到颜色

        colorNodes = sel.xpath('//div[@class="select"]/p[@class="colors"]//a')
        for node in colorNodes:
            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_other_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        #尝试从页面中取得model
        model = None
        modelNode = sel.xpath('//div[@class="select"]/p[contains(text(), "code")]')
        if modelNode:
            productCodeText = modelNode.xpath('./text()').extract()[0]
            mt = re.search(r'\b(\d+)\b', productCodeText)
            if mt:
                model = mt.group(1)
        #尝试从url取得model
        if not model:
            mt = re.search(r'_(\d+)\.', response.url)
            if mt:
                model = mt.group(1)

        if model:
            metadata['model'] = model
        else:
            return

        metadata['url'] = response.url

        description = '\r'.join(self.reformat(val) for val in sel.xpath('//p[contains(@id, "prod")]//text()').extract())
        description = self.reformat(description)
        if description:
            metadata['description'] = description

        detail = '\r'.join(self.reformat(val) for val in sel.xpath('//div[@class="select"]/p[preceding-sibling::p[1]][following-sibling::p[contains(text(), "code")]]//text()').extract())
        detail = self.reformat(detail)
        if detail:
            metadata['details'] = detail

        imageUrls = list(
            self.process_href(re.sub(r'90/', r'2048/', val), response.url)
            for val in sel.xpath('//div[@class="prodotto_dettagli"]//img/@src').extract()
        )

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = imageUrls
        item['metadata'] = metadata

        yield item

