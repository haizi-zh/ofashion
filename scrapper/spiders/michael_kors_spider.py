# coding=utf-8
import re
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
from utils.text import unicodify


__author__ = 'Zephyre'


class MichaelKorsSpider(MFashionSpider):
    spider_data = {'brand_id': 10259,
                   'ref_notation': {'cn': u'款号',
                                    'kr': u'스타일 번호',
                                    'br': u'Número do modelo',
                                    'jp': u'スタイルナンバー'},
                   'home_urls': {'cn': 'http://www.michaelkors.cn/catalog/',
                                 'jp': 'http://www.michaelkors.jp/catalog/',
                                 'kr': 'http://kr.michaelkors.com/catalog/',
                                 'br': 'http://br.michaelkors.com/catalog/',
                                 'us': 'http://www.michaelkors.com/?lang=en',  # 这里必须加lang=en，不然会被重定向到cn
                   }
    }

    @classmethod
    def get_supported_regions(cls):
        return MichaelKorsSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MichaelKorsSpider, self).__init__('michael_kors', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    @classmethod
    def process_href_for_us(cls, href):
        process_href = href

        if process_href:
            mt = re.search(ur'lang=en', process_href)
            if not mt:
                mt = re.search(ur'\?[^&]*', process_href)
                if mt:
                    process_href = str.format('{0}&lang=en', process_href)
                else:
                    process_href = str.format('{0}?lang=en', process_href)

        return process_href

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//nav/ul/li[@class="category-parent"]/a[@href]'):
            tag_text = self.reformat(unicodify(node1._root.text))
            if not tag_text:
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m1['category'] = [tag_text]

            for node2 in node1.xpath('../ul/li/a[@href]'):
                tag_text = self.reformat(unicodify(node2._root.text))
                if not tag_text:
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
                yield Request(url=self.process_href(node2._root.attrib['href'], response.url),
                              callback=self.parse_cat, errback=self.onerr, dont_filter=True,
                              meta={'userdata': m2, 'cat-level': 0})

        # 针对美国官网
        nav_nodes = sel.xpath('//div[@id="siloheader"]/div[@id="menusilo"]/div/ul/li/a[@href][text()]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = cm.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                    href = self.process_href_for_us(href)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_cat,
                              errback=self.onerr,
                              meta={'userdata': m}, )

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        region = metadata['region']

        if region != 'us':
            cat_level = response.meta['cat-level']

            node_list = []
            if cat_level == 0:
                node_list = sel.xpath('//ul[@class="product-categories"]/ul/li/a[@href]')
                for node in node_list:
                    # 还有下级目录
                    tag_text = self.reformat(unicodify(node._root.text))
                    if not tag_text:
                        continue
                    m = copy.deepcopy(metadata)
                    m['tags_mapping']['category-2'] = [{'name': tag_text.lower(), 'title': tag_text}]
                    yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                                  callback=self.parse_cat, errback=self.onerr, dont_filter=True,
                                  meta={'userdata': m, 'cat-level': 1})

            if not node_list:
                # 没有下级目录的情况，返回所有单品
                for node in sel.xpath('//ul[@id="list-content"]/li[contains(@class,"item")]/a[@href]'):
                    m = copy.deepcopy(metadata)
                    # tmp = node.xpath('./span[@class="product-name"]')
                    # if tmp:
                    #     m['name'] = self.reformat(unicodify(tmp[0]._root.text))
                    # tmp = node.xpath('.//span[@class="price"]')
                    # if tmp:
                    #     m['price'] = self.reformat(unicodify(tmp[0]._root.text))
                    yield Request(url=self.process_href(node._root.attrib['href'], response.url), dont_filter=True,
                                  callback=self.parse_details, errback=self.onerr, meta={'userdata': m})
        else:
            catalognav_nodes = sel.xpath('//div[@id="template"]/div[@class="catalognav"]/ul/li//a[@href][text()]')
            for node in catalognav_nodes:
                try:
                    tag_text = node.xpath('./text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()
                except(TypeError, IndexError):
                    continue

                if tag_text and tag_name:
                    m = copy.deepcopy(metadata)

                    m['tags_mapping']['catagory-1'] = [
                        {'name': tag_name, 'title': tag_text, },
                    ]

                    gender = cm.guess_gender(tag_name)
                    if gender:
                        m['gender'] = [gender]

                    try:
                        href = node.xpath('./@href').extract()[0]
                        href = self.process_href(href, response.url)
                        href = self.process_href_for_us(href)
                    except(TypeError, IndexError):
                        continue

                    yield Request(url=href,
                                  callback=self.parse_cat2_us,
                                  errback=self.onerr,
                                  meta={'userdata': m})

    def parse_cat2_us(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        cat_nodes = sel.xpath(
            '//div[@id="content"]/div[@id="categories"]/div[contains(@class, "category")]/a[@href][child::img[@title]]')
        for node in cat_nodes:
            try:
                tag_text = node.xpath('./img/@title').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = cm.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                    href = self.process_href_for_us(href)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list_us,
                              errback=self.onerr,
                              meta={'userdata': m})

        for val in self.parse_product_list_us(response):
            yield val

    def parse_product_list_us(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath(
            '//div[@id="content"]/div[@class="products"]/div[contains(@class, "product")]/a[@href]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)
                href = self.process_href_for_us(href)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_details,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

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

        tmp = sel.xpath('//div[@class="product-info"]/ul[@class="product"]/li/a[@href and @data-zoom-width and '
                        '@data-zoom-height]')
        image_urls = [self.process_href(val._root.attrib['href'], response.url) for val in tmp]
        # 针对美国，提取image_url
        if metadata['region'] == 'us' and not image_urls:
            origin_image_node = sel.xpath('//meta[@property="og:image"][@content]')
            if origin_image_node:
                try:
                    origin_image_url = origin_image_node.xpath('./@content').extract()[0]
                    if origin_image_url:
                        max_image_url = re.sub(ur'/mb/', u'/mz/', origin_image_url)
                        max_image_url = re.sub(ur'_mb\.', u'_mz.', max_image_url)
                        if max_image_url:
                            image_urls = [max_image_url]
                except(TypeError, IndexError):
                    pass

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

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
        if region != 'us':
            try:
                tmp = sel.xpath('//div[@class="product-info"]//span[@class="style-no"]/text()').extract()
                if tmp:
                    model = cls.reformat(re.sub(cls.spider_data['ref_notation'][region], '', tmp[0]))
            except(TypeError, IndexError):
                pass
        else:
            try:
                model_node = sel.xpath(
                    '//form[@name="productPage"]/table[1]//div[@class="productCutline"]/div[@class="vendor_style"][text()]')
                if model_node:
                    model_text = model_node.xpath('./text()').extract()[0]
                    model_text = cls.reformat(model_text)
                    if model_text:
                        mt = re.search(ur'(\S+)$', model_text)
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
        if region != 'us':
            price_node = sel.xpath('//div[@class="product-info"]//span[@class="price"][text()]')
            if price_node:
                try:
                    old_price = price_node.xpath('./text()').extract()[0]
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass
        else:
            price_node = sel.xpath('//form[@name="productPage"]/table[1]//font[@class="Black10V"][text()]')
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
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        name = None
        if region != 'us':
            name_node = sel.xpath('//div[@class="product-info"]/div[@class="product-info-content-top"]/h1[text()]')
            if name_node:
                try:
                    name = name_node.xpath('./text()').extract()[0]
                    name = cls.reformat(name)
                except(TypeError, IndexError):
                    pass
        else:
            name_node = sel.xpath('//form[@name="productPage"]/table[1]//td[@class="Black12VB"]/h1[text()]')
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

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        description = None
        if region == 'us':
            description_node = sel.xpath(
                '//form[@name="productPage"]/table[1]//div[@class="productCutline"]//li[text()]')
            if description_node:
                try:
                    description = '\r'.join(cls.reformat(val) for val in description_node.xpath('./text()').extract())
                    description = cls.reformat(description)
                except(TypeError, IndexError):
                    pass

        return description
