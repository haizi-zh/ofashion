# coding=utf-8
import copy
import json
import re
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils_core import unicodify

__author__ = 'Zephyre'


class HermesSpider(MFashionSpider):
    # TODO 部分字段有乱码，需要解决
    allowed_domains = ['hermes.com']

    spider_data = {'home_urls': {'us': 'http://usa.hermes.com', 'fr': 'http://france.hermes.com',
                                 'at': 'http://austria.hermes.com', 'be': 'http://belgium-nl.hermes.com',
                                 'dk': 'http://denmark.hermes.com', 'de': 'http://germany.hermes.com',
                                 'es': 'http://spain.hermes.com', 'fi': 'http://finland.hermes.com',
                                 'ie': 'http://ireland.hermes.com', 'it': 'http://italy.hermes.com',
                                 'lu': 'http://luxembourg.hermes.com', 'nl': 'http://netherlands.hermes.com',
                                 'no': 'http://norway.hermes.com', 'ch': 'http://switzerland-fr.hermes.com',
                                 'se': 'http://sweden.hermes.com', 'uk': 'http://uk.hermes.com',
                                 'jp': 'http://japan.hermes.com', 'ca': 'http://canada-en.hermes.com'},
                   'brand_id': 10166}
    spider_data['hosts'] = spider_data['home_urls']

    @classmethod
    def get_supported_regions(cls):
        return HermesSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        super(HermesSpider, self).__init__('hermes', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"main-menu")]//li[contains(@class,"level0")]'):
            node_class = node._root.attrib['class']
            mt = re.search(r'\b(\w+)\s*$', node_class)
            if not mt:
                continue

            tag_type = 'category-0'
            tag_name = unicodify(mt.group(1)).lower()
            temp = node.xpath('./a[@href]')
            if not temp:
                continue
            href = temp[0]._root.attrib['href']
            tag_text = u', '.join([cm.html2plain(unicodify(val.text)) for val in temp[0]._root.iterdescendants() if
                                   val.text and val.text.strip()])

            m = copy.deepcopy(metadata)
            m['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]
            gender = cm.guess_gender(tag_name)
            if gender:
                m['gender'] = [gender]

            if not href or not href.strip():
                continue
            else:
                yield Request(url=href, meta={'userdata': m}, callback=self.parse_category_0)

    def parse_category_0(self, response):
        def func(node, level, data):
            ret = []
            temp = node.xpath('./a[@href]')
            if temp:
                temp = temp[0]
                data[str.format('category-{0}', level)] = unicodify(temp._root.text).lower()
                href = temp._root.attrib['href']
                if 'javascript:void' not in href:
                    data['href'] = href

            temp = node.xpath(str.format('./ul/li[contains(@class,level{0})]', level + 1))
            if not temp and 'href' in data:
                # 到达叶节点
                ret.append(data)
            else:
                # 中间节点
                for node2 in temp:
                    data2 = data.copy()
                    ret.extend(func(node2, level + 1, data2))
            return ret

        metadata = response.meta['userdata']
        sel = Selector(response)
        node_list = []
        for node in sel.xpath('//li[contains(@class, "level1")]'):
            node_list.extend(func(node, 1, {}))

        for node in node_list:
            m = copy.deepcopy(metadata)
            url = None
            for k, v in node.items():
                if k == 'href':
                    url = self.process_href(v, response.url)
                elif re.search(r'category-\d+', k):
                    m['tags_mapping'][k] = [{'name': v.lower(), 'title': v}]
            if url:
                yield Request(url=url, meta={'userdata': m, 'main-page': True}, callback=self.parse_list)

    def parse_list(self, response):
        sel = Selector(response)
        temp = sel.xpath('//div[contains(@class,"offer-description")]')
        # 此为单品详细介绍页面
        if temp:
            for val in self.parse_details(response):
                yield val
        else:
            metadata = response.meta['userdata']
            for node in sel.xpath('//div[contains(@class,"category-products")]/div[@class="stand"]/'
                                  'ul[@class="products"]/li[@id]/a[@href]'):
                url = node._root.attrib['href']
                yield Request(url=url, meta={'userdata': copy.deepcopy(metadata)}, callback=self.parse_details)

            # 处理分页信息
            if 'main-page' in response.meta and response.meta['main-page']:
                for node in sel.xpath('//div[@class="pager"]//li//a[@href]'):
                    yield Request(url=node._root.attrib['href'],
                                  meta={'userdata': copy.deepcopy(metadata), 'main-page': False},
                                  callback=self.parse_list)

    def parse_details(self, response):
        def func(product_id):
            m = copy.deepcopy(metadata)

            # if product_id in data['simpleProductPrices']:
            #     m['price'] = data['simpleProductPrices'][product_id]

            image_url = data['baseImages'][product_id]
            # 尝试找到zoom图
            zoom_image_url = re.sub(r'/default/([^/]+)$', r'/zoom/\1', image_url)
            if zoom_image_url in unicodify(response.body):
                image_url = zoom_image_url
            elif zoom_image_url.replace('/', r'\/') in unicodify(response.body):
                image_url = zoom_image_url

            # m['description'] = self.reformat(data['descriptions'][product_id])
            # m['name'] = self.reformat(data['names'][product_id])
            # m['model'] = data['skus'][product_id]
            # # TODO 这里有可能导致网页的url找错，例如：http://usa.hermes.com/jewelry/gold-jewelry/bracelets/configurable-product-104820b-23578.html
            # if product_id in data['links']:
            #     m['url'] = data['links'][product_id]
            # else:
            #     m['url'] = response.url
            #
            for attrib in data['attributes']:
                attrib_name = attrib['code']
                #     if re.search(r'color[\b_]', attrib_name):
                #         attrib_name = 'color'
                #     elif re.search('size_sized', attrib_name):
                #         attrib_name = 'size'

                temp = [unicodify(val['label']).lower() for val in attrib['options'] if
                        product_id in val['products']]
                # if attrib_name == 'color':
                #     m['color'] = temp
                # else:
                #     m['tags_mapping'][unicodify(attrib_name).lower()] = \
                #         [{'name': val.lower(), 'title': val} for val in temp]
                if attrib_name != 'color':
                    m['tags_mapping'][unicodify(attrib_name).lower()] = \
                        [{'name': val.lower(), 'title': val} for val in temp]

            # if 'category-1' in m['tags_mapping']:
            #     m['category'] = [val['name'] for val in m['tags_mapping']['category-1']]

            item = ProductItem()
            item['image_urls'] = [image_url]
            item['url'] = m['url']
            item['model'] = m['model']
            item['metadata'] = m
            return item

        metadata = response.meta['userdata']

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

        idx = response.body.find('spConfig.init')
        if idx == -1:
            idx = response.body.find('ConfProduct.init')
            if idx == -1:
                return

        body = cm.extract_closure(response.body[idx:], '{', '}')[0]
        data = json.loads(body)
        for val in (func(product_id) for product_id in data['productIds']):
            yield val

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

        model = None
        model_node = sel.xpath('//div[@class="sidebar"]//*[@id="athena_product_sku"][text()]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@class="sidebar"]//*[@id="product-price"]/*[@class="price"][text()]')
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

        name = None
        name_node = sel.xpath('//div[@class="sidebar"]//*[@id="product_name"][text()]')
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

        description = None
        description_node = sel.xpath('//div[@class="sidebar"]//*[@id="product_description"][text()]')
        if description_node:
            try:
                description = description_node.xpath('./text()').extract()[0]
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        color_node = sel.xpath('//div[@class="sidebar"]//*[@id="option-container-375"]//li/img[@alt]')
        if color_node:
            try:
                colors = [cls.reformat(val).lower()
                          for val in color_node.xpath('./@alt').extract()]
            except(TypeError, IndexError):
                pass

        return colors
