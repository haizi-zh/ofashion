# coding=utf-8
import copy
import json
import re
from scrapy import log
import scrapy.contrib.spiders
from scrapy.http import Request
from scrapy.selector import Selector
import global_settings as glob
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider

__author__ = 'Zephyre'


class HermesSpider(MFashionSpider):
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
            tag_name = cm.unicodify(mt.group(1)).lower()
            temp = node.xpath('./a[@href]')
            if not temp:
                continue
            href = temp[0]._root.attrib['href']
            tag_text = u', '.join([cm.html2plain(cm.unicodify(val.text)) for val in temp[0]._root.iterdescendants() if
                                   val.text and val.text.strip()])

            m = copy.deepcopy(metadata)
            m['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]

            if tag_name in ('women', 'woman', 'femme', 'donna', 'damen', 'mujer', 'demes', 'vrouw', 'frauen'):
                m['gender'] = ['female']
            elif tag_name in ('man', 'men', 'homme', 'uomo', 'herren', 'hombre', 'heren', 'mann', 'signore'):
                m['gender'] = ['male']

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
                data[str.format('category-{0}', level)] = cm.unicodify(temp._root.text).lower()
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
                    url = self.process_href(v, metadata['region'])
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

            if product_id in data['simpleProductPrices']:
                m['price'] = data['simpleProductPrices'][product_id]

            image_url = data['baseImages'][product_id]
            # 尝试找到zoom图
            zoom_image_url = re.sub(r'/default/([^/]+)$', r'/zoom/\1', image_url)
            if zoom_image_url in cm.unicodify(response.body):
                image_url = zoom_image_url
            elif zoom_image_url.replace('/', r'\/') in cm.unicodify(response.body):
                image_url = zoom_image_url

            m['description'] = data['descriptions'][product_id]
            m['name'] = data['names'][product_id]
            m['model'] = data['skus'][product_id]
            if product_id in data['links']:
                m['url'] = data['links'][product_id]
            else:
                m['url'] = response._url

            for attrib in data['attributes']:
                attrib_name = attrib['code']
                if re.search(r'color[\b_]', attrib_name):
                    attrib_name = 'color'
                elif re.search('size_sized', attrib_name):
                    attrib_name = 'size'

                temp = [cm.unicodify(val['label']).lower() for val in attrib['options'] if
                        product_id in val['products']]
                if attrib_name == 'color':
                    m['color'] = temp
                else:
                    m['tags_mapping'][cm.unicodify(attrib_name).lower()] = \
                        [{'name': val.lower(), 'title': val} for val in temp]

            if 'category-1' in m['tags_mapping']:
                m['category'] = [val['name'] for val in m['tags_mapping']['category-1']]

            # 必须实现
            item = ProductItem()
            item['image_urls'] = [image_url]
            item['url'] = m['url']
            item['model'] = m['model']
            item['metadata'] = m
            # //

            return item

        metadata = response.meta['userdata']
        idx = response.body.find('spConfig.init')
        if idx == -1:
            idx = response.body.find('ConfProduct.init')
            if idx == -1:
                return

        body = cm.extract_closure(response.body[idx:], '{', '}')[0]
        data = json.loads(body)
        for val in (func(product_id) for product_id in data['productIds']):
            yield val

