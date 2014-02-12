# coding=utf-8
import copy
import json
import re
from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
from utils.utils import unicodify

__author__ = 'Zephyre'


class MonclerSpider(MFashionSpider):
    # TODO 有些单品没有description信息

    spider_data = {'hosts': {'cn': 'http://store.moncler.cn',
                             'us': 'http://store.moncler.com'},
                   'home_urls': {'cn': {
                       'http://store.moncler.cn/cn/%E7%94%B7%E5%A3%AB/%E6%96%B0%E5%93%81%E4%B8%8A%E7%BA%BF_gid24319': {
                           'gender': ['male']},
                       'http://store.moncler.cn/cn/%E5%A5%B3%E5%A3%AB/%E6%96%B0%E5%93%81%E4%B8%8A%E7%BA%BF_gid24318': {
                           'gender': ['female']}},
                                 'us': {'http://store.moncler.com/us/women': {'gender': ['female']},
                                        'http://store.moncler.com/us/men': {'gender': ['male']},
                                        'http://store.moncler.com/us/unisex': {'gender': 'children'}},
                                 'uk': {'http://store.moncler.com/gb/women': {'gender': ['female']},
                                        'http://store.moncler.com/gb/men': {'gender': ['male']},
                                        'http://store.moncler.com/gb/unisex': {'gender': 'children'}},
                                 'fr': {'http://store.moncler.com/fr/femme': {'gender': ['female']},
                                        'http://store.moncler.com/fr/homme': {'gender': ['male']},
                                        'http://store.moncler.com/fr/unisexe': {'gender': 'children'}},
                                 'it': {'http://store.moncler.com/it/donna': {'gender': ['female']},
                                        'http://store.moncler.com/it/uomo': {'gender': ['male']},
                                        'http://store.moncler.com/it/unisex': {'gender': 'children'}}
                   },
                   'brand_id': 13084}

    @classmethod
    def get_supported_regions(cls):
        return MonclerSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        self.spider_data['hosts'] = {k: 'http://store.moncler.com' if k != 'cn' else 'http://store.moncler.cn' for k in
                                     self.spider_data['home_urls']}
        super(MonclerSpider, self).__init__('moncler', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="mainmenu"]/ul[@id="menu"]/li/a[@href]'):
            m = copy.deepcopy(metadata)
            tag_type = 'category-0'
            tag_name = unicodify(node._root.text)
            if not tag_name:
                continue
            m['tags_mapping'][tag_type] = [{'name': tag_name.lower(), 'title': tag_name}]
            m['category'] = [tag_name.lower()]

            url = self.process_href(node._root.attrib['href'], response.url)
            yield Request(url=url, meta={'userdata': m, 'filter-level': 0}, callback=self.parse_filter,
                          errback=self.onerr, dont_filter=True)

    def parse_filter(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        filter_idx = response.meta['filter-level']
        tmp = sel.xpath('//ul[@id="menuFilter"]/li[@id="colorLink" or @id="microLink"]')
        if filter_idx >= len(tmp):
            # filter过滤完毕，开始解析单品
            for val in self.parse_product_list(response):
                yield val
        else:
            filter_node = tmp[filter_idx]
            if filter_node._root.attrib['id'] != 'colorLink':
                tag_type = 'category-1'
            else:
                tag_type = None
            for node in filter_node.xpath('./div/ul[contains(@class,"sub")]/li[contains(@class,"sub")]/a[@href]'):
                m = copy.deepcopy(metadata)
                tag_name = unicodify(node._root.text)
                if not tag_name:
                    continue
                if tag_type:
                    m['tags_mapping'][tag_type] = [{'name': tag_name.lower(), 'title': tag_name}]
                else:
                    m['color'] = [tag_name.lower()]

                url = self.process_href(node._root.attrib['href'], response.url)
                yield Request(url=url, meta={'userdata': m, 'filter-level': filter_idx + 1},
                              callback=self.parse_filter, errback=self.onerr)

    def parse_product_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath(
                '//div[@id="elementsContainer"]/div[contains(@id,"item")]/a[@class="itemImage" and @href]'):
            url = self.process_href(re.sub(r'\s', '', cm.html2plain(node._root.attrib['href'])), response.url)
            m = copy.deepcopy(metadata)
            yield Request(url=url, meta={'userdata': m}, dont_filter=True,
                          callback=self.parse_product_details, errback=self.onerr)

    def parse_product_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

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

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        image_urls = []
        mt = re.search(r'var\s+jsoninit_item', response.body)
        if mt:
            idx = response.body[mt.regs[0][1]:].find('AVAILABLEZOOM')
            if idx != -1:
                idx += mt.regs[0][1]
                tmp = json.loads(cm.extract_closure(response.body[idx:], '{', '}')[0].replace("'", '"'))
                for c in tmp:
                    model = metadata['model']
                    if re.search(c + '$', model, flags=re.I):
                        # 找到放大的图像
                        image_urls = [str.format('http://cdn.yoox.biz/{0}/{1}_{2}.jpg', model[:2], model, val) for val
                                      in tmp[c]]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

    def start_requests(self):
        for region in self.region_list:
            if region in self.get_supported_regions():
                metadata = {'region': region, 'brand_id': self.spider_data['brand_id'], 'tags_mapping': {}}
                for url, extra_tag in self.spider_data['home_urls'][region].items():
                    m = copy.deepcopy(metadata)
                    for k, v in extra_tag.items():
                        if k == 'gender' and v == 'children':
                            m['tags_mapping']['category-x'] = [{'name': 'children', 'title': 'Children'}]
                        else:
                            m[k] = v
                    yield Request(url=url, meta={'userdata': m}, callback=self.parse, errback=self.onerr)
            else:
                self.log(str.format('No data for {0}', region), log.WARNING)

    @classmethod
    def is_offline(cls, response):
        return not cls.fetch_model(response)

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        model = None
        try:
            mt = re.search(r'var\s+jsoninit_dejavu\s*=\s*\{\s*ITEM:', response.body)
            if not mt:
                return
            tmp = json.loads(cm.extract_closure(response.body[mt.regs[0][1]:], '{', '}')[0])
            if 'cod10' in tmp:
                model = tmp['cod10']
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            mt = re.search(r'var\s+jsoninit_dejavu\s*=\s*\{\s*ITEM:', response.body)
            if not mt:
                return
            tmp = json.loads(cm.extract_closure(response.body[mt.regs[0][1]:], '{', '}')[0])
            if 'price' in tmp:
                old_price = tmp['price']
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

        name = None
        try:
            name = cls.reformat(' '.join(unicodify(val._root.text) for val in
                                         sel.xpath('//div[@id="titlePrice"]/div[@class="itemTitle"]')
                                         if val._root.text))
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            tmp = sel.xpath('//div[@id="descr_content"]')
            if tmp:
                description = cls.reformat(unicodify(tmp[0]._root.text))
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        details = None
        try:
            tmp = sel.xpath('//div[@id="details_content"]')
            if tmp:
                details = cls.reformat(unicodify(tmp[0]._root.text))
        except(TypeError, IndexError):
            pass

        return details
