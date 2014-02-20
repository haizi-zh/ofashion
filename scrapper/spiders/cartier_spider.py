# coding=utf-8
import copy
import json
import re
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils import unicodify

__author__ = 'Zephyre'


class CartierSpider(MFashionSpider):
    handle_httpstatus_list = [403, 504]

    spider_data = {'brand_id': 10066,
                   'hosts': {'cn': 'http://www.cartier.cn', 'us': 'http://www.cartier.us',
                             'fr': 'http://www.cartier.fr', 'jp': 'http://www.cartier.jp',
                             'uk': 'http://www.cartier.co.uk', 'kr': 'http://www.cartier.co.kr',
                             'tw': 'http://www.tw.cartier.com', 'br': 'http://www.cartier.com.br',
                             'de': 'http://www.cartier.de', 'es': 'http://www.cartier.es',
                             'ru': 'http://www.ru.cartier.com', 'it': 'http://www.cartier.it',
                             'hk': 'http://www.cartier.hk', 'ii': 'http://www.cartier.com'
                   },
                   'home_urls': {'cn': 'http://www.cartier.cn/%E7%B3%BB%E5%88%97/',
                                 'us': 'http://www.cartier.us/collections/',
                                 'fr': 'http://www.cartier.fr/collections/',
                                 'jp': 'http://www.cartier.jp/%E3%82%B3%E3%83%AC%E3%82%AF%E3%82%B7%E3%83%A7%E3%83%B3/',
                                 'uk': 'http://www.cartier.co.uk/collections/',
                                 'kr': 'http://www.cartier.co.kr/%EC%BB%AC%EB%A0%89%EC%85%98/',
                                 'tw': 'http://www.tw.cartier.com/%E7%B3%BB%E5%88%97/',
                                 'br': 'http://www.cartier.com.br/colecoes/',
                                 'de': 'http://www.cartier.de/kollektionen/',
                                 'es': 'http://www.cartier.es/colecciones/',
                                 'ru': 'http://www.ru.cartier.com/%D0%BA%D0%BE%D0%BB%D0%BB%D0%B5%D0%BA%D1%86%D0%B8%D0%B8',
                                 'it': 'http://www.cartier.it/collezioni',
                                 'hk': 'http://www.cartier.hk/%E7%B3%BB%E5%88%97',
                                 'ii': 'http://www.cartier.com/collections'
                   },
                   'data_urls': {'cn': 'http://www.cartier.cn/ajax/navigation/',
                                 'us': 'http://www.cartier.us/ajax/navigation/',
                                 'fr': 'http://www.cartier.fr/ajax/navigation/',
                                 'jp': 'http://www.cartier.jp/ajax/navigation/',
                                 'uk': 'http://www.cartier.co.uk/ajax/navigation/',
                                 'kr': 'http://www.cartier.co.kr/ajax/navigation/',
                                 'tw': 'http://www.tw.cartier.com/ajax/navigation/',
                                 'br': 'http://www.cartier.com.br/ajax/navigation/',
                                 'de': 'http://www.cartier.de/ajax/navigation/',
                                 'es': 'http://www.cartier.es/ajax/navigation/',
                                 'ru': 'http://www.ru.cartier.com/ajax/navigation/',
                                 'it': 'http://www.cartier.it/ajax/navigation/',
                                 'hk': 'http://www.cartier.hk/ajax/navigation/',
                                 'ii': 'http://www.cartier.com/ajax/navigation/'
                   }}

    @classmethod
    def get_supported_regions(cls):
        return CartierSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        super(CartierSpider, self).__init__('cartier', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node_0 in sel.xpath('//ul[@id="secondary"]/li/a[@href]'):
            temp = node_0._root.text
            if not temp or not temp.strip():
                continue
            else:
                temp = temp.strip()
            tag_text = unicodify(temp)
            tag_name = tag_text.lower()
            metadata_0 = copy.deepcopy(metadata)
            metadata_0['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
            metadata_0['category'] = [tag_name]

            for node_1 in node_0.xpath('../div/ul/li/ul/li/a[@href]'):
                temp = node_1._root.text
                if not temp or not temp.strip():
                    continue
                else:
                    temp = temp.strip()
                tag_text = unicodify(temp)
                tag_name = tag_text.lower()
                metadata_1 = copy.deepcopy(metadata_0)
                metadata_1['tags_mapping']['category-1'] = [{'name': tag_name, 'title': tag_text}]
                metadata_1['page_id'] = 0

                yield Request(url=self.process_href(node_1._root.attrib['href'], response.url), dont_filter=True,
                              meta={'userdata': metadata_1}, callback=self.parse_list, errback=self.onerr)

    def parse_products(self, response):
        metadata = response.meta['userdata']
        # self.log(unicode.format(u'PROCESSING {0} -> {1} -> {2}: {3}', metadata['extra']['category-0'][0],
        #                         metadata['extra']['category-1'][0], metadata['name'], response.url).encode('utf-8'),
        #          log.DEBUG)
        for k in ('post_token', 'page_id'):
            if k in metadata:
                metadata.pop(k)
        sel = Selector(response)

        temp = sel.xpath('//div[@class="product-header"]//span[@class="page-product-title"]')
        if temp:
            collection = unicodify(temp[0]._root.text)
            if collection:
                metadata['tags_mapping']['collection'] = [{'name': collection.lower(), 'title': collection}]

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        if 'name' not in metadata or not metadata['name']:
            name = self.fetch_name(response)
            if name:
                metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        temp = sel.xpath('//div[@class="column-images"]//a[@href and contains(@class,"zoom-trigger-link")]')
        image_urls = [self.process_href(val._root.attrib['href'], response.url) for val in temp]

        metadata['url'] = response.url
        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        return item

    def parse_list(self, response):
        metadata = response.meta['userdata']
        # self.log(unicode.format(u'PROCESSING {0} -> {1} -> PAGE {2}: {3}', metadata['extra']['category-0'][0],
        #                         metadata['extra']['category-1'][0], metadata['page_id'], response.url).encode('utf-8'),
        #          log.DEBUG)
        if metadata['page_id'] == 0:
            sel = Selector(response)
        else:
            try:
                text = json.loads(response.body)['cartierFoAjaxSearch']['data']
                sel = Selector(text=text)
            except (ValueError, KeyError, TypeError):
                # 解析错误，作为普通HTML对待
                sel = Selector(response)
                # metadata['page_id'] = 0

        if sel.xpath('//div[@class="product-header"]//span[@class="page-product-title"]'):
        #     实际上是单品页面
            yield self.parse_products(response)
        else:
            flag = False
            for node in sel.xpath('//div[contains(@class,"hover-info")]/a[@href]/div[@class="model-info"]'):
                m = copy.deepcopy(metadata)
                temp = node.xpath('./div[@class="model-name"]')
                if not temp:
                    continue
                m['name'] = unicodify(temp[0]._root.text)
                temp = node.xpath('./div[@class="model-description"]')
                if not temp:
                    continue
                m['description'] = unicodify(temp[0]._root.text)
                flag = True
                yield Request(url=self.process_href(node.xpath('..')[0]._root.attrib['href'], response.url),
                              meta={'userdata': m}, callback=self.parse_products, errback=self.onerr, dont_filter=True)

            if flag:
                # 处理翻页
                post_token = metadata['post_token'] if 'post_token' in metadata else None
                if not post_token:
                    temp = sel.xpath('//body[contains(@class, "html") and contains(@class, "page-navigation")]')
                    if temp:
                        temp = filter(lambda val: re.search('^page-navigation-(.+)', val),
                                      re.split(r'\s+', temp[0]._root.attrib['class']))
                        if temp:
                            post_token = re.search('^page-navigation-(.+)', temp[0]).group(1).replace('-', '_')
                if post_token:
                    m = copy.deepcopy(metadata)
                    m['page_id'] += 1
                    m['post_token'] = post_token
                    body = {'facetsajax': 'true', 'limit': m['page_id'], 'params': ''}
                    yield Request(url=self.spider_data['data_urls'][m['region']] + post_token, method='POST',
                                  body='&'.join(str.format('{0}={1}', k, body[k]) for k in body),
                                  headers={'Content-Type': 'application/x-www-form-urlencoded',
                                           'X-Requested-With': 'XMLHttpRequest'},
                                  callback=self.parse_list, meta={'userdata': m}, errback=self.onerr,
                                  dont_filter=True)

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

        model = None
        try:
            temp = sel.xpath(
                '//div[@class="commerce-product-sku"]/span[@itemprop="productID" and @class="commerce-product-sku-id"]')
            if temp:
                model = temp[0]._root.text.strip()
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
            temp = sel.xpath('//div[@itemprop="offers"]//div[@itemprop="price" and @class="product-price"]')
            if temp:
                old_price = unicodify(temp[0]._root.text)
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
            temp = sel.xpath('//div[@class="product-main"]//span[@itemprop="name"]')
            if temp:
                # name = unicodify(temp[0]._root.text)
                try:
                    name = ''.join(cls.reformat(val) for val in temp.xpath('./text()').extract())
                    name = cls.reformat(name)
                except(TypeError, IndexError):
                    pass
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            temp = sel.xpath('//div[@class="product-aesthetics"]//span[@itemprop="description"]/p')
            description = '\n'.join(unicodify(val._root.text) for val in temp if val._root.text)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        details = None
        try:
            temp = sel.xpath('//div[@class="product-details"]//div[contains(@class,"field-item")]/p')
            details = '\n'.join(unicodify(val._root.text) for val in temp if val._root.text)
        except(TypeError, IndexError):
            pass

        return details
