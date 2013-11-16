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

__author__ = 'Zephyre'

brand_id = 10066


# 实例化
def create_spider():
    return CartierSpider()


def supported_regions():
    return CartierSpider.spider_data['supported_regions']


class CartierSpider(scrapy.contrib.spiders.CrawlSpider):
    name = 'cartier'

    spider_data = {'hosts': {'cn': 'http://www.cartier.cn', 'us': 'http://www.cartier.us',
                             'fr': 'http://www.cartier.fr', 'jp': 'http://www.cartier.jp',
                             'uk': 'http://www.cartier.uk', 'kr': 'http://www.cartier.co.kr',
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
    spider_data['supported_regions'] = spider_data['hosts'].keys()

    def process_href(self, href, region, host=None):
        if not href or not href.strip():
            return None
        else:
            href = href.strip()

        if re.search('^(http|https)://', href):
            return href
        elif re.search('^//', href):
            return 'http:' + href
        elif re.search('^/', href):
            if not host:
                host = self.spider_data['hosts'][region]
            return host + href

    def __init__(self, *a, **kw):
        super(CartierSpider, self).__init__(*a, **kw)
        self.spider_data = copy.deepcopy(CartierSpider.spider_data)
        self.spider_data['brand_id'] = brand_id
        for k, v in glob.BRAND_NAMES[self.spider_data['brand_id']].items():
            self.spider_data[k] = v

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.name = str.format('{0}-{1}', self.name, region)
        if region in self.spider_data['supported_regions']:
            metadata = {'region': region, 'brand_id': brand_id,
                        'brandname_e': glob.BRAND_NAMES[brand_id]['brandname_e'],
                        'brandname_c': glob.BRAND_NAMES[brand_id]['brandname_c'], 'tags_mapping': {}, 'extra': {}}
            return [Request(url=self.spider_data['home_urls'][region], meta={'userdata': metadata})]
        else:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node_0 in sel.xpath('//ul[@id="secondary"]/li/a[@href]'):
            temp = node_0._root.text
            if not temp or not temp.strip():
                continue
            else:
                temp = temp.strip()
            tag_name = cm.unicodify(temp).lower()
            tag_type = 'category-0'
            tag_text = tag_name
            metadata_0 = copy.deepcopy(metadata)
            metadata_0['extra'][tag_type] = [tag_name]
            metadata_0['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]
            metadata_0['category'] = [tag_name]

            for node_1 in node_0.xpath('../div/ul/li/ul/li/a[@href]'):
                href = self.process_href(node_1._root.attrib['href'], metadata['region'])
                temp = node_1._root.text
                if not temp or not temp.strip():
                    continue
                else:
                    temp = temp.strip()
                tag_name = cm.unicodify(temp).lower()
                tag_type = 'category-1'
                tag_text = tag_name

                metadata_1 = copy.deepcopy(metadata_0)
                metadata_1['extra'][tag_type] = [tag_name]
                metadata_1['tags_mapping'][tag_type] = [{'name': tag_name, 'title': tag_text}]
                metadata_1['page_id'] = 0

                yield Request(url=href, meta={'userdata': metadata_1}, callback=self.parse_list, dont_filter=True)

    def parse_products(self, response):
        metadata = response.meta['userdata']
        for k in ('post_token', 'page_id'):
            if k in metadata:
                metadata.pop(k)
        sel = Selector(response)

        temp = sel.xpath('//div[@class="product-header"]//span[@class="page-product-title"]')
        if temp:
            collection = cm.unicodify(temp[0]._root.text)
            if collection:
                metadata['extra']['collection'] = [collection]
                metadata['tags_mapping']['collection'] = [{'name': collection, 'title': collection}]

        temp = sel.xpath(
            '//div[@class="commerce-product-sku"]/span[@itemprop="productID" and @class="commerce-product-sku-id"]')
        if temp:
            metadata['model'] = temp[0]._root.text.strip()

        temp = sel.xpath('//div[@class="product-aesthetics"]//span[@itemprop="description"]/p')
        metadata['description'] = '\n'.join(cm.unicodify(val._root.text) for val in temp if val._root.text)

        temp = sel.xpath('//div[@class="product-details"]//div[contains(@class,"field-item")]/p')
        metadata['details'] = '\n'.join(cm.unicodify(val._root.text) for val in temp if val._root.text)

        temp = sel.xpath('//div[@itemprop="offers"]//div[@itemprop="price" and @class="product-price"]')
        if temp:
            metadata['price'] = cm.unicodify(temp[0]._root.text)

        temp = sel.xpath('//div[@class="column-images"]//a[@href and contains(@class,"zoom-cursor")]')
        image_urls = [self.process_href(val._root.attrib['href'], metadata['region']) for val in temp]

        metadata['url'] = response._url
        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        return item


    def parse_list(self, response):
        metadata = response.meta['userdata']
        if metadata['page_id'] == 0:
            sel = Selector(response)
        else:
            try:
                text = json.loads(response._body)['cartierFoAjaxSearch']['data']
                sel = Selector(text=text)
            except (ValueError, KeyError, TypeError):
                # 解析错误，作为普通HTML对待
                sel = Selector(response)
                metadata['page_id'] = 0

        if sel.xpath('//div[@class="product-header"]//span[@class="page-product-title"]'):
        #     实际上是单品页面
            return self.parse_products(response)

        ret = []
        for node in sel.xpath('//div[contains(@class,"hover-info")]/a[@href]/div[@class="model-info"]'):
            m = copy.deepcopy(metadata)
            temp = node.xpath('./div[@class="model-name"]')
            if not temp:
                continue
            m['name'] = cm.unicodify(temp[0]._root.text)
            temp = node.xpath('./div[@class="model-description"]')
            if not temp:
                continue
            m['description'] = cm.unicodify(temp[0]._root.text)
            href = self.process_href(node.xpath('..')[0]._root.attrib['href'], metadata['region'])
            ret.append(Request(url=href, meta={'userdata': m}, callback=self.parse_products, dont_filter=True))

        if not ret:
            return None

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
            ret.append(Request(url=self.spider_data['data_urls'][m['region']] + post_token, method='POST',
                               body='&'.join(str.format('{0}={1}', k, body[k]) for k in body),
                               headers={'Content-Type': 'application/x-www-form-urlencoded',
                                        'X-Requested-With': 'XMLHttpRequest'},
                               callback=self.parse_list, meta={'userdata': m}, dont_filter=True))

        return ret



