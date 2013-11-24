# coding=utf-8
import copy
import os
import re
from scrapy import log
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider

__author__ = 'Zephyre'


class FendiSpider(MFashionSpider):
    spider_data = {'home_urls': {'cn': 'http://www.fendi.com/cn/zh/collections/woman',
                                 'us': 'http://www.fendi.com/us/en/collections/woman',
                                 'fr': 'http://www.fendi.com/fr/fr/collections/femme',
                                 'it': 'http://www.fendi.com/it/it/collezioni/donna',
                                 'kr': 'http://www.fendi.com/kr/ko/collections/woman',
                                 'jp': 'http://www.fendi.com/jp/ja/collections/woman',
                                 'ii': 'http://www.fendi.com/ii/en/collections/woman',
                                 'es': 'http://www.fendi.com/ii/es/colecciones/mujer'},
                   'brand_id': 10135}
    spider_data['hosts'] = {k: 'http://www.fendi.com' for k in spider_data['home_urls'].keys()}

    @classmethod
    def get_supported_regions(cls):
        return FendiSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        super(FendiSpider, self).__init__('fendi', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    # def start_requests(self):
    #     entry = {'brand_id': 10135, 'extra': {}, 'region': 'cn'}
    #
    #     yield Request(
    #         url='http://www.fendi.com/cn/zh/collections/woman/fall-winter-2013-14/accessories/watches/for903-ggz-qel',
    #         callback=self.parse_details, meta={'userdata': entry})

    def parse(self, response):
        metadata = response.meta['userdata']
        if 'www.fendi.com/cn' in response.url:
            metadata['region'] = 'cn'
        elif 'www.fendi.com/us' in response.url:
            metadata['region'] = 'us'
        elif 'www.fendi.com/fr' in response.url:
            metadata['region'] = 'fr'
        elif 'www.fendi.com/it' in response.url:
            metadata['region'] = 'it'
        elif 'www.fendi.com/kr' in response.url:
            metadata['region'] = 'kr'
        elif 'www.fendi.com/jp' in response.url:
            metadata['region'] = 'jp'
        elif 'www.fendi.com/ii/en' in response.url:
            metadata['region'] = 'ii'
        elif 'www.fendi.com/ii/es' in response.url:
            metadata['region'] = 'es'
        else:
            return
        metadata['extra'] = {}

        sel = Selector(response)
        for item in sel.xpath("//header[@id='main-header']//ul[@class='links']/li/ul/li/a[@href]"):
            href = cm.unicodify(item._root.attrib['href'])
            title = cm.unicodify(item._root.text)

            if not title:
                continue
            temp = re.search(ur'/([^/]+)/?$', href)
            if not temp:
                continue
            cat = temp.group(1).lower()
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-1'] = [{'name': cat, 'title': title}]
            if cat in {'woman', 'women', 'femme', 'donna', 'mujer'}:
                m['gender'] = [u'female']
            elif cat in {'man', 'men', 'homme', 'uomo', 'hombre'}:
                m['gender'] = [u'male']
            url = self.process_href(href, metadata['region'])
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_1, dont_filter=True)


    def parse_category_1(self, response):
        self.log(unicode.format(u'PARSE_CAT_1: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        sel = Selector(response)
        for item in sel.xpath("//div[@id='page']//ul[@class='links']//li/a[@href]"):
            href = re.sub(ur'/cover/?', u'', cm.unicodify(item._root.attrib['href']))
            title = cm.unicodify(item._root.text)
            if not title:
                continue
            m = re.search(ur'/([^/]+)/?$', href)
            if not m:
                continue
            cat = m.group(1).lower()
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-2'] = [{'name': cat, 'title': title}]
            m['category'] = [cat]
            url = self.process_href(href, metadata['region'])
            yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_2)

    def parse_category_2(self, response):
        self.log(unicode.format(u'PARSE_CAT_2: URL={0}', response.url), level=log.DEBUG)
        metadata = response.meta['userdata']
        sel = Selector(response)

        # 是否有filter？
        ret = sel.xpath("//aside[@class='sidebar-actions']//div[@class='filter']//ul/li/a[@href]")
        if len(ret) > 0 and 'filter' not in metadata['extra']:
            for item in ret:
                href = cm.unicodify(item._root.attrib['href'])
                title = cm.unicodify(item._root.text)
                if not title:
                    continue
                m = re.search(ur'/([^/]+)/?$', href)
                if not m:
                    continue
                cat = m.group(1).lower().strip()
                if cat.lower() == u'all':
                    continue
                m = copy.deepcopy(metadata)
                m['extra']['filter'] = [cat]
                m['tags_mapping']['category-3'] = [{'name': cat, 'title': title}]
                url = self.process_href(href, metadata['region'])
                yield Request(url=url, meta={'userdata': m}, callback=self.parse_category_2)
        else:
            for item in sel.xpath(
                    "//div[@id='page']/div[@class='view-all']/ul[@id='slider']/li/a[@href and @data-id]"):
                href = cm.unicodify(item._root.attrib['href'])
                m = copy.deepcopy(metadata)
                url = self.process_href(href, metadata['region'])
                yield Request(url=url, meta={'userdata': m}, callback=self.parse_details)

    def parse_details(self, response):
        metadata = copy.deepcopy(response.meta['userdata'])

        # url必须满足一定的模式：http://www.fendi.com/it/it/collezioni/***
        if os.path.split(self.spider_data['home_urls'][metadata['region']])[0] not in response.url:
            return

        # 组合类页面也必须去掉，比如：http://www.fendi.com/cn/zh/collections/kids/junior/boys/look73
        if re.search(r'/look\d+$', response.url) or re.search(r'/\d+-baby$', response.url):
            return

        sel = Selector(response)
        ret = sel.xpath("//aside[@class='sidebar-actions']//div[@class='price']")
        if len(ret) > 0:
            temp = cm.unicodify(ret[0]._root.text)
            metadata['price'] = temp.strip() if temp else None
        ret = sel.xpath("//aside[@class='sidebar-actions']//div[@class='desc']")
        if len(ret) > 0:
            temp = cm.unicodify(ret[0]._root.text)
            metadata['description'] = temp.strip() if temp else None
        m = re.search(ur'/([^/]+)/?$', response.url)
        if m:
            metadata['model'] = cm.unicodify(m.group(1))
        else:
            return
        metadata['url'] = response.url

        # if 'price' not in metadata:
        #     return None

        item = ProductItem()
        item['image_urls'] = []
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        metadata.pop('extra')
        item['metadata'] = metadata

        ret = sel.xpath("//div[@id='page']/div[@class='fullscreen-image']/a[@href]")
        if ret > 0:
            # 小尺寸版本的图片，以供不时之需
            node = ret[0]
            tmp = node.xpath('./img[@src]')
            if tmp:
                href = tmp[0]._root.attrib['src']
                large_imgs = sorted((val for val in tmp[0]._root.attrib if re.match(r'data-src\d+', val)),
                                    key=lambda val: int(re.search(r'data-src(\d+)', val).group(1)), reverse=True)
                if large_imgs:
                    href = tmp[0]._root.attrib[large_imgs[0]]

                item['image_urls'] = [self.process_href(href, metadata['region'])]

            href = cm.unicodify(node._root.attrib['href'])
            url = self.process_href(href, metadata['region'])
            return Request(url=url, meta={'userdata': metadata, 'item': item}, callback=self.parse_image,
                           dont_filter=True)
        else:
        #     没有图片
            return item

    def parse_image(self, response):
        self.log(unicode.format(u'PARSE_IMAGE: URL={0}', response.url), level=log.DEBUG)
        item = response.meta['item']
        sel = Selector(response)
        image_urls = []
        for node in sel.xpath("//div[@id='zoom']/ul[@class='thumbs']/li"):
            temp = {}
            for lv in node.xpath("./ul[@class='levels']/li/a[@href]"):
                ret = lv.xpath("./span")
                if len(ret) == 0:
                    continue
                else:
                    ret = ret[0]
                lv_val = ret._root.text
                if not lv_val:
                    continue
                url = lv._root.attrib['href']
                temp[lv_val] = url
            image_urls.append(temp[max(temp.keys())])

        # 如果成功获得图像列表，则替代之前的小尺寸版本
        if image_urls:
            item['image_urls'] = image_urls
        return item
