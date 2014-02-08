# coding=utf-8
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
from utils.utils import unicodify


__author__ = 'Zephyre'


class ChopardSpider(MFashionSpider):
    spider_data = {'brand_id': 10080,
                   'home_urls': {'cn': 'http://www.chopard.cn',
                                 'us': 'http://us.chopard.com',
                                 'es': 'http://www.chopard.es',
                                 'de': 'http://www.chopard.de',
                                 'fr': 'http://www.chopard.fr',
                                 'it': 'http://www.chopard.it'}}
    spider_data['hosts'] = spider_data['home_urls']

    @classmethod
    def get_supported_regions(cls):
        return ChopardSpider.spider_data['hosts'].keys()

    @classmethod
    def get_instance(cls, region_list=None):
        return cls(region_list)

    def __init__(self, region_list):
        super(ChopardSpider, self).__init__('chopard', region_list)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@id="nav"]/li/a[@href]'):
            m = copy.deepcopy(metadata)
            tag_text = ', '.join(val for val in (self.reformat(unicodify(val.text)) for val in
                                                 node._root.iterdescendants()) if val)
            m['tags_mapping']['category-0'] = [{'name': tag_text.lower(), 'title': tag_text}]
            m['category'] = [tag_text.lower()]
            yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                          meta={'userdata': m}, callback=self.parse_cat1, errback=self.onerr, dont_filter=True)

    def parse_cat1(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//ul[@id="nav"]/li//ul/li/a[@href]')
        for node in node_list:
            m = copy.deepcopy(metadata)
            tag_text = ', '.join(val for val in (self.reformat(unicodify(val.text)) for val in
                                                 node._root.iterdescendants()) if val)
            m['tags_mapping']['category-1'] = [{'name': tag_text.lower(), 'title': tag_text}]
            yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                          meta={'userdata': m}, callback=self.parse_cat2, errback=self.onerr, dont_filter=True)

        if not node_list:
            for val in self.parse_list(response):
                yield val

    def parse_cat2(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//ul[@id="nav"]/li//ul/li//ul[@class="drilldown-list"]/li/a[@href]')
        for node in node_list:
            m = copy.deepcopy(metadata)
            tag_text = ', '.join(val for val in (self.reformat(unicodify(val.text)) for val in
                                                 node._root.iterdescendants()) if val)
            m['tags_mapping']['category-2'] = [{'name': tag_text.lower(), 'title': tag_text}]
            yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                          meta={'userdata': m}, callback=self.parse_list, errback=self.onerr, dont_filter=True)

        if not node_list:
            for val in self.parse_list(response):
                yield val

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@class="products-grid"]/li[contains(@class,"item")]/a[@href]'):
            m = copy.deepcopy(metadata)
            # tmp = node.xpath('../*[@class="product-name"]')
            # if not tmp:
            #     m['name'] = self.reformat(cm.unicodify(tmp[0]._root.text))
            yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                          meta={'userdata': m}, callback=self.parse_details, errback=self.onerr, dont_filter=True)

        for node in sel.xpath('//div[contains(@class,"home-widget")]/div[contains(@class,"widget-content")]/a[@href]'):
            url = self.process_href(node._root.attrib['href'], response.url)
            if url:
                yield Request(url=url, meta={'userdata': copy.deepcopy(metadata)}, callback=self.parse_details,
                              errback=self.onerr)

        for node in sel.xpath(
                '//div[contains(@class,"home-widget")]/div[contains(@class,"widget-content")]/ul/li/a[@href]'):
            url = self.process_href(node._root.attrib['href'], response.url)
            if url:
                yield Request(url=url, meta={'userdata': copy.deepcopy(metadata)}, callback=self.parse_details,
                              errback=self.onerr)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        tmp = sel.xpath('//div[@class="product-main-info"]/div[@class="product-name"]')
        if tmp:
            metadata['name'] = ', '.join(val for val in (self.reformat(unicodify(val.text)) for val in
                                                         tmp[0]._root.iterdescendants()) if val)

        # TODO 这里需要注意一个网页：http://www.chopard.fr/fiancailles/bijoux-mariage/pendentifs/an-elegant-diamond-pendant-810374-1001
        tmp = sel.xpath(
            '//div[@class="product-essential"]//div[contains(@class,"description-content")]//div[@class="std"]')
        if tmp:
            metadata['description'] = self.reformat(unicodify(tmp[0]._root.text))

        tmp = sel.xpath('//div[@id="features-content"]/*[contains(@class,"feature-value") and contains(@class,"ref")]')
        if not tmp:
            return
        metadata['model'] = unicodify(tmp[0]._root.text)
        if not metadata['model']:
            return
        metadata['url'] = response.url

        tmp = sel.xpath('//div[@class="product-shop"]//div[contains(@class,"price-box")]//span[@class="price"]')
        if tmp:
            metadata['price'] = self.reformat(unicodify(tmp[0]._root.text))

        def func(node):
            tmp = node.xpath('./*[@class="feature-title"]')
            if not tmp:
                return None
            title = self.reformat(unicodify(tmp[0]._root.text))
            tmp = node.xpath('./*[@class="feature-value"]')
            if not tmp:
                return None
            value = self.reformat(unicodify(tmp[0]._root.text))
            return ' '.join((title, value))

        metadata['details'] = '\r'.join(map(func, sel.xpath('//div[@id="features-content"]/div[@class="feature"]')))

        # image_processed = set([])
        image_candidates = []
        # # 找到各个图片的大小两个版本
        # for node in sel.xpath('//p[@class="product-image-zoom"]/a[@href]'):
        #     zoom_url = self.process_href(node._root.attrib['href'], metadata['region'])
        #     tmp = node.xpath('./img[@src]')
        #     small_url = self.process_href(tmp[0]._root.attrib['src'], metadata['region']) if tmp else None
        #
        #     hv = hashlib.md5(zoom_url).hexdigest()
        #     if hv not in image_processed:
        #         image_candidates.append({'zoom': zoom_url, 'small': small_url})
        #         image_processed.add(hv)

        for node in sel.xpath('//div[@class="other-products-carousel"]//ul/li/a[@rel and @data-zoomimg]'):
            small_url = self.process_href(node._root.attrib['data-zoomimg'], response.url)
            zoom_url = self.process_href(node._root.attrib['rel'], response.url)
            image_candidates.append({'zoom': zoom_url, 'small': small_url})

            # hv = hashlib.md5(zoom_url).hexdigest()
            # if hv not in image_processed:

            # image_processed.add(hv)

        response.meta['image_urls'] = []
        response.meta['image_candidates'] = image_candidates
        response.meta['skip-current'] = True
        for val in self.parse_image(response):
            yield val

    def parse_image(self, response):
        metadata = response.meta['userdata']
        image_urls = response.meta['image_urls']
        image_candidates = response.meta['image_candidates']

        if 'skip-current' not in response.meta:
            # 注意解析当前页面的放大版本图像
            sel = Selector(response)
            image_urls.pop()
            for node in sel.xpath('//p[@class="product-image-zoom"]/img[@src]'):
                image_urls.append(self.process_href(node._root.attrib['src'], response.url))

        if image_candidates:
            # 使用小版本图像。如果成功，则使用zoom版本将其替换。
            image_urls.append(image_candidates[0]['small'])
            zoom_url = image_candidates[0]['zoom']
            image_candidates = image_candidates[1:]
            yield Request(url=zoom_url,
                          meta={'userdata': metadata, 'image_urls': image_urls, 'image_candidates': image_candidates},
                          callback=self.parse_image, errback=self.image_err)
        else:
            item = ProductItem()
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            yield item

    def image_err(self, reason):
        metadata = reason.request.meta['userdata']
        image_urls = reason.request.meta['image_urls']
        image_candidates = reason.request.meta['image_candidates']

        if image_candidates:
            # 使用小版本图像。如果成功，则使用zoom版本将其替换。
            image_urls.append(image_candidates[0]['small'])
            zoom_url = image_candidates[0]['zoom']
            image_candidates = image_candidates[1:]
            yield Request(url=zoom_url,
                          meta={'userdata': metadata, 'image_urls': image_urls, 'image_candidates': image_candidates},
                          callback=self.parse_image, errback=self.image_err)
        else:
            item = ProductItem()
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            yield item
