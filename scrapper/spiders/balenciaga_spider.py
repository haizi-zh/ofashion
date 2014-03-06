# coding=utf-8
import copy
import re
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
from utils.utils import process_price, unicodify

__author__ = 'Zephyre'


class BalenciagaSpider(MFashionSpider):
    spider_data = {'hosts': {},
                   'home_urls': {},
                   'currency': {'au': 'USD', 'ca': 'USD', 'hk': 'USD', 'mo': 'USD', 'nz': 'USD', 'kr': 'USD',
                                'tw': 'USD', 'tm': 'USD'},
                   'brand_id': 10029}


    @classmethod
    def get_supported_regions(cls):
        return BalenciagaSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        region_list = (
            'us', 'cn', 'tw', 'mo', 'es', 'gb', 'at', 'fr', 'it', 'de', 'au', 'be', 'ca', 'ie', 'jp', 'kr', 'uk')
        self.spider_data['hosts'] = {k: 'http://www.balenciaga.com' for k in region_list}
        self.spider_data['home_urls'] = {k: str.format('http://www.balenciaga.com/{0}', 'gb' if k == 'uk' else k) for k
                                         in region_list}
        super(BalenciagaSpider, self).__init__('balenciaga', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def parse(self, response):
        metadata = response.meta['userdata']
        for node in Selector(response).xpath('//nav[@id="mainMenu"]/ul[@class="categories"]/'
                                             'li[contains(@class,"firstLevel") and not(contains(@class,"experience"))]/'
                                             'a[@href]'):
            m1 = copy.deepcopy(metadata)
            tag_type = 'category-0'
            tag_name = unicodify(node._root.text)
            if not tag_name:
                continue
            m1['tags_mapping'][tag_type] = [{'name': tag_name.lower(), 'title': tag_name}]
            m1['category'] = [tag_name.lower()]

            for node2 in node.xpath('../div/div[@class="col"]/ul/li/a[@href]'):
                m2 = copy.deepcopy(m1)
                tag_type = 'category-1'
                tag_name = unicodify(node2._root.text)
                if not tag_name:
                    continue
                m2['tags_mapping'][tag_type] = [{'name': tag_name.lower(), 'title': tag_name}]

                url = self.process_href(node2._root.attrib['href'], response.url)
                if re.search(r'/view-all[^/]+$', url):
                    continue
                yield Request(url=url, meta={'userdata': m2}, callback=self.parse_list, errback=self.onerr)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        image_urls = []
        for node in sel.xpath('//ul[@id="zoomAlternateImageList"]/li/div[@class="inner"]/img[@src]'):
            href = node._root.attrib['src']
            pattern = re.compile(r'_(\d+)([a-z]_[a-z]\.[^\./]+)$')
            mt = pattern.search(href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            for i in xrange(start_idx, 15):
                image_urls.append(pattern.sub(str.format(r'_{0}\2', i), href))

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

    def parse_list(self, response):
        metadata = response.meta['userdata']
        for node in Selector(response).xpath('//ul[contains(@class,"productsContainer")]/li[@data-position]/a[@href]'):
            color_nodes = node.xpath('..//div[@class="description"]//div[@class="colorLink"]/a[@href]/img[@title]')
            if not color_nodes:
                # 没有色彩信息
                yield Request(url=self.process_href(node._root.attrib['href'], response.url),
                              meta={'userdata': copy.deepcopy(metadata)}, dont_filter=True,
                              callback=self.parse_details, errback=self.onerr)
            else:
                # 枚举色彩
                for node2 in color_nodes:
                    tmp = unicodify(node2._root.attrib['title'])
                    if not tmp:
                        continue
                    m = copy.deepcopy(metadata)
                    # m['color'] = [tmp.lower()]
                    yield Request(url=self.process_href(node2.xpath('..')[0]._root.attrib['href'], response.url),
                                  meta={'userdata': m}, callback=self.parse_details, errback=self.onerr,
                                  dont_filter=True)

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        try:
            mt = re.search(r'_cod(\d+[a-zA-Z]{2})\.[^\.]+$', response.url)
            if mt:
                model = mt.group(1).upper()
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            tmp = sel.xpath('//div[@id="itemPrice"]//span[@class="priceValue"]')
            if tmp:
                val = unicodify(tmp[0]._root.text)
                if not val:
                    try:
                        val = unicodify(tmp[0]._root.iterdescendants().next().tail)
                    except StopIteration:
                        pass
                    val = cls.reformat(val)
                if val:
                    old_price = val
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
        try:
            tmp = sel.xpath('//div[@id="itemInfo"]/h1')
            if tmp:
                node = tmp[0]
                tmp = unicodify(node._root.text)
                name = cls.reformat(tmp) if tmp else None
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        try:
            tmp = sel.xpath('//div[@id="itemInfo"]/h2')
            if tmp:
                node = tmp[0]
                tmp = unicodify(node._root.text)
                description = cls.reformat(tmp) if tmp else None
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        details = None
        try:
            detail = []
            tmp = sel.xpath('//div[@id="description_pane"]/div[@class="itemDesc"]')
            if tmp:
                tmp1 = tmp[0].xpath('./span[@class="itemPropertyKey"]')
                item_key = unicodify(tmp1[0]._root.text) if tmp1 else None
                tmp1 = tmp[0].xpath('./span[@class="itemPropertyValue"]')
                item_val = unicodify(tmp1[0]._root.text) if tmp1 else None
                detail.append(' '.join(filter(lambda val: val, [item_key, item_val])))

            for node in sel.xpath('//div[@id="description_pane"]/div[@class="details"]/div'):
                tmp1 = node.xpath('./span[@class="itemPropertyKey"]')
                item_key = unicodify(tmp1[0]._root.text) if tmp1 else None
                tmp1 = node.xpath('./span[@class="itemPropertyValue"]')
                item_val = unicodify(tmp1[0]._root.text) if tmp1 else None
                detail.append(' '.join(filter(lambda val: val, [item_key, item_val])))

            if detail:
                details = '\r'.join(detail)
        except(TypeError, IndexError):
            pass

        return details

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        try:
            tmp = sel.xpath('//div[@id="itemInfo"]/h1/span[@class="modelColor"]')
            tmp = tmp and unicodify(tmp[0]._root.text)
            if tmp:
                colors = [cls.reformat(tmp).lower()]
        except(TypeError, IndexError):
            pass

        return colors
