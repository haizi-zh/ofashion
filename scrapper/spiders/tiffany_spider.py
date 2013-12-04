# coding=utf-8
import copy
import re
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils import unicodify

__author__ = 'Zephyre'


class TiffanySpider(MFashionSpider):
    spider_data = {'hosts': {'us': 'http://www.tiffany.com',
                             'ca': 'http://www.tiffany.ca',
                             'cn': 'http://www.tiffany.cn',
                             'mx': 'http://www.tiffany.com.mx',
                             'br': 'http://www.tiffany.com.br',
                             'jp': 'http://www.tiffany.co.jp',
                             'hk': 'http://zh.tiffany.com',
                             'kr': 'http://www.tiffany.kr',
                             'au': 'http://www.tiffany.com.au',
                             'uk': 'http://www.tiffany.co.uk',
                             'be': 'http://www.tiffany.be',
                             'de': 'http://www.tiffany.de',
                             'ie': 'http://www.tiffany.ie',
                             'it': 'http://www.tiffany.it',
                             'nl': 'http://nl.tiffany.com',
                             'es': 'http://www.tiffany.es',
                             'fr': 'http://www.tiffany.fr',
                             'ii': 'http://international.tiffany.com',
                             'at': 'http://www.tiffany.at'},
                   'image_url': 'http://media.tiffany.com/is/image/Tiffany/{0}?scl={1}&fmt=jpg',
                   'image_url2': 'http://media.tiffany.com/is/image/Tiffany/{0}?$EngagementItemXL$',
                   'brand_id': 10350}

    @classmethod
    def get_supported_regions(cls):
        return TiffanySpider.spider_data['hosts'].keys()

    def __init__(self, region):
        self.spider_data['home_urls'] = {k: str.format('{0}/Shopping', self.spider_data['hosts'][k]) for k in
                                         self.spider_data['hosts']}
        super(TiffanySpider, self).__init__('tiffany', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_map = {unicodify(val._root.text).lower(): val.xpath('..')[0] for val in sel.xpath(
            '//div[@id="nav"]/div[@id="flydown"]//div[@class="flydown-item"]/div[@class="links"]/h2[@class="t4"]')}

        for node in sel.xpath('//div[@id="nav"]/div[@class="flydowns l1"]/a[@href]'):
            cat = unicodify(node._root.text)
            if not cat:
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-0'] = [{'name': cat.lower(), 'title': cat}]
            m1['category'] = [cat.lower()]

            if cat.lower() not in node_map:
                continue

            for node2 in node_map[cat.lower()].xpath('./div[@class="l6"]/a[@href]'):
                cat = unicodify(node2._root.text)
                if not cat:
                    continue
                m2 = copy.deepcopy(m1)
                m2['tags_mapping']['category-1'] = [{'name': cat.lower(), 'title': cat}]
                yield Request(url=self.process_href(node2._root.attrib['href'], response.url),
                              callback=self.parse_cat, errback=self.onerr, meta={'userdata': m2})

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//noscript/ul/li/a[@href]'):
            m = copy.deepcopy(metadata)
            temp = unicodify(node._root.text)
            if temp:
                m['name'] = cm.reformat_addr(temp)
            yield Request(url=self.process_href(node._root.attrib['href'], response.url), callback=self.parse_details,
                          errback=self.onerr, meta={'userdata': m}, dont_filter=True)

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url
        mt = re.search(r'sku=([^&]+)', metadata['url'], flags=re.I)
        if mt:
            metadata['model'] = mt.group(1)
        else:
            return None

        if 'name' not in metadata or not metadata['name']:
            temp = sel.xpath('//div[@class="item-container"]/div[@class="iteminfo"]/h1[@class="t1"]')
            if temp:
                metadata['name'] = unicodify(temp[0]._root.text)

        temp = sel.xpath('//div[@class="item-container"]/div[@class="iteminfo"]//div[contains(@class,"item-desc")]')
        if temp:
            metadata['description'] = unicodify(temp[0]._root.text)

        temp = sel.xpath('//div[@class="item-container"]/div[@class="iteminfo"]//div[@class="l4" or @class="t8"]')
        if temp:
            metadata['price'] = unicodify(temp[0]._root.text)

        re.search(r'"BaseImg"\s*:\s*"([^"]+)"', response.body)

        item = ProductItem()
        image_urls = []
        for base_img in re.findall(r'"BaseImg"\s*:\s*"([^"]+)"', response.body):
            for i in xrange(1, 7):
                image_urls.append(str.format(self.spider_data['image_url'], base_img, i))
            image_urls.append(str.format(self.spider_data['image_url2'], base_img))
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        return item





