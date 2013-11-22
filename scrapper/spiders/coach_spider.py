# coding=utf-8
import json
import re
from scrapy import log
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import Rule
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider

__author__ = 'Zephyre'


class CoachSpider(MFashionSpider):
    spider_data = {'hosts': {'cn': 'http://china.coach.com'},
                   'home_urls': {},
                   'domains': {'cn': 'china.coach.com'},
                   'price_url': {'cn': 'http://china.coach.com/loadSkuDynamicInfo.json'},
                   'desc_url': {'cn': 'http://china.coach.com/getSkuDesInfo.json'},
                   'image_url': {'cn': 'http://china.coach.com/getImagesInfo.json'},
                   # 'currency': {'au': 'USD', 'ca': 'USD', 'hk': 'USD', 'mo': 'USD', 'nz': 'USD', 'kr': 'USD',
                   #              'tw': 'USD', 'tm': 'USD'},
                   'brand_id': 10093}


    @classmethod
    def get_supported_regions(cls):
        return CoachSpider.spider_data['hosts'].keys()

    def __init__(self, region):
        self.spider_data['home_urls'] = self.spider_data['hosts']

        if not region:
            region_list = self.get_supported_regions()
        elif cm.iterable(region):
            region_list = region
        else:
            region_list = [region]
        self.region = region
        self.allowed_domains = [self.spider_data['domains'][val] for val in region_list]

        super(CoachSpider, self).__init__('balenciaga', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def get_host_url(self, region):
        return self.spider_data['hosts'][region]

    def start_requests(self):
        self.rules = (
            Rule(SgmlLinkExtractor(allow=(r'detail\.htm\?[^/]+$',)), callback=self.parse_details),
            Rule(SgmlLinkExtractor(allow=(r'.+', )))
        )
        self._compile_rules()

        for region in self.region_list:
            if region not in self.get_supported_regions():
                self.log(str.format('No data for {0}', region), log.WARNING)
                continue

            yield Request(url=self.spider_data['home_urls'][region])

    def onerr(self, reason):
        url_main = None
        response = reason.value.response
        url = response.url

        next_func = None
        if 'next_func' in reason.request.meta:
            next_func = reason.request.meta['next_func']

        temp = reason.request.meta
        if 'userdata' in temp:
            metadata = temp['userdata']
            if 'url' in metadata:
                url_main = metadata['url']
        if url_main and url_main != url:
            msg = str.format('ERROR ON PROCESSING {0}, REFERER: {1}, CODE: {2}', url, url_main, response.status)
        else:
            msg = str.format('ERROR ON PROCESSING {1}, CODE: {0}', response.status, url)
        self.log(msg, log.ERROR)

        if next_func:
            # 举例：如果获得价格信息失败，并不影响爬虫继续前进，去获得其它信息
            for req in next_func(reason.value.response, prev_success=False):
                yield req

    def parse_details(self, response):
        metadata = {'region': self.region, 'brand_id': self.spider_data['brand_id'],
                    'tags_mapping': {}, 'category': []}

        sel = Selector(response)

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="styleCode" and @value]')
        if tmp:
            metadata['model'] = tmp[0]._root.attrib['value']
        if 'model' not in metadata or not metadata['model']:
            return

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="skuCode" and @value]')
        sku_code = None
        if tmp:
            sku_code = tmp[0]._root.attrib['value']
        if not sku_code:
            return

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="shareUrl" and @value]')
        if tmp:
            metadata['url'] = self.process_href(tmp[0]._root.attrib['value'], self.region)
        else:
            metadata['url'] = response.url

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="title" and @value]')
        if tmp:
            metadata['name'] = cm.unicodify(tmp[0]._root.attrib['value'])

        # 价格信息
        yield Request(url=self.spider_data['price_url'][self.region], method='POST', dont_filter=True,
                      body=str.format('skuCode={0}', sku_code), callback=self.get_info, errback=self.onerr,
                      headers={'Content-Type': 'application/x-www-form-urlencoded',
                               'Accept-Encoding': 'gzip,deflate,sdch',
                               'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                      meta={'userdata': metadata, 'next_func': self.get_info})

    def get_info(self, response, prev_success=True):
        metadata = response.meta['userdata']
        if prev_success:
            try:
                data = json.loads(response.body)
                metadata['price'] = data['skuPrice'] or data['retailPrice']
            except (ValueError, KeyError):
                pass

        # 说明信息
        yield Request(url=self.spider_data['desc_url'][self.region], method='POST', dont_filter=True,
                      body=str.format('styleCode={0}', metadata['model']), callback=self.get_images,
                      headers={'Content-Type': 'application/x-www-form-urlencoded',
                               'Accept-Encoding': 'gzip,deflate,sdch',
                               'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                      errback=self.onerr, meta={'userdata': metadata, 'next_func': self.get_images})

    def get_images(self, response, prev_success=True):
        metadata = response.meta['userdata']
        if prev_success:
            try:
                data = json.loads(response.body)
                metadata['description'] = self.reformat(
                    re.sub(ur'<\s*li\s*/?>', u'\r', data['description'][0]['description']))
                metadata['details'] = self.reformat(re.sub(ur'<\s*li\s*/?>', u'\r', data['description'][0]['detail']))
            except (ValueError, IndexError, KeyError):
                pass

        # 说明信息
        yield Request(url=self.spider_data['image_url'][self.region], method='POST', dont_filter=True,
                      body=str.format('styleCode={0}', metadata['model']), callback=self.parse_final,
                      headers={'Content-Type': 'application/x-www-form-urlencoded',
                               'Accept-Encoding': 'gzip,deflate,sdch',
                               'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                      errback=self.onerr, meta={'userdata': metadata, 'next_func': self.parse_final})

    def parse_final(self, response, prev_success=True):
        metadata = response.meta['userdata']
        image_urls = []
        if prev_success:
            try:
                data = json.loads(response.body)
                for k in (val for val in data if re.search('[a-z]Images', val)):
                    image_urls.extend(data[k])
            except (ValueError, IndexError, KeyError):
                pass

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

