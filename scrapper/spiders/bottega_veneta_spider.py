# coding=utf-8
import re
from scrapy import log
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import Rule
from scrapy.http import Request
from scrapy.selector import Selector
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm

__author__ = 'Zephyre'


class BottegaSpider(MFashionSpider):
    supported_regions = {'cn', 'us', 'fr', 'uk', 'it'}
    spider_data = {'brand_id': 10049}

    allowed_domains = ['bottegaveneta.com']

    @classmethod
    def get_supported_regions(cls):
        return list(cls.supported_regions)

    def __init__(self, region):
        super(BottegaSpider, self).__init__('bottega', region)

        region_code = {k: k if k not in {'cn', 'uk'} else None for k in self.supported_regions}
        region_code['cn'] = 'wy'
        region_code['uk'] = 'gb'

        self.spider_data['region_code'] = region_code
        self.spider_data['home_urls'] = {k: str.format('http://www.bottegaveneta.com/{0}', region_code[k]) for k in
                                         region_code}
        self.rules = (Rule(SgmlLinkExtractor(allow=(r'_cod[\da-zA-Z]+\.html?$',)), callback='parse_details'),
                      Rule(SgmlLinkExtractor(allow=(r'.+', ),
                                             deny=(str.format(r'bottegaveneta\.com/(?!({0}))',
                                                              '|'.join(self.spider_data['region_code'][val]
                                                                       for val in self.region_list)),))))
        self._compile_rules()

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def start_requests(self):
        for region in self.region_list:
            if region not in self.get_supported_regions():
                self.log(str.format('No data for {0}', region), log.WARNING)
                continue

            yield Request(url=self.spider_data['home_urls'][region], meta={'userdata': region})

    def parse_details(self, response):
        # 猜测region
        mt = re.search(r'bottegaveneta\.com/([a-z]{2})', response.url)
        if not mt:
            return
        region = None
        for k, v in self.spider_data['region_code'].items():
            if v == mt.group(1):
                region = k
                break
        if not region:
            return

        metadata = {'region': region, 'brand_id': self.spider_data['brand_id'],
                    'tags_mapping': {}, 'category': []}
        sel = Selector(response)

        tmp = sel.xpath('//div[@id="modelFabricColor"]/span[@class="mfcvalue"]/text()').extract()
        if not tmp:
            return
        metadata['model'] = self.reformat(tmp[0])
        metadata['url'] = response.url

        tmp = sel.xpath('//div[@id="itemInfoBox"]//h1[@class="product-title"]/*[@class="infoProd" and @itemprop="name"]'
                        '/text()').extract()
        if tmp:
            metadata['name'] = self.reformat(tmp[0])

        tmp = sel.xpath('//div[@class="itemBoxPrice"]/span[@class="price"]/text()').extract()
        if tmp:
            metadata['price'] = self.reformat(tmp[0])

        desc_list = []
        for tmp in sel.xpath('//div[@id="panelsMfcContainer"]/ul[@id="panels"]/li[contains(@class,"panel")]'
                             '/*[contains(@class,"editorialdescription")]/text()').extract():
            tmp = self.reformat(tmp)
            if tmp:
                desc_list.append(tmp)
        if desc_list:
            metadata['description'] = '\r'.join(desc_list)

        details_list = []
        for node in sel.xpath(
                '//div[@id="panelsMfcContainer"]/ul[@id="panels"]/li[contains(@class,"panel")]/*[contains(@class,"details")]'):
            tmp = node.xpath('text()').extract()
            if tmp:
                tmp = self.reformat(tmp[0])
                if tmp:
                    details_list.append(tmp)

            # 再检查是否有子标签需要添加
            term = None
            for node3 in node.xpath('.//span[@class="property" or @class="value"]'):
                node_class = node3.xpath('@class').extract()[0].lower().strip()
                tmp = node3.xpath('text()').extract()
                if not tmp or not self.reformat(tmp[0]):
                    continue
                node_text = self.reformat(tmp[0])

                if node_class == 'property':
                    if term:
                        details_list.append(unicode.format(u'{0}{1}', term['property'], ','.join(term['value'])))
                    term = {'property': node_text, 'value': []}
                elif node_class == 'value':
                    if term:
                        term['value'].append(node_text)
            if term:
                details_list.append(unicode.format(u'{0}{1}', term['property'], ','.join(term['value'])))
        if details_list:
            metadata['details'] = '\r'.join(details_list)

        image_urls = []
        for href in sel.xpath('//div[@id="thumbsContainer"]/span[@id="thumbsContainerImg"]'
                              '/img[contains(@class,"product-thumb") and @src]/@src').extract():
            mt = re.search(r'_(\d)+_[a-zA-Z]{1,2}\.', href)
            if mt:
                for idx in xrange(14, 17):
                    new_href = re.sub(r'_\d+_([a-zA-Z]{1,2})\.', str.format(r'_{0}_\1.', idx), href)
                    image_urls.append(self.process_href(new_href, response.url))
            else:
                image_urls.append(self.process_href(href, response.url))

        category_level = 0
        for node in sel.xpath('//div[@class="breadContainer"]//a[@href]'):
            href = node.xpath('@href').extract()[0]
            tmp = node.xpath('text()').extract()
            if not tmp:
                continue
            tmp = self.reformat(tmp[0])
            if not tmp:
                continue
            tag_name = tmp.lower()
            tag_text = tmp
            if href in self.spider_data['home_urls'][metadata['region']]:
                # 这是主页
                continue
            gender = cm.guess_gender(tag_name)
            if gender:
                metadata['gender'] = [gender]
                continue
            metadata['tags_mapping'][str.format('category-{0}', category_level)] = \
                [{'name': tag_name, 'title': tag_text}]
            if category_level == 0:
                metadata['category'] = [tag_name]
            category_level += 1

        tmp = sel.xpath('//div[@class="breadContainer"]/span[@class="active"]/h2/text()').extract()
        if tmp:
            gender = cm.guess_gender(self.reformat(tmp[0]).lower())
            if gender:
                metadata['gender'] = [gender]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item




