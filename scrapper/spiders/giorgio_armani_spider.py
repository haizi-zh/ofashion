# coding=utf-8
import urlparse
import copy
import re

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class GiorgioArmaniSpider(MFashionSpider):
    spider_data = {'brand_id': 10149,
                   'home_urls': {'cn': 'http://www.armani.cn/cn/giorgioarmani/accessories_section'}}

    @classmethod
    def get_supported_regions(cls):
        return GiorgioArmaniSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(GiorgioArmaniSpider, self).__init__('giorgio_armani', region)

    def parse(self, response, metadata=None, current_node=None, level=0):
        if not metadata:
            metadata = response.meta['userdata']
        sel = Selector(response)
        if current_node:
            node_list = current_node.xpath('../ul/li/a[@href]')
        else:
            node_list = sel.xpath('//*[@id="sidebarMenu"]/ul/li[contains(@class,"selected")]/a[@href]')

        if node_list:
            for node1 in node_list:
                try:
                    tag_text = self.reformat(node1.xpath('text()').extract()[0])
                    tag_name = tag_text.lower()
                except (IndexError, TypeError):
                    continue
                m1 = copy.deepcopy(metadata)
                gender = cm.guess_gender(tag_text)
                if gender:
                    m1['gender'] = [gender]
                    new_level = level
                else:
                    m1['tags_mapping'][str.format('category-{0}', level)] = [{'name': tag_name, 'title': tag_text}]
                    new_level = level + 1
                for val in self.parse(response, m1, node1, new_level):
                    yield val

        else:
            prod_list = sel.xpath('//*[@id="elementsContainer"]')
            if prod_list:
                # 到达单品页面
                for val in self.parse_list(response, metadata):
                    yield val
            else:
                # 继续
                try:
                    url = self.process_href(current_node.xpath('@href').extract()[0], response.url)
                    yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': metadata})
                except (IndexError, TypeError):
                    pass

    def parse_list(self, response, metadata=None):
        if not metadata:
            metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="elementsContainer"]/div[contains(@class,"item")]/div[@class="itemDesc"]'):
            m = copy.deepcopy(metadata)
            tmp = node.xpath('./a[@href]/@href').extract()
            if not tmp:
                continue
            url = self.process_href(tmp[0], response.url)
            tmp = node.xpath('./a[@href]/*[@data-item-modelname]/text()').extract()
            if tmp:
                m['name'] = self.reformat(tmp[0])
            tmp = node.xpath('./div[@class="itemPrice"]/*[@data-pricewithoutpromotion]/text()').extract()
            if tmp:
                m['price'] = self.reformat(tmp[0])
            tmp = node.xpath('./div[@class="itemPrice"]/*[@data-price]/text()').extract()
            if tmp:
                m['price_discount'] = self.reformat(tmp[0])
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        tmp = sel.xpath(
            '//*[@class="descriptionContainer"]/*[@class="articleName"]/descendant-or-self::text()').extract()
        if not tmp:
            return
        metadata['model'] = self.reformat(tmp[-1])

        tmp = sel.xpath('//*[@class="descriptionContainer"]//ul[@class="Colors"]/li/a[@title]/@title').extract()
        if tmp:
            metadata['color'] = [self.reformat(val) for val in tmp]

        desc_terms = []
        tmp = sel.xpath('//*[@class="descriptionContainer"]/*[@class="attributes"]/text()').extract()
        if tmp:
            desc_terms.extend([self.reformat(val) for val in tmp])
        if desc_terms:
            metadata['description'] = '\r'.join(desc_terms)

        details_terms = []
        tmp = sel.xpath('//*[@class="descriptionContainer"]/ul[@class="tabs"]/li/div[@class="descriptionContent"]'
                        '/text()').extract()
        if tmp:
            details_terms.extend([self.reformat(val) for val in tmp[:1]])
        if details_terms:
            metadata['details'] = '\r'.join(details_terms)

        image_urls = []
        for href in sel.xpath('//*[@id="thumbsWrapper"]//div[@class="thumbElement"]/img[@src]/@src').extract():
            url = self.process_href(href, response.url)
            mt = re.search(r'(\d+)[_a-z]+\.[a-z]+$', url)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            image_urls.extend(
                re.sub(r'\d+([_a-z]+\.[a-z]+$)', str.format(r'{0}\1', idx), url) for idx in xrange(start_idx, 17))

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item














