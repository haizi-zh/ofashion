# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import Rule
from scrapy import log

import re
import common
import copy
from utils.utils import iterable


class HMSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10155,
        'home_urls': {
            k: str.format('http://www.hm.com/{0}', k if k != 'uk' else 'gb')
            for k in {
                # 'au', 'br',
                'cn', 'us', 'fr', 'uk', 'hk',
                'jp', 'it', 'ae', 'sg', 'de',
                'ca', 'es', 'ch', 'ru', 'th',
                'kr', 'my', 'nl',
            }
        },
    }

    allow_domains = ['hm.com']

    @classmethod
    def get_supported_regions(cls):
        return HMSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(HMSpider, self).__init__('H&M', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def start_requests(self):

        domains = list(str.format('hm\\.com/{0}', k if k != 'uk' else 'gb') for k in self.region_list)

        allowFilter = set(str.format('.+{0}.*', val) for val in domains)
        allowProductFilter = set(str.format('.+{0}.*/product/.+', val) for val in domains)

        self.rules = (
            Rule(SgmlLinkExtractor(allow=allowProductFilter,
                                   allow_domains=['hm.com']),
                 callback=self.parse_product),
            Rule(SgmlLinkExtractor(allow=allowFilter,
                                   deny=r'.+(#|\?).*N=.+',
                                   allow_domains=['hm.com']))
        )
        self._compile_rules()

        for reg in self.region_list:
            if reg not in self.get_supported_regions():
                self.log(str.format('No data for {0}', reg), log.WARNING)
                continue

            yield Request(url=HMSpider.spider_data['home_urls'][reg])

    def parse_product(self, response):
        sel = Selector(response)
        metadata = {
            'brand_id': self.spider_data['brand_id'],
            'url': response.url,
            'tags_mapping': {},
            'color': [],
        }

        #单品model
        model = None
        mt = re.search(r'.+/product/(\d+).*', response.url)
        if mt:
            model = mt.group(1)
        model = self.reformat(model)
        if model:
            metadata['model'] = model
        else:
            return

        #单品region
        region = None
        mt = re.search('.+com/(\w+)/.+', response.url)
        if mt:
            region = mt.group(1)
        #替换gb为uk
        if region == 'gb':
            region = 'uk'
        region = self.reformat(region)
        if region:
            metadata['region'] = region
        else:
            return

        #左上类型标签
        typeNodes = sel.xpath('//ul[@class="breadcrumbs"]//li')
        categoryIndex = 0
        for node in typeNodes:
            typeNode = node.xpath('./a')
            if not typeNode:
                continue

            type_text = typeNode.xpath('./text()').extract()[0]
            type_text = self.reformat(type_text)
            type_name = type_text.lower()
            if type_text and type_name:
                categoryType = str.format('category={0}', categoryIndex)
                metadata['tags_mapping'][categoryType] = [
                    {'name': type_name, 'title': type_text}
                ]
                categoryIndex+=1

                gender = common.guess_gender(type_name)
                if gender:
                    metadata['gender'] = [gender]

        #价格标签
        priceNode = sel.xpath('//span[@id="text-price"]//span')
        if priceNode:
            price = priceNode.xpath('./text()').extract()[0]
            price = self.reformat(price)
            if price:
                metadata['price'] = price

        #单品名称
        nameNode = sel.xpath('//h1')
        if nameNode:
            name = nameNode.xpath('./text()').extract()[0]
            name = self.reformat(name)
            if name:
                metadata['name'] = name

        #详情标签
        descriptionNode = sel.xpath('//div[@class="description"]')
        if descriptionNode:
            descriptionTextNode = descriptionNode.xpath('.//p[1]')
            if descriptionTextNode:
                description = descriptionTextNode.xpath('./text()').extract()[0]
                description = self.reformat(description)
                if description:
                    metadata['description'] = description

            detailTextNode = descriptionNode.xpath('.//*[preceding::h2[2]]')
            if detailTextNode:
                detail = ''.join(detailTextNode.xpath('./text()').extract())
                detail = self.reformat(detail)
                if detail:
                    metadata['details'] = detail

        #颜色标签，获取各种颜色的图片
        colorNodes = sel.xpath('//*[@id="options-articles"]//li')
        for node in colorNodes:
            colorNode = node.xpath('.//span')
            if colorNode:
                tmp = colorNode.xpath('./text()').extract()
                if not tmp:
                    continue
                color_text = self.reformat(tmp[0])
                if color_text:
                    metadata['color'] += [color_text]

            colorImageNode = node.xpath('.//a')
            if colorImageNode:
                colorImageHref = colorImageNode.xpath('./@href').extract()[0]
                colorImageHref = re.sub(ur'\?.+', colorImageHref, response.url)

                m = copy.deepcopy(metadata)

                yield Request(url=colorImageHref,
                              callback=self.parse_images,
                              errback=self.onerr,
                              meta={'userdata': m})

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        yield item

    def parse_images(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        #图片
        image_urls = []
        imageNodes = sel.xpath('//div[@class="thumbs"]//img')
        for node in imageNodes:
            href = node.xpath('./@src').extract()[0]
            href = re.sub(ur'thumb', 'full', href)
            href = self.process_href(href, response.url)

            if href:
                image_urls += [href]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        yield item

