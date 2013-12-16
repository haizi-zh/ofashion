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

        allow_filter = set(str.format('.+{0}.*', val) for val in domains)
        allow_productFilter = set(str.format('.+{0}.*/product/.+', val) for val in domains)

        self.rules = (
            Rule(SgmlLinkExtractor(allow=allow_productFilter,
                                   allow_domains=['hm.com']),
                 callback=self.parse_product),
            Rule(SgmlLinkExtractor(allow=allow_filter,
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
        type_nodes = sel.xpath('//ul[@class="breadcrumbs"]//li')
        category_index = 0
        for node in type_nodes:
            type_node = node.xpath('./a')
            if not type_node:
                continue

            type_text = type_node.xpath('./text()').extract()[0]
            type_text = self.reformat(type_text)
            type_name = type_text.lower()
            if type_text and type_name:
                category_type = str.format('category={0}', category_index)
                metadata['tags_mapping'][category_type] = [
                    {'name': type_name, 'title': type_text}
                ]
                category_index+=1

                gender = common.guess_gender(type_name)
                if gender:
                    metadata['gender'] = [gender]

        #价格标签
        price_node = sel.xpath('//span[@id="text-price"]//span')
        if price_node:
            price = price_node.xpath('./text()').extract()[0]
            price = self.reformat(price)
            if price:
                metadata['price'] = price

        #单品名称
        name_node = sel.xpath('//h1')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)
            if name:
                metadata['name'] = name

        #详情标签
        description_node = sel.xpath('//div[@class="description"]')
        if description_node:
            description_text_node = description_node.xpath('.//p[1]')
            if description_text_node:
                description = description_text_node.xpath('./text()').extract()[0]
                description = self.reformat(description)
                if description:
                    metadata['description'] = description

            detailText_node = description_node.xpath('.//*[preceding::h2[2]]')
            if detailText_node:
                detail = ''.join(detailText_node.xpath('./text()').extract())
                detail = self.reformat(detail)
                if detail:
                    metadata['details'] = detail

        #颜色标签，获取各种颜色的图片
        color_nodes = sel.xpath('//*[@id="options-articles"]//li')
        for node in color_nodes:
            color_node = node.xpath('.//span')
            if color_node:
                tmp = color_node.xpath('./text()').extract()
                if not tmp:
                    continue
                color_text = self.reformat(tmp[0])
                if color_text:
                    metadata['color'] += [color_text]

            color_image_node = node.xpath('.//a')
            if color_image_node:
                color_image_href = color_image_node.xpath('./@href').extract()[0]
                color_image_href = re.sub(ur'\?.+', color_image_href, response.url)

                m = copy.deepcopy(metadata)

                yield Request(url=color_image_href,
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
        image_nodes = sel.xpath('//div[@class="thumbs"]//img')
        for node in image_nodes:
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

