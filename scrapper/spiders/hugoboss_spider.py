# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import Rule
from scrapy.http import Request
from scrapy.selector import Selector
from scrapy import log

import common
import copy
import re


class HogoBossSpider(MFashionSpider):

    region = ''

    spider_data = {
        'brand_id': 10169,
        'home_urls': {
            'cn': 'http://store.hugoboss.cn',
            'us': 'http://store-us.hugoboss.com',
            'fr': 'http://store-fr.hugoboss.com'
        }
    }

    @classmethod
    def get_supported_regions(cls):
        return HogoBossSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(HogoBossSpider, self).__init__('Hugo Boss', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def start_requests(self):
        self.rules = (
            Rule(SgmlLinkExtractor(allow=r'.+/product.+$|.+pd\..+$', allow_domains=['store.hugoboss.cn']),
                 callback=self.parse_product),
            Rule(SgmlLinkExtractor(allow=r'.+', allow_domains=['store.hugoboss.cn']))
        )
        self._compile_rules()

        for reg in self.region_list:
            if reg not in self.get_supported_regions():
                self.log(str.format('No data for {0}', reg), log.WARNING)
                continue

            self.region = reg

            yield Request(url=self.spider_data['home_urls'][reg])

    def parse_product(self, response):
        sel = Selector(response)
        metadata = {
            'brand_id': self.spider_data['brand_id'],
            'url': response.url,
            'tags_mapping': {},
            'color': []
        }

        '''
        单品model
        '''
        model = None
        mt = re.search(r'\+(\d+)_', response.url)
        if mt:
            model = mt.group(1)
        else:
            title = sel.xpath('//h2[@class="product-title"]/text()').extract()[0]
            mt = re.search(r'\b(\d+)\b', title)
            if not mt:
                return
            else:
                model = mt.group(1)
        metadata['model'] = model

        '''
        单品region
        '''
        #region = None
        #mt = re.search('-(\w*)\.|\.(\w{2})/', response.url)
        #for a in self.spider_data['home_urls'].keys():
        #    if a == mt:
        #        region = a
        #        break
        #if not region:
        #    return
        #metadata['region'] = region
        metadata['region'] = self.region

        '''
        左上类型标签
        '''
        typesNodes = sel.xpath('//ul[@class="container clearfix"]/li[not(contains(text(), "/"))]')[0:-1]
        categoryIndex = 0
        for node in typesNodes:

            '''
            前两个标签，字写在下属的a里边
            '''
            typeNode = node.xpath('./a')
            if not typeNode:
                typeNode = node

            type_text = typeNode.xpath('./text()').extract()[0]
            type_name = self.reformat(type_text).lower()
            categoryType = str.format('category-{0}', categoryIndex)
            metadata['tags_mapping'][categoryType] = [
                {'name': type_name, 'title': type_text}
            ]
            categoryIndex+=1

            '''
            顺便猜男女
            '''
            gender = common.guess_gender(type_name)
            if gender:
                metadata['gender'] = [gender]

        '''
        价格标签
        '''
        priceNode = sel.xpath('//dd[@class="saleprice"]')
        if priceNode:
            price = priceNode.xpath(ur'./text()').extract()[0]
            metadata['price'] = price

        '''
        单品名称
        '''
        nameNode = sel.xpath('//h1[@class="product-name"]')
        if nameNode:
            name = nameNode.xpath('./text()').extract()[0]
            name = self.reformat(name)
            metadata['name'] = name

        '''
        颜色标签
        '''
        colorNodes = sel.xpath('//dl[@class="product-colors"]//li')
        for node in colorNodes:
            color_text = node.xpath('.//a/@title').extract()[0]
            color_text = self.reformat(color_text)
            metadata['color'] += [color_text]

        '''
        描述标签
        '''
        descriptionNode = sel.xpath('//div[@class="tabpage description"]')
        if descriptionNode:
            desctiption = descriptionNode.xpath('./text()').extract()[0]
            desctiption = self.reformat(desctiption)
            metadata['description'] = desctiption

        '''
        详情标签
        '''
        detailNode = sel.xpath('//div[@class="tabpage inc"]')
        if detailNode:
            # TODO 详情不全，被<br>分开了
            detail = detailNode.xpath('./text()').extract()[0]
            detail = self.reformat(detail)
            metadata['details'] = detail

        '''
        材料及护理标签
        '''
        # TODO 这里该用什么标签名

        '''
        图片
        '''
        imageNodes = sel.xpath('//dl[@class="product-colors"]//li/a')
        for node in imageNodes:
            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            m = copy.deepcopy(metadata)

            yield Request(url=href,
                          callback=self.parse_images,
                          errback=self.onerr,
                          meta={'userdata': m})

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        yield item

    '''
    处理单品图片
    '''
    def parse_images(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        image_urls = []

        '''
        中国网站
        '''
        imageNodes = sel.xpath('//div[@id="gallery"]//a/img')
        for node in imageNodes:
            imageHref = node.xpath('./@big').extract()[0]
            image_urls += [imageHref]
        '''
        美国和法国网站(估计其他也是)
        '''
        imageNodes = sel.xpath('//img[@data-detailurl]')
        for node in imageNodes:
            imageHref = node.xpath('./@data-detailurl').extract()[0]
            image_urls += [imageHref]

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata

        yield item

    def onerr(self, reason):
        metadata = reason.request.meta

        if metadata:
            item = ProductItem()
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata

            yield item
        else:
            super(HogoBossSpider, self).onerr(reason)
