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
from utils.utils import iterable


class HogoBossSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10169,
        'home_urls': {
            reg: str.format('http://store-{0}.hugoboss.com', reg)
            if reg != 'cn' else 'http://store.hugoboss.cn'
            for reg in {
                'cn', 'us', 'fr', 'uk', 'de',
                'it', 'es', 'ch','nl',
            }
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return HogoBossSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        if iterable(region):
            HogoBossSpider.spider_data['home_urls'] = {
                reg: str.format('http://store-{0}.hugoboss.com', reg)
                if reg != 'cn' else 'http://store.hugoboss.cn'
                for reg in region
            }
        else:
            k = region
            HogoBossSpider.spider_data['home_urls'] = {
                k: str.format('http://store-{0}.hugoboss.com', k)
                if k != 'cn' else 'http://store.hugoboss.cn'
            }

        super(HogoBossSpider, self).__init__('Hugo Boss', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def start_requests(self):

        domains = list(
            str.format('store-{0}.hugoboss.com', reg)
            if reg != 'cn' else 'store.hugoboss.cn'
            for reg in self.region_list
        )

        self.rules = (
            Rule(SgmlLinkExtractor(allow=r'.+/product.+$|.+pd\..+$',
                                   allow_domains=domains),
                 callback=self.parse_product),
            Rule(SgmlLinkExtractor(allow=r'.+',
                                   deny=r'.+(#|\?).*(pre(\w+)|filter)=.*',
                                   allow_domains=domains)),
        )
        self._compile_rules()

        for reg in self.region_list:
            if reg not in self.get_supported_regions():
                self.log(str.format('No data for {0}', reg), log.WARNING)
                continue

            yield Request(url=HogoBossSpider.spider_data['home_urls'][reg])

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
        mt = re.search(r'\+(\d+)_,', response.url)
        if mt:
            model = mt.group(1)
        else:
            try:
                title = sel.xpath('//*[@class="product-title" or @class="model"]/text()').extract()[0]
                mt = re.search(r'(\d{6,})', title)
                if not mt:
                    return
                else:
                    model = mt.group(1)
            except(TypeError, IndexError):
                return
        if model:
            metadata['model'] = model
        else:
            return

        '''
        单品region
        '''
        region = None
        mt = re.search('-(\w*)\.|\.(\w+)/', response.url)
        if mt.group(1):
            mt = mt.group(1)
        else:
            mt = mt.group(2)
        for a in HogoBossSpider.spider_data['home_urls'].keys():
            if a == mt:
                region = a
                break
        if not region:
            return
        metadata['region'] = region

        '''
        左上类型标签
        '''
        #中国
        typesNodes = sel.xpath('//ul[@class="container clearfix"]/li[not(contains(text(), "/"))]')[0:-1]
        #其他
        if not typesNodes:
            typesNodes = sel.xpath('//a[contains(@name, "breadcrump")]')
        categoryIndex = 0
        for node in typesNodes:

            '''
            中国，前两个标签，字写在下属的a里边
            '''
            typeNode = node.xpath('./a')
            if not typeNode:
                typeNode = node

            try:
                type_text = typeNode.xpath('./text()').extract()[0]
                if type_text:
                    type_name = self.reformat(type_text).lower()
                    categoryType = str.format('category-{0}', categoryIndex)
                    metadata['tags_mapping'][categoryType] = [
                        {'name': type_name, 'title': type_text}
                    ]
                    categoryIndex+=1
            except(TypeError, IndexError):
                continue

            '''
            顺便猜男女
            '''
            gender = common.guess_gender(type_name)
            if gender:
                metadata['gender'] = [gender]

        '''
        价格标签
        '''
        priceNode = sel.xpath('//*[@class="saleprice" or @class="salesprice"]')
        if priceNode:
            try:
                price = priceNode.xpath(ur'./text()').extract()[0]
                if price:
                    metadata['price'] = price
            except(TypeError, IndexError):
                pass

        '''
        单品名称
        '''
        nameNode = sel.xpath('//h1[@class="product-name" or @class="productname label"]')
        if nameNode:
            try:
                name = nameNode.xpath('./text()').extract()[0]
                if name:
                    name = self.reformat(name)
                    metadata['name'] = name
            except(TypeError, IndexError):
                pass

        '''
        颜色标签
        '''
        #中国
        colorNodes = sel.xpath('//dl[@class="product-colors"]//li')
        for node in colorNodes:
            try:
                color_text = node.xpath('.//a/@title').extract()[0]
                if color_text:
                    color_text = self.reformat(color_text)
                    metadata['color'] += [color_text]
            except(TypeError, IndexError):
                continue
        #其他
        colorNodes = sel.xpath('//a[@class="swatchanchor"]')
        for node in colorNodes:
            try:
                color_text = node.xpath('./text()').extract()[0]
                if color_text:
                    color_text = self.reformat(color_text)
                    metadata['color'] += [color_text]
            except(TypeError, IndexError):
                continue

        '''
        描述标签
        '''
        #中国
        descriptionNode = sel.xpath('//div[@class="tabpage description"]')
        if descriptionNode:
            try:
                desctiption = descriptionNode.xpath('./text()').extract()[0]
                if desctiption:
                    desctiption = self.reformat(desctiption)
                    metadata['description'] = desctiption
            except(TypeError, IndexError):
                pass
        #其他
        descriptionNode = sel.xpath('//meta[@property="og:description"]')
        if descriptionNode:
            try:
                desctiption = descriptionNode.xpath('./@content').extract()[0]
                if desctiption:
                    desctiption = self.reformat(desctiption)
                    metadata['description'] = desctiption
            except(TypeError, IndexError):
                pass

        '''
        详情标签
        '''
        detailNode = sel.xpath('//div[@class="tabpage inc"]')
        if detailNode:
            try:
                # TODO 详情不全，被<br>分开了
                detail = detailNode.xpath('./text()').extract()[0]
                if detail:
                    detail = self.reformat(detail)
                    metadata['details'] = detail
            except(TypeError, IndexError):
                pass

        '''
        材料及护理标签
        '''
        # TODO 这里该用什么标签名

        '''
        图片
        '''
        #中国
        imageNodes = sel.xpath('//dl[@class="product-colors"]//li/a')
        for node in imageNodes:
            try:
                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                m = copy.deepcopy(metadata)

                yield Request(url=href,
                              callback=self.parse_images,
                              errback=self.onerr,
                              meta={'userdata': m})
            except(TypeError, IndexError):
                continue
        #其他
        imageNodes = sel.xpath('//a[@class="swatchanchor"]')
        for node in imageNodes:
            try:
                href = node.xpath('./@href').extract()[0]
                if re.search('#.+', response.url):
                    href = re.sub('#.+', href, response.url);
                else:
                    href = str.format('{0}{1}', response.url, href)

                m = copy.deepcopy(metadata)

                yield Request(url=href,
                              callback=self.parse_images,
                              errback=self.onerr,
                              meta={'userdata': m})
            except(TypeError, IndexError):
                continue

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
            try:
                imageHref = node.xpath('./@big').extract()[0]
                if imageHref:
                    image_urls += [imageHref]
            except(TypeError, IndexError):
                continue
        '''
        其他网站
        '''
        imageNodes = sel.xpath('//img[@data-detailurl]')
        for node in imageNodes:
            try:
                imageHref = node.xpath('./@data-detailurl').extract()[0]
                if imageHref:
                    image_urls += [imageHref]
            except(TypeError, IndexError):
                continue

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
