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

        # 从url和页面中找到model
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

        # 解析左上角标签
        #中国
        types_nodes = sel.xpath('//ul[@class="container clearfix"]/li[not(contains(text(), "/"))]')[0:-1]
        #其他
        if not types_nodes:
            types_nodes = sel.xpath('//a[contains(@name, "breadcrump")]')
        category_index = 0
        for node in types_nodes:

            # 中国，前两个标签，字写在下属的a里边
            type_node = node.xpath('./a')
            if not type_node:
                type_node = node

            type_name = None
            try:
                type_text = type_node.xpath('./text()').extract()[0]
                if type_text:
                    type_name = self.reformat(type_text).lower()
                    category_type = str.format('category-{0}', category_index)
                    metadata['tags_mapping'][category_type] = [
                        {'name': type_name, 'title': type_text}
                    ]
                    category_index+=1
            except(TypeError, IndexError):
                continue

            # 顺便猜男女
            gender = common.guess_gender(type_name)
            if gender:
                metadata['gender'] = [gender]

        # 价格标签，中国和其他还是分开抓，太不一样了
        # 折扣标签
        pre_price_node = sel.xpath('//div[@class="product-prices"]//dd[not(@class)]')
        # 中国
        price_node = sel.xpath('//div[@class="product-prices"]//dd[@class="saleprice"]')
        if not price_node:
            # 其他
            price_node = sel.xpath('//div[@class="price mainPrice"]//div[@class="standardprice" or @class="salesprice"]')

        if price_node:
            try:
                price = price_node.xpath('./text()').extract()[0]
                price = self.reformat(price)
                if not pre_price_node:
                    if price:
                        metadata['price'] = price

                        # 其他国家一样找不到pre_price_node，摒弃price_node找到的是原售价
                        # 这里检查是不是有折扣价
                        discount_price_node = sel.xpath('//div[@class="price mainPrice"]//div[@class="salesprice issalesprice"]')
                        if discount_price_node:
                            discount_price = discount_price_node.xpath('./text()').extract()[0]
                            discount_price = self.reformat(discount_price)
                            if discount_price:
                                metadata['price_discount'] = discount_price
                else:
                    if price:
                        metadata['price_discount'] = price
                    pre_price = pre_price_node.xpath('./text()').extract()[0]
                    pre_price = self.reformat(pre_price)
                    if pre_price:
                        metadata['price'] = pre_price
            except(TypeError, IndexError):
                pass

        # 单品名称
        name_node = sel.xpath('//h1[@class="product-name" or @class="productname label"]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                if name:
                    name = self.reformat(name)
                    metadata['name'] = name
            except(TypeError, IndexError):
                pass

        # 颜色标签
        #中国
        color_nodes = sel.xpath('//dl[@class="product-colors"]//li')
        for node in color_nodes:
            try:
                color_text = node.xpath('.//a/@title').extract()[0]
                if color_text:
                    color_text = self.reformat(color_text)
                    metadata['color'] += [color_text]
            except(TypeError, IndexError):
                continue
        #其他
        color_nodes = sel.xpath('//a[@class="swatchanchor"]')
        for node in color_nodes:
            try:
                color_text = node.xpath('./text()').extract()[0]
                if color_text:
                    color_text = self.reformat(color_text)
                    metadata['color'] += [color_text]
            except(TypeError, IndexError):
                continue

        # 描述标签
        #中国
        description_node = sel.xpath('//div[@class="tabpage description"]')
        if description_node:
            try:
                desctiption = description_node.xpath('./text()').extract()[0]
                if desctiption:
                    desctiption = self.reformat(desctiption)
                    metadata['description'] = desctiption
            except(TypeError, IndexError):
                pass
        #其他
        description_node = sel.xpath('//meta[@property="og:description"]')
        if description_node:
            try:
                desctiption = description_node.xpath('./@content').extract()[0]
                if desctiption:
                    desctiption = self.reformat(desctiption)
                    metadata['description'] = desctiption
            except(TypeError, IndexError):
                pass

        # 详情标签
        detail_node = sel.xpath('//div[@class="tabpage inc"]')
        if detail_node:
            try:
                # TODO 详情不全，被<br>分开了
                detail = detail_node.xpath('./text()').extract()[0]
                if detail:
                    detail = self.reformat(detail)
                    metadata['details'] = detail
            except(TypeError, IndexError):
                pass

        # TODO 材料及护理标签

        # 解析图片地址
        #中国
        image_nodes = sel.xpath('//dl[@class="product-colors"]//li/a')
        for node in image_nodes:
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
        image_nodes = sel.xpath('//a[@class="swatchanchor"]')
        for node in image_nodes:
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

    def parse_images(self, response):
        """
        处理单品图片
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        image_urls = []

        # 中国网站
        image_nodes = sel.xpath('//div[@id="gallery"]//a/img')
        for node in image_nodes:
            try:
                image_href = node.xpath('./@big').extract()[0]
                if image_href:
                    image_urls += [image_href]
            except(TypeError, IndexError):
                continue
        # 其他国家网站
        image_nodes = sel.xpath('//img[@data-detailurl]')
        for node in image_nodes:
            try:
                image_href = node.xpath('./@data-detailurl').extract()[0]
                if image_href:
                    image_urls += [image_href]
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
