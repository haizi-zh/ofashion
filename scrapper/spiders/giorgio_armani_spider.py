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
                   'home_urls': {'cn': 'http://www.armani.cn/cn/giorgioarmani/accessories_section',
                                 'us': ['http://www.armani.com/us/giorgioarmani/accessories_section',
                                        'http://www.armani.com/us/giorgioarmani/sunglasses_section'],
                                 'uk': ['http://www.armani.com/gb/giorgioarmani/accessories_section',
                                        'http://www.armani.com/gb/giorgioarmani/sunglasses_section']}}

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
            price_without = self.reformat(tmp[0]) if tmp else None
            tmp = node.xpath('./div[@class="itemPrice"]/*[@data-price]/text()').extract()
            price_norm = self.reformat(tmp[0]) if tmp else None
            if price_without:
                m['price'] = price_without
                m['price_discount'] = price_norm
            elif price_norm:
                m['price'] = price_norm
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m})

    def parse_details(self, response):
        metadata = response.meta['userdata']
        metadata['url'] = response.url
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

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

    @classmethod
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider):
        sel = Selector(response)

        model = None
        try:
            tmp = sel.xpath(
                '//*[@class="descriptionContainer"]/*[@class="articleName"]/descendant-or-self::text()').extract()
            if tmp:
                model = cls.reformat(tmp[-1])
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response, spider):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        origin_node = sel.xpath(
            '//aside[@class="descriptionContainer"]//div[@data-item-prop="priceWithoutPromotion"][@class="oldprice"][text()]')
        if origin_node:  # 打折
            try:
                old_price = ''.join(cls.reformat(val) for val in origin_node.xpath('.//text()').extract())
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

            discount_node = sel.xpath(
                '//aside[@class="descriptionContainer"]//div[@data-item-prop="price"][@class="newprice"][text()]')
            if discount_node:
                try:
                    new_price = ''.join(cls.reformat(val) for val in discount_node.xpath('.//text()').extract())
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
        else:
            old_price_node = sel.xpath(
                '//aside[@class="descriptionContainer"]//div[@data-item-prop="price"][@class="newprice"][text()]')
            if old_price_node:
                try:
                    old_price = ''.join(cls.reformat(val) for val in old_price_node.xpath('.//text()').extract())
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response, spider):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//aside[@class="descriptionContainer"]//*[@class="productName"][text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_description(cls, response, spider):
        sel = Selector(response)

        description = None
        try:
            desc_terms = []
            tmp = sel.xpath('//*[@class="descriptionContainer"]/*[@class="attributes"]/text()').extract()
            if tmp:
                desc_terms.extend([cls.reformat(val) for val in tmp])
            if desc_terms:
                description = '\r'.join(desc_terms)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response, spider):
        sel = Selector(response)

        details = None
        try:
            details_terms = []
            tmp = sel.xpath('//*[@class="descriptionContainer"]/ul[@class="tabs"]/li/div[@class="descriptionContent"]'
                            '/text()').extract()
            if tmp:
                details_terms.extend([cls.reformat(val) for val in tmp[:1]])
            if details_terms:
                details = '\r'.join(details_terms)
        except(TypeError, IndexError):
            pass

        return details

    @classmethod
    def fetch_color(cls, response, spider):
        sel = Selector(response)

        colors = None
        try:
            tmp = sel.xpath('//*[@class="descriptionContainer"]//ul[@class="Colors"]/li/a[@title]/@title').extract()
            if tmp:
                colors = [cls.reformat(val) for val in tmp]
        except(TypeError, IndexError):
            pass

        return colors
