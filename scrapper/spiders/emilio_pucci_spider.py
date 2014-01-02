# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class EmilioPucciSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10117,
        'currency': {
            'cn': 'EUR',
            'au': 'EUR',
            'bg': 'EUR',
            'ca': 'USD',
            'cz': 'EUR',
            'dk': 'EUR',
            'hk': 'EUR',
            'in': 'EUR',
            'nz': 'EUR',
            'ru': 'EUR',
            'sg': 'EUR',
            'se': 'EUR',
            'ch': 'EUR',
            'tw': 'EUR',
        },
        'home_urls': {
            'cn': 'http://www.emiliopucci.com/home.asp?tskay=06B33963',
            'uk': 'http://www.emiliopucci.com/home.asp?tskay=229040E8',
            'us': 'http://www.emiliopucci.com/home.asp?tskay=8D8F600C',
            'au': 'http://www.emiliopucci.com/home.asp?tskay=F3954597',
            'at': 'http://www.emiliopucci.com/home.asp?tskay=2ADBB68D',
            'be': 'http://www.emiliopucci.com/home.asp?tskay=E6A9F090',
            'bg': 'http://www.emiliopucci.com/home.asp?tskay=6C88664E',
            'ca': 'http://www.emiliopucci.com/home.asp?tskay=7EC826A9',
            'cz': 'http://www.emiliopucci.com/home.asp?tskay=678D1A7D',
            'dk': 'http://www.emiliopucci.com/home.asp?tskay=96511E71',
            'fr': 'http://www.emiliopucci.com/home.asp?tskay=7271828A',
            'de': 'http://www.emiliopucci.com/home.asp?tskay=C712808D',
            'hk': 'http://www.emiliopucci.com/home.asp?tskay=D2195088',
            'in': 'http://www.emiliopucci.com/home.asp?tskay=7BC52705',
            'ie': 'http://www.emiliopucci.com/home.asp?tskay=CE27E1D3',
            'it': 'http://www.emiliopucci.com/home.asp?tskay=B9526778',
            'jp': 'http://www.emiliopucci.com/home.asp?tskay=C0494252',
            'nz': 'http://www.emiliopucci.com/home.asp?tskay=63F3D281',
            'ru': 'http://www.emiliopucci.com/home.asp?tskay=4A5B5D93',
            'sg': 'http://www.emiliopucci.com/home.asp?tskay=1EBCD5CC',
            'se': 'http://www.emiliopucci.com/home.asp?tskay=00890543',
            'ch': 'http://www.emiliopucci.com/home.asp?tskay=8586A6D5',
            'tw': 'http://www.emiliopucci.com/home.asp?tskay=CFAF0777',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(EmilioPucciSpider, self).__init__('emilio_pucci', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        seasons_nodes = sel.xpath('//div[@id="div_body_head"]//div[@class="seasonsBox"]/h2')
        for seasons_node in seasons_nodes:
            m = copy.deepcopy(metadata)

            # 检查是不是当前选中状态
            href_node = seasons_node.xpath('./a[@href]')
            if not href_node:
                try:
                    tag_text = ''.join(
                        self.reformat(val)
                        for val in seasons_node.xpath('.//text()').extract()
                    )
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()
                except(TypeError, IndexError):
                    continue

                if tag_text and tag_name:
                    m['tags_mapping']['category-0'] = [
                        {'name': tag_name, 'title': tag_text,},
                    ]

                    left_nav_nodes = sel.xpath('//div[@class="page_content"]/div[@id="menu_sx"]/div[@class="left_menu_pad"]//div/ul/li')
                    for left_nav_node in left_nav_nodes:
                        mc = copy.deepcopy(m)

                        tag_node = left_nav_node.xpath('./h2[text()]')
                        if tag_node:
                            try:
                                tag_text = tag_node.xpath('./text()').extract()[0]
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()
                            except(TypeError, IndexError):
                                continue

                            if tag_text and tag_name:
                                mc['tags_mapping']['category-1'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mc['gender'] = [gender]

                                sub_nodes = left_nav_node.xpath('./ul/li[child::a]')
                                for sub_node in sub_nodes:
                                    mcc = copy.deepcopy(mc)

                                    try:
                                        tag_text = sub_node.xpath('./a/text()').extract()[0]
                                        tag_text = self.reformat(tag_text)
                                        tag_name = tag_text.lower()
                                    except(TypeError, IndexError):
                                        continue

                                    if tag_text and tag_name:
                                        mcc['tags_mapping']['category-2'] = [
                                            {'name': tag_name, 'title': tag_text,},
                                        ]

                                        gender = common.guess_gender(tag_name)
                                        if gender:
                                            mcc['gender'] = [gender]

                                        try:
                                            href = sub_node.xpath('./a/@href').extract()[0]
                                            href = self.process_href(href, response.url)
                                        except(TypeError, IndexError):
                                            continue

                                        yield Request(url=href,
                                                      callback=self.parse_product_list,
                                                      errback=self.onerr,
                                                      meta={'userdata': mcc})
                        else:
                            try:
                                tag_text = left_nav_node.xpath('./a/text()').extract()[0]
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()
                            except(TypeError, IndexError):
                                continue

                            if tag_text and tag_name:
                                mc['tags_mapping']['category-1'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mc['gender'] = [gender]

                                try:
                                    href = left_nav_node.xpath('./a/@href').extract()[0]
                                    href = self.process_href(href, response.url)
                                except(TypeError, IndexError):
                                    continue

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mc})
            else:
                try:
                    href = href_node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="elementsContainer"]/div[contains(@id, "item")]')
        for product_node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = product_node.xpath('.//a[@href]/@href').extract()[0]
                href = re.sub(r'\r|\n|\t', '', href)
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        other_product_nodes = sel.xpath('//div[@id="itemContainer"]//div[contains(@id, "item")]//a[@href]')
        for node in other_product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = re.sub(r'\r|\n|\t', '', href)
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        model = None
        try:
            mt = re.search(r'cod10=(\w+)', response.url)
            if not mt:
                mt = re.search(r'cod10/(\w+)/', response.url)
            if mt:
                model = mt.group(1)
        except(TypeError, IndexError):
            return
        if model:
            metadata['model'] = model
        else:
            return

        try:
            name = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@class="itemTitle"]/h1/text()').extract()[0]
            name = self.reformat(name)
            if name:
                metadata['name'] = name
        except(TypeError, IndexError):
            pass

        old_price_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@id="itemPriceContainer"]//div[@class="oldprice"][text()]')
        if old_price_node:
            try:
                price = old_price_node.xpath('./text()').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price
            except(TypeError, IndexError):
                pass

            discount_price_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@id="itemPriceContainer"]//div[@class="newprice"][text()]')
            if discount_price_node:
                try:
                    discount_price = discount_price_node.xpath('./text()').extract()[0]
                    discount_price = self.reformat(discount_price)
                    if discount_price:
                        metadata['price_discount'] = discount_price
                except(TypeError, IndexError):
                    pass
        else:
            price_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]/div[@id="itemPriceContainer"]')
            try:
                price = ''.join(
                    self.reformat(val)
                    for val in price_node.xpath('.//text()').extract()
                )
                price = self.reformat(price)
                if price:
                    metadata['price'] = price
            except(TypeError, IndexError):
                pass

        description_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]//div[@id="descr_content"][text()]')
        if description_node:
            try:
                description = '\r'.join(
                    self.reformat(val)
                    for val in description_node.xpath('.//text()').extract()
                )
                description = self.reformat(description)

                if description:
                    metadata['description'] = description
            except(TypeError, IndexError):
                pass

        detail_node = sel.xpath('//div[@id="innerContentCol"]//div[@id="inner"]//div[@id="details_content"][text()]')
        if detail_node:
            try:
                detail = '\r'.join(
                    self.reformat(val)
                    for val in detail_node.xpath('.//text()').extract()
                )
                detail = self.reformat(detail)

                if detail:
                    metadata['details'] = detail
            except(TypeError, IndexError):
                pass

        image_urls = []
        image_nodes = sel.xpath('//div[@id="innerContentCol"]//div[@id="thumbContainer"]//div[@class="thumbElement"]/img[@src]')
        for node in image_nodes:
            try:
                src = node.xpath('./@src').extract()[0]
                src = self.process_href(src, response.url)

                image_urls += [
                    re.sub(r'_\d+_', str.format('_{0}_', val), src)
                    for val in xrange(10, 15)
                ]
            except(TypeError, IndexError):
                pass


        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
