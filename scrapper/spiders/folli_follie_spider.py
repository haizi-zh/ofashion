# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class FolliFollieSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10138,
        'home_urls': {
            'cn': 'http://www.follifollie.com.cn/ch-ch',
            'us': 'http://www.follifollie.us.com/us-en',
            'uk': 'http://www.follifollie.co.uk/gb-en',
            'gr': 'http://www.follifollie.gr/gr-el',
            'es': 'http://www.follifollie.es/sp-sp',
            'hk': 'http://www.follifollie.com.hk/hk-ch',
            'jp': 'http://www.follifollie.co.jp/jp-jp',
            'tw': 'http://www.follifollie.com.tw/ch-tw',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(FolliFollieSpider, self).__init__('folli_follie', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="header"]//table[@class="main"]//td/a[@href][text()]')
        for nav_node in nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = nav_node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                href = nav_node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_sub_nav,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_sub_nav(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        sub_nav_nodes = sel.xpath('//*[@id="content"]/div[@class="main-slider"]/div[@class="list"]/div/ul/li/a[@href][text()]')
        for sub_nav_node in sub_nav_nodes:
            m = copy.deepcopy(metadata)

            tag_text = sub_nav_node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                href = sub_nav_node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//*[@id="gift-finder-main"]/ul/li[child::a]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        next_page_node = sel.xpath('//*[@id="paginationTop"]/ul/li[last()]/a[@href]')
        if next_page_node:
            m = copy.deepcopy(metadata)

            next_href = next_page_node.xpath('./@href').extract()[0]
            next_href = self.process_href(next_href, response.url)

            yield Request(url=next_href,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': m})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        other_nodes = sel.xpath('//div[@class="product-container"]//div[@class="prod-options"]/div[@class="colors"]/ul/li/a[@href]')
        for node in other_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('./@href').extract()[0]
            href = self.process_href(href, response.url)

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

        metadata['url'] = response.url

        model = None
        model_node = sel.xpath('//div[@class="product-container"]//div[@class="prod-descr-wrap"]//p[@class="code"][text()]')
        if model_node:
            model_text = model_node.xpath('./text()').extract()[0]
            model_text = self.reformat(model_text)
            if model_text:
                mt = re.search(r'(\w+)$', model_text)
                if mt:
                    model = mt.group(1)
        if model:
            metadata['model'] = model
        else:
            return

        name_node = sel.xpath('//div[@class="product-container"]//div[@class="product-inner"]/div[@class="right"]/h2[text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)
            if name:
                metadata['name'] = name

        price_node = sel.xpath('//div[@class="product-container"]//div[@class="product-inner"]//div[@class="prod-options"]/div[@id="prices"]')
        if price_node:
            old_price_node = price_node.xpath('./div[@class="price-offer"]/span[@class="strike"][text()]')
            if old_price_node:
                old_price = old_price_node.xpath('./text()').extract()[0]
                old_price = self.reformat(old_price)
                if old_price:
                    metadata['price'] = old_price

                new_price = price_node.xpath('./div[@class="price"]/span[text()]/text()').extract()[0]
                new_price = self.reformat(new_price)
                if new_price:
                    metadata['price_discount'] = new_price
            else:
                price = price_node.xpath('./div[@class="price"]/span[text()]/text()').extract()[0]
                price = self.reformat(price)
                if price:
                    metadata['price'] = price

        color_node = sel.xpath('//div[@class="product-container"]//div[@class="prod-options"]/div[@class="colors"]/ul/li[contains(@class, "active")]//div[@class="inner"][text()]')
        if color_node:
            color = color_node.xpath('./text()').extract()[0]
            color = self.reformat(color)
            if color:
                metadata['color'] = [color]

        description_node = sel.xpath('//div[@class="product-container"]//div[@class="right"]/div[@class="prod-descr-wrap"]/ul/li[1]/div[@class="cnt"]/div[@class="in"][text()]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('./text()').extract()
            )
            description = self.reformat(description)
            if description:
                metadata['description'] = description

        detail_node = sel.xpath('//div[@class="product-container"]//div[@class="right"]/div[@class="prod-descr-wrap"]/ul/li[2]/div[@class="cnt"]/div[@class="in"][text()]')
        if detail_node:
            detail = '\r'.join(
                self.reformat(val)
                for val in detail_node.xpath('./text()').extract()
            )
            detail = self.reformat(detail)
            if detail:
                metadata['details'] = detail

        image_urls = None
        image_node = sel.xpath('//link[@rel="image_src"][@href]')
        if image_node:
            image_href = image_node.xpath('./@href').extract()[0]
            image_href = re.sub(r'_\d+x\d+\$', '', image_href)

            image_urls = [image_href]

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

