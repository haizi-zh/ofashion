# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class VanCleffArpelsSpider(MFashionSpider):

    spider_data = {
        'brand_id': 10369,
        'home_urls': {
            'us': 'http://www.vancleefarpels.com/us/en/',
            'jp': 'http://www.vancleefarpels.com/jp/ja/',
            'uk': 'http://www.vancleefarpels.com/eu/en/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(VanCleffArpelsSpider, self).__init__('van_cleff_arpels', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//nav[@id="left-nav"]/ul[@id="left-ul"]/li[child::h4[text()]]')
        for node in nav_nodes:
            tag_text = node.xpath('./h4/text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('//nav[@id="left-nav"]/ul[@id="left-ul"]/li[child::h4[text()]]/div[@class="sub-nav"]/div/ul/li[child::a[@href][text()]]')
                for sub_node in sub_nodes:
                    tag_text = sub_node.xpath('./a[text()]/text()').extract()[0]
                    tag_text = self.reformat(tag_text)
                    tag_name = tag_text.lower()

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        href = sub_node.xpath('./a[@href]/@href').extract()[0]
                        href = self.process_href(href, response.url)

                        yield Request(url=href,
                                      callback=self.parse_collection,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

    def parse_collection(self, response):
        """
        有些是系列页面，有些是进入了单品列表页面
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        collection_ndoes = sel.xpath('//div[@id="collection-menu-content"]/ul/li/a[@href][text()]')
        for node in collection_ndoes:
            tag_text = node.xpath('./text()').extract()[0]
            tag_text = self.reformat(tag_text)
            tag_name = tag_text.lower()

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                href = node.xpath('./@href').extract()[0]
                href = self.process_href(href, response.url)

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

        for val in self.parse_product_list(response):
            yield val

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="purchase-grid"]/ul/li[descendant::a[@href]]')
        if not product_nodes:
            product_nodes = sel.xpath('//div[@id="collection-slider"]/ul/li[descendant::a[@href]]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            href = node.xpath('.//a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

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
        model_node = sel.xpath('//div[@id="details"]//p[@class="ref"][text()]')
        if model_node:
            model_text = model_node.xpath('./text()').extract()[0]
            model_text = self.reformat(model_text)
            if model_text:
                mt = re.search(ur'([\w ]+)$', model_text)
                if mt:
                    model = mt.group(1)
                    model = self.reformat(model)

        if model:
            metadata['model'] = model
        else:
            return


        name = None
        name_node = sel.xpath('//div[@id="product-right-part"]/h1[text()]')
        if name_node:
            name = name_node.xpath('./text()').extract()[0]
            name = self.reformat(name)

        if name:
            metadata['name'] = name


        color = None
        color_node = sel.xpath('//div[@id="product-right-part"]/p[@class="short-resume"][text()]')
        if color_node:
            color = color_node.xpath('./text()').extract()[0]
            color = self.reformat(color)

        if color:
            metadata['color'] = [color]


        description = None
        description_node = sel.xpath('//div[@id="product-right-part"]/div[@class="scroll-pane"]//p[text()]')
        if description_node:
            description = '\r'.join(
                self.reformat(val)
                for val in description_node.xpath('./text()').extract()
            )

        if description:
            metadata['description'] = description


        price = None
        price_node = sel.xpath('//div[@id="details"]/div[@class="price png_bg"]//span[@class="price-details"][text()]')
        if price_node:
            price = price_node.xpath('./text()').extract()[0]
            price = self.reformat(price)

        if price:
            metadata['price'] = price


        image_urls = []
        image_nodes = sel.xpath('//div[@id="product-left-part"]//ul[@class="caroussel-ul"]/li/img[@src]')
        for image_node in image_nodes:
            image_src = image_node.xpath('./@src').extract()[0]
            image_src = self.process_href(image_src, response.url)
            if image_src:
                image_url = re.sub(ur'/[/\w]+/\d+x\d+/', ur'/', image_src)
                if image_url:
                    image_urls += [image_url]


        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item
