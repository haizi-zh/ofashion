# coding=utf-8
import urlparse
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm
import re


__author__ = 'Zephyre'


class RalphLaurenSpider(MFashionSpider):
    spider_data = {'brand_id': 10429,
                   'home_urls': {'us': 'http://www.ralphlauren.com/shop/index.jsp?categoryId=1760781&ab=global_men',
                                 'uk': 'http://www.ralphlauren.co.uk/category/index.jsp?categoryId=3979761&ab=global_men',
                                 'fr': 'http://www.ralphlauren.fr/category/index.jsp?categoryId=4663481&ab=global_men',
                   }}

    @classmethod
    def get_supported_regions(cls):
        return RalphLaurenSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(RalphLaurenSpider, self).__init__('ralph_lauren', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@id="global-nav" or @id="rl-globalnav"]/li/a[@title and @href]'):
            try:
                cat_title = self.reformat(node.xpath('@title').extract()[0])
                cat_name = cat_title.lower()
            except (IndexError, TypeError):
                continue

            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'name': cat_name, 'title': cat_title}]
            gender = cm.guess_gender(cat_name)
            if gender:
                m['gender'] = [gender]
            url = self.process_href(node.xpath('@href').extract()[0], response.url)
            yield Request(url=url, callback=self.parse_1, errback=self.onerr, meta={'userdata': m}, dont_filter=True)

    def parse_1(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//ul[@class="nav-items" or @class="leftnav-group"]/li/a[@href]'):
            try:
                cat_title = self.reformat(node.xpath('text()').extract()[0])
                cat_name = cat_title.lower()
                url = self.process_href(node.xpath('@href').extract()[0], response.url)
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-1'] = [{'name': cat_name, 'title': cat_title}]
                yield Request(url=url, callback=self.parse_cat, errback=self.onerr, meta={'userdata': m},
                              dont_filter=True)
            except (IndexError, TypeError):
                continue

    def parse_cat_uk(self, response, metadata):
        sel = Selector(response)
        for node in sel.xpath('//ol[contains(@id,"products")]/li[@id and contains(@class,"product")]'):
            tmp = node.xpath('./a[@href and @class="product"]/@href').extract()
            if not tmp:
                continue
            url = self.process_href(tmp[0], response.url)

            # tmp = node.xpath('.//dl[@class="product-details"]/dt/text()').extract()
            # title = self.reformat(tmp[-1])
            # tmp = node.xpath('.//div[@class="money"]/text()').extract()
            # price = self.reformat(tmp[0]) if tmp else None
            # tmp = node.xpath('.//div[@class="money"]/*[@class="red"]/text()').extract()
            # sale_price = self.reformat(tmp[0]) if tmp else None

            m = copy.deepcopy(metadata)
            # m['name'] = title
            # if price:
            #     m['price'] = price
            # if sale_price:
            #     m['price_discount'] = sale_price
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m},
                          dont_filter=True)

    def parse_cat_us(self, response, metadata):
        sel = Selector(response)
        for node in sel.xpath('//ol[contains(@class,"products")]/li[@id and contains(@class,"product")]'):
            tmp = node.xpath('./dl[@class="product-details"]/dt/a[@href and @class="prodtitle"]')
            if not tmp:
                continue
            url = self.process_href(tmp[0].xpath('@href').extract()[0], response.url)
            # try:
            #     title = self.reformat(tmp[0].xpath('text()').extract()[0])
            #     if not title:
            #         continue
            # except (IndexError, TypeError):
            #     continue
            # tmp = node.xpath('./dl[@class="product-details"]/dd//span[@class="ourprice"]//a[@href]/text()').extract()
            # price = self.reformat(tmp[0]) if tmp else None
            # tmp = node.xpath(
            #     './dl[@class="product-details"]/dd//span[@class="templateSalePrice"]//a[@href]/text()').extract()
            # sale_price = self.reformat(tmp[0]) if tmp else None

            m = copy.deepcopy(metadata)
            # m['name'] = title
            # if price:
            #     m['price'] = price
            # if sale_price:
            #     m['price_discount'] = sale_price
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m},
                          dont_filter=True)

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # 获得下一个页面
        cur_page = None
        tot_pages = None
        next_page = None
        try:
            tmp = sel.xpath('//input[@class="current-page" and @value]/@value').extract()
            cur_page = int(tmp[0]) if tmp else None
            tmp = sel.xpath('//span[@class="total-pages"]/text()').extract()
            tot_pages = int(tmp[0]) if tmp else None
            tmp = sel.xpath('//a[@class="results" and @href and @rel="nofollow"]/@href').extract()
            next_page = self.process_href(tmp[-1], response.url)
        except (IndexError, ValueError, TypeError):
            pass
        if tot_pages > cur_page and next_page:
            yield Request(url=next_page, callback=self.parse_cat, errback=self.onerr,
                          meta={'userdata': copy.deepcopy(metadata)})

        if metadata['region'] == 'us':
            func = lambda: self.parse_cat_us(response, metadata)
        elif metadata['region'] == 'uk':
            func = lambda: self.parse_cat_uk(response, metadata)
        else:
            func = None

        if func:
            for tmp in func():
                yield tmp

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        metadata['url'] = response.url

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        # 试图找出图片地址
        image_terms = re.findall(r'Scene7Map\[\s*"[a-zA-Z\d\s]+"\s*\]\s*=\s*"([^"/]+)"', response.body)
        image_urls = [str.format(
            'http://s7d2.scene7.com/is/image/PoloGSI/{0}?$flyout_main$&cropN=0.12,0,0.75,1&wid=1080&hei=1440', tmp) for
                      tmp in image_terms]

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['image_urls'] = image_urls
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
    def fetch_model(cls, response):
        sel = Selector(response)

        model = None
        tmp = re.search(r'productid=(\d+)', response.url, flags=re.IGNORECASE)
        if tmp:
            try:
                model = tmp.group(1)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        name = None
        if region == 'us':
            name_node = sel.xpath('//meta[@property="og:title"][@content]')
            if name_node:
                try:
                    name = name_node.xpath('./@content').extract()[0]
                    name = cls.reformat(name)
                except(TypeError, IndexError):
                    pass
        else:
            name_node = sel.xpath('//div[@id="product-right"]//h4[@class="product-title fn"][text()]')
            if name_node:
                try:
                    name = name_node.xpath('./text()').extract()[0]
                    name = cls.reformat(name)
                except(TypeError, IndexError):
                    pass

        return name

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        old_price = None
        new_price = None
        if region == 'us':
            discount_node = sel.xpath('//*[@class="rightcolpad"]//div[@class="ProductPriceContainer"]/span[@class="templateSalePrice"][text()]')
            if discount_node:
                old_price_node = sel.xpath('//*[@class="rightcolpad"]//div[@class="ProductPriceContainer"]/span[@class="prodourprice"][text()]')
                if old_price_node:
                    try:
                        old_price = old_price_node.xpath('./text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass

                try:
                    new_price = discount_node.xpath('./text()').extract()[0]
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
            else:
                old_price_node = sel.xpath('//*[@class="rightcolpad"]//div[@class="ProductPriceContainer"]/span[@class="prodourprice"][text()]')
                if old_price_node:
                    try:
                        old_price = old_price_node.xpath('./text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass
        else:
            discount_node = sel.xpath('//div[@id="product-right"]/div[@id="product-information"]/div[@class="money"][text()]/span[@class="red"][text()]')
            if discount_node:
                old_price_node = sel.xpath('//div[@id="product-right"]/div[@id="product-information"]/div[@class="money"][text()]')
                if old_price_node:
                    try:
                        old_price = old_price_node.xpath('./text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass

                try:
                    new_price = discount_node.xpath('./text()').extract()[0]
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
            else:
                old_price_node = sel.xpath('//div[@id="product-right"]/div[@id="product-information"]/div[@class="money"][text()]')
                if old_price_node:
                    try:
                        old_price = old_price_node.xpath('./text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            desc = sel.xpath('//div[@id="longDescDiv"]/text()').extract()
            description = '\r'.join(cls.reformat(tmp) for tmp in desc).strip()
        except(TypeError, IndexError):
            pass

        return description
