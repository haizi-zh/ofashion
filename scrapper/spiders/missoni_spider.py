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


class MissoniSpider(MFashionSpider):
    spider_data = {'brand_id': 10263,
                   'currency': {'kr': 'USD'},
                   'home_urls': {
                       region: str.format('http://www.missoni.com/{0}/fashion', 'gb' if region == 'uk' else region)
                       for
                       region in {'us', 'uk', 'fr', 'it', 'au', 'at', 'be', 'ca', 'cz', 'dk', 'eg', 'fi', 'de', 'gr',
                                  'hk', 'ie', 'jp', 'mo', 'my', 'nl', 'nz', 'no', 'pt', 'ru', 'sg', 'kr', 'es', 'se',
                                  'ch', 'tw', 'th'}}}

    @classmethod
    def get_supported_regions(cls):
        return MissoniSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(MissoniSpider, self).__init__('missoni', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node0 in sel.xpath('//div[@id="seasonSwitcher"]/div/ul[@id]'):
            try:
                cat_title = self.reformat(node0.xpath('./li[contains(@class,"seasonCont")]/text()').extract()[0])
                cat_name = cat_title.lower()
            except (IndexError, TypeError):
                continue

            m0 = copy.deepcopy(metadata)
            m0['tags_mapping']['category-0'] = [{'title': cat_title, 'name': cat_name}]

            for node1 in node0.xpath('.//ul[contains(@class,"genderMenu")]'):
                try:
                    cat_title = self.reformat(node1.xpath('./li/a[@title]/@title').extract()[0])
                    cat_name = cat_title.lower()
                except (IndexError, TypeError):
                    continue
                m1 = copy.deepcopy(m0)
                m1['tags_mapping']['category-1'] = [{'title': cat_title, 'name': cat_name}]
                gender = cm.guess_gender(cat_title)
                if gender:
                    m1['gender'] = [gender]

                for node2 in node1.xpath('./ul[contains(@class,"macroMenu")]/li/a[@href]'):
                    try:
                        url = self.process_href(node2.xpath('@href').extract()[0], response.url)
                        tmp = node2.xpath('text()').extract()
                        cat_title = self.reformat(tmp[0])
                        cat_name = cat_title.lower()
                    except (IndexError, TypeError):
                        continue
                    m2 = copy.deepcopy(m1)
                    m2['tags_mapping']['category-2'] = [{'title': cat_title, 'name': cat_name}]
                    yield Request(url=url, callback=self.parse_list, errback=self.onerr, meta={'userdata': m2})

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//li[@class="productContainer realItem" and @data-code8]'):
            m = copy.deepcopy(metadata)
            # m['model'] = self.reformat(node.xpath('@data-code8').extract()[0])
            tmp = node.xpath('./div[@class="productImageContainer"]/a[@data-itemlink and @href]/@href').extract()
            if not tmp:
                continue
            url = self.process_href(tmp[0], response.url)
            # tmp = node.xpath(
            #     './div[contains(@class,"productDescriptionContainer")]/div[@class="productMicro"]/text()').extract()
            # name = self.reformat(tmp[0]) if tmp else None
            # if name:
            #     m['name'] = name
            #
            # tmp = node.xpath(
            #     './div[contains(@class,"productDescriptionContainer")]//div[@data-item-prop="price"]/'
            #     '*[@class="currency" or @class="priceValue"]/text()').extract()
            # price_new = ''.join(self.reformat(val) for val in tmp if val)
            # tmp = node.xpath(
            #     './div[contains(@class,"productDescriptionContainer")]//div[@data-item-prop="priceWithoutPromotion"]/'
            #     '*[@class="currency" or @class="priceValue"]/text()').extract()
            # price_old = ''.join(self.reformat(val) for val in tmp if val)
            #
            # if price_old and price_new:
            #     m['price'] = price_old
            #     m['price_discount'] = price_new
            # elif price_new and not price_old:
            #     m['price'] = price_new

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

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        image_urls = []
        for href in sel.xpath('//meta[@property="og:image" and @content]/@content').extract():
            mt = re.search(r'_(\d+)_[^\d]+$', href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            for idx in xrange(start_idx, 15):
                image_urls.append(re.sub(r'(.+)_\d+_([^\d]+)$', str.format(r'\1_{0}_\2', idx), href))

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
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        try:
            mt = re.search(ur'cod(\d+)', response.url)
            if mt:
                model = mt.group(1)
        except(TypeError, IndexError):
            pass

        return model

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        del_node = sel.xpath('//div[@id="infoContent"]//div[@class="itemBoxPrice"]//div[@class="oldprice"][text()]')
        if del_node:  # 打折
            try:
                old_price = del_node.xpath('./text()').extract()[0]
                old_price = cls.reformat(old_price)
            except(TypeError, IndexError):
                pass

            try:
                discount_node = sel.xpath(
                    '//div[@id="infoContent"]//div[@class="itemBoxPrice"]//div[@class="newprice"][text()]')
                new_price = discount_node.xpath('./text()').extract()[0]
                new_price = cls.reformat(new_price)
            except(TypeError, IndexError):
                pass
        else:  # 未打折
            try:
                old_price_node = sel.xpath('//div[@id="infoContent"]//div[@class="itemBoxPrice"]//span[text()]')
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
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        try:
            name_node = sel.xpath('//div[@id="infoContent"]//div[@id="productFirstInfoContainer"]/h3[text()]')
            if name_node:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        try:
            tmp = sel.xpath('//div[@id="prod_content"]//text()').extract()
            if tmp:
                description = '\r'.join(cls.reformat(val) for val in tmp)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        details = None
        try:
            tmp = sel.xpath('//div[@id="descr_content"]//text()').extract()
            if tmp:
                details = '\r'.join(cls.reformat(val) for val in tmp)
        except(TypeError, IndexError):
            pass

        return details
