# coding=utf-8
import re
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class ValentinoSpider(MFashionSpider):
    spider_data = {'brand_id': 10367,
                   'currency': {'cn': 'EUR', 'hk': 'EUR', 'tw': 'EUR', 'au': 'EUR', 'ca': 'USD', 'cz': 'EUR',
                                'my': 'EUR', 'ru': 'EUR', 'nz': 'EUR', 'no': 'EUR', 'sg': 'EUR', 'se': 'EUR',
                                'ch': 'EUR', 'th': 'EUR', 'dk': 'EUR'},
                   'home_urls': {'cn': 'http://store.valentino.com/VALENTINO/home/tskay/5A81B803/mm/112',
                                 'us': 'http://store.valentino.com/VALENTINO/home/tskay/B60ACEA7/mm/112',
                                 'fr': 'http://store.valentino.com/VALENTINO/home/tskay/D5C4AA66/mm/112',
                                 'it': 'http://store.valentino.com/VALENTINO/home/tskay/CD784FB3/mm/112',
                                 'uk': 'http://store.valentino.com/VALENTINO/home/tskay/112439D7/mm/112',
                                 'jp': 'http://store.valentino.com/VALENTINO/home/tskay/7D74C94E/mm/112',
                                 'hk': 'http://store.valentino.com/VALENTINO/home/tskay/3DC16A52/mm/112',
                                 'tw': 'http://store.valentino.com/VALENTINO/home/tskay/928128F6/mm/112',
                                 'au': 'http://store.valentino.com/VALENTINO/home/tskay/C6921D72/mm/112',
                                 'at': 'http://store.valentino.com/VALENTINO/home/tskay/F71D8A1F/mm/112',
                                 'be': 'http://store.valentino.com/VALENTINO/home/tskay/552ACA0C/mm/112',
                                 'ca': 'http://store.valentino.com/VALENTINO/home/tskay/30E5FB37/mm/112',
                                 'cz': 'http://store.valentino.com/VALENTINO/home/tskay/AD8236E3/mm/112',
                                 'dk': 'http://store.valentino.com/VALENTINO/home/tskay/8E38B618/mm/112',
                                 'fi': 'http://store.valentino.com/VALENTINO/home/tskay/CBF2ABDE/mm/112',
                                 'de': 'http://store.valentino.com/VALENTINO/home/tskay/C7EC0275/mm/112',
                                 'gr': 'http://store.valentino.com/VALENTINO/home/tskay/35CC0F8E/mm/112',
                                 'ie': 'http://store.valentino.com/VALENTINO/home/tskay/3D5AF91F/mm/112',
                                 'my': 'http://store.valentino.com/VALENTINO/home/tskay/5F3402F6/mm/112',
                                 'nl': 'http://store.valentino.com/VALENTINO/home/tskay/AE548791/mm/112',
                                 'nz': 'http://store.valentino.com/VALENTINO/home/tskay/F80CDC79/mm/112',
                                 'no': 'http://store.valentino.com/VALENTINO/home/tskay/813D3CFE/mm/112',
                                 'pt': 'http://store.valentino.com/VALENTINO/home/tskay/CA8091BE/mm/112',
                                 'ru': 'http://store.valentino.com/VALENTINO/home/tskay/6658A396/mm/112',
                                 'sg': 'http://store.valentino.com/VALENTINO/home/tskay/58B187DC/mm/112',
                                 'es': 'http://store.valentino.com/VALENTINO/home/tskay/27D69C18/mm/112',
                                 'se': 'http://store.valentino.com/VALENTINO/home/tskay/DD852A3F/mm/112',
                                 'ch': 'http://store.valentino.com/VALENTINO/home/tskay/138A41DE/mm/112',
                                 'th': 'http://store.valentino.com/VALENTINO/home/tskay/FFC03A39/mm/112',
                   }}

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(ValentinoSpider, self).__init__('valentino', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[contains(@class,"switchSeason")]/ul/li/*[contains(@class,"Season") and @href '
                              'and (name()="span" or name()="a")]'):
            try:
                tag_text = self.reformat(node.xpath('text()').extract()[0])
                tag_name = tag_text.lower()
            except (IndexError, TypeError):
                continue
            m = copy.deepcopy(metadata)
            m['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
            yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                          callback=self.parse_gender, errback=self.onerr, meta={'userdata': m})

        try:
            tag_text = self.reformat(sel.xpath('//div[contains(@class,"switchSeason")]/ul/li'
                                               '/span[@class="mainSeason"]/text()').extract()[0])
            tag_name = tag_text.lower()
        except (IndexError, TypeError):
            return
        metadata['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
        for val in self.parse_gender(response):
            yield val

    def parse_gender(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//div[contains(@class,"switchGender")]')
        if node_list:
            for node in node_list[0].xpath('./ul/li/a[@href and @class="notSelGender"]'):
                try:
                    tmp = self.reformat(node.xpath('text()').extract()[0]).lower()
                except (TypeError, IndexError):
                    continue
                m = copy.deepcopy(metadata)
                gender = cm.guess_gender(tmp)
                if gender:
                    m['gender'] = [gender]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              callback=self.parse_cat1, errback=self.onerr, meta={'userdata': m})
            try:
                tmp = self.reformat(node_list[0].xpath('./ul/li/span[@class="selGender"]/text()').extract()[0]).lower()
                gender = cm.guess_gender(tmp)
                if gender:
                    metadata['gender'] = [gender]
            except (TypeError, IndexError):
                pass

        for val in self.parse_cat1(response):
            yield val

    def parse_cat1(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node1 in sel.xpath('//div[@id="subMenu"]/ul[contains(@class,"menuNavigation")]/li/a[@title and @href]'):
            try:
                tag_text = self.reformat(node1.xpath('@title').extract()[0])
                tag_name = tag_text.lower()
            except (TypeError, IndexError):
                continue
            m1 = copy.deepcopy(metadata)
            m1['tags_mapping']['category-1'] = [{'name': tag_name, 'title': tag_text}]

            node_list = node1.xpath('../ul/li/a[@href and @title]')
            if node_list:
                for node2 in node_list:
                    try:
                        tag_text = self.reformat(node2.xpath('@title').extract()[0])
                        tag_name = tag_text.lower()
                    except (IndexError, TypeError):
                        continue
                    m2 = copy.deepcopy(m1)
                    m2['tags_mapping']['category-2'] = [{'name': tag_name, 'title': tag_text}]
                    yield Request(url=self.process_href(node2.xpath('@href').extract()[0], response.url),
                                  callback=self.parse_cat2, errback=self.onerr, meta={'userdata': m2})
            else:
                yield Request(url=self.process_href(node1.xpath('@href').extract()[0], response.url),
                              callback=self.parse_cat2, errback=self.onerr, meta={'userdata': m1})

    def parse_cat2(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//ul[@id="micro"]/li/a[@href and @title]')
        if node_list:
            for node in node_list:
                try:
                    tag_text = self.reformat(node.xpath('@title').extract()[0])
                    tag_name = tag_text.lower()
                except (IndexError, TypeError):
                    continue
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-3'] = [{'name': tag_name, 'title': tag_name}]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              callback=self.parse_filter, errback=self.onerr, meta={'userdata': m})
        else:
            for val in self.parse_filter(response):
                yield val

    def parse_filter(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        node_list = sel.xpath('//ul[@id="filterColor"]/li//a[@href]')
        if node_list:
            for node in node_list:
                # try:
                #     color = self.reformat(node.xpath('@title').extract()[0]).lower()
                # except (IndexError, TypeError):
                #     color = None
                m = copy.deepcopy(metadata)
                # if color:
                #     m['color'] = [color]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              callback=self.parse_list, errback=self.onerr, meta={'userdata': m})
        else:
            for val in self.parse_list(response):
                yield val

    def parse_list(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="elementsContainer"]/div[contains(@id,"item") and @class="productimage"]'):
            tmp = node.xpath('.//a[@class="itemContainer" and @href and @title]')
            if not tmp:
                continue
            tmp = tmp[0]
            # model = self.reformat(tmp.xpath('@title').extract()[0])
            url = self.process_href(tmp.xpath('@href').extract()[0], response.url)
            #
            # name = None
            # try:
            #     name = self.reformat(
            #         node.xpath('.//div[@class="descCont"]/span[@class="prodInfoViewAll"]/text()').extract()[0])
            # except IndexError:
            #     pass
            #
            # price = None
            # try:
            #     price = self.reformat(
            #         node.xpath('.//div[@class="priceCont"]/span[@class="prodPrice"]/text()').extract()[0])
            # except IndexError:
            #     pass

            m = copy.deepcopy(metadata)
            # m['model'] = model
            # if name:
            #     m['name'] = name
            # if price:
            #     m['price'] = price
            yield Request(url=url, callback=self.parse_details, errback=self.onerr, meta={'userdata': m},
                          dont_filter=True)

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

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = []
        for href in sel.xpath('//div[@id="innerThumbs"]//img[@src and contains(@class,"thumb")]/@src').extract():
            mt = re.search(r'_(\d)+_[a-zA-Z]\.[^/]+$', href)
            if not mt:
                continue
            start_idx = int(mt.group(1))
            image_urls.extend(re.sub(r'(?<=_)\d+(?=_[a-zA-Z]\.[^/]+)', str(val), href)
                              for val in xrange(start_idx, 15))

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
        model_node = sel.xpath('//div[@id="wrapColumns"]//*[@class="titleDetailItem"][last()][text()]')
        if model_node:
            try:
                model = model_node.xpath('./text()').extract()[0]
                model = cls.reformat(model)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_price(cls, response, spider):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        discount_node = sel.xpath('//div[@id="wrapColumns"]//div[@class="itemBoxPrice"]//*[@class="newprice"][text()]')
        if discount_node:  # 打折
            try:
                new_price = discount_node.xpath('./text()').extract()[0]
                new_price = cls.reformat(new_price)
            except(TypeError, IndexError):
                pass

            price_node = sel.xpath('//div[@id="wrapColumns"]//div[@class="itemBoxPrice"]//*[@class="oldprice"][text()]')
            if price_node:
                try:
                    old_price = price_node.xpath('./text()').extract()[0]
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass
        else:  # 未打折
            price_node = sel.xpath('//div[@id="wrapColumns"]//div[@class="itemBoxPrice"]/span[text()]')
            if price_node:
                try:
                    old_price = price_node.xpath('./text()').extract()[0]
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
        name_node = sel.xpath('//div[@id="wrapColumns"]//*[@class="titleDetailItem"][1][text()]')
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
            tmp = sel.xpath('//div[@id="descr"]/span').extract()
            tmp = '\r'.join(cls.reformat(val) for val in tmp)
            if tmp:
                description = tmp
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response, spider):
        sel = Selector(response)

        details = None
        try:
            tmp = sel.xpath('//div[@id="details"]/span').extract()
            tmp = '\r'.join(cls.reformat(val) for val in tmp)
            if tmp:
                details = tmp
        except(TypeError, IndexError):
            pass

        return details

    @classmethod
    def fetch_color(cls, response, spider):
        sel = Selector(response)

        colors = []
        color_node = sel.xpath(
            '//div[@class="innerCol"]//div[@id="colorsContainer"]//div[@class="colorBoxSelected"][@title]')
        if color_node:
            try:
                colors = [cls.reformat(val) for val in color_node.xpath('./text()').extract()]
            except(TypeError, IndexError):
                pass

        return colors
