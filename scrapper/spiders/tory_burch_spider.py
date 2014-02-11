# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re

class ToryBurchSpider(MFashionSpider):

    spider_data = {
        'brand_id': 11301,
        'home_urls': {
            'us': 'http://www.toryburch.com/',
            'uk': 'http://www.toryburch.co.uk/',
            'de': 'http://www.toryburch.de/',
            'it': 'http://www.toryburch.it/',
            'fr': 'http://www.toryburch.fr/',
            'at': 'http://www.toryburch.at/',
            'jp': 'http://www.toryburch.jp/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(ToryBurchSpider, self).__init__('tory_burch', region)

    def parse(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        if metadata['region'] == 'jp':
            for val in self.parse_jp(response):
                yield val

        nav_nodes = sel.xpath('//div[@id="navigation"]/ul/li[child::a[@href][text()]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./ul/li[child::span[text()]]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./span/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        third_nodes = sub_node.xpath('./ul/li[child::a[@href][text()]]')
                        for third_node in third_nodes:
                            try:
                                tag_text = third_node.xpath('./a/text()').extract()[0]
                                tag_text = self.reformat(tag_text)
                                tag_name = tag_text.lower()
                            except(TypeError, IndexError):
                                continue

                            if tag_text and tag_name:
                                mcc = copy.deepcopy(mc)

                                mcc['tags_mapping']['category-2'] = [
                                    {'name': tag_name, 'title': tag_text,},
                                ]

                                gender = common.guess_gender(tag_name)
                                if gender:
                                    mcc['gender'] = [gender]

                                try:
                                    href = third_node.xpath('./a/@href').extract()[0]
                                    href = self.process_href(href, response.url)
                                except(TypeError, IndexError):
                                    continue

                                yield Request(url=href,
                                              callback=self.parse_product_list,
                                              errback=self.onerr,
                                              meta={'userdata': mcc})

                try:
                    href = node.xpath('./a/@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="search"]/div[@class="productresultarea"]/div/div[contains(@class,"product")]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            href_node = node.xpath('.//a[@href]')
            if href_node:
                try:
                    href = href_node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product,
                              errback=self.onerr,
                              meta={'userdata': m},
                              dont_filter=True)

        # 页面下拉到底部会自动加载更多，需要模拟请求，解析返回的json
        # 测试发现，在原有url后边添加 ?start=99 也可以取到第二页内容
        # 如果当前页有内容，再考虑请求下一页，每页最多有99个
        if product_nodes:
            # 取的当前页数
            current_start = 0
            mt = re.search(r'start=(\d+)', response.url)
            if mt:
                current_page = (int)(mt.group(1))

            next_start = current_start + 99
            # 拼下一页的url
            if mt:
                next_url = re.sub(r'start=\d+', str.format('start={0}', next_start), response.url)
            else:
                next_url = str.format('{0}?start={1}', response.url, next_start)

            # 请求下一页
            yield Request(url=next_url,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': metadata})

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)


        other_nodes = sel.xpath('//div[@id="pdpATCDivsubProductDiv"]//ul[@id="swatchesselect"]/li/a[@name]')
        for node in other_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@name').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})


        metadata['url'] = response.url


        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return


        name = self.fetch_name(response)
        if name:
            metadata['name'] = name


        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_disount' in ret:
            metadata['price_discount'] = ret['price_discount']


        description = self.fetch_description(response)
        if description:
            metadata['description'] = description


        details = self.fetch_details(response)
        if details:
            metadata['details'] = details


        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors


        image_urls = []
        image_node = sel.xpath('//input[@id="pdpImgUrl"][@value]')
        if image_node:
            try:
                image_request_value = image_node.xpath('./@value').extract()[0]
                if image_request_value:
                    m = copy.deepcopy(metadata)
                    image_request_ref = str.format('http://s7d5.scene7.com/is/image/ToryBurchLLC/{0}_S?req=imageset', image_request_value)

                    yield Request(url=image_request_ref,
                                  callback=self.parse_image_request,
                                  errback=self.onerr,
                                  meta={'userdata': m})

                    image_urls += [str.format('http://s7d5.scene7.com/is/image/ToryBurchLLC/{0}?scl=2', image_request_value)]
            except(TypeError, IndexError):
                pass

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    def parse_image_request(self, response):

        metadata = response.meta['userdata']

        image_urls = []
        image_value_list = re.findall(ur',([^,;]+);', response.body)
        for value in image_value_list:
            try:
                image_url = re.sub(ur'/ToryBurchLLC/\w*\?', str.format('/{0}?', value), response.url)
                image_url = re.sub(ur'\?req=imageset', u'?scl=2', image_url)
                if image_url:
                    image_urls += [image_url]
            except(TypeError, IndexError):
                continue

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    def parse_jp(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@id="gNav"]/ul/li[child::a[@href][child::span[text()]]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./a/span/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text,},
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./div/ul/li[child::a[@href][text()]]')
                for sub_node in sub_nodes:
                    try:
                        tag_text = sub_node.xpath('./a/text()').extract()[0]
                        tag_text = self.reformat(tag_text)
                        tag_name = tag_text.lower()
                    except(TypeError, IndexError):
                        continue

                    if tag_text and tag_name:
                        mc = copy.deepcopy(m)

                        mc['tags_mapping']['category-1'] = [
                            {'name': tag_name, 'title': tag_text,},
                        ]

                        gender = common.guess_gender(tag_name)
                        if gender:
                            mc['gender'] = [gender]

                        try:
                            href = sub_node.xpath('./a/@href').extract()[0]
                            href = self.process_href(href, response.url)
                        except(TypeError, IndexError):
                            continue

                        yield Request(url=href,
                                      callback=self.parse_jp_product_list,
                                      errback=self.onerr,
                                      meta={'userdata': mc})

                try:
                    href = node.xpath('./a/@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_jp_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

    def parse_jp_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="search-set"]/div[@class="productresultarea"]/div[@class="productlisting clearfix"]/div[@class="product producttile"]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('.//a[@href]/@href').extract()[0]
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_jp_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

        next_node = sel.xpath('//div[@id="search-set"]/div[@class="sortArea"]//div[@id="static_common_SfPropelPagerView"]//li[@class="next"]/a[@href][text()]')
        if next_node:
            try:
                next_href = next_node.xpath('./@href').extract()[0]
                next_href = self.process_href(next_href, response.url)

                yield Request(url=next_href,
                              callback=self.parse_jp_product_list,
                              errback=self.onerr,
                              meta={'userdata': metadata})
            except(TypeError, IndexError):
                pass

    def parse_jp_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)


        metadata['url'] = response.url


        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return


        name = self.fetch_name(response)
        if name:
            metadata['name'] = name


        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_disount' in ret:
            metadata['price_discount'] = ret['price_discount']


        description = self.fetch_description(response)
        if description:
            metadata['description'] = description


        details = self.fetch_details(response)
        if details:
            metadata['details'] = details


        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors


        image_urls = []
        image_nodes = sel.xpath('//div[@id="detailTxt"]/ul[@class="inlineList"]/li/span/img[@src]')
        for image_node in image_nodes:
            try:
                image_src = image_node.xpath('./@src').extract()[0]
                image_src = re.sub(ur'_\d+x\d+', '', image_src)
                if image_src:
                    image_urls += [image_src]
            except(TypeError, IndexError):
                continue


        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        if image_urls:
            item['image_urls'] = image_urls
        item['metadata'] = metadata

        yield item

    @classmethod
    def is_offline(cls, response):
        return not cls.fetch_model(response)

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        if region != 'jp':
            model_node = sel.xpath('//input[@id="masterProduct"][@value]')
            if model_node:
                try:
                    model = model_node.xpath('./@value').extract()[0]
                    model = cls.reformat(model)
                except(TypeError, IndexError):
                    pass
        else:
            model_node = sel.xpath('//div[@id="wrapper"]//div[@id="detailArea"]//div[@id="kihonTxt"]/p[last()][contains(text(),"Style Number")]')
            if model_node:
                try:
                    model_text = model_node.xpath('./text()').extract()[0]
                    model_text = cls.reformat(model_text)
                    mt = re.search(ur': (\w*)$', model_text)
                    if mt:
                        model = mt.group(1)
                        model = model.upper()
                except(TypeError, IndexError):
                    pass

        return model

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
        if region != 'jp':
            old_price_node = sel.xpath('//div[contains(@id,"product-")]//div[@class="standardprice"][text()]')
            if old_price_node:
                try:
                    old_price = old_price_node.xpath('./text()').extract()[0]
                    old_price = cls.reformat(old_price)

                    new_price = sel.xpath('//div[contains(@id,"product-")]//div[@class="salesprice standardP"][text()]/text()').extract()[0]
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass
            else:
                try:
                    old_price = sel.xpath('//div[contains(@id,"product-")]//div[@class="salesprice standardP"][text()]/text()').extract()[0]
                    old_price = cls.reformat(old_price)
                except(TypeError, IndexError):
                    pass
        else:
            old_price_node = sel.xpath('//div[@id="detailTxt"]//dd[@class="kakaku"]/del[text()]')
            if old_price_node:
                try:
                    old_price = old_price_node.xpath('./text()').extract()[0]
                    old_price = cls.reformat(old_price)
                    old_price = re.sub(ur'-', '', old_price)
                except(TypeError, IndexError):
                    pass

                try:
                    new_price = sel.xpath('//div[@id="detailTxt"]//dd[@class="kakaku"]/span/text()').extract()[0]
                    new_price = cls.reformat(new_price)
                    new_price = re.sub(ur'-', '', new_price)
                except(TypeError, IndexError):
                    pass
            else:
                try:
                    old_price = sel.xpath('//div[@id="detailTxt"]//dd[@class="kakaku"]/text()').extract()[0]
                    old_price = cls.reformat(old_price)
                    old_price = re.sub(ur'-', '', old_price)
                except(TypeError, IndexError):
                    pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        name = None
        if region != 'jp':
            name_node = sel.xpath('//meta[@property="og:title"][@content]')
            if name_node:
                try:
                    name = name_node.xpath('./@content').extract()[0]
                    name = cls.reformat(name)
                    name = name.lower()
                except(TypeError, IndexError):
                    pass
        else:
            name_node = sel.xpath('//ul[@id="path"]/li[@class="last_child"][text()]')
            if name_node:
                try:
                    name = name_node.xpath('./text()').extract()[0]
                    name = cls.reformat(name)
                except(TypeError, IndexError):
                    pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        description = None
        if region != 'jp':
            description_node = sel.xpath('//div[contains(@id,"product-")]//div[@itemprop="description"]/div[@class="panelContent"][text()]')
            if description_node:
                try:
                    description = description_node.xpath('./text()').extract()[0]
                    description = cls.reformat(description)
                except(TypeError, IndexError):
                    pass
        else:
            description_node = sel.xpath('//div[@id="wrapper"]//div[@id="detailArea"]//div[@id="kihonTxt"]/p[not(contains(text(),"Style Number"))][text()]')
            if description_node:
                try:
                    description = description_node.xpath('./text()').extract()[0]
                    description = cls.reformat(description)
                except(TypeError, IndexError):
                    pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        details = None
        if region != 'jp':
            details_nodes = sel.xpath('//div[contains(@id,"product-")]//div[contains(@class,"collapsibleDetails")]//div[@class="detailsPanel open"][not(@itemprop)]/div[@class="panelContent"]')
            if details_nodes:
                try:
                    details = '\r'.join(
                        cls.reformat(val)
                        for val in details_nodes.xpath('.//text()').extract()
                    )
                except(TypeError, IndexError):
                    pass
        else:
            detail_nodes = sel.xpath('//div[@id="detailTxt"]/div[@id="sizeTxt"]/p | //div[@id="detailTxt"]/div[@id="sozaiTxt"]/p')
            if detail_nodes:
                try:
                    details = '\r'.join(
                        cls.reformat(val)
                        for val in detail_nodes.xpath('.//text()').extract()
                    )
                except(TypeError, IndexError):
                    pass

        return details

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        colors = []
        if region != 'jp':
            color_nodes = sel.xpath('//div[@id="pdpATCDivsubProductDiv"]//ul[@id="swatchesselect"]/li/a[@title]')
            for color_node in color_nodes:
                try:
                    color_text = color_node.xpath('./@title').extract()[0]
                    color_text = cls.reformat(color_text)
                    color_text = color_text.lower()
                    if color_text:
                        colors += [color_text]
                except(TypeError, IndexError):
                    pass
        else:
            color_nodes = sel.xpath('//div[@id="detailTxt"]/ul[@class="inlineList"]/li/span/img[@title]')
            if color_nodes:
                try:
                    colors = [
                        cls.reformat(val).lower()
                        for val in color_nodes.xpath('./@title').extract()
                    ]
                except(TypeError, IndexError):
                    pass

        return colors
