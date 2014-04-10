# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import common
import copy
import re
from utils.utils_core import process_price
import json

# TODO 爬虫下载图片会有部分提示错误


class LouisVuittonSpider(MFashionSpider):
    """
    这个爬虫针对lv的移动版网站
    注意：这个爬虫必须用 --user-agent iphone 来运行
    """

    spider_data = {
        'brand_id': 10226,
        'home_urls': {
            'cn': 'http://m.louisvuitton.cn/mobile/zhs_CN/%E4%BA%A7%E5%93%81%E7%B3%BB%E5%88%97',
            'us': 'http://m.louisvuitton.com',
            'fr': 'http://m.louisvuitton.fr',
            'de': 'http://m.louisvuitton.de',
            'es': 'http://m.louisvuitton.es',
            'it': 'http://m.louisvuitton.it',
            'uk': 'http://m.louisvuitton.uk',
            'ru': 'http://m.louisvuitton.ru',
            'br': 'http://m.louisvuitton.br',
            'ca': 'http://m.louisvuitton.ca',
            'hk': 'http://m.louisvuitton.hk',
            'jp': 'http://m.louisvuitton.jp',
            'kr': 'http://m.louisvuitton.kr',
            'tw': 'http://m.louisvuitton.tw',
            'au': 'http://m.louisvuitton.au',
            # 'eu': 'http://m.louisvuitton.eu',
        },
        'currency': {
            'ca': 'USD',
        },
        'image_host': 'http://images.louisvuitton.com/content/dam/lv/online/picture/',
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(LouisVuittonSpider, self).__init__('louis_vuitton', region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        nav_nodes = sel.xpath('//div[@class="header"]/noscript/li[child::h2[text()]]')
        for node in nav_nodes:
            try:
                tag_text = node.xpath('./h2/text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m = copy.deepcopy(metadata)

                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                sub_nodes = node.xpath('./ul/li')
                for sub_node in sub_nodes:
                    # 这里有两种，一种是有下属的，一种是没有下属的
                    title_node = sub_node.xpath('./h3[text()]')
                    if title_node:  # 有下属的
                        try:
                            tag_text = title_node.xpath('./text()').extract()[0]
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()
                        except(TypeError, IndexError):
                            continue

                        if tag_text and tag_name:
                            mc = copy.deepcopy(m)

                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text, },
                            ]

                            gender = common.guess_gender(tag_text)
                            if gender:
                                mc['gender'] = [gender]

                            third_nodes = sub_node.xpath('./ul/li')
                            for third_node in third_nodes:
                                try:
                                    tag_text = ''.join(
                                        self.reformat(val) for val in third_node.xpath('.//text()').extract())
                                    tag_text = self.reformat(tag_text)
                                    tag_name = tag_text.lower()
                                except(TypeError, IndexError):
                                    continue

                                if tag_text and tag_name:
                                    mcc = copy.deepcopy(mc)

                                    mcc['tags_mapping']['category-2'] = [
                                        {'name': tag_name, 'title': tag_text, },
                                    ]

                                    gender = common.guess_gender(tag_text)
                                    if gender:
                                        mcc['gender'] = [gender]

                                    try:
                                        href = third_node.xpath('./a/@href').extract()[0]
                                        href = re.sub(ur'\s', '', href)
                                        href = self.process_href(href, response.url)
                                    except(TypeError, IndexError):
                                        continue

                                    yield Request(url=href,
                                                  callback=self.parse_product_list,
                                                  errback=self.onerr,
                                                  meta={'userdata': mcc})
                    else:
                        try:
                            tag_text = ''.join(self.reformat(val) for val in sub_node.xpath('.//text()').extract())
                            tag_text = self.reformat(tag_text)
                            tag_name = tag_text.lower()
                        except(TypeError, IndexError):
                            continue

                        if tag_text and tag_name:
                            mc = copy.deepcopy(m)

                            mc['tags_mapping']['category-1'] = [
                                {'name': tag_name, 'title': tag_text, },
                            ]

                            gender = common.guess_gender(tag_name)
                            if gender:
                                mc['gender'] = [gender]

                            try:
                                href = sub_node.xpath('./a/@href').extract()[0]
                                href = re.sub(ur'\s', '', href)
                                href = self.process_href(href, response.url)
                            except(TypeError, IndexError):
                                continue

                            yield Request(url=href,
                                          callback=self.parse_product_list,
                                          errback=self.onerr,
                                          meta={'userdata': mc})

    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        # 有些是进入单品的链接，有些是进入更细的分类，有些事进入下一页（当存在上一页时，会被filter掉）
        # 这里忽略掉了这一层级的记录
        # 如果进入了单品页面，在次函数最后，调用parse_product处理
        # 否则，继续深入
        product_nodes = sel.xpath('//div[@class="content"]/noscript/a[@href]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
                href = re.sub(ur'\s', '', href)
                href = self.process_href(href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=href,
                          callback=self.parse_product_list,
                          errback=self.onerr,
                          meta={'userdata': m})

        # next_node = sel.xpath('//div[@class="content"]/noscript/a[@href][contains(text(), "-NEXT-")]')
        # if next_node:
        #     try:
        #         next_href = next_node.xpath('./@href').extract()[0]
        #         next_href = re.sub(ur'\s', '', next_href)
        #         next_href = self.process_href(next_href, response.url)
        #
        #         yield Request(url=next_href,
        #                       callback=self.parse_product_list,
        #                       errback=self.onerr,
        #                       meta={'userdata': metadata},)
        #     except(TypeError, IndexError):
        #         pass

        # 这里判断是不是已经进入了单品页，并解析
        if not product_nodes:
            for val in self.parse_product(response):
                yield val

    def parse_product(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        metadata['url'] = response.url

        other_nodes = sel.xpath('//div[@class="attributePanel"]//div[@class="palette"]/a[@href]')
        for node in other_nodes:
            m = copy.deepcopy(metadata)

            try:
                other_href = node.xpath('./@href').extract()[0]
                other_href = re.sub(ur'\s', '', other_href)
                other_href = self.process_href(other_href, response.url)
            except(TypeError, IndexError):
                continue

            yield Request(url=other_href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m})

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
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = []
        image_nodes = sel.xpath(
            '//div[@id="mainPictureBlock"]//div[@id="productSheetSlideshow"]//li[not(@id)]/img[@data-src]')
        for node in image_nodes:
            try:
                image_src = node.xpath('./@data-src').extract()[0]
                # mt = re.search(ur'/([^/]+)\.\w+$', image_src)
                # if mt:
                #     image_name = mt.group(1)
                #
                #     image_src = str.format("{0}{1}/jcr:content/renditions/{2}_550x550.jpg",
                #                            self.spider_data['image_host'], image_src, image_name)
                #     if image_src:
                #         image_urls += [image_src]
                image_src = str.format("{0}{1}",
                                       self.spider_data['image_host'], image_src)
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
    def fetch_other_offline_identifier(cls, response, spider=None):

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        ret = cls.fetch_price(response, spider)

        price = None
        if 'price' in ret:
            price = process_price(ret['price'], region)

        if not price:
            return True
        else:
            return False

    @classmethod
    def is_notinstock(cls, response, spider=None):
        sel = Selector(response)

        store_lang = None
        mt = re.search(ur'mobile/(\w+)/', response.url)
        if mt:
            store_lang = mt.group(1)

        sku_node = sel.xpath('//div[@id="infoProductBlockTop"]/div[@class="sku"][text()]')
        if sku_node:
            try:
                sku_text = sku_node.xpath('./text()').extract()[0]
                sku_text = cls.reformat(sku_text)
                if sku_text:
                    mt = re.search(ur'(\w+)$', sku_text)
                    if mt:
                        sku = mt.group(1)
                        if sku and store_lang:
                            url = str.format('https://secure.louisvuitton.com/mobile/ajaxsecure/getStockLevel.jsp?storeLang={0}&skuId={1}', store_lang, sku)

                            # cookie = {
                            #     'v1st': 'C29E9C5C813A8B71',
                            #     'JSESSIONID': '6AD05F7CC8D1152132163C08C1284CF9.mobile11',
                            #     'utag_main': '_st:1397126830351$ses_id:1397123762243%3Bexp-session',
                            # }
                            ret = Request(url=url,
                                           callback=cls.is_notinstock_server,
                                           errback=spider.onerror,
                                           meta=response.meta)
                                           # cookies=cookie)
                            # ret.headers.setdefault('User-Agent', 'Mozilla/5.0 (iPhone; U; CPU iPhone OS 4_3_2 like Mac OS X; en-us) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8H7 Safari/6533.18.5')

                            return ret
            except(TypeError, IndexError):
                pass

        return None

    @classmethod
    def is_notinstock_server(cls, response, spider=None):
        sel = Selector(response)

        # response
        # {"inStock":true,"backOrder":false,"commerceActive":true}
        try:
            json_data = json.loads(response.body)
            if json_data:
                in_stock = json_data['inStock']
                if in_stock:
                    return False
        except (TypeError, IndexError):
            pass

        return True

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        other_offline_identifier = cls.fetch_other_offline_identifier(response, spider)

        if model and not other_offline_identifier:

            ret = cls.is_notinstock(response, spider)
            if ret:
                return ret

            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        model = None
        model_nodes = sel.xpath('//input[contains(@name, "catalogRefIds")][@value]')
        for model_node in model_nodes:
            try:
                model_text = model_node.xpath('./@value').extract()[0]
                model_text = cls.reformat(model_text).upper()
                if model_text:
                    model = model_text
                    break
            except(TypeError, IndexError):
                continue

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        name = None
        name_node = sel.xpath('//div[@id="infoProductBlockTop"]//div[@id="productName"]/h1[text()]')
        if name_node:
            try:
                name = name_node.xpath('./text()').extract()[0]
                name = cls.reformat(name)
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@id="infoProductBlockTop"]//*[@class="priceValue"][text()]')
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
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        description = None
        description_node = sel.xpath('//div[@id="infoProductBlockTop"]//div[@id="productDescription"][text()]')
        if description_node:
            try:
                description = '\r'.join(cls.reformat(val) for val in description_node.xpath('.//text()').extract())
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        colors = []
        color_nodes = sel.xpath('//div[@class="attributePanel"]//div[@class="palette"]//img[@alt]')
        for node in color_nodes:
            try:
                color_text = node.xpath('./@alt').extract()[0]
                color_text = cls.reformat(color_text)
                if color_text:
                    colors += [color_text]
            except(TypeError, IndexError):
                continue

        return colors
