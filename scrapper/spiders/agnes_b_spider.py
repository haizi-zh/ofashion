# coding=utf-8

__author__ = 'Ryan'

from scrapper.spiders.mfashion_spider import MFashionSpider
from scrapper.items import ProductItem
from scrapy.http import Request
from scrapy.selector import Selector

import copy
import common
import re


class AgnesBSpider(MFashionSpider):
    spider_data = {
        'brand_id': 10006,
        'home_urls': {
            'us': 'http://usa.agnesb.com/en/',
            'uk': 'http://europe.agnesb.com/en/',
            'fr': 'http://europe.agnesb.com/fr/',
        },
    }

    @classmethod
    def get_supported_regions(cls):
        return cls.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(AgnesBSpider, self).__init__('agnes b', region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # 左侧边栏第一栏中带连接的那些node
        nav_nodes = sel.xpath('//div[@id="sidebar"]/ul/ul/li/a[@href]')
        for node in nav_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-0'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_filter1,
                              errback=self.onerr,
                              meta={'userdata': m})


    def parse_filter1(self, response):
        """
        解析当前打开的那个左边栏的下属node
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        sub_nodes = sel.xpath('//div[@id="sidebar"]/ul/ul/ul/li/a[@href]')
        for sub_node in sub_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = sub_node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-1'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = sub_node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_filter2,
                              errback=self.onerr,
                              meta={'userdata': m})

                # 这个页面右下角也有几个推荐单品，但是在别的路径都有，就不单独解析了


    def parse_filter2(self, response):
        """
        有些类别有二级的分类
        比如：http://usa.agnesb.com/en/shopping_online/tous-produits/accessories/women-1
        """

        metadata = response.meta['userdata']
        sel = Selector(response)

        sub_nodes = sel.xpath('//div[@id="sidebar"]/ul/ul/ul/ul/li/a[@href]')
        for sub_node in sub_nodes:
            m = copy.deepcopy(metadata)

            try:
                tag_text = sub_node.xpath('./text()').extract()[0]
                tag_text = self.reformat(tag_text)
                tag_name = tag_text.lower()
            except(TypeError, IndexError):
                continue

            if tag_text and tag_name:
                m['tags_mapping']['category-2'] = [
                    {'name': tag_name, 'title': tag_text, },
                ]

                gender = common.guess_gender(tag_name)
                if gender:
                    m['gender'] = [gender]

                try:
                    href = sub_node.xpath('./@href').extract()[0]
                    href = self.process_href(href, response.url)
                except(TypeError, IndexError):
                    continue

                yield Request(url=href,
                              callback=self.parse_product_list,
                              errback=self.onerr,
                              meta={'userdata': m})

        for val in self.parse_product_list(response):
            yield val


    def parse_product_list(self, response):

        metadata = response.meta['userdata']
        sel = Selector(response)

        product_nodes = sel.xpath('//div[@id="contenu_principal"]//div[@class="lignes"]/div[contains(@class, "prod")]')
        for node in product_nodes:
            m = copy.deepcopy(metadata)

            try:
                name_node = node.xpath('./p/a[text()]')
                if name_node:
                    name = name_node.xpath('./text()').extract()[0]
                    name = self.reformat(name)
                    if name:
                        metadata['name'] = name
            except(TypeError, IndexError):
                continue

            # old_price_node = node.xpath('./span[@class="prix_old"][text()]')
            # if old_price_node:
            #     old_price = old_price_node.xpath('./text()').extract()[0]
            #     old_price = self.reformat(old_price)
            #     if old_price:
            #         metadata['price'] = old_price
            #
            #     new_price_node = node.xpath('./span[contains(@class, "prix discount")][text()]')
            #     if new_price_node:
            #         new_price = new_price_node.xpath('./text()').extract()[0]
            #         new_price = self.reformat(new_price)
            #         if new_price:
            #             metadata['price_discount'] = new_price
            # else:
            #     price_node = node.xpath('./span[@class="prix"][text()]')
            #     if price_node:
            #         price = price_node.xpath('./text()').extract()[0]
            #         price = self.reformat(price)
            #         if price:
            #             metadata['price'] = price

            try:
                colors = [
                    self.reformat(val).lower()
                    for val in node.xpath('/p/a/img[@title]/@title').extract()
                ]
                if colors:
                    metadata['color'] = colors
            except(TypeError, IndexError):
                continue

            # 这里随便取一个a标签就行
            href = node.xpath('./a[@href]/@href').extract()[0]
            href = self.process_href(href, response.url)

            # 这里dont_filter来保证从不同路径进入单品，生成不同的tag
            yield Request(url=href,
                          callback=self.parse_product,
                          errback=self.onerr,
                          meta={'userdata': m},
                          dont_filter=True)

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}
        old_price_node = sel.xpath(
            '//div[@class="description_bas"]/div[@id="test"]/div[@class="tarif_old"]/span[text()]')
        if old_price_node:
            try:
                old_price = old_price_node.xpath('./text()').extract()[0]
                old_price = cls.reformat(old_price)
                if old_price:
                    ret['price'] = old_price
            except(TypeError, IndexError):
                pass

            new_price_node = sel.xpath(
                '//div[@class="description_bas"]/div[@id="test"]/div[contains(@class, "discount")][text()]')
            if new_price_node:
                try:
                    new_price = new_price_node.xpath('./text()').extract()[0]
                    new_price = cls.reformat(new_price)
                    if new_price:
                        ret['price_discount'] = new_price
                except(TypeError, IndexError):
                    pass
        else:
            try:
                price = sel.xpath('//div[@class="description_bas"]/div[@id="test"]/div[text()]/text()').extract()[0]
                price = cls.reformat(price)
                if price:
                    ret['price'] = price
            except(TypeError, IndexError):
                pass

        return ret

    @classmethod
    def is_offline(cls, response):
        return not cls.fetch_model(response)

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        # 货号在点开+details中的reference后边
        model = None
        model_node = sel.xpath('//div[@id="details_popup"]/div[not(@class)][text()]')
        if model_node:
            try:
                model_text = model_node.xpath('./text()').extract()[0]
                model_text = cls.reformat(model_text)
                mt = re.search(r'\b(\w+)\b$', model_text)
                if mt:
                    model = mt.group(1)
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_color(cls, response):
        sel = Selector(response)
        return [cls.reformat(val).lower() for val in
                sel.xpath('//div[@class="carre_couleur"]/a/img[@title]/@title').extract()]

    @classmethod
    def fetch_name(cls, response):
        tmp = Selector(response).xpath('//div[@id="infos_produit"]/div/div/p[@class="titre"][text()]/text()').extract()
        return cls.reformat(tmp[0]) if tmp else None

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        # 这里你点开那个+details，才能看到完整的description
        # 我把上面作为description，下面作为detail
        description_node = sel.xpath('//div[@id="details_popup"]/div[@class="gauche"]')
        if description_node:
            description = cls.reformat(''.join(cls.reformat(val) for val in
                                               description_node.xpath('./text()').extract()))
        else:
            description = None

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)
        details = None

        # 这里你点开那个+details，才能看到完整的description
        # 我把上面作为description，下面作为detail
        description_node = sel.xpath('//div[@id="details_popup"]/div[@class="gauche"]')
        if description_node:
            detail_node = description_node.xpath('./div/p[text()]')
            if detail_node:
                details = cls.reformat('\r'.join(cls.reformat(val) for val in detail_node.xpath('./text()').extract()))

        return details

    def parse_product(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        # 不同颜色的单品页面
        color_nodes = sel.xpath('//div[@class="carre_couleur"]/a[@href]')
        for node in color_nodes:
            m = copy.deepcopy(metadata)

            try:
                href = node.xpath('./@href').extract()[0]
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

        if not metadata.get('name'):
            try:
                name = self.fetch_name(response)
                if name:
                    metadata['name'] = name
            except(TypeError, IndexError):
                pass

        if 'price' not in metadata:
            ret = self.fetch_price(response)
            if 'price' in ret:
                metadata['price'] = ret['price']
            if 'price_discount' in ret:
                metadata['price_discount'] = ret['price_discount']

        if not metadata.get('color'):
            try:
                colors = [
                    self.reformat(val).lower()
                    for val in sel.xpath('//div[@class="carre_couleur"]/a/img[@title]/@title').extract()
                ]
                if colors:
                    metadata['color'] = colors
            except(TypeError, IndexError):
                pass

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        # 这里，1500的图片应该都有
        # 我觉得他有更大的图，虽然更大的值返回404，我觉得是对加载大图片有限制它
        image_urls = None
        image_nodes = sel.xpath('//div[@id="ProductDetailThumbList"]//a/img[@src]')
        if image_nodes:
            try:
                image_urls = [
                    re.sub(r'_80', r'_1500', self.process_href(val, response.url))
                    for val in image_nodes.xpath('./@src').extract()
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

    @classmethod
    def fetch_name(cls, response):
        sel = Selector(response)

        name = None
        try:
            name = sel.xpath('//div[@id="infos_produit"]/div/div/p[@class="titre"][text()]/text()').extract()[0]
            name = cls.reformat(name)
        except(TypeError, IndexError):
            pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        # 这里你点开那个+details，才能看到完整的description
        # 我把上面作为description，下面作为detail
        description = None
        description_node = sel.xpath('//div[@id="details_popup"]/div[@class="gauche"]')
        if description_node:
            try:
                description = ''.join(
                    cls.reformat(val)
                    for val in description_node.xpath('./text()').extract()
                )
                description = cls.reformat(description)
            except(TypeError, IndexError):
                pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        # 这里你点开那个+details，才能看到完整的description
        # 我把上面作为description，下面作为detail
        detail = None
        description_node = sel.xpath('//div[@id="details_popup"]/div[@class="gauche"]')
        if description_node:
            detail_node = description_node.xpath('./div/p[text()]')
            if detail_node:
                try:
                    detail = '\r'.join(
                        cls.reformat(val)
                        for val in detail_node.xpath('./text()').extract()
                    )
                    detail = cls.reformat(detail)
                except(TypeError, IndexError):
                    pass

        return detail
