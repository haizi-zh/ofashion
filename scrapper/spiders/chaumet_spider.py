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


class ChaumetSpider(MFashionSpider):
    spider_data = {'brand_id': 10076,
                   'model_pattern': {'cn': u'(参考|产品)(编号|型号)\s*(：|:)?\s*',
                                     'jp': u'商品番号\s*(：|:)?\s*',
                                     'us': 'Reference\s*:?\s*',
                                     'hk': u'(參考|参考|產品|产品)(编号|型號|編號)\s*(：|:)?\s*',
                                     'fr': u'(Reference|Référence)\s*:?\s*'},
                   'home_urls': {'cn': 'http://www.chaumet.cn',
                                 'jp': 'http://www.chaumet.jp',
                                 'fr': 'http://www.chaumet.fr',
                                 'hk': 'http://cht.chaumet.com',
                                 'us': 'http://www.chaumet.com'}}

    @classmethod
    def get_supported_regions(cls):
        return ChaumetSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        super(ChaumetSpider, self).__init__('chaumet', region)

    def parse(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@id="nav"]/ul[@class="hList"]/li[contains(@id,"section")]/a[@href]'):
            try:
                tag_text = self.reformat(node.xpath('text()').extract()[0])
                tag_name = tag_text.lower()
                m = copy.deepcopy(metadata)
                m['tags_mapping']['category-0'] = [{'name': tag_name, 'title': tag_text}]
                yield Request(url=self.process_href(node.xpath('@href').extract()[0], response.url),
                              meta={'userdata': m}, callback=self.parse_cat, errback=self.onerr)
            except (TypeError, IndexError):
                continue

    def parse_cat(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)
        checked_all = ('checked-all' in response.meta)

        if not checked_all:
            # 右边是否有”显示所有“之类的按钮？
            node_list = filter(lambda node: self.reformat(node.xpath('@title').extract()[0]).lower() in \
                                            (u'显示所有', 'view all', 'show all', u'すべて表示する', 'tout afficher'),
                               sel.xpath('//div[contains(@id,"pageContent")]/div[@class="btnCtn right"]'
                                         '/a[@title and @href]'))
            if node_list:
                yield Request(url=self.process_href(node_list[0].xpath('@href').extract()[0], response.url),
                              meta={'userdata': metadata, 'checked-all': True}, callback=self.parse_cat,
                              errback=self.onerr)
                return

        # 查找grid内容
        prod_nodes = sel.xpath('//div[contains(@id,"pageContent")]/div[contains(@class,"grid")]'
                               '/div[contains(@class,"Cell")]/div[contains(@class,"layerProduit")]'
                               '/div[@class="inner"]/a[@href]')
        if prod_nodes:
            # 这是单品节点
            for node in prod_nodes:
                href = node.xpath('@href').extract()[0]
                m = copy.deepcopy(metadata)
                # # 尝试查找价格信息
                # try:
                #     tmp = self.reformat(node.xpath('../*[@class="price"]/text()').extract()[0])
                #     if tmp:
                #         m['price'] = tmp
                # except IndexError:
                #     pass
                yield Request(url=self.process_href(href, response.url), callback=self.parse_details,
                              errback=self.onerr, meta={'userdata': m})
        else:
            # 这是类别节点
            cat_nodes = sel.xpath('//div[contains(@id,"pageContent")]/div[contains(@class,"grid")]'
                                  '/div[contains(@class,"Cell")]/a[@href]')
            if cat_nodes:
                for node in cat_nodes:
                    m = copy.deepcopy(metadata)
                    url = self.process_href(node.xpath('@href').extract()[0], response.url)
                    # 尝试查找分类信息
                    tmp = node.xpath('./img[@title]/@title').extract()
                    try:
                        tag_text = self.reformat(tmp[0])
                        tag_name = tag_text.lower()
                        if tag_text:
                            # 目前metadata中最深层次的category
                            deepest = sorted(filter(lambda val: re.search(r'^category-\d+', val),
                                                    m['tags_mapping'].keys()))[-1]
                            new_level = int(re.search(r'^category-(\d+)', deepest).group(1)) + 1
                            m['tags_mapping'][str.format('category-{0}', new_level)] = [
                                {'name': tag_name, 'title': tag_text}]
                    except (TypeError, IndexError):
                        pass
                    yield Request(url=url, meta={'userdata': m}, callback=self.parse_cat, errback=self.onerr)
                    # else:
                    #     # 到达叶节点
                    #     for val in self.parse_details(response):
                    #         yield val

    def parse_details(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        ## 查找类似单品
        #others = sel.xpath('//div[@class="meazone"]//div[@class="carouselWrapper"]/ul/li/a[@href]/@href').extract()
        #if others:
        #    for href in others:
        #        m = copy.deepcopy(metadata)
        #        yield Request(url=self.process_href(href, response.url), callback=self.parse_details,
        #                      errback=self.onerr, meta={'userdata': m})

        metadata['url'] = response.url

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

        # # 查找型号
        # pattern = self.spider_data['model_pattern'][metadata['region']]
        # desc_terms = filter(lambda val: val, (self.reformat(val) for val in
        #                                       sel.xpath('//div[@class="mod productInfosMod"]/div[@class="inner"]'
        #                                                 '/p[@class="globalInfos"]'
        #                                                 '/descendant-or-self::text()').extract()))
        # if desc_terms:
        #     metadata['description'] = '\r'.join(desc_terms)
        #
        # tmp = list(filter(lambda val: re.search(pattern, val, flags=re.U | re.I), desc_terms))
        # if tmp:
        #     tmp = tmp[-1][re.search(pattern, tmp[-1], flags=re.U | re.I).end():]
        #     mt = re.search(r'[a-zA-Z\d\-]+', tmp)
        #     if mt:
        #         metadata['model'] = mt.group()
        #
        # details_terms = filter(lambda val: val,
        #                        (self.reformat(val) for val in
        #                         sel.xpath(
        #                             '//div[@id="description"]//div[@class="contentProductLayer"]/div[@class="rte" or '
        #                             '@class="intro"]/descendant-or-self::text()').extract()))
        # if 'model' not in metadata:
        #     tmp = list(filter(lambda val: re.search(pattern, val, flags=re.U | re.I), details_terms))
        #     if not tmp:
        #         return
        #     tmp = tmp[-1][re.search(pattern, tmp[-1], flags=re.U | re.I).end():]
        #     if not tmp:
        #         return
        #     mt = re.search(r'[a-zA-Z\d\-]+', tmp)
        #     if not mt:
        #         return
        #     metadata['model'] = mt.group()
        # if details_terms:
        #     metadata['details'] = '\r'.join(details_terms)

        tmp = sel.xpath('//div[@class="mod productImageMod"]/div[@class="inner"]/a[@href]/@href').extract()
        if tmp:
            yield Request(url=self.process_href(tmp[0], response.url), meta={'userdata': metadata},
                          callback=self.parse_image, errback=self.onerr)
        else:
            item = ProductItem()
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            item['image_urls'] = []
            yield item

    def parse_image(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        image_urls = []
        for href in sel.xpath('//div[@class="zoomView"]//div[@id="thumbnailBox"]//ul/li/a/img[@src]/@src').extract():
            url = self.process_href(href, response.url)
            url_comp = urlparse.urlparse(url)
            image_urls.append(urlparse.urlunparse(
                (url_comp.scheme, url_comp.netloc, url_comp.path, url_comp.params, '', url_comp.fragment)))

        item = ProductItem()
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        item['image_urls'] = image_urls
        yield item

    @classmethod
    def is_offline(cls, response):
        model = cls.fetch_model(response)
        name = cls.fetch_name(response)

        if model and name:
            return True
        else:
            return False

    @classmethod
    def fetch_model(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        # 查找型号
        tmp = None
        pattern = None
        try:
            pattern = cls.spider_data['model_pattern'][region]
            desc_terms = filter(lambda val: val, (cls.reformat(val) for val in
                                                  sel.xpath('//div[@class="mod productInfosMod"]/div[@class="inner"]'
                                                            '/p[@class="globalInfos"]'
                                                            '/descendant-or-self::text()').extract()))
            tmp = list(filter(lambda val: re.search(pattern, val, flags=re.U | re.I), desc_terms))
        except(TypeError, IndexError):
            pass
        if tmp:
            try:
                tmp = tmp[-1][re.search(pattern, tmp[-1], flags=re.U | re.I).end():]
                mt = re.search(r'[a-zA-Z\d\-]+', tmp)
                if mt:
                    model = mt.group()
            except(TypeError, IndexError):
                pass

        if not model:
            try:
                details_terms = filter(lambda val: val,
                                       (cls.reformat(val) for val in
                                        sel.xpath(
                                            '//div[@id="description"]//div[@class="contentProductLayer"]/div[@class="rte" or '
                                            '@class="intro"]/descendant-or-self::text()').extract()))
                tmp = list(filter(lambda val: re.search(pattern, val, flags=re.U | re.I), details_terms))
            except(TypeError, IndexError):
                pass
            if tmp:
                try:
                    tmp = tmp[-1][re.search(pattern, tmp[-1], flags=re.U | re.I).end():]
                    if tmp:
                        mt = re.search(r'[a-zA-Z\d\-]+', tmp)
                        if mt:
                            model = mt.group()
                except(TypeError, IndexError):
                    pass

        return model

    @classmethod
    def fetch_price(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        price_node = sel.xpath('//div[@id="pageContent"]//div[@class="mod productInfosMod"]//*[@itemprop="price"][text()]')
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
    def fetch_name(cls, response):
        sel = Selector(response)

        name = NameError
        try:
            tmp = cls.reformat(sel.xpath('//div[@class="productHead"]/*[@itemprop="name"]/text()').extract()[0])
            if tmp:
                name = tmp
        except IndexError:
            pass

        return name

    @classmethod
    def fetch_description(cls, response):
        sel = Selector(response)

        description = None
        try:
            desc_terms = filter(lambda val: val, (cls.reformat(val) for val in
                                                  sel.xpath('//div[@class="mod productInfosMod"]/div[@class="inner"]'
                                                            '/p[@class="globalInfos"]'
                                                            '/descendant-or-self::text()').extract()))
            if desc_terms:
                description = '\r'.join(desc_terms)
        except(TypeError, IndexError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response):
        sel = Selector(response)

        details = None
        try:
            details_terms = filter(lambda val: val,
                                   (cls.reformat(val) for val in
                                    sel.xpath(
                                        '//div[@id="description"]//div[@class="contentProductLayer"]/div[@class="rte" or '
                                        '@class="intro"]/descendant-or-self::text()').extract()))
            if details_terms:
                details = '\r'.join(details_terms)
        except(TypeError, IndexError):
            pass

        return details
