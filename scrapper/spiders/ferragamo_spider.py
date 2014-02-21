# coding=utf-8
import json
import os
import re
import copy

from scrapy.http import Request
from scrapy.selector import Selector

from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
import common as cm


__author__ = 'Zephyre'


class FerragamoSpider(MFashionSpider):
    spider_data = {'brand_id': 10308,
                   # 提取型号
                   'model_template': {'cn': ur'型号代码([\s\da-zA-Z]+)',
                                      'us': r'model code\s+([0-9A-Z]+\s+[0-9A-Z]+)',
                                      'fr': '(.+)', 'es': '(.+)', 'ca': '(.+)', 'mx': '(.+)', 'jp': '(.+)',
                                      'it': r'CODICE MODELLO\s+([0-9A-Z]+\s+[0-9A-Z]+)',
                                      'uk': '(.+)',
                   },
                   'home_urls': {'cn': 'http://www.ferragamo.cn',
                                 'us': 'http://www.ferragamo.com/shop/en/usa',
                                 'es': 'http://www.ferragamo.com/shop/es/esp',
                                 'ca': 'http://www.ferragamo.com/shop/en/can',
                                 'mx': 'http://www.ferragamo.com/shop/es/mex',
                                 'fr': 'http://www.ferragamo.com/shop/fr/fra',
                                 'it': 'http://www.ferragamo.com/shop/it/ita',
                                 'jp': 'http://www.ferragamo.com/shop/ja/jpn',
                                 'uk': 'http://www.ferragamo.com/shop/en/uk'}}
    # TODO 多国家支持

    @classmethod
    def get_supported_regions(cls):
        return FerragamoSpider.spider_data['home_urls'].keys()

    def __init__(self, region):
        self.spider_data['callbacks'] = {'cn': [self.parse_cn, self.parse_cat_cn, self.parse_details_cn],
                                         'us': [self.parse_us, self.parse_cat_us, self.parse_details_us],
                                         'ca': [self.parse_fr, self.parse_cat_fr, self.parse_details_us],
                                         'mx': [self.parse_fr, self.parse_cat_fr, self.parse_details_us],
                                         'fr': [self.parse_fr, self.parse_cat_fr, self.parse_details_us],
                                         'jp': [self.parse_fr, self.parse_cat_fr, self.parse_details_us],
                                         'it': [self.parse_us, self.parse_cat_us, self.parse_details_us],
                                         'es': [self.parse_fr, self.parse_cat_fr, self.parse_details_us],
                                         'uk': [self.parse_fr, self.parse_cat_fr, self.parse_details_us]
        }
        super(FerragamoSpider, self).__init__('ferragamo', region)

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def parse(self, response):
        metadata = response.meta['userdata']
        for val in self.spider_data['callbacks'][metadata['region']][0](response):
            yield val

    def parse_cn(self, response):
        for val in self.parse_base(response,
                                   xpath_dict={'cat_level_0': ['//div[@class="nav"]/ul/li/a[@href]', None],
                                               'cat_level_extra': '../ul/li/a[@href]'}):
            yield val

    def parse_us(self, response):
        for val in self.parse_base(response,
                                   xpath_dict={'cat_level_0':
                                                   ['//nav[@id="frg_main_menu"]/ul[@id="frg_menu"]/li/a[@href]',
                                                    '//nav[@id="frg_main_menu"]/ul[@id="frg_menu"]/li/span'],
                                               'cat_level_extra': '../ul/li/a[@href]'}):
            yield val

    def parse_fr(self, response):
        def is_leaf(node):
            if node.xpath('name()').extract()[0] != 'a':
                return False
            else:
                return not node.xpath('@class').extract()

        for val in self.parse_base(response,
                                   xpath_dict={'cat_level_0':
                                                   ['//nav[@id="frg_main_menu"]/ul[@id="frg_menu"]/li/a[@href]',
                                                    '//nav[@id="frg_main_menu"]/ul[@id="frg_menu"]/li/span'],
                                               'cat_level_1': '../div/ul/li[@class="frg_second_lev_menu_cat"]/a[@href]',
                                               'cat_level_extra': '../../li[not(@class="frg_second_lev_menu_cat")]'
                                                                  '/a[@href]'}, is_leaf=is_leaf):
            yield val

    def parse_base(self, response, xpath_dict, sel=None, metadata=None, cat_level=0, is_leaf=None):
        """
        @param is_leaf: 函数：判断当前节点是否为叶节点
        @param response:
        @param xpath_dict: 形式：{'cat_level_0: [xpath, xpath_extra], 'cat_level_1': xpath, ...'cat_level_extra': xpath}
        @param sel:
        @param metadata:
        @param cat_level:
        """
        if not metadata:
            metadata = response.meta['userdata']
        if not sel:
            sel = Selector(response)
        if not is_leaf:
            is_leaf = lambda x: False
        if cat_level == 0:
            xpath, xpath_extra = xpath_dict['cat_level_0']
            node_list = sel.xpath(xpath)
            if xpath_extra:
                node_list.extend(sel.xpath(xpath_extra))
        else:
            cat_key = str.format('cat_level_{0}', cat_level)
            if cat_key in xpath_dict:
                xpath = xpath_dict[cat_key]
            else:
                xpath = xpath_dict['cat_level_extra']
            node_list = sel.xpath(xpath)

        if node_list and not is_leaf(sel):
            # 深度优先递归，继续下级分支
            for node in node_list:
                try:
                    tag_title = self.reformat(node.xpath('text()').extract()[0])
                    tag_name = tag_title.lower()
                except (TypeError, IndexError):
                    continue
                m1 = copy.deepcopy(metadata)
                if cat_level == 0:
                    gender = cm.guess_gender(tag_name)
                    if gender:
                        m1['gender'] = [gender]
                m1['tags_mapping'][str.format('category-{0}', cat_level)] = [{'name': tag_name, 'title': tag_title}]

                for val in self.parse_base(response, xpath_dict, node, m1, cat_level + 1, is_leaf=is_leaf):
                    yield val
        else:
            # 到达叶节点
            tmp = sel.xpath('@href').extract()
            if tmp:
                yield Request(url=self.process_href(tmp[0], response.url),
                              callback=self.spider_data['callbacks'][metadata['region']][1], errback=self.onerr,
                              meta={'userdata': metadata})

    def parse_cat_base(self, response, xpath_1, xpath_2):
        metadata = response.meta['userdata']
        sel = Selector(response)
        for node in sel.xpath(xpath_1):
            yield Request(url=self.process_href(node.xpath(xpath_2).extract()[0], response.url),
                          callback=self.spider_data['callbacks'][metadata['region']][2],
                          errback=self.onerr, meta={'userdata': copy.deepcopy(metadata)})

    def parse_cat_cn(self, response):
        for val in self.parse_cat_base(response,
                                       '//div[@class="view-content"]/div[contains(@class,"page-wrapper-product")]/div'
                                       '/a[@href]/*[@class="prodcaption"]', '../@href'):
            yield val

    def parse_cat_us(self, response):
        for val in self.parse_cat_base(response,
                                       '//div[@id="category_product_container"]//div[contains(@class,"frg_grid_row")]'
                                       '/figure/a[@href]', '@href'):
            yield val

    def parse_cat_fr(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)

        for node in sel.xpath('//div[@class="product"]/div[@class="frg_prod_ins"]/div[@class="product_info"]'):
            m = copy.deepcopy(metadata)
            tmp = node.xpath('./div[@class="product_name"]/a[@href]/@href').extract()
            if tmp:
                url = self.process_href(tmp[0], response.url)
            else:
                continue
            tmp = node.xpath('./div[@class="product_name"]/a/text()').extract()
            if tmp:
                m['name'] = self.reformat(tmp[0])
            tmp = node.xpath('./div[@class="product_price"]//*[@itemprop="price" and contains(@id,"offerPrice")]'
                             '/text()').extract()
            if tmp:
                m['price'] = self.reformat(tmp[0])
            yield Request(url=url, callback=self.spider_data['callbacks'][metadata['region']][2],
                          errback=self.onerr, meta={'userdata': m})

    def parse_details_us(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)
        metadata['url'] = response.url

        # 查找不同的颜色版本
        try:
            idx = response.body.find('var productURLs')
            data = json.loads(cm.extract_closure(response.body[idx:], '\{', '\}')[0].replace("'", '"'))
            for color_key in data:
                tmp = sel.xpath(str.format('//select/option[@value="{0}"]', color_key))
                if not tmp:
                    continue
                color_node = tmp[0]
                # 是否为当前选择的颜色？
                if not color_node.xpath('@selected'):
                    m = copy.deepcopy(metadata)
                    tmp = color_node.xpath('text()').extract()
                    if tmp:
                        m['color'] = [self.reformat(tmp[0])]
                    yield Request(url=self.process_href(data[color_key], response.url),
                                  callback=self.spider_data['callbacks'][metadata['region']][2],
                                  errback=self.onerr, meta={'userdata': m})
                else:
                    tmp = color_node.xpath('text()').extract()
                    if tmp:
                        metadata['color'] = [self.reformat(tmp[0])]
        except ValueError:
            pass

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        model = self.fetch_model(response)
        if model:
            metadata['model'] = model
        else:
            return

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

        ret = self.fetch_price(response)
        if 'price' in ret:
            metadata['price'] = ret['price']
        if 'price_discount' in ret:
            metadata['price_discount'] = ret['price_discount']

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        image_urls = []
        for img_node in sel.xpath('//div[contains(@class,"slider_selector") or @id="frg_thumb_list"]/ul'
                                  '/li[contains(@id,"productAngle")]//img[@src or @data-url]'):
            tmp = img_node.xpath('@data-url').extract()
            if tmp:
                image_urls.append(self.process_href(tmp[0], response.url))
            else:
                tmp = img_node.xpath('@src').extract()[0]
                a, b = os.path.splitext(tmp)
                image_urls.append(self.process_href(str.format('{0}_zoom{1}', a, b), response.url))

        #image_urls = [self.process_href(val, response.url) for val in
        #              sel.xpath('//div[contains(@class,"slider_selector") or @id="frg_thumb_list"]/ul'
        #                        '/li[contains(@id,"productAngle")]/img[@src and @data-url]/@data-url').extract()]
        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        yield item

    def parse_details_cn(self, response):
        metadata = response.meta['userdata']
        sel = Selector(response)
        metadata['url'] = response.url

        name = self.fetch_name(response)
        if name:
            metadata['name'] = name

        description = self.fetch_description(response)
        if description:
            metadata['description'] = description

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

        detail = self.fetch_details(response)
        if detail:
            metadata['details'] = detail

        tmp = sel.xpath('//div[@class="product-collection"]/text()').extract()
        if tmp and tmp[0]:
            tag_text = self.reformat(tmp[0])
            metadata['tags_mapping']['collection'] = [{'name': tag_text.lower(), 'title': tag_text}]

        colors = self.fetch_color(response)
        if colors:
            metadata['color'] = colors

        image_urls = [self.process_href(val, response.url) for val in
                      sel.xpath('//div[@class="item-list"]/ul[contains(@class,"field-slideshow-pager")]/li'
                                '/a[@href]/img[@src]/@src').extract()]

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
    def fetch_model(cls, response):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        if region == 'cn':
            # 试图找出产品编号
            try:
                tmp = sel.xpath('//div[@class="product-code"]/text()').extract()
                desc = None
                if tmp:
                    desc = cls.reformat(tmp[0])

                model = None
                if desc:
                    mt = re.search(cls.spider_data['model_template'][region], desc, flags=re.I | re.U)
                    if mt and mt.group(1).strip():
                        model = mt.group(1).strip()
                if not model:
                    mt = re.search(r'/([0-9a-zA-Z]+)$', response.url)
                    if mt:
                        model = mt.group(1)
            except(TypeError, IndexError):
                pass
        else:
            tmp = cls.reformat(''.join(sel.xpath('//div[contains(@id, "product_name_")]/*[contains(@id,"product_SKU_")]'
                                              '/descendant-or-self::text()').extract()))
            try:
                # 试图找出产品编号
                mt = re.search(cls.spider_data['model_template'][region], tmp, flags=re.I | re.U)
                if not mt:
                    return
                model = mt.group(1).strip().upper()
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
        if region == 'cn':
            try:
                tmp = sel.xpath('//div[@class="product-price"]/text()').extract()
                if tmp:
                    old_price = cls.reformat(tmp[0])
            except(TypeError, IndexError):
                pass
        else:
            discount_node = sel.xpath('//div[contains(@id, "product_name_")]/*[contains(@id,"price_display_")]/span[@itemprop="price"][@class="discount"][text()]')
            if discount_node:   # 打折
                try:
                    new_price = discount_node.xpath('./text()').extract()[0]
                    new_price = cls.reformat(new_price)
                except(TypeError, IndexError):
                    pass

                price_node = sel.xpath('//div[contains(@id, "product_name_")]/*[contains(@id,"price_display_")]/span[@itemprop="price"][@class="product_original_price"][text()]')
                if price_node:
                    try:
                        old_price = price_node.xpath('./text()').extract()[0]
                        old_price = cls.reformat(old_price)
                    except(TypeError, IndexError):
                        pass
            else:   # 未打折
                try:
                    tmp = '\r'.join(cls.reformat(val) for val in
                                    sel.xpath('//div[contains(@id, "product_name_")]'
                                              '/*[contains(@id,"price_display_")]/span[@itemprop="price"]'
                                              '/text()').extract())
                    if tmp:
                        old_price = tmp
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
        if region == 'cn':
            try:
                tmp = sel.xpath('//div[@class="product-title"]/text()').extract()
                if tmp:
                    name = cls.reformat(tmp[0])
            except(TypeError, IndexError):
                pass
        else:
            tmp = sel.xpath('//div[contains(@id, "product_name_")]/*[@itemprop="name"]/h1/text()').extract()
            try:
                tmp = cls.reformat(tmp[0])
                if tmp:
                    name = tmp
            except IndexError:
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
        if region == 'cn':
            try:
                tmp = sel.xpath('//div[@class="product-code"]/text()').extract()
                desc = None
                if tmp:
                    desc = cls.reformat(tmp[0])
                    description = desc
            except(TypeError, IndexError):
                pass
        else:
            try:
                desc_terms = []
                # desc_terms.extend([cls.reformat(val) for val in
                #                    sel.xpath('//div[contains(@id, "product_name_")]/*[contains(@id,"product_SKU_")]'
                #                              '/text()').extract()])
                desc_terms.extend([cls.reformat(val) for val in sel.xpath('//div[contains(@id, "product_name_")]'
                                                                           '/p[contains(@class,"model_shortdescription")]'
                                                                           '/text()').extract()])
                desc_terms.extend([cls.reformat(val) for val in
                                   sel.xpath('//div[@id="tabContainer"]//*[@itemprop="description"]/text()').extract()])
                desc = '\r'.join(val for val in desc_terms if val)
                if desc:
                    description = desc
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
        if region == 'cn':
            try:
                tmp = '\r'.join(cls.reformat(val) for val in sel.xpath('//div[@class="product-desc"]'
                                                                        '/div[@class="field-content"]/text()').extract())
                if tmp:
                    details = tmp
            except(TypeError, IndexError):
                pass
        else:
            try:
                tmp = '\r'.join(cls.reformat(val) for val in
                                sel.xpath('//div[contains(@id, "product_name_")]'
                                          '/p[contains(@class,"model_description")]/text()').extract())
                if tmp:
                    details = tmp
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
        if region == 'cn':
            try:
                tmp = sel.xpath('//select[@class="select-color"]/option//a[@href]/text()').extract()
                if tmp:
                    colors = [cls.reformat(val) for val in tmp]
            except(TypeError, IndexError):
                pass
        else:
            try:
                idx = response.body.find('var productURLs')
                data = json.loads(cm.extract_closure(response.body[idx:], '\{', '\}')[0].replace("'", '"'))
                for color_key in data:
                    tmp = sel.xpath(str.format('//select/option[@value="{0}"]', color_key))
                    if not tmp:
                        continue
                    color_node = tmp[0]
                    # 是否为当前选择的颜色？
                    if color_node.xpath('@selected'):
                        tmp = color_node.xpath('text()').extract()
                        if tmp:
                            colors = [cls.reformat(tmp[0])]
            except ValueError:
                pass

        return colors
