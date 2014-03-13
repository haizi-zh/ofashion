# coding=utf-8
import json
import re
from scrapy import log
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import Rule
from scrapy.http import Request
from scrapy.selector import Selector
import common as cm
from scrapper.items import ProductItem
from scrapper.spiders.mfashion_spider import MFashionSpider
from utils.utils_core import unicodify, iterable

__author__ = 'Zephyre'


class CoachSpider(MFashionSpider):
    spider_data = {'domains': {'cn': 'china.coach.com',
                               'hk': 'hongkong.coach.com',
                               'jp': 'japan.coach.com',
                               'kr': 'korea.coach.com',
                               'my': 'malaysia.coach.com',
                               'sg': 'singapore.coach.com',
                               'tw': 'taiwan.coach.com',
                               'us': 'www.coach.com',
                               'uk': 'uk.coach.com'},
                   'price_url': {'cn': 'http://china.coach.com/loadSkuDynamicInfo.json'},
                   'desc_url': {'cn': 'http://china.coach.com/getSkuDesInfo.json'},
                   'image_url': {'cn': 'http://china.coach.com/getImagesInfo.json'},
                   # 'currency': {'au': 'USD', 'ca': 'USD', 'hk': 'USD', 'mo': 'USD', 'nz': 'USD', 'kr': 'USD',
                   #              'tw': 'USD', 'tm': 'USD'},
                   'brand_id': 10093}

    # TODO 多国家支持


    @classmethod
    def get_supported_regions(cls):
        return CoachSpider.spider_data['domains'].keys()

    def __init__(self, region):
        super(CoachSpider, self).__init__('coach', region)
        self.allowed_domains = [self.spider_data['domains'][val] for val in self.region_list]
        # 每个单品页面的referer，都对应于某些category
        self.url_cat_dict = {}
        self.rules = ()

    @classmethod
    def get_instance(cls, region=None):
        return cls(region)

    def start_requests(self):
        self.rules = (
            Rule(SgmlLinkExtractor(allow=(r'detail\.htm\?[^/]+$',)), callback=self.parse_details_cn),
            Rule(SgmlLinkExtractor(allow=(r'/Product-[^/]+$', )), callback=self.parse_details),
            Rule(SgmlLinkExtractor(allow=(r'.+', )))
        )
        self._compile_rules()

        for region in self.region_list:
            if region not in self.get_supported_regions():
                self.log(str.format('No data for {0}', region), log.WARNING)
                continue

            yield Request(url='http://' + self.spider_data['domains'][region])

    def onerr(self, reason):
        url_main = None
        response = reason.value.response
        url = response.url

        next_func = None
        if 'next_func' in reason.request.meta:
            next_func = reason.request.meta['next_func']

        temp = reason.request.meta
        if 'userdata' in temp:
            metadata = temp['userdata']
            if 'url' in metadata:
                url_main = metadata['url']
        if url_main and url_main != url:
            msg = str.format('ERROR ON PROCESSING {0}, REFERER: {1}, CODE: {2}', url, url_main, response.status)
        else:
            msg = str.format('ERROR ON PROCESSING {1}, CODE: {0}', response.status, url)
        self.log(msg, log.ERROR)

        if next_func:
            # 举例：如果获得价格信息失败，并不影响爬虫继续前进，去获得其它信息
            for req in next_func(reason.value.response, prev_success=False):
                yield req

    def parse_cat(self, response):
        def func(node):
            try:
                return self.reformat(node.xpath('text()').extract()[0])
            except (IndexError, TypeError):
                return None

        sel = Selector(response)
        tags = filter(lambda val: val,
                      (func(node) for node in sel.xpath(
                          '//div[@class="noheaderBreadcrumb" or @class="pageBreadcrumb"]/a[@href]')))
        if not tags:
            # 美国样式
            term_list = []
            for tmp in sel.xpath('//div[@id="breadcrumbs"]/a[@href]/text()').extract():
                term_list.append(tmp)
            for tmp in sel.xpath('//div[@id="breadcrumbs"]/text()').extract():
                term_list.append(tmp)

            tags = []
            for tmp in term_list:
                tmp = self.reformat(tmp)
                if not tmp:
                    continue
                tag = self.reformat(re.sub(r'^\s*/\s*', '', tmp))
                if tag:
                    tags.append(tag)

        tag_list = []
        for idx, tag_name in enumerate(tags):
            tag_list.append({'type': str.format('category-{0}', idx), 'name': tag_name.lower(), 'title': tag_name})

        referer = response.meta['coach-referer']
        self.url_cat_dict[referer] = tag_list

        if not tag_list:
            self.log(str.format('No category info found in referer: {0}', referer), log.WARNING)
        return response.meta['callback'](response.meta['stash'])

    def parse_details(self, response):
        # 确定所属国家
        region = None
        for tmp in self.spider_data['domains']:
            if self.spider_data['domains'][tmp] in response.url:
                region = tmp
                break
        if not region:
            return

        metadata = {'region': region, 'brand_id': self.spider_data['brand_id'], 'tags_mapping': {}, 'url': response.url}

        # 根据referer，获得category信息
        referer = response.request.headers['Referer']
        if referer not in self.url_cat_dict:
            return Request(url=referer, callback=self.parse_cat,
                           meta={'stash': response, 'coach-referer': referer, 'callback': self.parse_details},
                           errback=self.onerr, dont_filter=True)
        tag_list = self.url_cat_dict[referer]
        for tag in tag_list:
            metadata['tags_mapping'][tag['type']] = [{'name': tag['name'], 'title': tag['title']}]

        # 商品信息在var productJSONObject中
        mt = re.search(r'var\s+productJSONObject\s*=', response.body)
        if not mt:
            return
        try:
            data = json.loads(cm.extract_closure(response.body[mt.end():], "{", "}")[0].replace(r'\"',
                                                                                                '"').replace(r"\'", "'"))
        except(TypeError, IndexError, ValueError):
            return
        if 'style' not in data:
            return
        metadata['model'] = data['style']
        if 'productName' in data:
            metadata['name'] = self.reformat(data['productName'])

        try:
            metadata['color'] = [self.reformat(swatch['color']).lower() for swatch in data['swatchGroup']['swatches']
                                 if 'color' in swatch]
        except KeyError:
            pass

        # 价格信息
        try:
            for item in data['swatchGroup']['swatches']:
                if 'listPrice' in item:
                    metadata['price'] = self.reformat(item['listPrice'])
                    if 'unitPrice' in item:
                        metadata['price_discount'] = self.reformat(item['unitPrice'])
                    break
        except KeyError:
            pass

        # 图像链接
        image_urls = []
        try:
            image_host = 'http://s7d2.scene7.com/is/image/Coach/{0}{1}'
            style_for_images = data['styleForImages']
            for item in data['swatchGroup']['swatches']:
                for subimg in ('aImages', 'nImages', 'mImages'):
                    for tmp in [val['imageName'] for val in item[subimg]]:
                        if tmp not in image_urls:
                            image_urls.append(tmp)
            image_urls = [str.format(image_host, style_for_images, val) for val in image_urls]
        except KeyError:
            pass

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

    def parse_details_cn(self, response):
        """
        抓取中国的数据
        @param response:
        @return:
        """
        region = 'cn'
        metadata = {'region': region, 'brand_id': self.spider_data['brand_id'], 'tags_mapping': {}}

        # 根据referer，获得category信息
        referer = response.request.headers['Referer']
        if referer not in self.url_cat_dict:
            return Request(url=referer, callback=self.parse_cat,
                           meta={'stash': response, 'coach-referer': referer, 'callback': self.parse_details_cn},
                           errback=self.onerr, dont_filter=True)
        tag_list = self.url_cat_dict[referer]
        for tag in tag_list:
            metadata['tags_mapping'][tag['type']] = [{'name': tag['name'], 'title': tag['title']}]
        if tag_list:
            metadata['category'] = [tag_list[-1]['name']]

        sel = Selector(response)

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="styleCode" and @value]')
        if tmp:
            metadata['model'] = tmp[0]._root.attrib['value']
        if 'model' not in metadata or not metadata['model']:
            return

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="skuCode" and @value]')
        sku_code = None
        if tmp:
            sku_code = tmp[0]._root.attrib['value']
        if not sku_code:
            return

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="shareUrl" and @value]')
        if tmp:
            metadata['url'] = self.process_href(tmp[0]._root.attrib['value'], response.url)
        else:
            metadata['url'] = response.url

        tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="title" and @value]')
        if tmp:
            metadata['name'] = unicodify(tmp[0]._root.attrib['value'])

        # 价格信息
        return Request(url=self.spider_data['price_url'][region], method='POST', dont_filter=True,
                       body=str.format('skuCode={0}', sku_code), callback=self.get_info, errback=self.onerr,
                       headers={'Content-Type': 'application/x-www-form-urlencoded',
                                'Accept-Encoding': 'gzip,deflate,sdch',
                                'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                       meta={'userdata': metadata, 'next_func': self.get_info})

    def get_info(self, response, prev_success=True):
        metadata = response.meta['userdata']
        if prev_success:
            try:
                data = json.loads(response.body)
                price_set = set([])
                for key in ['retailPrice', 'skuPrice']:
                    if key not in data:
                        continue
                    try:
                        price_set.add(float(data[key]))
                    except (ValueError, TypeError):
                        continue
                if len(price_set) >= 2:
                    metadata['price'] = str(max(price_set))
                    metadata['price_discount'] = str(min(price_set))
                elif len(price_set) == 1:
                    metadata['price'] = str(list(price_set)[0])
            except (ValueError, KeyError):
                pass

        # 说明信息
        yield Request(url=self.spider_data['desc_url'][metadata['region']], method='POST', dont_filter=True,
                      body=str.format('styleCode={0}', metadata['model']), callback=self.get_images,
                      headers={'Content-Type': 'application/x-www-form-urlencoded',
                               'Accept-Encoding': 'gzip,deflate,sdch',
                               'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                      errback=self.onerr, meta={'userdata': metadata, 'next_func': self.get_images})

    def get_images(self, response, prev_success=True):
        metadata = response.meta['userdata']
        if prev_success:
            try:
                data = json.loads(response.body)
                if data['description'][0]['description']:
                    metadata['description'] = self.reformat(
                        re.sub(ur'<\s*li\s*/?>', u'\r', data['description'][0]['description']))
                if data['description'][0]['detail']:
                    metadata['details'] = self.reformat(
                        re.sub(ur'<\s*li\s*/?>', u'\r', data['description'][0]['detail']))
            except (ValueError, IndexError, KeyError):
                pass

        # 说明信息
        yield Request(url=self.spider_data['image_url'][metadata['region']], method='POST', dont_filter=True,
                      body=str.format('styleCode={0}', metadata['model'].lower()), callback=self.parse_final,
                      headers={'Content-Type': 'application/x-www-form-urlencoded',
                               'Accept-Encoding': 'gzip,deflate,sdch',
                               'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                      errback=self.onerr, meta={'userdata': metadata, 'next_func': self.parse_final})

    def parse_final(self, response, prev_success=True):
        metadata = response.meta['userdata']
        image_urls = []
        if prev_success:
            try:
                data = json.loads(response.body)
                for k in (val for val in data if re.search('[a-z]Images', val)):
                    image_urls.extend(data[k])
            except (ValueError, IndexError, KeyError):
                pass

        item = ProductItem()
        item['image_urls'] = image_urls
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

    @classmethod
    def is_offline(cls, response, spider=None):
        model = cls.fetch_model(response)

        if model:
            return False
        else:
            return True

    @classmethod
    def fetch_model(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        model = None
        if region != 'cn':
            try:
                # 商品信息在var productJSONObject中
                mt = re.search(r'var\s+productJSONObject\s*=', response.body)
                if mt:
                    data = json.loads(cm.extract_closure(response.body[mt.end():], "{", "}")[0].replace(r'\"',
                                                                                                        '"').replace(r"\'",
                                                                                                                     "'"))
                    if 'style' in data:
                        model = data['style']
            except(TypeError, IndexError, ValueError):
                pass
        else:
            try:
                tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="styleCode" and @value]')
                if tmp:
                    model = tmp[0]._root.attrib['value']
            except(TypeError, IndexError):
                pass

        return model

    @classmethod
    def fetch_name(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        name = None
        if region != 'cn':
            try:
                # 商品信息在var productJSONObject中
                mt = re.search(r'var\s+productJSONObject\s*=', response.body)
                if mt:
                    data = json.loads(cm.extract_closure(response.body[mt.end():], "{", "}")[0].replace(r'\"',
                                                                                                        '"').replace(r"\'",
                                                                                                                     "'"))
                    if 'productName' in data:
                        name = cls.reformat(data['productName'])
            except(TypeError, IndexError, ValueError):
                pass
        else:
            try:
                tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="title" and @value]')
                if tmp:
                    name = unicodify(tmp[0]._root.attrib['value'])
            except(TypeError, IndexError):
                pass

        return name

    @classmethod
    def fetch_price(cls, response, spider=None):
        sel = Selector(response)
        ret = {}

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        old_price = None
        new_price = None
        if region != 'cn':
            # 商品信息在var productJSONObject中
            mt = re.search(r'var\s+productJSONObject\s*=', response.body)
            if mt:
                try:
                    data = json.loads(cm.extract_closure(response.body[mt.end():], "{", "}")[0].replace(r'\"',
                                                                                                        '"').replace(r"\'",
                                                                                                                 "'"))
                except(TypeError, IndexError, ValueError):
                    return ret
                # 价格信息
                try:
                    for item in data['swatchGroup']['swatches']:
                        if 'listPrice' in item:
                            old_price = cls.reformat(item['listPrice'])
                            if 'unitPrice' in item:
                                new_price = cls.reformat(item['unitPrice'])
                            break
                except KeyError:
                    pass
        else:
            tmp = sel.xpath('//div[@id="hidden_sku_value"]/input[@id="skuCode" and @value]')
            sku_code = None
            if tmp:
                sku_code = tmp[0]._root.attrib['value']
            if sku_code:
                # 价格信息
                return Request(url=cls.spider_data['price_url'][region], method='POST', dont_filter=True,
                               body=str.format('skuCode={0}', sku_code), callback=cls.fetch_price_request,
                               errback=spider.onerror,
                               headers={'Content-Type': 'application/x-www-form-urlencoded',
                                        'Accept-Encoding': 'gzip,deflate,sdch',
                                        'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                               meta=response.meta)

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_price_request(cls, response):
        sel = Selector(response)
        ret = {}

        old_price = None
        new_price = None
        try:
            data = json.loads(response.body)
            price_set = set([])
            for key in ['retailPrice', 'skuPrice']:
                if key not in data:
                    continue
                try:
                    price_set.add(float(data[key]))
                except (ValueError, TypeError):
                    continue
            if len(price_set) >= 2:
                old_price = str(max(price_set))
                new_price = str(min(price_set))
            elif len(price_set) == 1:
                old_price = str(list(price_set)[0])
        except (ValueError, KeyError):
            pass

        if old_price:
            ret['price'] = old_price
        if new_price:
            ret['price_discount'] = new_price

        return ret

    @classmethod
    def fetch_description(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        description = None
        if region != 'cn':
            # TODO 没找到原爬虫解析非中国的单品描述的代码
            pass
        else:
            # 说明信息
            return Request(url=cls.spider_data['desc_url'][region], method='POST', dont_filter=True,
                           body=str.format('styleCode={0}', cls.fetch_model(response)),
                           callback=cls.fetch_description_request,
                           headers={'Content-Type': 'application/x-www-form-urlencoded',
                                    'Accept-Encoding': 'gzip,deflate,sdch',
                                    'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                           errback=spider.onerror, meta=response.meta)

        return description

    @classmethod
    def fetch_description_request(cls, response):
        sel = Selector(response)

        description = None
        try:
            data = json.loads(response.body)
            if data['description'][0]['description']:
                description = cls.reformat(
                    re.sub(ur'<\s*li\s*/?>', u'\r', data['description'][0]['description']))
        except (ValueError, IndexError, KeyError):
            pass

        return description

    @classmethod
    def fetch_details(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        details = None
        if region != 'cn':
            # TODO 没找到原爬虫解析非中国的单品描述的代码
            pass
        else:
            # 说明信息
            return Request(url=cls.spider_data['desc_url'][region], method='POST', dont_filter=True,
                           body=str.format('styleCode={0}', cls.fetch_model(response)),
                           callback=cls.fetch_description_request,
                           headers={'Content-Type': 'application/x-www-form-urlencoded',
                                    'Accept-Encoding': 'gzip,deflate,sdch',
                                    'X-Requested-With': 'XMLHttpRequest', 'Accept': '*/*'},
                           errback=spider.onerror, meta=response.meta)

        return details

    @classmethod
    def fetch_details_request(cls, response):
        sel = Selector(response)

        details = None
        try:
            data = json.loads(response.body)
            if data['description'][0]['detail']:
                details = cls.reformat(
                    re.sub(ur'<\s*li\s*/?>', u'\r', data['description'][0]['detail']))
        except (ValueError, IndexError, KeyError):
            pass

        return details

    @classmethod
    def fetch_color(cls, response, spider=None):
        sel = Selector(response)

        region = None
        if 'userdata' in response.meta:
            region = response.meta['userdata']['region']
        else:
            region = response.meta['region']

        colors = []
        if region != 'cn':
            try:
                # 商品信息在var productJSONObject中
                mt = re.search(r'var\s+productJSONObject\s*=', response.body)
                if mt:
                    data = json.loads(cm.extract_closure(response.body[mt.end():], "{", "}")[0].replace(r'\"',
                                                                                                        '"').replace(r"\'", "'"))
                    colors = [cls.reformat(swatch['color']).lower() for swatch in data['swatchGroup']['swatches']
                              if 'color' in swatch]
            except (KeyError, ValueError, TypeError, IndexError):
                colors = None
                pass
        else:
            # TODO 没找到原爬虫解析中国的单品颜色的代码
            pass

        return colors
