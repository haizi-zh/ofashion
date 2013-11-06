# coding=utf-8
import json
import os
import datetime
import re
from scrapy import log
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
import global_settings as glob
import common as cm
from scrapper.items import ProductItem
import copy

__author__ = 'Zephyre'

chanel_data = {'base_url': {'cn': 'zh_CN', 'us': 'en_US', 'fr': 'fr_FR', 'it': 'it_IT', 'uk': 'en_GB', 'hk': 'en_HK',
                            'jp': 'ja_JP', 'kr': 'ko_KR', 'au': 'en_AU', 'sg': 'en_SG', 'ca': 'en_CA', 'de': 'de_DE',
                            'es': 'es_ES', 'ru': 'ru_RU', 'br': 'pt_BR'},
               'fashion_term': {'cn': 'fashion', 'us': 'fashion', 'it': 'moda', 'fr': 'mode', 'uk': 'fashion',
                                'hk': 'fashion',
                                'jp': 'fashion', 'kr': 'fashion', 'au': 'fashion', 'sg': 'fashion', 'ca': 'fashion',
                                'de': 'mode', 'es': 'moda', 'ru': 'fashion', 'br': 'moda'},
               'pricing': 'https://secure.chanel.com/global-service/frontend/pricing/%s/fashion/%s/?format=json',
               'host': 'http://www-cn.chanel.com',
               'supported_regions': {'cn', 'us', 'fr', 'it', 'uk', 'hk', 'jp', 'kr', 'au', 'sg', 'ca', 'de', 'es', 'ru',
                                     'br'},
               'brand_id': 10074, 'brandname_e': 'Chanel', 'brandname_c': u'香奈儿', 'brandname_s': 'chanel',
               'description_hdr': {u'产品介绍', u'Description'},
               'details_hdr': {u'使用方法', u'How to use', u"Conseils d'utilisation", u'How-to'}
}


def get_spider_data():
    return dict((k, chanel_data[k]) for k in chanel_data if
                k in ('host', 'supported_regions', 'brand_id', 'brandname_e', 'brandname_c', 'brandname_s'))


def create_spider():
    return ChanelSpider()


class ChanelSpider(CrawlSpider):
    name = 'chanel'
    allowed_domains = ['chanel.com']

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.name = str.format('{0}-{1}', self.name, region)
        if region not in chanel_data['supported_regions']:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []

        region_code = chanel_data['base_url'][region]
        self.rules = (
            Rule(SgmlLinkExtractor(allow=(str.format(r'chanel\.com/{0}/.+\?sku=\d+$', region_code), )),
                 callback='parse_sku1'),
            Rule(SgmlLinkExtractor(allow=(str.format(r'chanel\.com/{0}/.+/sku/\d+$', region_code), )),
                 callback='parse_sku2'),
            Rule(SgmlLinkExtractor(
                allow=(str.format(r'chanel\.com/{0}/{1}/[^/]+/[^/]+/.*(?<=/)s\.[^/]+\.html', region_code,
                                  chanel_data['fashion_term'][region]), )),
                 callback='parse_fashion'),
            Rule(SgmlLinkExtractor(allow=(r'.+', ),
                                   deny=(str.format(r'chanel\.com(?!/{0}/)', region_code), )))
        )
        self._compile_rules()

        self.log(str.format('Fetching data for {0}', region), log.INFO)
        return [Request(url=str.format('{0}/{1}/', chanel_data['host'], region_code))]
        # return [Request(
        #     url='http://www-cn.chanel.com/fr_FR/mode/produits/lunettes/g/s.lunettes-de-soleil-en-nylon-et.13K.A71014X02128S0133.sun.run.html')]
        # return [Request(url='http://www-cn.chanel.com/zh_CN/fashion/products/eyewear/g/s.acetate-pilot-inspired-sunglasses.13K.A71011X02120S0133.sun.run.html')]

    def parse_fashion(self, response):
        self.log(str.format('PARSE_FASHION: {0}', response.url), level=log.DEBUG)
        mt = re.search(r'chanel\.com/([^/]+)/', response.url)
        region = None
        for a, b in chanel_data['base_url'].items():
            if b == mt.group(1):
                region = a
                break
        if not region:
            return None

        metadata = {'region': region, 'brand_id': chanel_data['brand_id'],
                    'brandname_e': chanel_data['brandname_e'], 'url': response.url,
                    'brandname_c': chanel_data['brandname_c'], 'tags_mapping': {}, 'extra': {},
                    'gender': set([]), 'category': set([]), 'texture': set([]), 'color': set([])}

        mt = re.search(r'var\s+settings', response._body)
        if not mt:
            return None
        content = cm.extract_closure(response._body[mt.start():], '{', '}')[0]
        try:
            data = json.loads(content)
        except ValueError:
            return None

        try:
            metadata['pricing_service'] = data['servicesURL']['pricing']
        except KeyError:
            metadata['pricing_service'] = None

        # images
        metadata['image_urls'] = set([])
        # hxs = HtmlXPathSelector(response)
        # temp = hxs.select('//div[contains(@class, "productimage")]/img[@src]')
        # if len(temp) > 0:
        #     image_url = temp[0]._root.attrib['src']
        #     if re.search(r'\.medium\.[^\.]+$', image_url):
        #         image_url = re.sub(r'\.medium\.([^\.]+)$', r'.hi.\1', image_url)
        #     metadata['image_urls'].append(cm.norm_url(image_url, host=chanel_data['host']))
        #
        # temp = hxs.select('//div[contains(@class, "lookimage")]/img[@src]')
        # if len(temp) > 0:
        #     image_url = temp[0]._root.attrib['src']
        #     if re.search(r'\.look-sheet\.medium\.[^\.]+$', image_url):
        #         image_url = re.sub(r'\.look-sheet\.medium\.([^\.]+)$', r'.hi.\1', image_url)
        #     metadata['image_urls'].append(cm.norm_url(image_url, host=chanel_data['host']))

        if 'detailsGridJsonUrl' in data['sectionCache']:
            temp = data['sectionCache']['detailsGridJsonUrl']
            if re.search(r'^http://', temp):
                url = temp
            else:
                url = str.format('{0}/{1}', chanel_data['host'], temp)
            return Request(url=url, meta={'userdata': metadata}, callback=self.parse_json_request)
        else:
            return self.parse_json(metadata, data['sectionCache'])

    def parse_json_request(self, response):
        metadata = response.meta['userdata']
        return self.parse_json(metadata, json.loads(response._body))

    def process_image_url(self, href):
        if re.search(r'\.([a-zA-Z]{3})\.fashionImg(\.look-sheet)*$', href):
            href = re.sub(r'\.([a-zA-Z]{3})\.fashionImg(\.look-sheet)*$', r'.\1.fashionImg.hi.\1', href)
        elif re.search(r'\.[a-zA-Z]{3}$', href):
            href = re.sub(r'\.([a-zA-Z]{3})$', r'.\1.fashionImg.hi.\1', href)
        else:
            href = None

        if href:
            return cm.norm_url(href, host=chanel_data['host'])
        else:
            return href

    def parse_json(self, metadata, json_data):
        for url, product_info in json_data.items():
            if url not in metadata['url']:
                continue

            cat_idx = 0
            cat_list = []
            for temp in product_info['navItems']:
                if 'title' not in temp:
                    continue
                cat = cm.unicodify(temp['title'])
                if not cat or cat in cat_list:
                    continue
                cat_idx += 1
                cat_list.append(cat)
                cat_name = str.format('category-{0}', cat_idx)
                metadata['extra'][cat_name] = [cat]
                metadata['tags_mapping'][cat_name] = [{'name': cat, 'title': cat}]
            if len(cat_list) > 0 and cat_list[-1]:
                metadata['category'].add(cat_list[-1])

            # images
            image_data = product_info['data']
            href = None
            try:
                href = image_data['zoom']['imgsrc']
            except KeyError:
                if 'imgsrc' in image_data:
                    href = image_data['imgsrc']
            href = self.process_image_url(href)
            if href:
                metadata['image_urls'].add(href)

            # # module images
            # if 'modulesJsonUrl' in image_data:
            #     metadata['modules_url'] = cm.norm_url(image_data['modulesJsonUrl'], host=chanel_data['host'])
            # else:
            #     metadata['modules_url'] = None
            metadata['modules_url'] = None

            info = product_info['data']['details']['information']

            if 'ref' in info:
                return self.func1(metadata, info)
            else:
                results = []
                for t1 in info:
                    m1 = copy.deepcopy(metadata)
                    for t2 in t1['datas']:
                        m2 = copy.deepcopy(m1)
                        results.append(self.func1(m2, t2))
                return results
        return None

    def func2(self, metadata):
        modules_url = metadata.pop('modules_url')
        if modules_url:
            return Request(url=modules_url, meta={'userdata': metadata}, callback=self.parse_modules)
        else:
            return self.init_item(metadata)

    def parse_modules(self, response):
        metadata = response.meta['userdata']
        json_data = json.loads(response._body)
        hrefs = []
        for data_body in json_data:
            try:
                hrefs.extend(val['imgsrc'] for val in data_body['data']['images'])
            except (ValueError, KeyError):
                pass
            try:
                hrefs.append(data_body['data']['imgsrc'])
            except KeyError:
                pass

        for url in hrefs:
            url = self.process_image_url(url)
            if url:
                metadata['image_urls'].add(url)

        return self.init_item(metadata)

    def reformat(self, text):
        """
        格式化地址字符串，将多余的空格、换行、制表符等合并
        """
        if text is None:
            return None
        text = cm.html2plain(text.strip())
        # <br/>换成换行符
        text = re.subn(ur'<\s*br\s*/?>', u'\r\n', text)[0]
        # 去掉多余的标签
        text = re.subn(ur'<[^<>]*?>', u'', text)[0]
        # 换行转换
        text = re.subn(ur'(?:[\r\n])+', ', ', text)[0]
        return text


    def func1(self, metadata, info):
        pricing_service = metadata.pop('pricing_service')

        if 'description' in info:
            metadata['description'] = self.reformat(cm.unicodify(info['description']))
        if 'title' in info:
            metadata['name'] = cm.unicodify(info['title'])

        if 'ref' in info:
            metadata['model'] = cm.unicodify(info['ref'])
        else:
            info = info['data'][0]
            metadata['model'] = cm.unicodify(info['ref'])

        # price
        if pricing_service and 'refPrice' in info:
            url = chanel_data['pricing'] % (chanel_data['base_url'][metadata['region']], info['refPrice'])
            return Request(url=url, meta={'userdata': metadata, 'handle_httpstatus_list': [400]},
                           callback=self.parse_price)
        else:
            return self.func2(metadata)

    def init_item(self, metadata):
        metadata['color'] = list(metadata['color'])
        metadata['texture'] = list(metadata['texture'])
        metadata['gender'] = list(metadata['gender'])
        metadata['category'] = list(metadata['category'])
        metadata['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        item = ProductItem()
        item['image_urls'] = list(metadata.pop('image_urls'))
        item['url'] = metadata['url']
        item['model'] = metadata['model']
        item['metadata'] = metadata
        return item

    def parse_price(self, response):
        metadata = response.meta['userdata']
        if response.status == 200:
            price_data = json.loads(response._body)
            if len(price_data) > 0:
                try:
                    price_data = price_data[0]['price']
                    if 'amount' in price_data and 'currency-symbol' in price_data:
                        metadata['price'] = price_data['amount']
                        metadata['currency'] = price_data['currency-symbol']
                    else:
                        metadata['price'] = price_data['formatted-amount']
                except (IndexError, KeyError):
                    pass
        return self.func2(metadata)

    def parse_sku1(self, response):
        self.log(str.format('PARSE_SKU1: {0}', response.url), level=log.DEBUG)
        mt = re.search(r'chanel\.com/([^/]+)/', response.url)
        region = None
        for a, b in chanel_data['base_url'].items():
            if b == mt.group(1):
                region = a
                break
        if not region:
            return None

        mt = re.search(r'\?sku=(\d+)$', response.url)
        if not mt:
            return None
        model = mt.group(1)

        metadata = {'region': region, 'brand_id': chanel_data['brand_id'],
                    'brandname_e': chanel_data['brandname_e'], 'model': model, 'url': response.url,
                    'brandname_c': chanel_data['brandname_c'], 'tags_mapping': {}, 'extra': {},
                    'gender': set([]), 'category': set([]), 'texture': set([]), 'color': set([])}

        hxs = HtmlXPathSelector(response)
        cat_idx = 0
        cat_list = []
        for node in hxs.select('//div[contains(@class,"trackingSettings")]/span[@class]'):
            cat = cm.unicodify(node._root.text)
            if not cat:
                continue
            if node._root.attrib['class'] == 'WT_cg_s':
                metadata['category'].add(cat)
            if cat in cat_list:
                continue

            cat_idx += 1
            cat_list.append(cat)
            cat_name = str.format('category-{0}', cat_idx)
            metadata['extra'][cat_name] = [cat]
            metadata['tags_mapping'][cat_name] = [{'name': cat, 'title': cat}]
            if u'男士' in cat:
                metadata['gender'].add(u'male')
            if u'女士' in cat:
                metadata['gender'].add(u'female')

        temp = hxs.select('//div[@class="productName"]')
        name_list = []
        if len(temp) > 0:
            product_name = temp[0]
            temp = product_name.select('./h1[@class="family"]/span[@class="familyText"]')
            if len(temp) > 0:
                name = cm.unicodify(temp[0]._root.text)
                if name:
                    name_list.append(name)
                name = u', '.join([cm.unicodify(val.text) for val in temp[0]._root.iterdescendants() if
                                   val.text and val.text.strip()])
                if name:
                    name_list.append(name.strip())
            temp = product_name.select('./h2[@class="name"]')
            if len(temp) > 0:
                name = cm.unicodify(temp[0]._root.text)
                if name:
                    name_list.append(name)
                name = u', '.join([cm.unicodify(val.text) for val in temp[0]._root.iterdescendants() if
                                   val.text and val.text.strip()])
                if name:
                    name_list.append(name.strip())
        name = u' - '.join(name_list)
        metadata['name'] = name if name else None

        # Description and details
        temp = hxs.select('//div[@class="tabHolderFullWidth tabHolder"]')
        if len(temp) > 0:
            content_node = temp[0]
            content_map = {}
            for node in content_node.select('./div[@class="tabs"]//a[@rel]'):
                temp = cm.unicodify(node._root.text)
                if temp and temp in chanel_data['description_hdr']:
                    content_map['description'] = node._root.attrib['rel']
                if temp and temp in chanel_data['details_hdr']:
                    content_map['details'] = node._root.attrib['rel']

            for term in ('description', 'details'):
                if term in content_map:
                    temp = content_node.select(str.format('./div[@id="{0}"]', content_map[term]))
                    if len(temp) > 0:
                        content_list = []
                        content = cm.unicodify(temp[0]._root.text)
                        if content:
                            content_list.append(content)
                        content_list.extend(
                            [cm.unicodify(val.text) for val in temp[0]._root.iterdescendants() if
                             val.text and val.text.strip()])
                        metadata[term] = u', '.join(content_list)

        # Images
        # image_urls = []
        # for node in hxs.select('//div[@class="major productImg"]/img[@src]'):
        #     href = node._root.attrib['src']
        #     if re.search(r'^http://', href):
        #         image_urls.append(href)
        #     else:
        #         image_urls.append(str.format('{0}/{1}', chanel_data['host'], href))
        # image_urls = list(set([re.sub(r'\.+', '.', val) for val in image_urls]))

        image_urls = list(set(cm.norm_url(node._root.attrib['src'], chanel_data['base_url'])
                              for node in hxs.select('//div[@class="major productImg"]/img[@src]') if
                              node._root.attrib['src'] and node._root.attrib['src'].strip()))

        metadata['color'] = list(metadata['color'])
        metadata['texture'] = list(metadata['texture'])
        metadata['gender'] = list(metadata['gender'])
        metadata['category'] = list(metadata['category'])

        metadata['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if 'model' in metadata:
            item = ProductItem()
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            return item
        else:
            return None

    def parse_sku2(self, response):
        self.log(str.format('PARSE_SKU2: {0}', response.url), level=log.DEBUG)
        mt = re.search(r'chanel\.com/([^/]+)/', response.url)
        region = None
        for a, b in chanel_data['base_url'].items():
            if b == mt.group(1):
                region = a
                break
        if not region:
            return None

        mt = re.search(r'/sku/(\d+)$', response.url)
        if not mt:
            return None
        model = mt.group(1)

        metadata = {'region': region, 'brand_id': chanel_data['brand_id'],
                    'brandname_e': chanel_data['brandname_e'], 'model': model, 'url': response.url,
                    'brandname_c': chanel_data['brandname_c'], 'tags_mapping': {}, 'extra': {},
                    'gender': set([]), 'category': set([]), 'texture': set([]), 'color': set([])}

        hxs = HtmlXPathSelector(response)
        cat_idx = 0
        cat_list = []
        for node in hxs.select('//div[contains(@class,"trackingSettings")]/span[@class]'):
            cat = cm.unicodify(node._root.text)
            if not cat:
                continue
            if node._root.attrib['class'] == 'WT_cg_s':
                metadata['category'].add(cat)
            if cat in cat_list:
                continue

            cat_idx += 1
            cat_list.append(cat)
            cat_name = str.format('category-{0}', cat_idx)
            metadata['extra'][cat_name] = [cat]
            metadata['tags_mapping'][cat_name] = [{'name': cat, 'title': cat}]
            if u'男士' in cat or re.search(r'\bman\b', cat, flags=re.I) or re.search(r'\bmen\b', cat, flags=re.I) or \
                    re.search(r'\bhomme\b', cat, flags=re.I) or re.search(r'\buomo\b', cat, flags=re.I) or \
                    re.search(r'\bhombre\b', cat, flags=re.I) or u'男性' in cat or \
                    re.search(r'\bherren\b', cat, flags=re.I) or re.search(r'\bmasculinos\b', cat, flags=re.I) or \
                    re.search(r'\bhomens \b', cat, flags=re.I):
                # Masculinos
                metadata['gender'].add(u'male')
            if u'女士' in cat or re.search(r'\bwoman\b', cat, flags=re.I) or re.search(r'\bwomen\b', cat, flags=re.I) or \
                    re.search(r'\bfemme\b', cat, flags=re.I) or re.search(r'\bdonna\b', cat, flags=re.I) or \
                    re.search(r'\bmujer\b', cat, flags=re.I) or u'女性' in cat or \
                    re.search(r'\bdamen\b', cat, flags=re.I) or re.search(r'\bfemeninos\b', cat, flags=re.I) or \
                    re.search(r'\bmulheres \b', cat, flags=re.I):
                # Femeninos
                metadata['gender'].add(u'female')

        temp = hxs.select('//div[contains(@class, "product_detail_container")]')
        name_list = []
        if len(temp) > 0:
            product_name = temp[0]
            temp = product_name.select('./h1[@class="product_name"]')
            if len(temp) > 0:
                name = cm.unicodify(temp[0]._root.text)
                if name:
                    name_list.append(name)
            temp = product_name.select('./h2[@class="product_subtitle"]')
            if len(temp) > 0:
                name = cm.unicodify(temp[0]._root.text)
                if name:
                    name_list.append(name)

            temp = product_name.select('.//h3[@class="product_price"]')
            if len(temp) > 0:
                metadata['price'] = cm.unicodify(temp[0]._root.text)
        name = u' - '.join(name_list)
        metadata['name'] = name if name else None

        # Description and details
        temp = hxs.select('//div[@class="description_container"]')
        if len(temp) > 0:
            content_node = temp[0]
            content_map = {}
            for node in content_node.select('.//div[@class="accordion-heading"]/a[@href]'):
                temp = cm.unicodify(node._root.text)
                if temp and temp in chanel_data['description_hdr']:
                    content_map['description'] = re.sub(r'^#', '', node._root.attrib['href'])
                if temp and temp in chanel_data['details_hdr']:
                    content_map['details'] = re.sub(r'^#', '', node._root.attrib['href'])

            for term in ('description', 'details'):
                if term in content_map:
                    temp = content_node.select(str.format('.//div[@id="{0}"]', content_map[term]))
                    if len(temp) > 0:
                        content_list = []
                        content = cm.unicodify(temp[0]._root.text)
                        if content:
                            content_list.append(content)
                        content_list.extend(
                            [cm.unicodify(val.text) for val in temp[0]._root.iterdescendants() if
                             val.text and val.text.strip()])
                        metadata[term] = u', '.join(content_list)

        # Images
        image_urls = list(set(cm.norm_url(node._root.attrib['src'], chanel_data['base_url']) for node in hxs.select(
            '//section[@class="product_image_container"]/img[@src and @class="product_image"]') if
                              node._root.attrib['src'] and node._root.attrib['src'].strip()))

        metadata['color'] = list(metadata['color'])
        metadata['texture'] = list(metadata['texture'])
        metadata['gender'] = list(metadata['gender'])
        metadata['category'] = list(metadata['category'])

        metadata['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if 'model' in metadata:
            item = ProductItem()
            item['image_urls'] = image_urls
            item['url'] = metadata['url']
            item['model'] = metadata['model']
            item['metadata'] = metadata
            return item
        else:
            return None