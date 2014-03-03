# coding=utf-8
import copy

import os
import datetime
import re
import urllib
from scrapy import log
from scrapy.contrib.spiders import CrawlSpider
from scrapy.http import Request
from scrapy.selector import HtmlXPathSelector
import global_settings

__author__ = 'Zephyre'

# TODO 现在这个lv的爬虫没法跑起来

lv_data = {'host': {'cn': 'http://m.louisvuitton.cn'},
           'start_url': {'cn': 'http://m.louisvuitton.cn/mobile/zhs_CN/%E4%BA%A7%E5%93%81%E7%B3%BB%E5%88%97',
                         'us': 'http://m.louisvuitton.com',
                         'fr': 'http://m.louisvuitton.fr',
                         'de': 'http://m.louisvuitton.de',
                         'es': 'http://m.louisvuitton.es',
                         'it': 'http://m.louisvuitton.it'},
           'image_host': 'http://images.louisvuitton.com/content/dam/lv/online/picture/',
           'data_host': {
               'cn': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=zhs_CN&cache=Medium&category=',
               'us': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=eng_US&cache=Medium&category=',
               'fr': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=fra_FR&cache=Medium&category=',
               'de': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=deu_DE&cache=Medium&category=',
               'es': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=esp_ES&cache=Medium&category=',
               'it': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=ita_IT&cache=Medium&category='},
           'brand_id': 10226, 'brandname_e': 'Louis Vuitton', 'brandname_c': u'路易威登', 'brandname_s': 'lv'}

post_keys = ("_dyncharset",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProductsSuccessUrl",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProductsSuccessUrl",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProducts",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProducts",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.onlyResults",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.onlyResults",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageNumber",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageNumber",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender",
             # "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category",
             # "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.functionik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.functionik",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subcategoryik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subcategoryik",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.shapeik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.shapeik",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subsubcategoryik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subsubcategoryik",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.color",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.color",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.collectionik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.collectionik",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.soldonline",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.soldonline",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.casematerialik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.casematerialik",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.carryonsize",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.carryonsize",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.lineik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.lineik",
             "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.typeik",
             "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.typeik",
             "_DARGS")

basic_query = {"_dyncharset": "UTF-8",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProductsSuccessUrl": "/ajax/productFinderResults.jsp?storeLang=zhs_CN",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProductsSuccessUrl": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProducts": "--",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.findProducts": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.onlyResults": "false",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.onlyResults": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageNumber": "1",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageNumber": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender": "+",
               # "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category": '',
               # "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category": '+',
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.functionik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.functionik": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subcategoryik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subcategoryik": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.shapeik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.shapeik": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subsubcategoryik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subsubcategoryik": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.color": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.color": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.collectionik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.collectionik": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.soldonline": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.soldonline": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.casematerialik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.casematerialik": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.carryonsize": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.carryonsize": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.lineik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.lineik": "+",
               "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.typeik": "",
               "_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.typeik": "+",
               "_DARGS": "/mobile/collections/catalogBrowsing/productFinder.jsp"}

details_pattern = {'model_pattern': {'cn': r'产品编号\s*([a-zA-Z0-9]+)', 'us': r'sku\s*([a-zA-Z0-9]+)',
                                     'fr': r'sku\s*([a-zA-Z0-9]+)', 'de': r'REF\s*([a-zA-Z0-9]+)',
                                     'es': r'REFERENCIA\s*([a-zA-Z0-9]+)', 'it': r'CODE\s*([a-zA-Z0-9]+)'}}


def make_post_str(post_data):
    """
    给定post数据，生成post字符串。
    :param post_data:
    """
    return u'&'.join(
        unicode.format(u'{0}={1}', urllib.quote_plus(item[0]), urllib.quote_plus(item[1])) for item in
        post_data.items())

    # def func(val):
    #     if val == "+":
    #         return val
    #     else:
    #         return urllib.quote(val, safe="")
    #
    # # category比较特殊。如果为空，则不能将其加入post数据中
    # post_data = post_data.copy()
    # if not post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category"]:
    #     del post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category"]
    #     del post_data["_D:/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category"]
    #
    # return u'&'.join(unicode.format(u'{0}={1}', func(val[0]), func(val[1])) for val
    #                  in [[k, post_data[k]] for k in post_keys if k in post_data])
    # # return u'&'.join(map(lambda k: unicode.format(u'{0}={1}', func(k), func(post_data.get(k))), post_keys))


def create_spider():
    return LouisVuittonSpider()


def get_job_path():
    return os.path.normpath(
        os.path.join(getattr(global_settings, 'STORAGE_PATH'),
                     unicode.format(u'products/crawl/{0}', lv_data['brandname_s'])))


def get_log_path():
    return os.path.normpath(os.path.join(global_settings.STORAGE_PATH, u'products/log',
                                         unicode.format(u'{0}_{1}_{2}.log', lv_data['brand_id'],
                                                        lv_data['brandname_s'],
                                                        datetime.datetime.now().strftime('%Y%m%d'))))


class LouisVuittonSpider(CrawlSpider):
    name = 'louis_vuitton'
    region = None

    def __init__(self, region=None):
        super(CrawlSpider, self).__init__()
        self.region = region

    def start_requests(self):
        region = self.crawler.settings['REGION']
        self.log(str.format('Fetching data for {0}', region), log.INFO)
        if region in lv_data['start_url']:
            return [Request(url=lv_data['start_url'][region], dont_filter=True)]
        else:
            self.log(str.format('No data for {0}', region), log.WARNING)
            return []

    def parse(self, response):
        self.log(unicode.format(u'PARSE_HOME: URL={0}', response.url), level=log.DEBUG)
        if 'm.louisvuitton.cn' in response.url:
            metadata = {'region': 'cn'}
        elif 'm.louisvuitton.com' in response.url:
            metadata = {'region': 'us'}
        elif 'm.louisvuitton.fr' in response.url:
            metadata = {'region': 'fr'}
        elif 'm.louisvuitton.de' in response.url:
            metadata = {'region': 'de'}
        elif 'm.louisvuitton.es' in response.url:
            metadata = {'region': 'es'}
        elif 'm.louisvuitton.it' in response.url:
            metadata = {'region': 'it'}

        metadata['tags_mapping'] = {}
        metadata['extra'] = {}
        region = metadata['region']

        hxs = HtmlXPathSelector(response)
        for div in hxs.select(
                '//div[@class="content"]/div[@id="wrapper"]/div[contains(@class, "woman") or contains(@class, "man")]'):
            m = metadata.copy()
            post_data = basic_query.copy()
            m['post_data'] = post_data
            if 'woman' in div._root.attrib['class']:
                m['gender'] = 'female'
                post_data['/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender'] = 'women'
                url = lv_data['data_host'][region] + 'women'
            elif 'man' in div._root.attrib['class']:
                m['gender'] = 'male'
                post_data['/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender'] = 'men'
                url = lv_data['data_host'][region] + 'men'

            for cat_node in div.select('./div[@class="menuWrapper"]//ul//li/a[@onclick and @href]'):
                temp = cat_node.select('.//span/h3')
                if len(temp) == 0:
                    continue
                title = temp[0]._root.text
                temp = cat_node._root.attrib['onclick']
                idx = temp.find('lid')
                if idx == -1:
                    continue
                else:
                    temp = temp[idx:]
                    # mt = re.search(r'{[^{}]+}', cat_node._root.attrib['onclick'])
                mt = re.search('/([^/]+)\'\\s*}', temp)
                if not mt:
                    continue
                cat = mt.group(1)
                m1 = copy.deepcopy(m)
                m1['extra']['category-1'] = cat
                m1['tags_mapping']['category-1'] = [{'name': cat, 'title': title}]
                m1['filter_idx'] = 0
                m1['post_data']['/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId'] = cat
                m1['data_url'] = url

                if m1['gender'] != 'female' or cat != 'handbags':
                    continue

                yield Request(url=m1['data_url'], method='POST', body=make_post_str(m1['post_data']),
                              meta={'userdata': m1}, callback=self.parse_filter, dont_filter=True)

    def parse_filter(self, response):
        metadata = response.meta['userdata']
        hxs = HtmlXPathSelector(response)
        div_list = hxs.select(
            '//div[@id="facetsWrapperTable"]/div[@id="facetsWrapperLine"]/div[contains(@class, "facetBloc")]')
        if metadata['filter_idx'] >= len(div_list):
            metadata['post_data']['/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageNumber'] = str(1)
            # yield Request(url=metadata['data_url'], method='POST', body=make_post_str(metadata['post_data']),
            # meta={'userdata': metadata}, callback=self.parse_product_list)
        else:
            div = div_list[metadata['filter_idx']]
            div_id = div._root.attrib['id']
            if div_id in ('subcategoryik', 'typeik', 'subsubcategoryik', 'shapeik', 'casematerialik', 'functionik',
                          'collectionik'):
                for item in div.select('.//div[@class="radioFacet"]/div[@onclick]'):
                    ret = item.select('./input[@id]')
                    if len(ret) == 0:
                        continue

                    m = copy.deepcopy(metadata)
                    if div_id == 'subcategoryik':
                        k = str.format('/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.{0}',
                                       div_id)
                        v = ret[0]._root.attrib['value']
                        # m['post_data'][
                        #     '/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.category'] = \
                        #     m['post_data'][
                        #         '/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId']
                        m['post_data'][k] = v
                    else:
                        k = str.format('/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.{0}',
                                       div_id)
                        v = ret[0]._root.attrib['value']
                        m['post_data'][k] = v

                    if v != 'top-handles':
                        continue

                    tag_type = str.format('{0}-{1}', div_id, metadata['filter_idx'])
                    m['extra'][tag_type] = v
                    ret = item.select('./label/h2')
                    if len(ret) > 0 and ret[0]._root.text:
                        m['tags_mapping'][tag_type] = [{'name': v, 'title': ret[0]._root.text}]
                    m['filter_idx'] += 1
                    yield Request(url=m['data_url'], method='POST', body=make_post_str(m['post_data']),
                                  meta={'userdata': m},
                                  callback=self.parse_filter, dont_filter=True)
            elif div_id in ('lineik', 'color'):
                for item in div.select(
                        './/div[@id="pictureFacetWrapper"]/div[@class="pictureFacet"]/a[@href and @data-type="lineik"]'):
                    ret = item.select('./img[@alt]')
                    if len(ret) == 0:
                        continue
                    title = ret[0]._root.attrib['alt']
                    k = str.format('/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.{0}',
                                   div_id)
                    v = item._root.attrib['data-value']
                    m = copy.deepcopy(metadata)
                    m['post_data'][k] = v
                    tag_type = div_id
                    m['extra'][tag_type] = v
                    m['tags_mapping'][tag_type] = [{'name': v, 'title': title}]
                    m['filter_idx'] += 1

                    if v != 'monogram canvas' and v != 'brown':
                        continue

                    yield Request(url=m['data_url'], method='POST', body=make_post_str(m['post_data']),
                                  meta={'userdata': m}, callback=self.parse_filter, dont_filter=True)


    def parse_product_list(self, response):
        metadata = response.meta['userdata']
        region = metadata['region']
        hxs = HtmlXPathSelector(response)
        request_list = []
        page_key = '/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageNumber'
        while True:
            ret = hxs.select('//ul[@id="liCarousel"]/li[@data-url]')
            if len(ret) == 0:
                break
            for item in ret:
                url = lv_data['host'][region] + '/' + item._root.attrib['data-url']
                m = copy.deepcopy(metadata)
                request_list.append(Request(url=url, meta={'userdata': m}, callback=self.parse_details))
            page_number = int(metadata['post_data'][page_key]) + 1
            metadata['post_data'][page_key] = str(page_number)
        return request_list


    def parse_details(self, response):
        metadata = response.meta['userdata']
        region = metadata['region']
        hxs = HtmlXPathSelector(response)
        pass