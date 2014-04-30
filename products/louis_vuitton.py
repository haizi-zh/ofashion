# coding=utf-8
import hashlib
import logging
import logging.config
import re
import os
import socket
import urllib
from urllib2 import URLError
import json

from lxml.etree import ParserError
from pyquery import PyQuery as pq

import common as cm
from utils.db import RoseVisionDb
from products.products_utils import get_image_path, fetch_image, get_data_path
import global_settings as glob
from scrapper.items import ProductItem
from scrapper.pipelines import ProductPipeline, ProductImagePipeline
from utils.text import unicodify
from utils.utils_core import process_price


__author__ = 'Zephyre'


def get_logger():
    logging.config.fileConfig('products/louis_vuitton.cfg')
    return logging.getLogger('firenzeLogger')
    # return logging.getLogger()


product_pipeline = ProductPipeline(getattr(glob, 'DATABASE')['DB_SPEC'])
store_uri = os.path.normpath(os.path.join(glob.STORAGE_PATH, 'products/images', '10226_louis_vuitton'))
image_pipeline = ProductImagePipeline(store_uri, db_spec=getattr(glob, 'DATABASE')['DB_SPEC'])
spider = None

logger = get_logger()
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

hosts = {'url_host': {'cn': 'http://m.louisvuitton.cn',
                      'us': 'http://m.louisvuitton.com',
                      'fr': 'http://m.louisvuitton.fr',
                      'uk': 'http://m.louisvuitton.co.uk',
                      'de': 'http://m.louisvuitton.de',
                      'es': 'http://m.louisvuitton.es',
                      'it': 'http://m.louisvuitton.it',
                      'ca': 'http://m.louisvuitton.ca',
                      'au': 'http://m.louisvuitton.com.au',
                      'jp': 'http://m.louisvuitton.jp',
                      'ru': 'http://m.louisvuitton.ru',
                      'br': 'http://m.louisvuitton.com.br',
                      'kr': 'http://m.louisvuitton.kr',
                      'tw': 'http://m.louisvuitton.tw'},
         'image_host': 'http://images.louisvuitton.com/content/dam/lv/online/picture/',
         'data_host': {
             'cn': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=zhs_CN&cache=Medium&category=',
             'us': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=eng_US&cache=Medium&category=',
             'fr': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=fra_FR&cache=Medium&category=',
             'uk': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=eng_GB&cache=Medium&category=',
             'de': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=deu_DE&cache=Medium&category=',
             'es': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=esp_ES&cache=Medium&category=',
             'it': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=ita_IT&cache=Medium&category=',
             'ca': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=eng_CA&cache=Medium&category=',
             'au': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=eng_AU&cache=Medium&category=',
             'jp': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=jpn_JP&cache=Medium&category=',
             'ru': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=rus_RU&cache=Medium&category=',
             'br': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=por_BR&cache=Medium&category=',
             'kr': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=kor_KR&cache=Medium&category=',
             'tw': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=zht_TW&cache=Medium&category=',
         }}

details_pattern = {'model_pattern': {'cn': r'产品编号\s*([a-zA-Z0-9]+)', 'us': r'sku\s*([a-zA-Z0-9]+)',
                                     'fr': r'sku\s*([a-zA-Z0-9]+)', 'de': r'REF\s*([a-zA-Z0-9]+)',
                                     'es': r'REFERENCIA\s*([a-zA-Z0-9]+)', 'it': r'CODICE\s*([a-zA-Z0-9]+)',
                                     'uk': r'sku\s*([a-zA-Z0-9]+)', 'ca': r'sku\s*([a-zA-Z0-9]+)',
                                     'au': r'sku\s*([a-zA-Z0-9]+)', 'jp': ur'製品番号\s*([a-zA-Z0-9]+)',
                                     'ru': r'sku\s*([a-zA-Z0-9]+)', 'br': r'sku\s*([a-zA-Z0-9]+)',
                                     'kr': ur'제품\s*번호\s*([a-zA-Z0-9]+)', 'tw': r'sku\s*([a-zA-Z0-9]+)'}}

brand_id = 10226

categories = {'books--stationery', 'handbags', 'travel', 'watches', 'timepieces', 'shoes', 'fine-jewelry',
              'ready-to-wear',
              'show-fall-winter-2013', 'mens-bags', 'small-leather-goods', 'icons', 'accessories/scarves-and-more',
              'accessories/belts', 'accessories/sunglasses', 'accessories/fashion-jewelry',
              'accessories/key-holders-bag-charms-and-more', 'accessories/scarves-ties-and-more',
              'accessories/key-holders-and-other-accessories'}

db = RoseVisionDb()
db.conn(getattr(glob, 'DATABASE')['DB_SPEC'])


def make_post_str(post_data):
    """
    给定post数据，生成post字符串。
    :param post_data:
    """

    def func(val):
        if val == "+":
            return val
        else:
            return urllib.quote(val, safe="")

    return u'&'.join(map(lambda k: unicode.format(u'{0}={1}', func(k), func(post_data[k])), post_keys))


def close_html_term(body):
    """
    有的html中，<input>标签没有关闭，导致pyquery可能会出现问题。
    """
    return re.sub('<(\s*input[^<>]*)>', r'<\1/>', body)


def fetch_filter(region, category, gender, idx, data):
    """
    返回从左开始指定位置的筛选器。
    """
    post_data = data['post_data']
    post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId"] = category
    post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender"] = gender
    response = cm.retry_helper(lambda val: cm.post_data(url=val, data=post_data, client="iPad"),
                               param=hosts['data_host'][region] + gender,
                               logger=logger,
                               except_class=(URLError, socket.timeout),
                               retry_delay=10)
    if response is None:
        return
    body = response['body']
    ret = []
    # 过滤器的数量
    filter_cnt = len(pq(body)('#facetsWrapperLine>div'))
    if filter_cnt == 0:
        return ret

    sub_body = pq(pq(body)('#facetsWrapperLine>div')[idx])
    filter_bodies = sub_body('div[onclick]')
    if len(filter_bodies) > 0:
        # input类型的过滤器
        for temp in filter_bodies:
            k = pq(temp)('input')[0].attrib['name']
            v = pq(temp)('input')[0].attrib['value']
            # 查看过滤器的label：
            # label = unicode(pq(temp)('label')[0].text_content()).strip().encode('utf-8')
            # utils.update_tags_mapping(brand_id, region, v, label)
            logger.info(unicode.format(u'Found filter key/value pairs: {0}={1}', k,
                                       v.decode('utf-8') if isinstance(v, str) else v))
            d = data.copy()
            d['post_data'] = data['post_data'].copy()
            d['post_data'][k] = v
            if idx < filter_cnt - 1:
                ret.extend(fetch_filter(region, category, gender, idx + 1, d))
            else:
                ret.append(d)
    else:
        filter_bodies = sub_body('li[onclick]')
        if len(filter_bodies) > 0:
            for temp in filter_bodies:
                k = pq(temp)('input')[0].attrib['name']
                for v in ["true", "false"]:
                    logger.info(unicode.format(u'Found filter key/value pairs: {0}={1}', k,
                                               v.decode('utf-8') if isinstance(v, str) else v))
                    d = data.copy()
                    d['post_data'] = data['post_data'].copy()
                    d['post_data'][k] = v
                    if idx < filter_cnt - 1:
                        ret.extend(fetch_filter(region, category, gender, idx + 1, d))
                    else:
                        ret.append(d)
        else:
            # 判断是lineik还是color
            if len(sub_body('#lineik a.imgFacet')) > 0:
                k = "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.lineik"
            elif len(sub_body('#color a.imgFacet')) > 0:
                k = "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.color"
            else:
                k = None

            if k is None:
                # 未知的过滤器
                d = data.copy()
                if idx < filter_cnt - 1:
                    ret.extend(fetch_filter(region, category, gender, idx + 1, d))
                else:
                    ret.append(d)
            else:
                filter_bodies = sub_body('a.imgFacet')
                for temp in filter_bodies:
                    v = temp.attrib['data-value']
                    # if len(pq(temp)('img[alt]')) > 0:
                    # color_text = pq(temp)('img[alt]')[0].attrib['alt']
                    # color_text = color_text.encode('utf-8') if isinstance(color_text, unicode) else color_text
                    # if color_text is not None:
                    # utils.update_tags_mapping(brand_id, region, v, color_text.strip())
                    logger.info(unicode.format(u'Found filter key/value pairs: {0}={1}', k,
                                               v.decode('utf-8') if isinstance(v, str) else v))
                    d = data.copy()
                    d['post_data'] = data['post_data'].copy()
                    d['post_data'][k] = v
                    if idx < filter_cnt - 1:
                        ret.extend(fetch_filter(region, category, gender, idx + 1, d))
                    else:
                        ret.append(d)

    return ret


def init_product_item(init_data=None):
    """
    根据过滤器设置，初始化一个单品
    """
    item = {}
    init_data = {} if init_data is None else init_data
    for k in init_data.keys():
        item[k] = init_data[k]
    return item


def reformat(text):
    """
    格式化字符串，将多余的空格、换行、制表符等合并
    """
    if text is None:
        return None
    text = cm.html2plain(text.strip())
    # <br/>换成换行符
    text = re.sub(ur'<\s*br\s*/?>', u'\r\n', text)
    # 去掉多余的标签
    text = re.sub(ur'<[^<>]*?>', u'', text)
    # # 换行转换
    text = re.sub('[\r\n]+', '\r', text)
    # text = re.subn(ur'(?:[\r\n])+', ', ', text)[0]
    return text


def fetch_product_details(region, url, filter_data, download_image=True, extra=None):
    """
    获得单品的详细信息
    """
    product_url = hosts['url_host'][region] + url
    response = cm.retry_helper(lambda val: cm.get_data(url=val, client='iPad'),
                               param=product_url,
                               logger=logger,
                               except_class=(URLError, socket.timeout),
                               retry_delay=10)
    if response is None:
        return
    body = response['body']
    if not body:
        return

    # 型号
    model = None
    try:
        temp = pq(body)('div.sku')
    except ParserError:
        return

    if len(temp) > 0:
        mt = re.search(details_pattern['model_pattern'][region], temp[0].text.encode('utf-8'), re.M | re.I)
        if mt:
            model = mt.group(1)
        else:
            mt = re.search(r'[a-zA-Z]\d{5}', temp[0].text.encode('utf-8'))
            if mt:
                model = mt.group()
    if model is None:
        return None

    temp = pq(body)('td.priceValue')
    price = unicodify(temp[0].text) if temp else None

    product_name = ''
    temp = pq(body)('#productName h1')
    if temp:
        product_name = unicodify(temp[0].text)

    description = ''
    temp = pq(body)('#productDescription')
    if temp:
        description = unicodify(temp[0].text)

    details = ''
    temp = pq(body)('#productDescription div.productDescription')
    if temp:
        details = reformat(unicodify(temp[0].text_content()))

    post_data = filter_data['post_data']
    init_data = {}
    temp = unicodify(post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.color"])
    init_data['color'] = [temp] if temp else []
    extra = {}
    temp = unicodify(post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.lineik"])
    if temp:
        extra['texture'] = [temp]
    temp = unicodify(post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId"])
    if temp:
        extra['category-0'] = [temp]
    temp = unicodify(post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.functionik"])
    if temp:
        extra['function'] = [temp]
    temp = unicodify(post_data[
        "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.casematerialik"])
    if temp:
        extra['material'] = [temp]
    temp = unicodify(post_data[
        "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.collectionik"])
    if temp:
        extra['collection'] = [temp]
    temp = unicodify(post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.shapeik"])
    if temp:
        extra['shape'] = [temp]
    temp = unicodify(
        post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subcategoryik"])
    if temp:
        extra['category-1'] = [temp]
    temp = unicodify(post_data[
        '/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subsubcategoryik'])
    if temp:
        extra['category-2'] = [temp]
    temp = unicodify(post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.typeik"])
    if temp:
        extra['typeik'] = [temp]

    init_data['tags_mapping'] = {k: [{'name': val.lower(), 'title': val} for val in extra[k]] for k in extra}

    # init_data['extra'] = extra
    init_data['model'] = model
    init_data['name'] = product_name
    init_data['price'] = price
    init_data['description'] = description
    init_data['details'] = details
    temp = unicodify(filter_data['tags']['category'])
    init_data['category'] = [temp] if temp else []
    init_data['brand_id'] = filter_data['tags']['brand_id']

    temp = filter_data['tags']['gender']
    if temp.lower() in ('women', 'woman', 'femme', 'donna', 'damen', 'mujer', 'demes', 'vrouw', 'frauen',
                        'womenswear'):
        init_data['gender'] = ['female']
    elif temp.lower() in ('man', 'men', 'homme', 'uomo', 'herren', 'hombre', 'heren', 'mann', 'signore',
                          'menswear'):
        init_data['gender'] = ['male']

    region = filter_data['tags']['region']
    init_data['region'] = region
    init_data['url'] = product_url
    # product = init_product_item(init_data)
    product = init_data
    price = process_price(u'2 350,00 €', 'fr')

    if download_image:
        results = fetch_image(body, model)
    else:
        results = []

    item = ProductItem()
    item['image_urls'] = []
    item['url'] = init_data['url']
    item['model'] = init_data['model']
    item['metadata'] = init_data

    product_pipeline.process_item(item, spider)
    image_pipeline.item_completed(results, item, None)

    return item

    # db.start_transaction()
    # try:
    #     results = db.query(
    #         str.format('SELECT * FROM {0} WHERE model="{1}" && region="{2}" && brand_id={3}', 'products', model,
    #                    region, init_data['brand_id'])).fetch_row(maxrows=0, how=1)
    #     if not results:
    #         for k in ('extra', 'color', 'gender', 'category'):
    #             if k in product:
    #                 product[k] = json.dumps(product[k], ensure_ascii=False)
    #
    #         db.insert(product, 'products', ['fetch_time', 'update_time', 'touch_time'])
    #         logger.info(unicode.format(u'INSERT: {0}, {1}, {2}', model, product_name, region))
    #     else:
    #         # 需要处理合并的字段
    #         merge_keys = ('gender', 'category', 'color')
    #         dest = dict((k, json.loads(results[0][k])) for k in merge_keys if results[0][k])
    #         src = dict((k, product[k]) for k in merge_keys if k in product)
    #         dest = sutils.product_tags_merge(src, dest)
    #
    #         s2 = json.loads(results[0]['extra'])
    #         dest['extra'] = sutils.product_tags_merge(s2, extra)
    #         try:
    #             dest = dict((k, json.dumps(dest[k], ensure_ascii=False)) for k in merge_keys + ('extra',) for k in dest)
    #             # 处理product中其它字段（覆盖现有记录）
    #             skip_keys = merge_keys + ('model', 'region', 'brand_id', 'extra', 'fetch_time')
    #             for k in product:
    #                 if k in skip_keys:
    #                     continue
    #                 dest[k] = product[k]
    #
    #             # 检查是否有改变
    #             modified = False
    #             for k in dest:
    #                 if cm.unicodify(results[0][k]) != cm.unicodify(dest[k]):
    #                     modified = True
    #                     break
    #             if modified:
    #                 db.update(dest, 'products', str.format('idproducts={0}', results[0]['idproducts']),
    #                           ['update_time', 'touch_time'])
    #                 logger.info(unicode.format(u'UPDATE: {0}, {1}', product['model'], region))
    #             else:
    #                 db.update({}, 'products', str.format('idproducts={0}', results[0]['idproducts']), ['touch_time'])
    #         except UnicodeDecodeError:
    #             pass
    #     db.commit()
    # except:
    #     db.rollback()
    #     raise
    # return product


def fetch_image(body, model, refetch=False):
    """
    抓取单品详情页面中的相关图片，并保存在数据库中。
    :param body:
    :param model:
    :param retry:
    :param cool_time:
    :param refetch: 是否强制重新抓取图片
    """
    temp = get_image_path(brand_id)
    image_dir = temp['full']
    image_thumb_dir = temp['thumb']
    brand_name = cm.norm_brand_name(cm.fetch_brand_by_id(brand_id)['brandname_e'])
    cm.make_sure_path_exists(image_dir)
    cm.make_sure_path_exists(image_thumb_dir)

    results = []
    for img_body in pq(body)('#productSheetSlideshow ul.bigs li img'):
        temp = img_body.attrib['data-src'] if 'data-src' in img_body.attrib else (img_body.attrib['src']
                                                                                  if 'src' in img_body.attrib else '')
        mt = re.search(ur'RENDITIONS\["tablet"\]\["productMain"\]\s*=\s*\'([^\']+)\'', body)
        if not mt:
            continue
        jcr = mt.group(1)
        base_name = os.path.splitext(os.path.split(temp)[1])[0]
        if re.search(r'^http://', temp) is None:
            url = hosts['image_host'] + temp
        else:
            url = temp
        url_thumb = unicode.format(u'{0}/jcr:content/renditions/{1}_{2}.jpg', url, base_name, jcr)
        m = re.search(r'([^/]+$)', url)
        if m is None:
            continue

        # flist = tuple(os.listdir(image_dir))
        # if refetch or fname not in flist:

        response = fetch_image(url_thumb, logger)
        if response is None or len(response['body']) == 0:
            continue
            # 写入图片文件

        # fname = str.format('{0}_{1}_{2}_{3}', brand_id, brand_name, model, m.group())
        fname = str.format('{0}.{1}', hashlib.sha1(url_thumb).hexdigest(), response['image_ext'])
        full_name = os.path.normpath(os.path.join(image_dir, fname))
        path_db = os.path.normpath(os.path.join('10226_louis_vuitton/full', fname))

        with open(full_name, 'wb') as f:
            f.write(response['body'])
        buf = response['body']

        # else:
        #     with open(full_name, 'rb') as f:
        #         buf = f.read()

        md5 = hashlib.md5()
        md5.update(buf)
        checksum = md5.hexdigest()

        results.append(['True', {'checksum': checksum, 'url': url_thumb, 'path': str.format('full/{0}', fname)}])

        # db.start_transaction()
        # try:
        #     # If the file already exists
        #     rs = db.query(
        #         str.format('SELECT path,width,height,format,url FROM products_image WHERE checksum="{0}"',
        #                    checksum)).fetch_row(how=1)
        #     if rs:
        #         path_db = cm.unicodify(rs[0]['path'])
        #         width = rs[0]['width']
        #         height = rs[0]['height']
        #         fmt = rs[0]['format']
        #         url = rs[0]['url']
        #     else:
        #         img = Image.open(full_name)
        #         width, height = img.size
        #         fmt = img.format
        #         url = url_thumb
        #
        #     rs = db.query(str.format('SELECT * FROM products_image WHERE path="{0}" AND model="{1}"', path_db,
        #                              model)).fetch_row(maxrows=0)
        #     if not rs:
        #         db.insert({'model': model, 'url': url, 'path': path_db, 'width': width,
        #                    'height': height, 'format': fmt, 'brand_id': brand_id, 'checksum': checksum},
        #                   'products_image', ['fetch_time', 'update_time'])
        #
        #     db.commit()
        # except:
        #     db.rollback()
        #     raise

    return results


def fetch_products(region, category, gender, refresh_post_data=False):
    """
    抓取单品信息
    """
    # 获得过滤器的信息
    brand_name = cm.norm_brand_name(cm.fetch_brand_by_id(brand_id)['brandname_e'])
    data_dir = get_data_path(brand_id)
    cm.make_sure_path_exists(data_dir)
    fname = os.path.normpath(
        os.path.join(data_dir,
                     str.format('{0}_{1}_{2}_{3}_{4}.json', brand_id, brand_name, category.replace('/', '_'), gender,
                                region)))
    if not os.path.isfile(fname) or refresh_post_data:
        logger.info(str.format('Fetch filter set for {0}, {1}', category, gender).decode('utf-8'))
        filter_combinations = fetch_filter(region, category, gender, 0,
                                           {'post_data': basic_query.copy(), 'tags': {}, 'processed': False})

        post_data = basic_query.copy()
        post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId"] = category
        post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.gender"] = gender
        filter_combinations.append({'post_data': post_data, 'processed': False, 'tags': {}})
        with open(fname, 'w') as f:
            json.dump(filter_combinations, f)
    else:
        with open(fname, 'r') as f:
            filter_combinations = json.load(f, encoding='utf-8')

    processed_urls = set([])
    for filter_data in filter_combinations:
        # 跳过已经处理过的post数据
        if filter_data['processed']:
            continue

        filter_data['tags']['brand_id'] = 10226
        filter_data['tags']['brandname_e'] = 'Louis Vuitton'
        filter_data['tags']['brandname_c'] = '路易威登'
        filter_data['tags']['category'] = category
        filter_data['tags']['gender'] = gender
        filter_data['tags']['region'] = region
        page = 1
        while True:
            filter_data['post_data']["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageNumber"] = page
            response = cm.retry_helper(lambda val: cm.post_data(url=val,
                                                                data=filter_data['post_data'],
                                                                client="iPad"),
                                       param=hosts["data_host"][region] + gender,
                                       logger=logger,
                                       except_class=(URLError, socket.timeout),
                                       retry_delay=10)
            if response is None:
                continue
                # 得到单品的列表
            product_list = pq(response['body'])('li[data-url]')
            if len(product_list) == 0:
                break
                # logger.info(str.format('{0} products found at page {1}', len(product_list), page).decode('utf-8'))
            page += 1
            for item in product_list:
                url_component = item.attrib['data-url']
                m = re.search(r'[^-]+$', url_component)
                if m is None:
                    continue
                url = item.attrib['data-url']
                if url in processed_urls:
                    continue
                else:
                    processed_urls.add(url)
                    fetch_product_details(region, url, filter_data)

        filter_data['processed'] = True
        with open(fname, 'w') as f:
            json.dump(filter_combinations, f)


def main(region):
    logger.info(str.format('Crawler started: {0}', region))
    category_list = {'books--stationery', 'handbags', 'travel', 'watches', 'timepieces', 'shoes', 'fine-jewelry',
                     'ready-to-wear', 'the-legendary-monogram', 'ready-to-wear',
                     'show-fall-winter-2013', 'mens-bags', 'small-leather-goods', 'icons',
                     'accessories/scarves-and-more',
                     'accessories/belts', 'accessories/sunglasses', 'accessories/fashion-jewelry',
                     'accessories/key-holders-bag-charms-and-more', 'accessories/scarves-ties-and-more',
                     'accessories/key-holders-and-other-accessories'}

    for category in category_list:
        logger.info(str.format('{0}/{1}/{2}', region, category, 'men'))
        fetch_products(region, category, 'men')
        logger.info(str.format('{0}/{1}/{2}', region, category, 'women'))
        fetch_products(region, category, 'women')

    db.close()
