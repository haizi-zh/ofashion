# coding=utf-8
import logging
import logging.config
import re
import os
import socket
import urllib
import datetime
import _mysql
from urllib2 import HTTPError, URLError
import common as cm
from pyquery import PyQuery as pq
import json
import Image
from products import utils

__author__ = 'Zephyre'


def get_logger():
    logging.config.fileConfig('products/louis_vuitton.cfg')
    return logging.getLogger('firenzeLogger')


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
             'it': 'http://a.louisvuitton.com/mobile/ajax/productFinderResults.jsp?storeLang=ita_IT&cache=Medium&category='}}

details_pattern = {'model_pattern': {'cn': r'产品编号\s*([a-zA-Z0-9]+)', 'us': r'sku\s*([a-zA-Z0-9]+)',
                                     'fr': r'sku\s*([a-zA-Z0-9]+)', 'de': r'REF\s*([a-zA-Z0-9]+)',
                                     'es': r'REFERENCIA\s*([a-zA-Z0-9]+)', 'it': r'CODE\s*([a-zA-Z0-9]+)'}}

brand_id = 10226

categories = {'books--stationery', 'handbags', 'travel', 'watches', 'timepieces', 'shoes', 'fine-jewelry',
              'ready-to-wear',
              'show-fall-winter-2013', 'mens-bags', 'small-leather-goods', 'icons', 'accessories/scarves-and-more',
              'accessories/belts', 'accessories/sunglasses', 'accessories/fashion-jewelry',
              'accessories/key-holders-bag-charms-and-more', 'accessories/scarves-ties-and-more',
              'accessories/key-holders-and-other-accessories'}
db = _mysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='spider_stores')
db.query("SET NAMES 'utf8'")


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
            label = unicode(pq(temp)('label')[0].text_content()).strip().encode('utf-8')
            utils.update_tags_mapping(brand_id, region, v, label)
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
                    if len(pq(temp)('img[alt]')) > 0:
                        color_text = pq(temp)('img[alt]')[0].attrib['alt']
                        color_text = color_text.encode('utf-8') if isinstance(color_text, unicode) else color_text
                        if color_text is not None:
                            utils.update_tags_mapping(brand_id, region, v, color_text.strip())
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


def fetch_product_details(region, url, filter_data, download_image=True, extra=None):
    """
    获得单品的详细信息
    """
    response = cm.retry_helper(lambda val: cm.get_data(url=val, client='iPad'),
                               param=hosts['url_host'][region] + url,
                               logger=logger,
                               except_class=(URLError, socket.timeout),
                               retry_delay=10)
    if response is None:
        return
    body = response['body']

    # 型号
    model = None
    temp = pq(body)('div.sku')
    if len(temp) > 0:
        temp = re.search(details_pattern['model_pattern'][region], temp[0].text.encode('utf-8'), re.M | re.I)
        if temp is not None:
            model = temp.group(1)
    if model is None:
        return None

    temp = pq(body)('td.priceValue')
    if len(temp) > 0:
        price_body = (temp[0].text if isinstance(temp[0].text, unicode) else temp[0].text.decode('utf-8')).strip()
        price = price_body.strip()
        price = price.encode('utf-8') if isinstance(price, unicode) else price
        currency = ''
        # m = re.search(ur'(Ұ|\$|€)', price_body)
        # if m is None:
        #     currency = u''
        # else:
        #     currency = common.currency_lookup(m.group())
        #
        # m = re.search(ur'[\d\.,\s]+', price_body, flags=re.U)
        # if m is None:
        #     price = 0
        # else:
        #     price = common.format_price_text(m.group())

    product_name = ''
    temp = pq(body)('#productName h1')
    if len(temp) > 0:
        product_name = temp[0].text.encode('utf-8').strip()

    description = ''
    temp = pq(body)('#productDescription')
    if len(temp) > 0:
        description = temp[0].text.encode('utf-8').strip()

    details = ''
    temp = pq(body)('#productDescription div.productDescription')
    if len(temp) > 0:
        details = unicode(temp[0].text_content()).encode('utf-8').strip()

    post_data = filter_data['post_data']
    init_data = {}
    init_data['color'] = post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.color"]
    init_data['texture'] = post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.lineik"]
    extra = {}
    extra['page_id'] = post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.pageId"]
    extra['function'] = post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.functionik"]
    extra['material'] = post_data[
        "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.casematerialik"]
    extra['collection'] = post_data[
        "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.collectionik"]
    extra['shape'] = post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.shapeik"]
    extra['subcategory'] = post_data[
        "/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subcategoryik"]
    extra['subsubcategory'] = post_data[
        '/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.subsubcategoryik']
    extra['typeik'] = post_data["/vuitton/ecommerce/commerce/catalog/FindProductsFormHandler.facetValues.typeik"]
    init_data['extra'] = json.dumps(extra)
    init_data['model'] = model
    init_data['name'] = product_name
    init_data['currency'] = currency
    init_data['price'] = price
    init_data['description'] = description
    init_data['details'] = details
    init_data['category'] = filter_data['tags']['category']
    init_data['brand_id'] = filter_data['tags']['brand_id']
    init_data['brandname_e'] = filter_data['tags']['brandname_e']
    init_data['brandname_c'] = filter_data['tags']['brandname_c']
    init_data['gender'] = filter_data['tags']['gender']
    region = filter_data['tags']['region']
    init_data['region'] = region
    init_data['fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    product = init_product_item(init_data)

    if download_image:
        fetch_image(body, model)

    db.query('LOCK TABLES products WRITE')
    db.query(str.format('SELECT * FROM {0} WHERE model="{1}" && region="{2}" && brand_id={3}', 'products', model,
                        region, init_data['brand_id']))
    results = db.store_result().fetch_row(maxrows=0, how=1)
    if len(results) == 0:
        cm.insert_record(db, product, 'products')
        logger.info(
            str.format('INSERT: {0}, {1}, {2}{3}, {4}', model, product_name, currency, price, description).decode(
                'utf-8'))
    else:
        # 需要合并的字段：gender，category, extra, texture, color
        to_data = {'gender': results[0]['gender'], 'category': results[0]['category'], 'color': results[0]['color'],
                   'texture': results[0]['texture']}
        old_extra = json.loads(results[0]['extra'])
        for k in extra:
            if k not in old_extra:
                old_extra[k] = extra[k]
            else:
                temp = set(
                    (val.encode('utf-8') if isinstance(val, unicode) else val) for val in old_extra[k].split('|'))
                temp.add(extra[k])
                old_extra[k] = '|'.join(temp)
        extra_str = json.dumps(old_extra)
        temp = cm.product_merge(
            {'gender': init_data['gender'], 'category': init_data['category'], 'color': init_data['color'],
             'texture': init_data['texture']}, to_data)
        if extra_str != results[0]['extra'] or temp:
            to_data['extra'] = extra_str
            cm.update_record(db, to_data, 'products', str.format('idproducts={0}', results[0]['idproducts']))
            logger.info(
                str.format('UPDATE: {0}, {1}, {2}{3}, {4}', model, product_name, currency, price, description).decode(
                    'utf-8'))
    db.query('UNLOCK TABLES')
    return product


def fetch_image(body, model, refetch=False):
    """
    抓取单品详情页面中的相关图片，并保存在数据库中。
    :param body:
    :param model:
    :param retry:
    :param cool_time:
    :param refetch: 是否强制重新抓取图片
    """
    image_dir = utils.get_image_path(brand_id)
    brand_name = cm.norm_brand_name(cm.fetch_brand_by_id(brand_id)['brandname_e'])
    cm.make_sure_path_exists(image_dir)

    for img_body in pq(body)('#productSheetSlideshow ul.bigs li img'):
        temp = img_body.attrib['data-src'] if 'data-src' in img_body.attrib else (img_body.attrib['src']
                                                                                  if 'src' in img_body.attrib else '')
        if re.search(r'^http://', temp) is None:
            url = hosts['image_host'] + temp
        else:
            url = temp
        m = re.search(r'([^/]+$)', url)
        if m is None:
            continue

        fname = str.format('{0}_{1}_{2}_{3}', brand_id, brand_name, model, m.group())
        full_name = os.path.normpath(os.path.join(image_dir, fname))
        flist = tuple(os.listdir(image_dir))
        if refetch or fname not in flist:
            response = utils.fetch_image(url, logger)
            if response is None or len(response['body']) == 0:
                continue
                # 写入图片文件
            with open(full_name, 'wb') as f:
                f.write(response['body'])

        try:
            img = Image.open(full_name)
            db.query('LOCK TABLES products_image WRITE')    # 检查数据库
            db.query(str.format('SELECT * FROM products_image WHERE path="{0}"', full_name))
            if len(db.store_result().fetch_row(maxrows=0)) == 0:
                cm.insert_record(db, {'model': model, 'url': url, 'path': full_name, 'width': img.size[0],
                                      'height': img.size[1], 'format': img.format, 'brand_id': brand_id,
                                      'fetch_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                                 'products_image')
            db.query('UNLOCK TABLES')
        except IOError:
            pass


def fetch_products(region, category, gender, refresh_post_data=False):
    """
    抓取单品信息
    """
    # 获得过滤器的信息
    brand_name = cm.norm_brand_name(cm.fetch_brand_by_id(brand_id)['brandname_e'])
    data_dir = utils.get_data_path(brand_id)
    cm.make_sure_path_exists(data_dir)
    fname = os.path.normpath(
        os.path.join(data_dir,
                     str.format('{0}_{1}_{2}_{3}_{4}.json', brand_id, brand_name, category.replace('/', '_'), gender,
                                region)))
    if not os.path.isfile(fname) or refresh_post_data:
        logger.info(str.format('Fetch filter set for {0}, {1}', category, gender).decode('utf-8'))
        filter_combinations = fetch_filter(region, category, gender, 0,
                                           {'post_data': basic_query.copy(), 'tags': {}, 'processed': False})
        with open(fname, 'w') as f:
            json.dump(filter_combinations, f)
    else:
        with open(fname, 'r') as f:
            filter_combinations = json.load(f, encoding='utf-8')

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
            logger.info(str.format('{0} products found at page {1}', len(product_list), page).decode('utf-8'))
            page += 1
            for item in product_list:
                url_component = item.attrib['data-url']
                m = re.search(r'[^-]+$', url_component)
                if m is None:
                    continue
                fetch_product_details(region, item.attrib['data-url'], filter_data)

        filter_data['processed'] = True
        with open(fname, 'w') as f:
            json.dump(filter_combinations, f)


def main():
    category_list = {'books--stationery', 'handbags', 'travel', 'watches', 'timepieces', 'shoes', 'fine-jewelry',
                     'ready-to-wear', 'the-legendary-monogram', 'ready-to-wear',
                     'show-fall-winter-2013', 'mens-bags', 'small-leather-goods', 'icons',
                     'accessories/scarves-and-more',
                     'accessories/belts', 'accessories/sunglasses', 'accessories/fashion-jewelry',
                     'accessories/key-holders-bag-charms-and-more', 'accessories/scarves-ties-and-more',
                     'accessories/key-holders-and-other-accessories'}
    region_list = {'cn', 'us', 'fr', 'de', 'es', 'it', 'gb', 'ru', 'br', 'ca', 'jp', 'kr', 'tw', 'au'}
    region = 'de'
    for category in category_list:
        logger.info(str.format('{0}/{1}/{2}', region, category, 'men'))
        fetch_products(region, category, 'men')
        logger.info(str.format('{0}/{1}/{2}', region, category, 'women'))
        fetch_products(region, category, 'women')

    db.close()

