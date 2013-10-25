# coding=utf-8
import urllib2

import common
import json
import logging
import logging.config
import os
import re
from urllib2 import HTTPError
import Image
import _mysql
import datetime

from pyquery import PyQuery as pq


__author__ = 'Zephyre'

fendi_data = {u'url_base': {u'cn': u'http://www.fendi.com/cn/zh/collections/{0}/fall-winter-2013-14/{1}',
                            u'us': u'http://www.fendi.com/us/en/collections/{0}/fall-winter-2013-14/{1}',
                            u'it': u'http://www.fendi.com/it/it/collezioni/{0}/autunno-inverno-2013-14/{1}',
                            u'fr': u'http://www.fendi.com/fr/fr/collections/{0}/automne-hiver-2013-14/{1}',
                            u'kr': u'http://www.fendi.com/kr/ko/collections/{0}/fall-winter-2013-14/{1}',
                            u'jp': u'http://www.fendi.com/jp/ja/collections/{0}/fall-winter-2013-14/{1}'},
              u'host': u'http://www.fendi.com',
              u'category': {'cn': (
                  u'bags', u'small-leather-goods', u'shoes', u'accessories/belts', u'accessories/other-accessories',
                  u'accessories/eyewear', u'accessories/watches', u'wallets', u'accessories/textiles',
                  u'accessories/jewelry'),
                            'kr': (
                                u'bags', u'small-leather-goods', u'shoes', u'accessories/belts',
                                u'accessories/other-accessories',
                                u'accessories/eyewear', u'accessories/watches', u'wallets', u'accessories/textiles',
                                u'accessories/jewelry'),
                            'jp': (
                                u'bags', u'small-leather-goods', u'shoes', u'accessories/belts',
                                u'accessories/other-accessories',
                                u'accessories/eyewear', u'accessories/watches', u'wallets', u'accessories/textiles',
                                u'accessories/jewelry'),
                            'us': (u'bags', u'small-leather-goods', u'shoes', u'accessories/belts',
                                   u'accessories/other-accessories', u'accessories/eyewear', u'accessories/watches',
                                   u'wallets', u'accessories/textiles', u'accessories/jewelry'),
                            'fr': (u'sacs', u'portefeuilles', u'petite-maroquinerie', u'souliers',
                                   u'accessoires/ceintures', u'accessoires/echarpes-et-foulards',
                                   u'accessoires/joaillerie',
                                   u'accessoires/lunettes-de-soleil', u'accessoires/montres',
                                   u'accessoires/petits-accessoires'),
                            'it': (u'borse', u'portafogli', u'piccola-pelletteria', u'calzature',
                                   u'accessori/cinture', u'accessori/sciarpe-e-foulards', u'accessori/gioielli',
                                   u'accessori/occhiali', u'accessori/orologi', u'accessori/altri-accessori')},
              u'details_tag': {u'cn': u'产品信息', u'kr': u'제품 코드', u'us': u'Product code', u'it': u'Codice prodotto',
                               u'fr': u'Code produit', u'jp': u'商品コード'}}


def get_logger():
    logging.config.fileConfig('products/fendi.cfg')
    return logging.getLogger('firenzeLogger')


logger = get_logger()

db = _mysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='spider_stores')
db.query("SET NAMES 'utf8'")


def fetch_filter(region, gender, category):
    """
    获取过滤器的值
    """
    body = pq(url=unicode.format(fendi_data[u'url_base'][region], gender, category))('div.filter div.panel ul li a')
    filter_items = filter(lambda item: item[u'desc'].lower().strip() != u'all',
                          tuple(dict(((u'url', fendi_data[u'host'] + temp.attrib['href']),
                                      (u'desc', temp.text if temp.text is not None else u'')))
                                for temp in body))
    return filter_items


def fetch_image(url, model, refetch=False):
    m = re.search(ur'[^/]+$', url)
    if m is None:
        return
    common.make_sure_path_exists('../images/products/fendi/')
    fname = str.format('../images/products/fendi/{0}_{1}', model, m.group())
    if refetch or not os.path.isfile(fname):
        response = None
        while True:
            try:
                response = common.get_data(url, binary_data=True, client='iPad')
                break
            except HTTPError as e:
                if e.code == 404:
                    break
                else:
                    if raw_input('PRESS ENTER TO CONTINUE, SKIP TO SKIP') == 'SKIP':
                        break
                    else:
                        continue

        if response is not None and len(response['body']) > 0:
        # 写入图片文件
            with open(fname, 'wb') as f:
                f.write(response['body'])

    try:
        img = Image.open(fname)
        db.query('LOCK TABLES products_image WRITE')    # 检查数据库
        db.query(str.format('SELECT * FROM products_image WHERE path="{0}"', fname))
        if len(db.store_result().fetch_row(maxrows=0)) == 0:
            common.insert_record(db, {'model': model, 'url': url, 'path': fname, 'width': img.size[0],
                                      'height': img.size[1], 'format': img.format,
                                      'fetch_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')},
                                 'products_image')
        db.query('UNLOCK TABLES')
    except IOError:
        pass


def fetch_product_details(entry, download_image=True):
    region = entry[u'region']
    model = entry[u'model']
    while True:
        try:
            body = pq(url=entry[u'url'])
            break
        except urllib2.URLError as e:
            raw_input('PRESS ENTER TO CONTINUE')
            continue

    temp = body('div.wrapper_bottom div.desc')
    if len(temp) > 0:
        text = temp[0].text_content().strip()
        idx = text.lower().find(fendi_data[u'details_tag'][region].lower())
        if idx != -1:
            text = text[:idx].strip()
        entry[u'description'] = text

    temp = body('div.wrapper_bottom div.price')
    if len(temp) > 0:
        text = temp[0].text.strip()
        entry[u'price'] = text
        # m = re.search(ur'(Ұ|￥|\$|€)', text)
        # if m is not None:
        #     entry[u'currency'] = common.currency_lookup(m.group())
        #
        # m = re.search(ur'[\d\.,\s]+', text, flags=re.U)
        # if m is not None:
        #     entry[u'price'] = common.format_price_text(m.group())

    entry[u'extra'] = json.dumps(entry[u'extra'])
    entry[u'fetch_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    entry[u'brandname_e'] = u'Fendi'
    entry[u'brandname_c'] = u'芬迪'
    entry[u'brand_id'] = 10135

    if download_image:
        temp = body('#page a img.front-img')
        if len(temp) > 0:
            max_res = 0
            image_url = temp[0].attrib['src']
            for data_src in temp[0].attrib:
                m = re.search(ur'data-src([\d]+)', data_src)
                if m is None:
                    continue
                res = int(m.group(1))
                if res > max_res:
                    max_res = res
                    image_url = temp[0].attrib[data_src]
            fetch_image(image_url, model)

    db.query('LOCK TABLES products WRITE')
    db.query(str.format('SELECT * FROM {0} WHERE model="{1}" && region="{2}"', 'products', model, region))
    results = db.store_result().fetch_row(maxrows=0, how=1)
    if len(results) == 0:
        common.insert_record(db, entry, 'products')
        logger.info(
            str.format('INSERT: {0}', model).decode('utf-8'))
    else:
        # 需要合并的字段：gender，category, extra, texture, color
        to_data = {'gender': results[0]['gender'], 'category': results[0]['category']}
        if common.product_merge(
                {'gender': entry['gender'], 'category': entry['category']}, to_data):
            common.update_record(db, to_data, 'products', str.format('idproducts={0}', results[0]['idproducts']))
        logger.info(
            str.format('UPDATE: {0}', model).decode('utf-8'))
    db.query('UNLOCK TABLES')

    pass


def fetch_product_list(region, gender, category, refresh_filter_data=False):
    fname = str.format('../out/products/fendi/{0}_{1}_{2}.txt', category.replace('/', '_'), gender, region)
    if not os.path.isfile(fname) or refresh_filter_data:
        logger.info(str.format('Fetch filter set for {0}, {1}', category, gender).decode('utf-8'))
        # filter_data =

        filter_data = fetch_filter(region, gender, category)
        for item in filter_data:
            item[u'processed'] = False
        with open(fname, 'w') as f:
            json.dump(filter_data, f)
    else:
        with open(fname, 'r') as f:
            filter_data = json.load(f, encoding='utf-8')

    for entry in filter_data:
        # 跳过已经处理过的post数据
        if entry[u'processed']:
            continue

        logger.info(unicode.format(u'Processing {0}/{1}/{2}', region, category, entry[u'desc']))
        for item in pq(url=entry[u'url'])('#iscroll-wrapper ul li a'):
            data = {}
            data[u'model'] = item.attrib[u'data-id']
            data[u'url'] = fendi_data[u'host'] + item.attrib[u'href']
            data[u'extra'] = {}
            data[u'extra'][u'subcategory'] = entry[u'desc']
            data[u'region'] = region
            data[u'gender'] = gender
            data[u'category'] = category
            fetch_product_details(data)

        entry['processed'] = True
        with open(fname, 'w') as f:
            json.dump(filter_data, f)


def male_female_translate(gender, region):
    """
    根据区域和语言的不同，翻译性别词语
    """
    if region == 'it':
        return {'female': 'donna', 'male': 'uomo'}[gender.lower()]
    elif region == 'fr':
        return {'female': 'femme', 'male': 'homme'}[gender.lower()]
    else:
        return {'female': 'woman', 'male': 'man'}[gender.lower()]


def main():
    common.make_sure_path_exists('../out/products/fendi/')
    region = u'jp'
    for category in fendi_data[u'category'][region]:
        fetch_product_list(region, male_female_translate('female', region), category)
        fetch_product_list(region, male_female_translate('male', region), category)

    db.close()


main()


