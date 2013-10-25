# coding=utf-8

import logging
import logging.config
import re
import socket
from urllib2 import URLError, HTTPError
import datetime
import Image
import _mysql
from pyquery import PyQuery as pq
import common as cm
import os
import json
from products import utils

__author__ = 'Zephyre'


def get_logger():
    logging.config.fileConfig('products/burberry.cfg')
    return logging.getLogger('firenzeLogger')


logger = get_logger()

burberry_data = {u'url_base': {u'cn': u'http://cn.burberry.com', u'us': u'http://us.burberry.com',
                               u'fr': u'http://fr.burberry.com', u'it': u'http://it.burberry.com'},
                 u'dispatch': {u'cn': (u'women', u'men', u'children', u'beauty', u'the-trench-coat'),
                               u'us': (u'women', u'men', u'children', u'beauty', u'the-trench-coat'),
                               u'fr': (u'femme', u'homme', u'enfant', u'beaute', u'le-trench-coat'),
                               u'it': (u'donna', u'uomo', u'bambini', u'beauty', u'il-trench-coat')}}

db = _mysql.connect(host='127.0.0.1', port=3306, user='root', passwd='123456', db='spider_stores')
db.query("SET NAMES 'utf8'")


def fetch_category(url, metadata=None):
    """
    获得一级类别
    :param url:
    """
    logger.info(unicode.format(u'PROCESSING CATEGORY: {0} / {1}', metadata[u'category-1'], url))
    response = cm.retry_helper(lambda val: cm.get_data(url=val), param=url, logger=logger,
                               except_class=(URLError, socket.timeout),
                               retry_delay=10,
                               retry_message=unicode.format(u'Failed to fetch URL: {0}', url),
                               abort_message=unicode.format(u'Abort to fetch URL: {0}', url))
    if response is None:
        return
    body = pq(response['body'])
    node = metadata[u'walk_node']
    fname = metadata[u'trail_fname']
    walk_trail = metadata[u'walk_trail']
    siblings = node[u'siblings']

    for c in (val.attrib['href'] for val in body('#nav ul.l-2-set li a')):
        m = metadata.copy()
        c = re.sub(r'(^/|/$)', r'', c)
        m[u'category-2'] = c
        url = unicode.format(u'{0}/{1}', burberry_data[u'url_base'][m[u'region']], c)

        if url in siblings and siblings[url][u'completed']:
            continue
        else:
            if url not in siblings:
                siblings[url] = {u'name': url, u'completed': False, u'siblings': {}}
            m[u'walk_node'] = siblings[url]
            fetch_subcategory(url=url, metadata=m)
            siblings[url][u'completed'] = True
            siblings[url][u'siblings'] = {}
            with open(fname, 'w') as f:
                json.dump(walk_trail, f)


def fetch_subcategory(url, metadata=None):
    """
    获得二级类别
    :param url:
    """
    logger.info(unicode.format(u'PROCESSING SUBCATEGORY: {0} / {1}', metadata[u'category-2'], url))
    response = cm.retry_helper(lambda val: cm.get_data(url=val), param=url, logger=logger,
                               except_class=(URLError, socket.timeout),
                               retry_delay=10,
                               retry_message=unicode.format(u'Failed to fetch URL: {0}', url),
                               abort_message=unicode.format(u'Abort to fetch URL: {0}', url))
    if response is None:
        return
    body = pq(response['body'])
    node = metadata[u'walk_node']
    walk_trail = metadata[u'walk_trail']
    fname = metadata[u'trail_fname']
    siblings = node[u'siblings']

    for c in body('div.products ul.product-set li.product a[data-product-id]'):
        m = metadata.copy()
        url = unicode.format(u'{0}{1}', burberry_data[u'url_base'][m[u'region']], c.attrib['href'])
        if url in siblings and siblings[url][u'completed']:
            continue
        else:
            if url not in siblings:
                siblings[url] = {u'name': url, u'completed': False, u'siblings': {}}

            m[u'model'] = c.attrib['data-product-id']
            m[u'name'] = c.text.strip() if c.text is not None else u''
            fetch_details(url=url, metadata=m)

            siblings[url][u'completed'] = True
            siblings[url][u'siblings'] = {}
            with open(fname, 'w') as f:
                json.dump(walk_trail, f)


def fetch_image(url, entry, refetch=False):
    def func(url):
        try:
            return cm.get_data(url, binary_data=True)
        except HTTPError as e:
            if e.code != 404:
                raise

    model = entry[u'model']
    brand_id = entry[u'brand_id']
    dir_path = u'../images/products/burberry'
    cm.make_sure_path_exists(dir_path)
    m = re.search(ur'[^/]+$', url)
    if m is None:
        return
    temp = re.sub(ur'\?', u'', m.group())
    fname = unicode.format(unicode.format(u'{0}_{1}', model, temp))
    flist = tuple(os.path.splitext(val)[0] for val in os.listdir(u'../images/products/burberry'))
    full_name = None
    if refetch or fname not in flist:
        response = cm.retry_helper(func, param=url, logger=logger, except_class=(HTTPError, URLError, socket.timeout),
                                   retry_delay=10,
                                   retry_message=unicode.format(u'Failed to fetch URL: {0}', url),
                                   abort_message=unicode.format(u'Abort to fetch URL: {0}', url))
        if response is not None and len(response['body']) > 0:
            ctype = response['headers']['content-type']
            if ctype.lower() == 'image/jpeg':
                ext = u'jpg'
            elif ctype.lower() == 'image/png':
                ext = u'png'
            elif ctype.lower() == 'image/gif':
                ext = u'gif'
            else:
                ext = u''

            full_name = unicode.format(u'{0}/{1}.{2}', dir_path, fname, ext)
            # 写入图片文件
            with open(full_name, 'wb') as f:
                f.write(response['body'])

    if full_name is not None:
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


def fetch_details(url, metadata=None, download_image=True):
    """
    获得单品详细信息
    :param url:
    :param meta:
    """
    logger.info(unicode.format(u'PROCESSING PRODUCT: {0} / {1}', metadata[u'model'], url))
    response = cm.retry_helper(lambda val: cm.get_data(url=val), param=url, logger=logger,
                               except_class=(URLError, socket.timeout),
                               retry_delay=10,
                               retry_message=unicode.format(u'Failed to fetch URL: {0}', url),
                               abort_message=unicode.format(u'Abort to fetch URL: {0}', url))
    if response is None:
        return
    body = pq(response['body'])
    region = metadata[u'region']
    entry = {u'brand_id': 10057, u'region': region, u'url': url, u'brandname_e': u'Burberry', u'brandname_c': u'博柏丽',
             u'fetch_time': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'), u'model': metadata[u'model'],
             u'name': metadata[u'name']}
    model = entry[u'model']
    gender_map = {u'women': u'female', u'men': u'male', u'femme': u'female', u'homme': u'male',
                  u'donna': u'female', u'uomo': u'male'}
    temp = gender_map.get(metadata[u'category-1'])
    entry[u'gender'] = temp if temp is not None else u''

    entry[u'category'] = metadata[u'category-2']
    # 价格
    temp = body('p.price-info span.price-amount')
    if len(temp) == 0:
        entry[u'price'] = u''
    else:
        entry[u'price'] = temp[0].text if temp[0].text is not None else u''

    # 颜色
    temp = body('div.colors span.color-name')
    if len(temp) == 0:
        entry[u'color'] = u''
    else:
        entry[u'color'] = temp[0].text if temp[0].text is not None else u''

    # 描述
    temp = body('#description-panel ul li')
    entry[u'description'] = (u'，' if region == 'cn' else u', ').join(
        filter(lambda x: x is not None, (val.text for val in temp)))

    # 详细信息
    temp = body('#feature-care-panel ul li')
    entry[u'details'] = (u'，' if region == 'cn' else u', ').join(
        filter(lambda x: x is not None, (val.text for val in temp)))

    # 图片下载
    if download_image:
        for src in (val.attrib['data-zoomed-src'] for val in
                    body('ul.product-media-set li.product-image[data-zoomed-src]')):
            fetch_image(src, entry)

    db.query('LOCK TABLES products WRITE')
    db.query(str.format('SELECT * FROM {0} WHERE model="{1}" && region="{2}"', 'products', model, region))
    results = db.store_result().fetch_row(maxrows=0, how=1)
    if len(results) == 0:
        cm.insert_record(db, entry, 'products')
        # logger.info(unicode.format(u'INSERT: {0}', model))
    else:
        # 需要合并的字段：gender，category, extra, texture, color
        to_data = {'gender': results[0]['gender'], 'category': results[0]['category']}
        if cm.product_merge(
                {'gender': entry['gender'], 'category': entry['category']}, to_data):
            cm.update_record(db, to_data, 'products', str.format('idproducts={0}', results[0]['idproducts']))
            # logger.info(unicode.format(u'UPDATE: {0}', model))
    db.query('UNLOCK TABLES')

    pass


def main():
    utils.update_tags_mapping(10226, 'cn', 'hand-bags', '男士箱包')
    utils.update_tags_mapping(10226, 'cn', 'hand-bags', 'Hakoya')
    utils.update_tags_mapping(10226, 'us', 'hand-bags', 'Hand Bags')
    pass


def main1():
    region = u'fr'
    logger.info(unicode.format(u'PROCESSING IN REGION: {0}', region))
    cm.make_sure_path_exists(u'../out/products/burberry')
    fname = unicode.format(u'{0}_{1}.txt', u'../out/products/burberry/burberry', region)
    if not os.path.isfile(fname):
        walk_trail = {u'name': region, u'completed': False, u'siblings': {}}
        with open(fname, 'w') as f:
            json.dump(walk_trail, f)
    else:
        with open(fname, 'r') as f:
            walk_trail = json.load(f)

    if walk_trail[u'completed']:
        return

    siblings = walk_trail[u'siblings']
    for c in burberry_data[u'dispatch'][region]:
        # 已经完成
        if c in siblings and siblings[c][u'completed']:
            continue
        else:
            # 需要后续处理
            if c not in siblings:
                siblings[c] = {u'name': c, u'completed': False, u'siblings': {}}
            fetch_category(unicode.format(u'{0}/{1}', burberry_data[u'url_base'][region], c),
                           metadata={u'category-1': c, u'region': region, u'trail_fname': fname,
                                     u'walk_trail': walk_trail, u'walk_node': siblings[c]})
            siblings[c][u'completed'] = True
            siblings[c][u'siblings'] = {}
            with open(fname, 'w') as f:
                json.dump(walk_trail, f)