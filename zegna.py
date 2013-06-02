# coding=utf-8

__author__ = 'Zephyre'

import json
import string
import urllib
import urllib2
import re
import common

host = 'http://storelocator.zegna.com'


def get_countries():
    url = host + '/en/countries'
    countries = []

    try:
        html = common.get_data(url)
    except Exception:
        print('Error while getting the country list. level=0')
        return countries

    start = 0
    while True:
        start = html.find('<div class="country-list">', start)
        if start == -1:
            break
        end = html.find('</div>', start)
        sub_html = html[start:end]
        start = end

        # <li><a href="/en/panama">PANAMA</a></li>
        m = re.findall(r'<li><a href="(.+?)">(.+?)</a></li>', sub_html)
        for val in m:
            name = val[1]
            # 美国需要单独处理
            if 'UNITED STATES' in name:
                state = name[name.find('(') + 1:-1]
                name = 'UNITED STATES'
                countries.append({'url': host + val[0], 'name': name, 'state': state})
            else:
                countries.append({'url': host + val[0], 'name': name})

    return countries


def get_stores(url):
    html = common.get_data(url)
    start = 0
    stores = []
    while True:
        start = html.find('<li class="info-store clearfix">', start)
        if start == -1:
            break
        end = html.find('<li class="info-store clearfix">', start + 1)
        sub_html = html[start:end]
        start = end
        store_proto = {}

        for m in re.findall(r'<h1><a href="(.*?)">(.*?)</a>', sub_html):
            store_proto['url'] = host + m[0]
            store_proto['name'] = common.html2plain(m[1].strip())

        for m in re.findall(r'<span style="display:none" class="ll">\s*(-?\d+\.\d+),\s*(-?\d+\.\d+)\s*</span>',
                            sub_html):
            store_proto['coord'] = {'lat': string.atof(m[0]), 'lng': string.atof(m[1])}

        for m in re.findall(r'<span class="map-address">(.*?)</span>', sub_html):
            store_proto['addr'] = common.html2plain(m.strip())

        # phone: <span class="type">phone:</span> +86 (0431) 88981642<br />
        for m in re.findall(r'<span class="type">phone:</span>(.*?)<br />', sub_html):
            store_proto['tel'] = m.strip()

        for m in re.findall(r'<a class="email" href="mailto:(.*?@.*?)">', sub_html):
            store_proto['email'] = m.strip()

        opening_s = sub_html.find('<ul class="opening-hours')
        if opening_s != -1:
            opening_e = sub_html.find('</ul>', opening_s)
            o_str = sub_html[opening_s:opening_e]
            store_proto['opening_hour'] = [m for m in re.findall(r'<li>(.+?)</li>', o_str)]

        brand_s = sub_html.find('<ul class="brands clearfix">')
        if brand_s != -1:
            brand_e = sub_html.find('</ul>', brand_s)
            b_str = sub_html[brand_s:brand_e]
            for m in re.findall(r'<li><a href=".*?">(.+?)</a></li>', b_str):
                store = dict(store_proto)
                store['brand'] = common.html2plain(m)
                stores.append(store)
        else:
            stores.append(store_proto)

    for store in stores:
        print('Found store: %s: %s' % (store['name'], store['addr']))
    return stores


def recur_stores(url, data=None, level=0):
    """
递归调用，获得所有的商店信息
"""
    stores = []
    if level == 0:
        # 国家
        for c in get_countries():
            print('Fetching for %s' % c['name'])
            stores.extend(recur_stores(c['url'], level=1))
    elif level == 1:
        # 商店列表
        stores.extend(get_stores(url))
    return stores

func_map = {'get_countries': get_countries, 'get_stores': get_stores}

def fetch():
    # 根节点
    root_node = {'url': None, 'level': 0}
    recur_stores(None)
