# coding=utf-8

__author__ = 'Zephyre'

import json
import string
import urllib
import urllib2
import re
import common

host = 'http://storelocator.zegna.com'
brand_id = 10121
brandname_e = 'Ermenegildo Zegna'
brandname_c = u'杰尼亚'


def get_countries(url):
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


def get_stores(data):
    url = data['url']
    try:
        html = common.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'url': url}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    start = 0
    store_list = []
    while True:
        start = html.find('<li class="info-store clearfix">', start)
        if start == -1:
            break
        end = html.find('<li class="info-store clearfix">', start + 1)
        sub_html = html[start:end]
        start = end

        entry = common.init_store_entry(brand_id)
        for m in re.findall(r'<h1><a href="(.*?)">(.*?)</a>', sub_html):
            entry[common.url] = host + m[0]
            entry[common.name_e] = common.html2plain(m[1].strip())
            break

        for m in re.findall(r'<span style="display:none" class="ll">\s*(-?\d+\.\d+),\s*(-?\d+\.\d+)\s*</span>',
                            sub_html):
            common.update_entry(entry, {common.lat: string.atof(m[0]), common.lng: string.atof(m[1])})
            break

        for m in re.findall(r'<span class="map-address">(.*?)</span>', sub_html):
            entry[common.addr_e] = common.reformat_addr(m)
            break

        for m in re.findall(r'<span class="type">phone:</span>(.*?)<br />', sub_html):
            entry[common.tel] = m.strip()
            break

        for m in re.findall(r'<a class="email" href="mailto:(.*?@.*?)">', sub_html):
            entry[common.email] = m.strip()
            break

        opening_s = sub_html.find('<ul class="opening-hours')
        if opening_s != -1:
            opening_e = sub_html.find('</ul>', opening_s)
            o_str = sub_html[opening_s:opening_e]
            entry[common.hours] = ', '.join([m for m in re.findall(r'<li>(.+?)</li>', o_str)])

        brand_s = sub_html.find('<ul class="brands clearfix">')
        if brand_s != -1:
            brand_e = sub_html.find('</ul>', brand_s)
            b_str = sub_html[brand_s:brand_e]
            entry[common.store_type] = ', '.join([common.html2plain(m)
                                                  for m in re.findall(r'<li><a href=".*?">(.+?)</a></li>', b_str)])

        # Geo
        if 'state' in data:
            entry[common.province_e] = data['state']
        country_e = data['name']
        ret = common.geo_translate(country_e)
        if len(ret) > 0:
            common.update_entry(entry, {common.continent_c: ret[common.continent_c],
                                        common.continent_e: ret[common.continent_e],
                                        common.country_c: ret[common.country_c],
                                        common.country_e: ret[common.country_e]})
        else:
            entry[common.country_e] = country_e
        common.update_entry(entry, {common.brandname_c: brandname_c, common.brandname_e: brandname_e})
        common.chn_check(entry)

        print '%s Found store: %s, %s (%s, %s)' % (
            brandname_e, entry[common.name_e], entry[common.addr_e], entry[common.country_e],
            entry[common.continent_e])
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return store_list


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


def fetch1():
    # 根节点
    root_node = {'url': None, 'level': 0}
    recur_stores(None)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 1: 国家；2：商店
        """
        # stores = fetch_stores(data)
        # return [{'func': None, 'data': s} for s in stores]

        stores = []
        if level == 1:
            # 国家
            countries = get_countries(data['url'])
            return [{'func': lambda data: func(data, 2), 'data': c} for c in countries]
        elif level == 2:
            # 商店列表
            stores = get_stores(data)
            return [{'func': None, 'data': s} for s in stores]
        else:
            return []

    global db
    db = common.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': host + '/en/countries'}
    results = common.walk_tree({'func': lambda data: func(data, 1), 'data': data})
    db.disconnect_db()
    return results