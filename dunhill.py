# coding=utf-8
import string
import urllib

__author__ = 'Zephyre'

import re
import common
import geosense as gs

db = None
url = 'http://www.dunhill.com/stores/'
brand_id = 10113
brandname_e = u'Dunhill'
brandname_c = u'登喜路'


def get_countries(url):
    """
    得到国家列表
    :rtype : [{'name': 'United States', 'code': 'US'}]
    :param url:
    """
    try:
        html = common.get_data(url)
    except Exception:
        print 'Error occured in getting the list of countries: %s' % url
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'data': url}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    return [{'country_e': m[1], 'country_code': m[0]} for m in
            re.findall(ur'<option (?:.*?)value="([a-zA-Z]{2})">(.*)</option>', html)]


def get_cities(data):
    try:
        d = {'country': data['country_code'], 'city': '', 'service': -1}
        html = common.post_data(url, d)
    except Exception:
        print 'Error occured in getting the list of countries: %s' % url
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'data': url}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    start = html.find(u'<select id="city" name="city">')
    if start == -1:
        return []
    end = html.find(u'</select>', start)
    html = html[start:end]
    city_list = []
    for m in re.findall(ur'<option value="(.+?)">', html):
        if data['country_code'] == 'GB' and '2 davies street' in m.lower():
            continue
        elif data['country_code'] == 'RO' and '13 september street' in m.lower():
            continue
        elif 'b1603daq' in m.lower():
            continue
        else:
            city_list.append({'city_e': m, 'country_e': data['country_e'], 'country_code': data['country_code']})
    return city_list


def get_subcat(html, pat):
    """
    比如，dunhill需要单独拿出来
    :param html:
    :param pat:
    """
    it = re.finditer(pat, html)
    try:
        m = it.next()
        sub_html, start, end = common.extract_closure(html[m.start():], ur'<ul\b', ur'</ul>')
        return sub_html
    except StopIteration:
        return ''


def get_stores(data):
    try:
        d = {'country': data['country_code'], 'city': urllib.quote(data['city_e']), 'service': -1}
        html = common.post_data(url, d)
    except Exception:
        print 'Error occured in getting the list of countries: %s' % url
        dump_data = {'level': 1, 'time': common.format_time(), 'data': {'data': url}, 'brand_id': brand_id}
        common.dump(dump_data)
        return []

    def get_detailed_store(html, store_cat):
        store_list = []
        start = 0
        while True:
            sub_html, start, end = common.extract_closure(html, ur'<li\b', ur'</li>')
            if end == 0:
                break

            # 得到单个门店的页面代码
            html = html[end:]
            entry = common.init_store_entry(brand_id, brandname_e, brandname_c)

            m = re.findall(ur'<div class="store-title -h3a">(.+?)</div>', sub_html)
            if len(m) > 0:
                entry[common.name_e] = common.reformat_addr(m[0])
            m = re.findall(ur'<div class="store-address">(.+?)</div>', sub_html, re.S)
            if len(m) > 0:
                addr = common.reformat_addr(m[0])
                # 最后一行是否为电话号码？
                terms = addr.split(', ')
                tel = common.extract_tel(terms[-1])
                if tel != '':
                    addr = ', '.join(terms[:-1])
                    entry[common.tel] = tel
                entry[common.addr_e] = addr

            # 获得门店类型
            # store_type = [store_cat]
            type_html, type_start, type_end = common.extract_closure(sub_html, ur'<ul class="service-list">',
                                                                     ur'</ul>')
            if type_end != 0:
                store_type = [m for m in re.findall(ur'<li class="service-item">(.+?)</li>',
                                                    type_html)]
                store_type.insert(0, store_cat)
                entry[common.store_type] = ', '.join(store_type)
            else:
                entry[common.store_type] = store_cat

            # 获得经纬度
            m = re.findall(ur'data-latitude="(-?\d+\.\d+)"', sub_html)
            if len(m) > 0:
                entry[common.lat] = string.atof(m[0])
            m = re.findall(ur'data-longitude="(-?\d+\.\d+)"', sub_html)
            if len(m) > 0:
                entry[common.lng] = string.atof(m[0])

            entry[common.city_e] = common.extract_city(data[common.city_e])[0]
            entry[common.country_e] = common.reformat_addr(data[common.country_e]).strip().upper()
            gs.field_sense(entry)

            print '%s: Found store: %s, %s (%s, %s, %s)' % (
                brandname_e, entry[common.name_e], entry[common.addr_e], entry[common.city_e], entry[common.country_e],
                entry[common.continent_e])
            db.insert_record(entry, 'stores')
            store_list.append(entry)

        return store_list

    stores = {}
    for m in re.finditer(ur'<h3 class="-h -h2">(.+?)\(\d+\)\s*</h3>', html):
        cat_name = m.group(1)
        sub_html, start, end = common.extract_closure(html[m.start():], ur'<ul\b', ur'</ul>')
        if end != 0:
            stores[cat_name] = get_detailed_store(sub_html, cat_name)
    return stores


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 1: 国家列表；2：城市列表；3：获得单独的门店信息
        """
        if level == 1:
            countries = get_countries(data['url'])
            return [{'func': lambda data: func(data, 2), 'data': c} for c in countries]
        elif level == 2:
            cities = get_cities(data)
            return [{'func': lambda data: func(data, 3), 'data': c} for c in cities]
        elif level == 3:
            store_list = get_stores(data)
            return [{'func': None, 'data': s} for s in store_list]

    global db
    db = common.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', brand_id))

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = common.walk_tree({'func': lambda data: func(data, level), 'data': data})
    db.disconnect_db()
    return results
