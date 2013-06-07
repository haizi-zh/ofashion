# coding=utf-8
import string
import urllib

__author__ = 'Zephyre'

import re
import common

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
    city_list = [{'city_e': m, 'country_e': data['country_e'], 'country_code': data['country_code']}
                 for m in re.findall(ur'<option value="(.+?)">', html)]
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
            html = html[end:]
            # 得到单个门店的页面代码
            entry = common.init_store_entry(brand_id)
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

            entry[common.city_e] = data[common.city_e]
            country_e = data[common.country_e]
            term = common.geo_translate(country_e)
            if len(term) == 0:
                print 'Error in geo translating: %s' % country_e
                entry[common.country_e] = country_e
            else:
                common.update_entry(entry, {common.continent_e: term[common.continent_e],
                                            common.continent_c: term[common.continent_c],
                                            common.country_e: term[common.country_e],
                                            common.country_c: term[common.country_c]})
            common.update_entry(entry, {common.brandname_e: brandname_e,
                                        common.brandname_c: brandname_c})
            common.chn_check(entry)
            print '%s: Found store: %s, %s (%s, %s)' % (
                brandname_e, entry[common.name_e], entry[common.addr_e], entry[common.country_e],
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
    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': url}
    results = common.walk_tree({'func': lambda data: func(data, level), 'data': data})
    db.disconnect_db()
    return results
