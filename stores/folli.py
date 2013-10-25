# coding=utf-8
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for m in re.findall(ur'<li><a href="" rel="([A-Z]{2})"', body):
        d = data.copy()
        d['country_code'] = m
        results.append(d)
    return results


def fetch_cities(data):
    url = data['host'] + '/ajax/esiajaxProxy.asp'
    try:
        body = cm.get_data(url, {'c': 'FF_StoreLocator2',
                                 'm': 'getCountiesAjax',
                                 'ws': 'ch-ch',
                                 'pid': 178,
                                 'cid': data['country_code'], 'CT': 0})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for m in re.findall(ur'<li><a href="" data-value="(.+?)">', body):
        d = data.copy()
        d['city'] = m
        results.append(d)
    return results


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.post_data(url, {'cCode': data['country_code'], 'city': data['city'], 'postsearch': 1})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    results = []
    for m in re.finditer(ur'<td class\s*=\s*"ftd"', body):
        end = body.find('</tr>', m.start())
        sub = body[m.start():end]
        m1 = re.search(ur'<td class="ltd"><a href="(.+?)">', sub)
        if m1 is None:
            print 'Cannot find details: %s / %s' % (data['country_code'], data['city'])
        else:
            d = data.copy()
            d['url'] = data['host'] + m1.group(1)
            results.append(d)

    return results


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.post_data(url, {'cCode': data['country_code'], 'city': data['city'], 'postsearch': 1})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = body.find('<div class="store_locator')
    if start == -1:
        print 'Failed processing %s' % url
        return []
    sub, start, end = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    m = re.search(ur'<p><span class="bold">Address</span>(.+?)</p>', sub, re.S)
    if m is not None:
        addr_list = cm.reformat_addr(m.group(1)).split(', ')
        ret = cm.extract_tel(addr_list[-1])
        if ret != '':
            entry[cm.tel] = ret
            del addr_list[-1]
        entry[cm.addr_e] = ', '.join(addr_list)

    addr_text=sub[m.end():]
    m = re.search(ur'<div class="title locator">', addr_text)
    if m is not None:
        tmp = cm.extract_closure(addr_text[m.start():], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<h2>(.+?)</h2>', tmp, re.S)
        if m1 is not None:
            entry[cm.addr_e] = cm.reformat_addr(m1.group(1))

    m = re.search(ur'google.maps.LatLng\(\s*(-?\d+\.\d+),\s*(-?\d+\.\d+)', body, re.S)
    if m is not None:
        entry[cm.lat] = string.atof(m.group(1))
        entry[cm.lng] = string.atof(m.group(2))

    entry[cm.country_e] = data['country_code']
    entry[cm.city_e] = data['city']
    gs.field_sense(entry)
    print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                      entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                      entry[cm.continent_e])
    db.insert_record(entry, 'stores')
    return [entry]


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 3:
            # 商店详情
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.follifollie.com.cn/ch-ch/store-locator',
                'host': 'http://www.follifollie.com.cn',
                'brand_id': 10138, 'brandname_e': u'Folli Follie', 'brandname_c': u'芙丽芙丽'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results