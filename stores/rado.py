# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'rado_log.txt'


def fetch_countries(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    sub = json.loads(body)['form']
    start = sub.find(ur'<select class="select ajax" name="country" id="ctrl_country">')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    results = []
    for m in re.findall(ur'<option value="([a-z]{2})">([^<>]+)', sub):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = m[1].strip()
        results.append(d)
    return results


def fetch_states(data):
    url = data['data_url']
    param = {'action': 'fmd', 'id': 50, 'g': 1, 'page': 7, 'country': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching states: %s, %s' % (url, param), log_name)
        return []

    sub = json.loads(body)['form']
    start = sub.find(ur'<select class="select ajax" name="subdivision"')
    if start == -1:
        d = data.copy()
        d['state_code'] = ''
        d['state'] = ''
        return [d]
    else:
        results = []
        sub = cm.extract_closure(sub[start:], ur'<select\b', ur'</select>')[0]
        for m in re.findall(ur'<option value="([^"]+)">([^<>]+)</option>', sub):
            d = data.copy()
            d['state_code'] = m[0]
            d['state'] = m[1].strip().upper()
            results.append(d)
        return results


def fetch_cities(data):
    url = data['data_url']
    param = {'action': 'fmd', 'id': 50, 'g': 1, 'page': 7, 'country': data['country_code'],
             'subdivision': data['state_code']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    sub = json.loads(body)['form']
    start = sub.find(ur'<select class="select ajax" name="city"')
    if start == -1:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    results = []
    sub = cm.extract_closure(sub[start:], ur'<select\b', ur'</select>')[0]
    for m in re.findall(ur'<option value="([^"]+)">([^<>]+)</option>', sub):
        d = data.copy()
        d['city'] = m[0]
        results.append(d)
    return results


def fetch_stores(data):
    if 'content' in data:
        sub = data['content']
    else:
        url = data['data_url']
        param = {'action': 'fmd', 'id': 50, 'g': 1, 'page': 7, 'country': data['country_code'],
                 '_escaped_fragment_': data['fragment']}
        try:
            body = cm.get_data(url, param)
        except Exception, e:
            cm.dump('Error in fetching store: %s, %s' % (url, param), log_name)
            return []
        sub = json.loads(body)['content']

    start = sub.find(ur'<div class="details">')
    if start == -1:
        cm.dump('Error in fetching store: %s, %s' % (url, param), log_name)
        return []
    detail_sub = cm.extract_closure(sub[start:], ur'<div\b', ur'</div>')[0]
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = data['country_code'].upper()
    entry[cm.province_e] = data['state']
    entry[cm.city_e] = cm.extract_city(data['city'])[0]

    m = re.search(ur'<h3>([^<>]+)</h3>', detail_sub, re.S)
    if m is not None:
        entry[cm.name_e] = m.group(1).strip()

    start = detail_sub.find(ur'<p class="address">')
    if start != -1:
        m = re.search(ur'<p\s*[^<>]+>(.+?)</p>', detail_sub[start:], re.S)
        if m is not None:
            entry[cm.addr_e] = cm.reformat_addr(m.group(1))

    start = detail_sub.find(ur'<p class="phone">')
    if start != -1:
        m = re.search(ur'<p\s*[^<>]+>(.+?)</p>', detail_sub[start:], re.S)
        if m is not None:
            entry[cm.tel] = cm.reformat_addr(m.group(1))

    start = detail_sub.find(ur'<p class="website">')
    if start != -1:
        m = re.search(ur'<p\s*[^<>]+>(.+?)</p>', detail_sub[start:], re.S)
        if m is not None:
            entry[cm.url] = cm.reformat_addr(m.group(1))

    m = re.search(ur'Rado.storeMap\(\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)', sub, re.S)
    if m is not None:
        entry[cm.lat] = string.atof(m.group(1))
        entry[cm.lng] = string.atof(m.group(2))

    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None and entry[cm.province_e] == '':
        entry[cm.province_e] = ret[1]
    if ret[2] is not None and entry[cm.city_e] == '':
        entry[cm.city_e] = ret[2]
    gs.field_sense(entry)

    cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                        entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                        entry[cm.continent_e]), log_name)
    db.insert_record(entry, 'stores')
    return [entry]


def fetch_store_list(data):
    url = data['data_url']
    param = {'action': 'fmd', 'id': 50, 'g': 1, 'country': data['country_code'], 'subdivision': data['state_code'],
             'city': data['city'], 'page': 7}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching store list: %s, %s' % (url, param), log_name)
        return []

    sub = json.loads(body)['content']
    results = []
    for m in re.finditer(ur'<div class="store">', sub):
        d = data.copy()
        store_sub = cm.extract_closure(sub[m.start():], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<p class="more">(.+?)</p>', store_sub)
        if m1 is None:
            d['content'] = store_sub
            results.append(d)
        else:
            m2 = re.search(ur'<a href="([^"]+)"', m1.group(1))
            if m2 is None:
                d['content'] = store_sub
                results.append(d)
            else:
                d['fragment'] = m2.group(1).split('/')[-1].split('.')[0]
                results.append(d)

    return results


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
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 2:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 4:
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.rado.com/ajax.php', 'host': 'http://www.rado.com',
                'home_url': 'http://www.rado.com/en/watch-shop.html',
                'brand_id': 10302, 'brandname_e': u'Rado', 'brandname_c': u'雷达'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results