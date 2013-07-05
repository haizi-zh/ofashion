# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'audemars_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<ul id="country" class="dropdown-dk">(.+?)</ul>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    sub = m.group(1)
    results = []
    for item in re.findall(ur'<li[^<>]*><a href="([^"]+)">([^<>]+)', sub):
        d = data.copy()
        d['url'] = data['host'] + item[0]
        d['country'] = item[1].strip().upper()
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for m in re.finditer(ur'<div class="vcard storeListing">', body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = cm.extract_city(data['city'])[0]

        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        m = re.search(ur'<h3 class="org">(.+)', sub)
        if m is not None:
            entry[cm.name_e] = cm.reformat_addr(m.group(1)).upper()

        m = re.search(ur'<span class="street-address">(.+?)</span>', sub, re.S)
        if m is not None:
            entry[cm.addr_e] = cm.reformat_addr(m.group(1))

        m = re.search(ur'<span class="postal-code">([^<>]+)</span>', sub, re.S)
        if m is not None:
            entry[cm.zip_code] = m.group(1).strip()

        m = re.search(ur'maps\.google\.com/\?q=(-?\d+\.?\d*),(-?\d+\.?\d*)', sub)
        if m is not None:
            entry[cm.lat] = string.atof(m.group(1))
            entry[cm.lng] = string.atof(m.group(2))

        for m in re.findall(ur'<div class="tel">(.+?)</div>', sub):
            if 'voice' in m:
                entry[cm.tel] = cm.extract_tel(cm.reformat_addr(m).replace('t.', ''))
            elif 'fax' in m:
                entry[cm.fax] = cm.extract_tel(cm.reformat_addr(m).replace('f.', ''))

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
        store_list.append(entry)

    return store_list


def fetch_cities(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []

    m = re.search(ur'<ul id="city" class="dropdown-dk">(.+?)</ul>', body, re.S)
    if m is None:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []
    sub = m.group(1)
    results = []
    for item in re.findall(ur'<li[^<>]*><a href="([^"]+)">([^<>]+)', sub):
        d = data.copy()
        d['url'] = data['host'] + item[0]
        d['city'] = item[1].strip().upper()
        results.append(d)
    return tuple(results)


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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://www.audemarspiguet.com',
                'url': 'http://www.audemarspiguet.com/en/where-to-buy',
                'brand_id': 10022, 'brandname_e': u'Audemars Piguet', 'brandname_c': u'爱彼'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


