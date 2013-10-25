# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'rolex_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    start = body.find(ur'<div id="rlx-locator-list-country"')
    if start == -1:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<li [^<>]+><a href="([^"]+)">([^<>]+)', body):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['country'] = cm.html2plain(m[1]).strip()
        results.append(d)
    return results


def fetch_states(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return []

    start = body.find(ur'<div id="rlx-locator-list-department"')
    if start == -1:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<li [^<>]+><a href="([^"]+)">([^<>]+)', body):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['state'] = cm.html2plain(m[1]).strip()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []

    start = body.find(ur'<div id="rlx-locator-list-city"')
    if start == -1:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<li [^<>]+><a href="([^"]+)">([^<>]+)', body):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['city'] = cm.html2plain(m[1]).strip()
        results.append(d)
    return results


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching store list: %s' % url, log_name)
        return []

    start = body.find(ur'<div id="rlx-retailers-container"')
    if start == -1:
        cm.dump('Error in fetching store list: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
    results = []
    for m in re.finditer(ur'<div class="rlx-retailer rlx-component vcard"', body):
        store_sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        m1 = re.search(ur'<li><a href="([^"]+)">\s*MORE DETAILS', store_sub, re.S)
        if m1 is None:
            cm.dump('Error in fetching store details: %s' % url, log_name)
            continue
        d = data.copy()
        d['url'] = data['host'] + m1.group(1)
        results.append(d)
    return results


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []

    start = body.find(ur'<div class="rlx-textblock rlx-retailer-vcard"')
    if start == -1:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = data['country'].upper()
    entry[cm.province_e] = data['state'].upper()
    entry[cm.city_e] = cm.extract_city(data['city'])[0]

    m = re.search(ur'<h1 itemprop="name"[^<>]*>([^<>]+)</h1>', body)
    if m is not None:
        entry[cm.name_e] = cm.reformat_addr(m.group(1))

    start = body.find(ur'<div class="rlx-retailer-adr">')
    if start != -1:
        addr_sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
        m = re.search(ur'<span itemprop="streetAddress"[^<>]*>(.+?)</span>', addr_sub, re.S)
        if m is not None:
            entry[cm.addr_e] = cm.reformat_addr(m.group(1))
        m = re.search(ur'<p itemprop="telephone"[^<>]*>(.+?)</p>', addr_sub, re.S)
        if m is not None:
            entry[cm.tel] = cm.extract_tel(m.group(1))
        m = re.search(ur'<p itemprop="faxNumber"[^<>]*>(.+?)</p>', addr_sub, re.S)
        if m is not None:
            entry[cm.fax] = cm.extract_tel(m.group(1))

    start = body.find(ur'<div class="rlx-retailer-openinghours">')
    if start != -1:
        hours_sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
        m = re.search(ur'<p>(.+?)</p>', hours_sub, re.S)
        if m is not None:
            entry[cm.hours] = cm.reformat_addr(m.group(1))

    start = body.find(ur'<div class="rlx-nav-wrapper">')
    if start != -1:
        nav_sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
        m = re.search(ur'll=(-?\d+\.\d+),(-?\d+\.\d+)', nav_sub)
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
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://www.rolex.com',
                'url': 'http://www.rolex.com/rolex-dealers/dealers-locator.html',
                'brand_id': 10306, 'brandname_e': u'Rolex', 'brandname_c': u'劳力士'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
