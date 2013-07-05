# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'longines_log.txt'
national_added = False


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'Choose a country', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[m.start():], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<a href="([^"]+)">([^"]+)</a>', body):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['country'] = cm.html2plain(m[1]).strip().upper()
        if '/es' in d['url']:
            results.append(d)
    return results


def fetch_continents(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching continents: %s' % url, log_name)
        return []

    m = re.search(ur'<nav\s+id\s*=\s*"retailers_nav">', body)
    if m is None:
        cm.dump('Error in fetching continents: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[m.start():], ur'<nav\b', ur'</nav>')[0]
    results = []
    for m in re.findall(ur'<a href="([^"]+)">([^<>]+)</a>', body):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        if 'europe' in d['url']:
            results.append(d)
    return results


def fetch_states(data):
    global national_added

    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching states: %s' % url, log_name)
        return []

    national_added = False

    m = re.search(ur'Choose a (state|region|province)', body)
    if m is None:
        d = data.copy()
        d['state'] = ''
        return [d]

    body = cm.extract_closure(body[m.start():], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<a href="([^"]+)">([^<>]+)</a>', body):
        d = data.copy()
        d['url'] = data['host'] + m[0]
        d['state'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return results


def fetch_cities(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []

    m = re.search(ur'Choose a city', body)
    if m is None:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[m.start():], ur'<ul>', ur'</ul>')[0]
    results = []
    for m in re.findall(ur'<a href="([^"]+)">([^<>]+)</a>', body):
        d = data.copy()
        d['url'] = data['host'] + cm.html2plain(m[0])
        d['city'] = cm.html2plain(m[1]).strip().upper()
        results.append(d)
    return results


def fetch_stores(data):
    global national_added

    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    if national_added:
        pat = re.compile(ur'<div\s+class\s*=\s*"retailer_address"')
    else:
        pat = re.compile(ur'<div\s+class\s*=\s*"retailer_address(\s+national)?"')
    national_added = True

    for m in re.finditer(pat, body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.province_e] = data['state']
        entry[cm.city_e] = cm.extract_city(data['city'])[0]
        if u'national' in m.group():
            entry[cm.store_type] = u'National Distributor'
        else:
            entry[cm.store_type] = u'Retailer'

        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        name_list = [cm.reformat_addr(tmp) for tmp in re.findall(ur'<h3 itemprop="name">([^<>]+)</h3>', sub)]
        entry[cm.name_e] = ', '.join(name_list)
        addr_list = [cm.reformat_addr(tmp) for tmp in
                     re.findall(ur'<span itemprop="street-address">([^<>]+)</span>', sub)]
        city_addr = ''
        m1 = re.search(ur'<span itemprop="locality">([^<>]+)</span>', sub)
        if m1 is not None:
            city_addr = cm.html2plain(m1.group(1)).strip()
        m1 = re.search(ur'<span itemprop="postal-code">([^<>]+)</span>', sub)
        if m1 is not None:
            entry[cm.zip_code] = cm.html2plain(m1.group(1)).strip()
            if city_addr != '':
                city_addr = u'%s %s' % (entry[cm.zip_code], city_addr)
        if city_addr != '':
            addr_list.append(city_addr)
        m1 = re.search(ur'<span itemprop="region">([^<>]+)</span>', sub)
        if m1 is not None:
            addr_list.append(cm.html2plain(m1.group(1)).strip())
            if entry[cm.province_e] == '':
                entry[cm.province_e] = cm.html2plain(m1.group(1)).strip().upper()
        entry[cm.addr_e] = ', '.join(addr_list)
        m1 = re.search(ur'<span itemprop="tel">([^<>]+)</span>', sub)
        if m1 is not None:
            entry[cm.tel] = m1.group(1).strip()
        m1 = re.search(ur'Fax\s*:\s*([^<>]+)', sub)
        if m1 is not None:
            entry[cm.fax] = m1.group(1).strip()
        m1 = re.search(ur'll=(-?\d+\.\d+),(-?\d+\.\d+)', sub)
        if m1 is not None:
            entry[cm.lat] = string.atof(m1.group(1))
            entry[cm.lng] = string.atof(m1.group(2))

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


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 洲列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 2:
            # 州列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        if level == 3:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 4:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.longines.com/retailers/',
                'host': 'http://www.longines.com',
                'brand_id': 10222, 'brandname_e': u'Longines', 'brandname_c': u'浪琴'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

