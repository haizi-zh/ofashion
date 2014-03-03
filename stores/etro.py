# coding=utf-8
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_continents(data):
    result = []
    for i in xrange(1, 5):
        d = data.copy()
        d['url'] = '%s%d/' % (data['url'], i)
        result.append(d)

    return result


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    m = re.search(ur'<\s*nav\s+class\s*=\s*"country-list"\s*>', body)
    if m is None:
        return []
    sub, start, end = cm.extract_closure(body[m.start():], ur'<nav\b', ur'</nav>')

    result = []
    for m in re.findall(ur'<\s*li\s*>\s*<a\s+href\s*=\s*"(.+?)"\s+title=.*?>\s*(.+?)\s*<\s*/\s*a\s*>', sub):
        d = data.copy()
        d['url'] = m[0].strip()
        d['country'] = m[1].strip().upper()
        result.append(d)
    return result


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    result = []
    for m in re.findall(ur'<li class="store">.+?<a href="(.+?)".+?</li>', body, re.S):
        d = data.copy()
        d['url'] = m.strip()
        result.append(d)
    return result


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    ret = gs.look_up(data['country'], 1)
    if ret is not None:
        entry[cm.country_e] = ret['name_e']
    m = re.search(ur'<span class="type">Address</span>\s*<p>(.+?)</p>', body, re.S)
    if m is not None:
        addr = cm.reformat_addr(m.group(1))
        country, province, city = gs.addr_sense(addr)
        if country is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = country
        if province is not None:
            entry[cm.province_e] = province
        if city is not None:
            entry[cm.city_e] = city
        entry[cm.addr_e] = addr

    m = re.search(ur'<span class="type">Phone</span>\s*<p>(.+?)</p>', body, re.S)
    if m is not None:
        entry[cm.tel] = m.group(1)

    m = re.search(ur'<span class="type">Opening hours</span>\s*<p>(.+?)</p>', body, re.S)
    if m is not None:
        entry[cm.hours] = cm.reformat_addr(m.group(1))

    m = re.search(ur'<span class="type">You can find</span>\s*<p>(.+?)</p>', body, re.S)
    if m is not None:
        entry[cm.store_type] = cm.reformat_addr(m.group(1))

    m = re.search(ur'google.maps.LatLng\(\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)', body, re.S)
    entry[cm.lat] = string.atof(m.group(1))
    entry[cm.lng] = string.atof(m.group(2))

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
            # 洲列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_continents(data)]
        if level == 1:
            # 国家列表
            return [{'func': lambda data: func(data, 2), 'data': s} for s in fetch_countries(data)]
        if level == 2:
            # 商店列表
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_stores(data)]
        if level == 3:
            # 商店详情
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.etro.com/en_wr/storefinder/get/list/continent/',
                'brand_id': 10127, 'brandname_e': u'Etro', 'brandname_c': u'艾特罗'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results