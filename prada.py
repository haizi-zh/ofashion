# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'prada_log.txt'


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching store lists: %s' % url, log_name)
        return []

    start = body.find(ur"<div class='store-country'>")
    if start == -1:
        cm.dump('Error in fetching store lists: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    start_stores = body.find(ur'<h3><a href="/store-locator/index">Stores</a></h3>')
    start_outlets = body.find(ur"<h3 class='outlets'>")
    store_sub = body[start_stores:start_outlets]
    outlet_sub = body[start_outlets:]

    results = []
    for m1 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="country">([^<>]+)</a>', store_sub):
        country_id = string.atoi(m1.group(1))
        country = m1.group(2).strip()
        sub1 = cm.extract_closure(store_sub[m1.end():], ur'<ul>', ur'</ul>')[0]
        for m2 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="city">([^<>]+)</a>', sub1):
            city_id = string.atoi(m2.group(1))
            city = m2.group(2).strip()
            sub2 = cm.extract_closure(sub1[m2.end():], ur'<ul>', ur'</ul>')[0]
            for m3 in re.finditer(ur'<a href="([^"]+)"[^<>]*data-id="([^"]+)"[^<>]*data-type="store">([^<>]+)</a>',
                                  sub2):
                d = data.copy()
                d['country_id'] = country_id
                d['country'] = country
                d['city_id'] = city_id
                d['city'] = city
                d['url'] = m3.group(1).strip()
                d['store_id'] = string.atoi(m3.group(2))
                d['store'] = cm.html2plain(m3.group(3).strip())
                # d['store_type'] = 'store'
                results.append(d)

    for m1 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="country">([^<>]+)</a>', outlet_sub):
        country_id = string.atoi(m1.group(1))
        country = m1.group(2).strip()
        sub1 = cm.extract_closure(outlet_sub[m1.end():], ur'<ul>', ur'</ul>')[0]
        for m2 in re.finditer(ur'<a [^<>]*data-id="([^"]+)"[^<>]*data-type="city">([^<>]+)</a>', sub1):
            city_id = string.atoi(m2.group(1))
            city = m2.group(2).strip()
            sub2 = cm.extract_closure(sub1[m2.end():], ur'<ul>', ur'</ul>')[0]
            for m3 in re.finditer(ur'<a href="([^"]+)"[^<>]*data-id="([^"]+)"[^<>]*data-type="store">([^<>]+)</a>',
                                  sub2):
                d = data.copy()
                d['country_id'] = country_id
                d['country'] = country
                d['city_id'] = city_id
                d['city'] = city
                d['url'] = m3.group(1).strip()
                d['store_id'] = string.atoi(m3.group(2))
                d['store'] = m3.group(3).strip()
                d['store_type'] = 'outlet'
                results.append(d)

    return results


def fetch_store_details(data):
    url = data['host'] + data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    start = body.find(ur'<h3>available in store</h3>')
    if start != -1:
        type_sub = cm.extract_closure(body[start:], ur'<ul\b', ur'</ul>')[0]
        entry[cm.store_type] = ', '.join(
            cm.html2plain(tmp).strip() for tmp in re.findall(ur'<li[^<>]*>(.+?)</li>', type_sub, re.S))

    start = body.find(ur"<div class='gmap_info_box'")
    if start == -1:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    raw = json.loads(cm.extract_closure(body, ur'\{', ur'\}')[0])['table']
    entry[cm.name_e] = cm.html2plain(raw['name'])
    entry[cm.city_e] = data['city'].strip().upper()
    entry[cm.country_e] = data['country'].strip().upper()
    # entry[cm.store_type] = data['store_type']
    entry[cm.addr_e] = cm.reformat_addr(raw['address'])
    m = re.search(re.compile(ur'phone:(.*?)fax:(.*?)', re.I | re.S), raw['phone'])
    if m is not None:
        entry[cm.tel] = m.group(1).strip()
        entry[cm.fax] = m.group(2).strip()
    else:
        m = re.search(re.compile(ur'phone:(.*?)', re.I | re.S), raw['phone'])
        if m is not None:
            entry[cm.tel] = m.group(1).strip()
        m = re.search(re.compile(ur'fax:(.*?)', re.I | re.S), raw['phone'])
        if m is not None:
            entry[cm.fax] = m.group(1).strip()
    entry[cm.hours] = raw['hours']
    if raw['lat'] is not None and raw['lat'] != '':
        entry[cm.lat] = string.atof(raw['lat'])
    if raw['lng'] is not None and raw['lng'] != '':
        entry[cm.lat] = string.atof(raw['lng'])
    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None:
        entry[cm.province_e] = ret[1]
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
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.prada.com/en/store-locator?cc=CN',
                'host': 'http://www.prada.com',
                'brand_id': 10300, 'brandname_e': u'Prada', 'brandname_c': u'普拉达'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results