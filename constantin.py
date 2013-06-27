# coding=utf-8
import json
import string
import re
import urllib
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'constantin_log.txt'


def fetch_continents(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching continents: %s' % url, log_name)
        return []

    start = body.find(ur'<select name="code_continent"')
    if start == -1:
        cm.dump('Error in fetching continents: %s' % url, log_name)
        return []
    results = []
    for m in re.findall(ur'<\s*option\s+value\s*=\s*"([A-Z]{3})"\s*>([^<>]+)',
                        cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]):
        d = data.copy()
        d['continent_code'] = m[0]
        d['continent'] = m[1].strip().upper()
        results.append(d)
    return tuple(results)


def proc_store(sub, data):
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.country_e] = data['country']
    m1 = re.search(ur'<strong class="name" itemprop="name">([^<>]+)</strong>', sub)
    if m1 is not None:
        entry[cm.store_class] = m1.group(1).strip()

    m1 = re.search(ur'<span itemprop="address"', sub)
    if m1 is not None:
        addr_sub = cm.extract_closure(sub[m1.start():], ur'<span\b', ur'</span>')[0]
        m2 = re.search(ur'<span itemprop="postal-code">([^<>]+)</span>', addr_sub, re.S)
        if m2 is not None:
            entry[cm.zip_code] = m2.group(1).strip()
        m2 = re.search(ur'<span itemprop="locality">([^<>]+)</span>', addr_sub, re.S)
        if m2 is not None:
            entry[cm.city_e] = cm.html2plain(m2.group(1)).strip().upper()
        entry[cm.addr_e] = cm.reformat_addr(addr_sub)

    m2 = re.search(ur'<span itemprop="tel">([^<>]+)</span>', sub, re.S)
    if m2 is not None:
        entry[cm.tel] = m2.group(1).strip()

    m2 = re.search(ur'Fax\b(.+?)</p>', sub)
    if m2 is not None:
        entry[cm.fax] = cm.extract_tel(m2.group(1))

    m2 = re.search(ur'<a href="([^"]+)"[^<>]+itemprop="url"\s*>\s*Find on a map\s*</a>', sub)
    if m2 is not None:
        geo_url = data['host'] + urllib.quote(m2.group(1).encode('utf-8'))
        param = {'brepairs': True, 'restrictedtemplate': 2, 'bretailers': True, 'bshops': True, 'brepairs': True}
        try:
            geo_body = cm.get_data(geo_url, param)
            m3 = re.search(ur'maps\.google\.com/maps\?daddr\s*=\s*(-?\d+\.\d+)\s*,\s*(-?\d+\.\d+)', geo_body)
            if m3 is not None:
                entry[cm.lat] = string.atof(m3.group(1))
                entry[cm.lng] = string.atof(m3.group(2))
        except Exception, e:
            cm.dump('Error in fetching geo info: %s, %s' % (geo_url, param), log_name)

    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None and entry[cm.province_e] == '':
        entry[cm.province_e] = ret[1]
    if ret[2] is not None and entry[cm.city_e] == '':
        entry[cm.city_e] = ret[2]
    gs.field_sense(entry)

    return entry


def fetch_stores(data):
    url = data['data_url']
    param = {'module': 'pointsOfSaleAdvanced', 'action': 'ajaxGetURI',
             'code_uri': '%s/%s/%s/' % tuple(data[key] for key in ('continent_code', 'country_code', 'place_code')),
             'languageId': 1}
    try:
        new_url = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    url = data['host'] + urllib.quote(new_url.encode('utf-8'))
    param = {'type': '10,5,15'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    start = body.find(ur'<div id="results"')
    if start == -1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    store_list = []
    for m in re.finditer(ur'<div class="boutique"', body):
        sub = cm.extract_closure(body[m.start():], ur'<div\b', ur'</div>')[0]
        entry = proc_store(sub, data)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    for sub in re.findall(ur'<td [^<>]+>(.+?)</td>', body, re.S):
        entry = proc_store(sub, data)
        entry[cm.store_class] = 'Authorized retailer'
        m = re.search(ur'<strong>([^<>]+)</strong>', sub)
        if m is not None:
            entry[cm.store_class] = m.group(1).strip()

        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return tuple(store_list)


def fetch_countries(data):
    url = data['data_url']
    param = {'module': 'pointsOfSaleAdvanced', 'action': 'ajaxLoadCountries',
             'code_continent': data['continent_code'], 'languageId': 1}
    url += '?' + urllib.urlencode(param) + '&type[]=10&type[]=5&type[]=15'
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    results = []
    for m in re.findall(ur'<option\s+value\s*=\s*"([A-Z]{2})"\s*>([^<>]+)', body, re.S):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = cm.html2plain(m[1]).strip().upper()
        # if d['country_code'] == 'IT':
        results.append(d)
    return tuple(results)


def fetch_places(data):
    url = data['data_url']
    param = {'module': 'pointsOfSaleAdvanced', 'action': 'ajaxLoadCountries',
             'code_continent': data['continent_code'], 'code_pays': data['country_code'], 'languageId': 1}
    url += '?' + urllib.urlencode(param) + '&type[]=10&type[]=5&type[]=15'

    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    results = []
    for m in re.findall(ur'<option\s+value\s*=\s*"([^"]+)"[^<>]*>([^<>]+)', body, re.S):
        d = data.copy()
        d['place_code'] = m[0]
        d['place'] = cm.html2plain(m[1]).strip().upper()
        # if d['place_code']=='IT_LUC':
        results.append(d)
    return tuple(results)


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
            # 州/城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_places(data)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.vacheron-constantin.com/index.php',
                'host': 'http://www.vacheron-constantin.com',
                'url': 'http://www.vacheron-constantin.com/en2/boutiques-authorized-retailers-customer-service',
                'brand_id': 10366, 'brandname_e': u'Vacheron Constantin', 'brandname_c': u'江诗丹顿'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


