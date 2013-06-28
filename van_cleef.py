# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'van_cleef_log.txt'


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    results = []
    for m in re.findall(ur'VCAGMapPlugin.addMarker\s*\((.+?)\);', body, re.S):
        term_list = cm.argument_parse(m)
        # term_list = m.split(',')
        d = data.copy()
        d['lat'] = string.atof(term_list[0])
        d['lng'] = string.atof(term_list[1])
        d['store_name'] = re.sub(ur"^\s*['\"](.+?)['\"]\s*$", ur'\1', term_list[2]).strip()
        m1 = re.search(ur"<a\s+href='([^']+)'\s+class='link'", m)
        if m1 is None:
            cm.dump(u'Cannot find details for %s' % m, log_name)
            continue
        d['url'] = data['host'] + m1.group(1)
        results.append(d)
    return tuple(results)


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []

    start = body.find(ur'<div id="store-address">')
    if start == -1:
        cm.dump('Error in fetching store details: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.lat] = data['lat']
    entry[cm.lng] = data['lng']
    entry[cm.name_e] = cm.html2plain(data['store_name'])

    pat_addr = re.compile(ur'<address>(.+?)</address>', re.S)
    m = re.search(pat_addr, body)
    if m is not None:
        entry[cm.addr_e] = cm.reformat_addr(m.group(1))
        body = re.sub(pat_addr, u'', body)

    for m in re.findall(ur'<li>(.+?)</li>', body, re.S):
        if u'Phone' in m:
            m1 = re.search(ur'<p>(.+?)</p>', m)
            if m1 is not None:
                entry[cm.tel] = cm.extract_tel(m1.group(1))
        elif u'Opening hours' in m:
            m1 = re.search(ur'<p>(.+?)</p>', m)
            if m1 is not None:
                entry[cm.hours] = cm.reformat_addr(m1.group(1).strip())

    entry[cm.lat] = data['lat']
    entry[cm.lng] = data['lng']

    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e])
    if ret[0] is not None and entry[cm.country_e] == '':
        entry[cm.country_e] = ret[0]
    if ret[1] is not None and entry[cm.province_e] == '':
        entry[cm.province_e] = ret[1]
    if ret[2] is not None and entry[cm.city_e] == '':
        entry[cm.city_e] = ret[2]
    gs.field_sense(entry)

    if entry[cm.country_e] == '' or entry[cm.city_e] == '':
        ret = None
        if entry[cm.lat] != '' and entry[cm.lng] != '':
            ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
        if ret is None:
            ret = gs.geocode(entry[cm.addr_e])

        if ret is not None:
            city = ''
            province = ''
            country = ''
            zip_code = ''
            tmp = ret[0]['address_components']
            for v in tmp:
                if 'locality' in v['types']:
                    city = v['long_name'].strip().upper()
                elif 'administrative_area_level_1' in v['types']:
                    province = v['long_name'].strip().upper()
                elif 'country' in v['types']:
                    country = v['long_name'].strip().upper()
                elif 'postal_code' in v['types']:
                    zip_code = v['long_name'].strip()
            entry[cm.country_e] = country
            entry[cm.province_e] = province
            entry[cm.city_e] = city
            entry[cm.zip_code] = zip_code

            gs.field_sense(entry)
            ret = gs.addr_sense(entry[cm.addr_e])
            if ret[0] is not None and entry[cm.country_e] == '':
                entry[cm.country_e] = ret[0]
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
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.vancleefarpels.com/cn/en/store-locator',
                'host': 'http://www.vancleefarpels.com',
                'brand_id': 10369, 'brandname_e': u'Van Cleef & Arpels', 'brandname_c': u'梵克雅宝'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


