# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'alexander_mcqueen_log.txt'


def fetch_store_list(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching store list: %s' % url, log_name)
        return []

    results = []
    for sub in re.findall(ur'<div class="box-title-store">(.+?)</div>', body, re.S):
        d = data.copy()
        m = re.search(ur'href="([^"]+)"', sub)
        d['url'] = m.group(1).strip() if m else ''
        m = re.search(ur'<h5>(.+?)</h5>', sub)
        d['store_name'] = cm.html2plain(m.group(1)).strip()
        results.append(d)
    return tuple(results)


def fetch_store_details(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.name_e] = data['store_name']

    m = re.search(ur'json_init_map\s*=\s*\["(-?\d+\.?\d*)"\s*,\s*"(-?\d+\.?\d*)"', body)
    if m is not None:
        entry[cm.lat] = string.atof(m.group(1))
        entry[cm.lng] = string.atof(m.group(2))

    start = body.find(ur'<div class="box-testuale-right">')
    if start == -1:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []
    sub = cm.extract_closure(body[start:], ur'<div\b', ur'</div>')[0]
    m = re.search(ur'<div class="box-adress-store">(.+?)</div>', sub, re.S)
    if m is None:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []
    entry[cm.addr_e] = cm.reformat_addr(m.group(1))
    m = re.search(ur'<h4>(.+?)</h4>', sub)
    if m is not None and 't:' in m.group(1).lower():
        entry[cm.tel] = cm.extract_tel(m.group(1))
    m = re.search(ur'<div class="box-open-store">(.+?)</div>', body, re.S)
    if m is not None:
        entry[cm.hours] = cm.reformat_addr(m.group(1))

    ret = None
    if entry[cm.lat] != '' and entry[cm.lng] != '':
        ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))
    if ret is None:
        ret = gs.geocode(', '.join((entry[cm.addr_e], data['zone'])))
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
        data = {'url': 'http://www.alexandermcqueen.com/experience/en/alexandermcqueen/store/',
                'brand_id': 10008, 'brandname_e': u'Alexander McQueen', 'brandname_c': u'亚历山大·麦昆'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


