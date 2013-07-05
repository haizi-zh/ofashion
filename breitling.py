# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'breitling_log.txt'
store_map = {}


def fetch_continents(data):
    url = data['url'] % 0
    try:
        raw = json.loads(cm.get_data(url))['places']
    except Exception, e:
        cm.dump('Error in fetching continents: %s' % url, log_name)
        return ()

    results = []
    for key in raw:
        if key == '_listId':
            continue
        d = data.copy()
        d['continent_id'] = raw[key]['id']
        d['continent'] = raw[key]['name'].strip().upper()
        # if d['continent_id'] == 1199:
        results.append(d)
    return tuple(results)


def fetch_store_detail(s, data, isOfficial=False):
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

    entry[cm.name_e] = cm.html2plain(s['name']).strip()
    entry[cm.country_e] = data['country']
    val = cm.html2plain(s['city']).strip().upper()
    entry[cm.city_e] = cm.extract_city(val if val and val != '' else data['city'])[0]
    entry[cm.addr_e] = cm.html2plain(s['address']).strip()
    entry[cm.email] = s['email'].strip()
    entry[cm.tel] = s['phone'].strip()
    entry[cm.fax] = s['fax'].strip()
    entry[cm.store_class] = 'Official Retailer' if isOfficial else 'Retailer'

    try:
        entry[cm.lat] = string.atof(s['lat']) if s['lat'] != '' else ''
    except (ValueError, KeyError, TypeError) as e:
        cm.dump('Error in fetching lat: %s' % str(e), log_name)
    try:
        entry[cm.lng] = string.atof(s['lng']) if s['lng'] != '' else ''
    except (ValueError, KeyError, TypeError) as e:
        cm.dump('Error in fetching lng: %s' % str(e), log_name)

    gs.field_sense(entry)
    ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
    if ret[1] is not None and entry[cm.province_e] == '':
        entry[cm.province_e] = ret[1]
    if ret[2] is not None and entry[cm.city_e] == '':
        entry[cm.city_e] = ret[2]
    gs.field_sense(entry)

    return entry


def fetch_stores(data):
    url = data['url'] % data['city_id']
    try:
        raw = json.loads(cm.get_data(url))
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    store_list = []
    for key in raw['retailers']:
        if key == '_listId':
            continue
        entry = fetch_store_detail(raw['retailers'][key], data, False)

        cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                                                                entry[cm.country_e], entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    s = raw['officialRetailer']
    if s:
        entry = fetch_store_detail(s, data, True)
        sid = s['id']
        if sid not in store_map:
            cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                                entry[cm.continent_e]), log_name)
            db.insert_record(entry, 'stores')
            store_list.append(entry)
            store_map[sid] = entry

    return tuple(store_list)


def fetch_countries(data):
    url = data['url'] % data['continent_id']
    try:
        raw = json.loads(cm.get_data(url))['places']
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    results = []
    for key in raw:
        if key == '_listId':
            continue
        d = data.copy()
        d['country_id'] = raw[key]['id']
        d['country'] = raw[key]['name'].strip().upper()
        # if d['country'] == 'USA':
        results.append(d)
    return tuple(results)


def fetch_cities(data):
    url = data['url'] % data['country_id']
    try:
        raw = json.loads(cm.get_data(url))['places']
    except Exception, e:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()

    results = []
    for key in raw:
        if key == '_listId':
            continue
        d = data.copy()
        d['city_id'] = raw[key]['id']
        d['city'] = raw[key]['name'].strip().upper()
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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'xxxxxxxxxx',
                'url': 'http://www.breitling.com/en/retailers/getData/%d/retailer',
                'brand_id': 10054, 'brandname_e': u'Breitling', 'brandname_c': u'百年灵'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


