# coding=utf-8
import json
import string
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'bulgari_log.txt'

type_map = {'wa': u'Watches', 'pfm': u'Fragrances', 'eyw': u'Eyewear', 'jwl': u'Jewels',
            'acc': u'Accessories', 'skn': u'Skincare'}


def fetch_continents(data):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': 1}

    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching continents: %s, %s' % (url, param), log_name)
        return []

    results = []
    for c in json.loads(body)['geoEntityLocaleList']:
        d = data.copy()
        d['continent_id'] = string.atoi(c['geoEntity']['id'])
        d['continent'] = c['geoEntity']['name'].strip()
        results.append(d)
    return results


def fetch_countries(data):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': data['continent_id']}

    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return []

    results = []
    for c in json.loads(body)['geoEntityLocaleList']:
        d = data.copy()
        d['country_id'] = string.atoi(c['geoEntity']['id'])
        d['country'] = cm.html2plain(c['geoEntity']['name']).strip()
        results.append(d)

    for item in results:
        if gs.look_up(item['country'].upper(), 1) is None:
            print 'Cannot look up %s' % item['country']
    return results


def fetch_states(data):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': data['country_id']}

    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching states: %s, %s' % (url, param), log_name)
        return []

    results = []
    raw=json.loads(body)
    if 'geoEntityLocaleList' not in raw:
        return []
    for c in raw['geoEntityLocaleList']:
        d = data.copy()
        if 'geoAccurayId' not in c['geoEntity'] or c['geoEntity']['geoAccurayId'] != '2':
            d['state_id'] = data['country_id']
            d['state'] = ''
            return [d]
        else:
            d['state_id'] = string.atoi(c['geoEntity']['id'])
            d['state'] = cm.html2plain(c['geoEntity']['name']).strip()
            results.append(d)
    return results


def fetch_cities(data):
    url = data['host'] + data['geo_url']
    param = {'lang': 'EN_US', 'geo_id': data['state_id']}

    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    results = []
    raw = json.loads(body)
    if 'geoEntityLocaleList' not in raw:
        return []
    for c in raw['geoEntityLocaleList']:
        d = data.copy()
        d['city_id'] = string.atoi(c['geoEntity']['id'])
        d['city'] = cm.html2plain(c['geoEntity']['name']).strip()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['host'] + data['store_url']
    param = {'lang': 'EN_US', 'geo_id': data['city_id']}

    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    raw = json.loads(body)
    if 'storeList' not in raw:
        return []
    for s in raw['storeList']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.city_e] = cm.extract_city(data['city'])[0]
        entry[cm.province_e] = data['state'].upper()
        entry[cm.country_e] = data['country'].upper()
        entry[cm.store_class] = s['type']['name']
        entry[cm.store_type] = ', '.join(type_map[item['name']] for item in s['categories'])
        entry[cm.name_e] = s['name'].strip()

        loc = s['location']
        entry[cm.addr_e] = cm.reformat_addr(loc['address'])
        if 'phone' in loc and loc['phone'] is not None:
            entry[cm.tel] = loc['phone'].strip()
        if 'fax' in loc and loc['fax'] is not None:
            entry[cm.fax] = loc['fax'].strip()
        if 'postalCode' in loc and loc['postalCode'] is not None:
            entry[cm.zip_code] = loc['postalCode'].strip()
        if 'latitude' in loc and loc['latitude'] is not None and loc['latitude'].strip() != '':
            entry[cm.lat] = string.atof(loc['latitude'])
        if 'longitude' in loc and loc['longitude'] is not None and loc['longitude'].strip() != '':
            entry[cm.lng] = string.atof(loc['longitude'])

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
        data = {'host': 'http://stores.bulgari.com',
                'geo_url': '/blgsl/js-geoentities.html',
                'store_url': '/blgsl/js-stores.html',
                'brand_id': 10058, 'brandname_e': u'BVLGARI', 'brandname_c': u'宝格丽'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

