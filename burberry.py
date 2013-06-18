# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'burberry_log.txt'


def fetch_continents(data):
    values = ['Europe', 'Americas', 'Asia Pacific', 'Middle East', 'Rest of the World']
    results = []
    for v in values:
        d = data.copy()
        d['continent'] = v
        results.append(d)
    return results


def fetch_countries(data):
    url = data['host'] + data['data_url']
    param = {'region': data['continent']}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return []

    raw = json.loads(body)['countries']
    results = []
    for item in raw:
        d = data.copy()
        d['country_code'] = item['id']
        d['country'] = item['name']
        results.append(d)
    return results


def fetch_provinces(data):
    url = data['host'] + data['data_url']
    param = {'region': data['continent'], 'country': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching provinces: %s, %s' % (url, param), log_name)
        return []

    raw = json.loads(body)['states']
    if len(raw) == 0:
        d = data.copy()
        d['province_code'] = ''
        d['province'] = ''
        return [d]
    else:
        results = []
        for item in raw:
            d = data.copy()
            d['province_code'] = item['id']
            d['province'] = item['name']
            results.append(d)
        return results


def fetch_stores(data):
    url = data['host'] + data['data_url']
    if data['province_code']!='':
        param = {'region': data['continent'], 'country': data['country_code'], 'state': data['province_code']}
    else:
        param = {'region': data['continent'], 'country': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    raw = json.loads(body)['stores']
    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = s['name']
        entry[cm.province_e] = data['province_code'].strip().upper()
        entry[cm.country_e] = data['country_code'].strip().upper()
        entry[cm.city_e] = s['city'].strip().upper()

        addr_list = []
        for tmp in ['address%d' % idx for idx in xrange(1, 4)]:
            if tmp in s and s[tmp] is not None and s[tmp].strip() != '':
                addr_list.append(s[tmp].strip())
        entry[cm.addr_e] = ', '.join(addr_list)
        entry[cm.tel] = s['phone']
        entry[cm.zip_code] = s['postalCode']
        hours = ''
        for item in s['openingHours']:
            hours += '%s %s' % (item['days'], item['openingHours'])
        entry[cm.hours] = hours
        entry[cm.url] = s['storePageUrl']
        coords = s['coords']
        entry[cm.lat] = string.atof(coords['lat'])
        entry[cm.lng] = string.atof(coords['lng'])

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
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_provinces(data)]
        if level == 3:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': '/store/burberry/storeset/json/stores.jsp',
                'host': 'http://us.burberry.com',
                'brand_id': 10057, 'brandname_e': u'Burberry', 'brandname_c': u'博柏丽'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results