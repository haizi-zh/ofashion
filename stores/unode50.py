# coding=utf-8
import json
import string
import re
import urllib
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_cities(data):
    url = data['home_url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching cities: %s' % url, 'unode50_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    m = re.search(ur'countries\s*=\s*\{', body)
    if m is None:
        cm.dump('Error in fetching cities: %s' % url, 'unode50_log.txt')
        return []
    body = cm.extract_closure(body[m.start():], ur'\{', ur'\}')[0]
    raw = json.loads(body)
    results = []
    for key in raw:
        d = data.copy()
        d['country'] = raw[key]['name'].strip().upper()
        d['country_id'] = key
        results.append(d)
    return results


def fetch_stores(data):
    url = '%s/en/shops/%s.json' % (data['host'], urllib.quote(data['country_id'].encode('utf-8')))
    try:
        body = cm.get_data(url)
        raw = json.loads(body)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, 'unode50_log.txt')
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    store_items = []
    for s in raw['distributors']:
        s['store_class'] = 'distributor'
        store_items.append(s)
    for s in raw['shops']:
        s['store_class'] = 'shop'
        store_items.append(s)

    store_list = []
    for s in store_items:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.store_class] = s['store_class']
        entry[cm.country_e] = data['country']
        entry[cm.name_e] = s['title']
        if s['lat'] is not None:
            entry[cm.lat] = string.atof(str(s['lat']))
        if s['lng'] is not None:
            entry[cm.lng] = string.atof(str(s['lng']))
        entry[cm.addr_e] = s['address']
        entry[cm.city_e] = cm.extract_city(s['city'])[0]
        entry[cm.tel] = s['phone']
        entry[cm.zip_code] = s['postal_code']
        gs.field_sense(entry)
        cm.dump('(%s / %d) Found store: %s, %s (%s, %s, %s)' % (data['brandname_e'], data['brand_id'],
                                                                entry[cm.name_e], entry[cm.addr_e], entry[cm.city_e],
                                                                entry[cm.country_e],
                                                                entry[cm.continent_e]), 'unode50_log.txt')
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
            # 城市列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_cities(data)]
        if level == 1:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://us.unode50.com/en/shops',
                'host': 'http://us.unode50.com',
                'brand_id': 10363, 'brandname_e': u'Uno de 50', 'brandname_c': u'Uno de 50'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results