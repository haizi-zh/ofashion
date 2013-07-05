# coding=utf-8
import json
import string
import re
import urllib
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'ecco_log.txt'
store_map = {}


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_countries(data):
    results = []
    for item in data['city_map']:
        d = data.copy()
        d['country'] = item
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['url']
    lat, lng = data['city_lat'], data['city_lng']
    param = {'json': 1, 'lat': lat, 'lng': lng, 'latLow': lat - 0.25, 'latHigh': lat + 0.25, 'lngLow': lng - 0.25,
             'lngHigh': lng + 0.25, 'includeResellers': 'true'}
    try:
        body = cm.post_data('%s?%s' % (url, urllib.urlencode(param)))
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for s in json.loads(body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        sid = s['StoreId']
        if sid in store_map:
            continue
        store_map[sid] = entry

        entry[cm.store_type] = ', '.join(s['CategoryList']) if s['CategoryList'] else ''
        entry[cm.name_e] = cm.html2plain(s['Name']).strip().upper() if s['Name'] else ''
        entry[cm.city_e] = cm.extract_city(s['City'] if s['City'] else data['city'])[0]
        entry[cm.country_e] = data['country']
        entry[cm.email] = s['Email'].strip() if s['Email'] else ''
        entry[cm.tel] = s['Phone'].strip() if s['Phone'] else ''
        entry[cm.zip_code] = s['PostalCode'].strip() if s['PostalCode'] else ''
        entry[cm.lat] = s['Latitude'] if s['Latitude'] else ''
        entry[cm.lng] = s['Longitude'] if s['Longitude'] else ''

        weekdays = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        entry[cm.hours] = ', '.join(u'%s: %s - %s' % (day, s['%sOpen' % day], s['%sClose' % day]) for day in weekdays)

        addr_list = []
        term = cm.html2plain(s['Floor']).strip() if s['Floor'] else ''
        if term != '':
            addr_list.append(term)
        term = cm.html2plain(s['HouseNumber']).strip() if s['HouseNumber'] else ''
        if term != '':
            addr_list.append(term)
        term = cm.html2plain(s['Street']).strip() if s['Street'] else ''
        if term != '':
            addr_list.append(term)
        addr_list.append(('%s %s' % (entry[cm.zip_code], entry[cm.city_e])).strip())
        entry[cm.addr_e] = ', '.join(addr_list)

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

    return tuple(store_list)


def fetch_cities(data):
    ret = gs.look_up(data['country'], 1)
    if ret is None:
        return ()

    country = ret['name_e']
    city_map = data['city_map']
    results = []
    if country in city_map:
        for city in city_map[country]:
            d = data.copy()
            d['country'] = country
            d['city'] = city
            d['city_lat'] = city_map[country][city]['lat']
            d['city_lng'] = city_map[country][city]['lng']
            results.append(d)
    return tuple(results)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://global.ecco.com/en/service/find-a-store',
                'brand_id': 10114, 'brandname_e': u'Ecco', 'brandname_c': u'爱步',
                'city_map': gen_city_map()}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


