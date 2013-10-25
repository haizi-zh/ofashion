# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'bershka_log.txt'
store_map = {}


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'SEARCH_URL:\s*"([^"]+)"', body)
    if m is None:
        cm.dump('Error in finding SEARCH_URL: %s' % url, log_name)
        return []
    data['search_url'] = m.group(1)

    m = re.search(ur'<select class="styled" name="filt_shops_country_input"[^<>]*>(.+?)</select>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    sub = m.group(1)
    results = []
    for item in re.findall(ur'<option[^<>]+value\s*=\s*"([A-Z]{2})"[^<>]*>([^<>]+)', sub):
        d = data.copy()
        d['country_code'] = item[0]
        d['country'] = cm.html2plain(item[1]).strip().upper()
        results.append(d)
    return tuple(results)


def gen_city_map():
    with open('city_lite.dat', 'r') as f:
        sub = f.readlines()
    return json.loads(sub[0])


def fetch_cities(data):
    ret = gs.look_up(data['country'], 1)
    if ret is None:
        return []

    country = ret['name_e']
    city_map = gen_city_map()
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


def fetch_stores(data):
    url = data['search_url'].replace('_country_', data['country_code']).replace('_latitude_',
                                                                                '%f' % data['city_lat']).replace(
        '_longitude_', '%f' % data['city_lng'])
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    store_list = []
    for s in json.loads(body)['near']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        store_id = string.atoi(s['physicalStoreId'])
        if store_id in store_map:
            item = store_map[store_id]
            cm.dump('Duplicated: %s, %s' % (item[cm.addr_e], item[cm.country_e]), log_name)
            continue

        entry[cm.country_e] = cm.html2plain(s['country']).strip().upper()
        entry[cm.city_e] = cm.html2plain(s['city']).strip().upper()
        entry[cm.addr_e] = cm.reformat_addr(s['address'])

        val = str(s['latitude'])
        if val is not None and val.strip() != '':
            entry[cm.lat] = string.atof(val)
        val = str(s['longitude'])
        if val is not None and val.strip() != '':
            entry[cm.lng] = string.atof(val)

        entry[cm.name_e] = cm.html2plain(s['name'])
        entry[cm.tel] = s['phone1'].strip() if s['phone1'] else ''
        entry[cm.zip_code] = s['postalCode'].strip()
        entry[cm.store_type] = s['sections'].strip()

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
        store_map[store_id] = entry

    return store_list


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
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.bershka.com/webapp/wcs/stores/servlet/category/bershkagb/en/bershkasales/308507/Stores',
                'brand_id': 10040, 'brandname_e': u'Bershka', 'brandname_c': u'巴适卡'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


