# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None


def fetch_countries(data):
    url = data['home_url']
    try:
        html = cm.get_data(url, {'brand': 'oasis', 'countryISO': 'GB'})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    country_list = []
    for m in re.findall(ur'<option value="([A-Z]{2})">(.+?)</option>', html):
        d = data.copy()
        d['country_code'] = m[0]
        country = m[1].strip().upper()
        ret = gs.look_up(country, 1)
        if ret is not None:
            country = ret['name_e']
        d['country_e'] = country
        country_list.append(d)
    return country_list


def fetch_cities(data):
    url = data['city_url']
    code = data['country_code']
    try:
        html = cm.get_data(url, {'country': code, 'brand': 'oasis'})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(html)
    city_list = []
    for m in raw:
        # city=raw['city'].strip().upper()
        # ret=gs.look_up(city, 3)
        # if ret is not None:
        #     city=ret['name_e']
        d = data.copy()
        d['city_e'] = m['city']
        city_list.append(d)

    return city_list


def fetch_stores(data):
    url = data['city_url']
    code = data['country_code']
    city = data['city_e']

    try:
        html = cm.get_data(url, {'country': code, 'brand': 'oasis', 'city': city})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(html)
    store_list = []
    for m in raw:
        d = data.copy()
        d['store_name'] = m['name']
        # d['lat'] = string.atof(m['latitude'])
        # d['lng'] = string.atof(m['longitude'])
        d['lat'] = m['latitude']
        d['lng'] = m['longitude']
        store_list.append(d)
    return store_list


def fetch_store_details(data):
    # http://maps.oasis-stores.com/index-v2.php?coutnryISO=GB&brand=oasis&lat=51.42014&lng=-0.20954
    url = data['store_url']
    code = data['country_code']
    city = data['city_e']

    try:
        html = cm.get_data(url, {'latitude': data['lat'], 'longitude': data['lng'], 'brand': 'oasis'})
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(html)
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    entry[cm.name_e] = raw['name']
    addr_list = []
    for i in xrange(1, 4):
        tmp = cm.html2plain(raw['address%d' % i]).strip()
        if tmp != '':
            addr_list.append(tmp)
    entry[cm.addr_e] = ', '.join(addr_list)
    state = raw['countryRegion']
    if state is not None and state.strip() != '':
        entry[cm.province_e] = state.strip().upper()
    state = raw['state']
    if state is not None and state.strip() != '':
        entry[cm.province_e] = state.strip().upper()
    state = raw['county']
    if state is not None and state.strip() != '':
        entry[cm.province_e] = state.strip().upper()
    entry[cm.zip_code] = raw['postcode']
    entry[cm.country_e] = data['country_e']
    entry[cm.city_e] = cm.extract_city(data['city_e'])[0]
    entry[cm.lat] = string.atof(data['lat'])
    entry[cm.lng] = string.atof(data['lng'])
    entry[cm.tel] = raw['phone']
    entry[cm.email] = raw['email']
    tmp = ['mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun']
    entry[cm.hours] = ', '.join([raw[d + '_open_times'] for d in tmp])
    gs.field_sense(entry)
    print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                      entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                      entry[cm.continent_e])
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
            return [{'func': lambda data: func(data, 1), 'data': c} for c in fetch_countries(data)]
        elif level == 1:
            # 城市列表
            return [{'func': lambda data: func(data, 2), 'data': c} for c in fetch_cities(data)]
        elif level == 2:
            # 商店信息
            return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_stores(data)]
        elif level == 3:
            # 商店详情
            return [{'func': None, 'data': s} for s in fetch_store_details(data)]
        # elif level == 2:
        #     # 城市列表
        #     return [{'func': lambda data: func(data, 3), 'data': s} for s in fetch_cities(data)]
        # elif level == 3:
        #     # 商店的具体信息
        #     return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://maps.oasis-stores.com/index-v2.php',
                'city_url': 'http://maps.oasis-stores.com/php/country-stores.php',
                'store_url': 'http://maps.oasis-stores.com/php/store-data.php',
                # 'store_url': 'http://maps.oasis-stores.com/php/list-stores.php',
                'brand_id': 10285, 'brandname_e': u'Oasis', 'brandname_c': u'绿洲'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results