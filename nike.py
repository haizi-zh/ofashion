# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'nike_log.txt'
store_map = {}


def fetch_countries(data):
    url = data['country_url']
    try:
        body = cm.get_data(url, {'display_country': 'CN'})
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    m = re.search(ur'<select class="country-selector[^<>]+>(.+?)</select>', body, re.S)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    results = []
    for m in re.findall(ur'<option value="([A-Z]{2})"[^<>]+>([^<>]+)</option>', sub):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = cm.html2plain(m[1]).strip().upper()
        # if m[0] == 'US':
        results.append(d)
    return tuple(results)


def fetch_store_detail(data):
    url = data['data_url']
    param = {'format': 'JSON', 'location_id': data['store_id'], 'type': 'location'}
    try:
        s = json.loads(cm.get_data(url, param))['locations'][0]
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_id = data['store_id']
    entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
    store_map[store_id] = entry

    entry[cm.city_e] = cm.html2plain(s['city']).strip().upper()
    entry[cm.country_e] = s['countryCode']
    val = s['fax']
    if val:
        entry[cm.fax] = val.strip()
    val = s['phone']
    if val:
        entry[cm.tel] = val.strip()
    val = s['geoLat']
    if val:
        entry[cm.lat] = val
    val = s['geoLon']
    if val:
        entry[cm.lng] = val
    val = s['hours']
    if val:
        entry[cm.hours] = val.strip()
    entry[cm.name_e] = cm.html2plain(s['name'])
    val = s['postalCode']
    if val:
        entry[cm.zip_code] = val.strip()
    val = s['stateCode']
    if val:
        entry[cm.province_e] = cm.html2plain(val).strip().upper()
    val = s['description']
    if val:
        entry[cm.comments] = cm.html2plain(val).strip().upper()
    addr_list = []
    for key in ('street', 'street2'):
        if not s[key]:
            continue

        term = cm.reformat_addr(s[key])
        if term != '':
            addr_list.append(term)
    entry[cm.addr_e] = ', '.join(addr_list)
    entry[cm.store_type] = ', '.join(item['code'] for item in s['categories'])

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
    return entry


def fetch_stores(data):
    url = data['data_url']
    param = {'format': 'JSON', 'nike_owned_filter': True, 'sw_lat': data['lat'] - 0.5, 'sw_lon': data['lng'] - 0.5,
             'ne_lat': data['lat'] + 0.5, 'ne_lon': data['lng'] + 0.5, 'type': 'limited'}
    try:
        raw = json.loads(cm.get_data(url, param))['locations']
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()

    store_list = []
    for s in raw:
        store_id = s['id']
        if store_id in store_map:
            continue

        data['store_id'] = store_id
        entry = fetch_store_detail(data)
        store_list.append(entry)

    return tuple(store_list)


def fetch_store_list(data):
    #
    url = data['country_url']
    param = {'display_country': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching store list: %s, %s' % (url, param), log_name)
        return ()

    results = []
    for m in re.findall(ur'<a href="/us/en_us/sl/store-locator\?lat=([^&]+)&lon=([^&]+)&storeID=(\d+)[^"]*"', body):
        d = data.copy()
        d['lat'] = string.atof(m[0])
        d['lng'] = string.atof(m[1])
        d['store_id'] = string.atoi(m[2])
        # if m[2] == '600012629':
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
            # 商店列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_store_list(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.nike.com/store-locator/v2/locations',
                'country_url': 'http://www.nike.com/us/en_us/sl/find-a-store',
                'brand_id': 10277, 'brandname_e': u'Nike', 'brandname_c': u'耐克'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


