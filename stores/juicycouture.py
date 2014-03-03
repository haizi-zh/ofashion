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
        body = cm.get_data(url)
    except Exception:
        print 'Error occured: %s' % url
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    start = body.find(u'<label>COUNTRY</label>')
    if start == -1:
        print 'Error occured in fetching country list: %s' % url
    body = cm.extract_closure(body[start:], ur'<select\b', ur'</select>')[0]

    results = []
    for m in re.findall(ur'<option value="([A-Z]{2})">.+?</option>', body):
        d = data.copy()
        d['country_code'] = m
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url, {'countryCode': data['country_code']})
    except Exception:
        print 'Error occured in fetching stores: %s, %s' % (url, data['country_code'])
        dump_data = {'level': 0, 'time': cm.format_time(), 'data': {'url': url}, 'brand_id': data['brand_id']}
        cm.dump(dump_data)
        return []

    raw = json.loads(body)['stores']
    store_list = []
    for key in raw:
        store = raw[key]
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = store['name']
        addr = store['address1']
        if store['address2'] != '':
            addr += ', ' + store['address2']
        entry[cm.addr_e] = addr
        entry[cm.zip_code] = store['postalCode']
        entry[cm.city_e] = store['city'].strip().upper()
        entry[cm.province_e] = store['stateCode'].strip().upper()
        entry[cm.country_e] = data['country_code']
        entry[cm.tel] = store['phone']
        entry[cm.fax] = store['fax']
        entry[cm.email] = store['email']
        entry[cm.hours] = store['storeHours']
        if store['latitude'] != '':
            entry[cm.lat] = string.atof(store['latitude'])
        if store['longitude'] != '':
            entry[cm.lng] = string.atof(store['longitude'])

        gs.field_sense(entry)
        print '(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                          entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                          entry[cm.continent_e])
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
            # 国家列表
            return [{'func': lambda data: func(data, 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'home_url': 'http://www.juicycouture.com/storelocator',
                'url': 'http://www.juicycouture.com/on/demandware.store/Sites-JuicyCouture-Site/en_US/Stores-GetNearestStores',
                'brand_id': 10186, 'brandname_e': u'Juicy Couture', 'brandname_c': u'橘滋'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results