# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'victoriassecret_log.txt'


def fetch_countries(data):
    vals = {'CAN': 'CA', 'USA': 'US'}
    results = []
    for key in vals:
        d = data.copy()
        d['country_code'] = key
        d['country'] = vals[key]
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    param = {'state': data['country_code']}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for s in json.loads(body)['stores']:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        entry[cm.country_e] = data['country']
        entry[cm.city_e] = cm.html2plain(s['city']).strip().upper()

        val = s['latitudeDeg']
        if val is not None and val.strip() != '':
            entry[cm.lat] = string.atof(val)
        val = s['longitudeDeg']
        if val is not None and val.strip() != '':
            entry[cm.lng] = string.atof(val)

        entry[cm.name_e] = s['mallName'].strip()
        entry[cm.addr_e] = s['streetAddress'].strip()
        entry[cm.tel] = s['phone'].strip()
        state = s['state'].strip().upper()
        if data['country'] == 'US':
            entry[cm.province_e] = state
        entry[cm.zip_code] = s['postalCode'].strip()

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
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.victoriassecret.com/store-locator/store-locator/stateSearch.action',
                'brand_id': 10376, 'brandname_e': u"Victoria's Secret", 'brandname_c': u'维多利亚的秘密'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results


