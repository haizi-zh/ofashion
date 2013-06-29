# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'chopard_log.txt'


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.post_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()
    raw = json.loads(body)

    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        entry[cm.country_e] = cm.html2plain(s['country']).strip().upper() if s['country'] else ''
        entry[cm.city_e] = cm.html2plain(s['city']).strip().upper() if s['city'] else ''
        entry[cm.province_e] = cm.html2plain(s['region']).strip().upper() if s['region'] else ''
        entry[cm.name_e] = cm.html2plain(s['name']) if s['name'] else ''
        entry[cm.hours] = s['opening_hours'] if s['opening_hours'] else ''
        entry[cm.tel] = s['phone'] if s['phone'] else ''
        entry[cm.zip_code] = s['zipcode'] if s['name'] else ''

        addr_list = []
        for term in (s[key] for key in ('address_%d' % idx for idx in xrange(1, 4))):
            if term:
                term = cm.html2plain(term).strip()
                if term != '':
                    addr_list.append(term)
        entry[cm.addr_e] = ', '.join(addr_list)

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

        cm.dump('(%s / %d) Found store: %s, %s (%s, %s)' % (data['brandname_e'], data['brand_id'],
                                                            entry[cm.name_e], entry[cm.addr_e], entry[cm.country_e],
                                                            entry[cm.continent_e]), log_name)
        db.insert_record(entry, 'stores')
        store_list.append(entry)

    return tuple(store_list)


def fetch(level=1, data=None, user='root', passwd=''):
    def func(data, level):
        """
        :param data:
        :param level: 0：国家；1：城市；2：商店列表
        """
        if level == 0:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.chopard.com/storelocator/ajax/getStorePoints',
                'brand_id': 10080, 'brandname_e': u'Chopard', 'brandname_c': u'萧邦'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


