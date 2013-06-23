# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'h&m_log.txt'


def fetch_countries(data):
    url = data['country_url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    results = []
    for m in re.findall(ur'<storeCountry><countryId>([A-Z]{2})</countryId><name>([^<>]+?)</name>', body):
        d = data.copy()
        d['country_code'] = m[0]
        d['country'] = m[1]
        d['url'] = '%s/%s' % (data['store_url'], m[0])
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url, hdr={'Accept': 'application/json'})
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    body = cm.extract_closure(body, ur'\{', ur'\}')[0]
    raw = json.loads(body)[u'storesCompleteResponse'][u'storesComplete'][u'storeComplete']
    if not isinstance(raw, list):
        raw = [raw]

    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        if 'name' in s and s['name'] is not None:
            tmp = s['name']
            if isinstance(tmp, str) or isinstance(tmp, unicode):
                entry[cm.name_e] = cm.html2plain(s['name'])

        if 'address' in s and s['address'] is not None:
            tmp = s['address']
            if 'addressLine' in tmp and tmp['addressLine'] is not None:
                tmp = tmp['addressLine']
                if isinstance(tmp, list):
                    for i in xrange(len(tmp)):
                        tmp[i] = unicode(tmp[i])
                    entry[cm.addr_e] = ', '.join(tmp)
                else:
                    entry[cm.addr_e] = unicode(tmp)


        entry[cm.country_e] = data['country_code']
        if 'latitude' in s and s['latitude'] is not None and s['latitude'] != '':
            entry[cm.lat] = string.atof(s['latitude'])
        if 'longitude' in s and s['longitude'] is not None and s['longitude'] != '':
            entry[cm.lng] = string.atof(s['longitude'])
        if 'openingHours' in s and s['openingHours'] is not None:
            tmp = s['openingHours']
            if tmp is not None and 'openingHour' in tmp:
                tmp = tmp['openingHour']
                if tmp is not None and isinstance(tmp, list):
                    entry[cm.hours] = ', '.join(tmp)
        if 'phone' in s and s['phone'] is not None:
            entry[cm.tel] = s['phone']
        if 'region' in s and s['region'] is not None and 'name' in s['region']:
            tmp = s['region']['name']
            if tmp is not None:
                entry[cm.province_e] = tmp.strip().upper()
        if 'city' in s and s['city'] is not None:
            entry[cm.city_e] = s['city'].strip().upper()
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
        data = {'country_url': 'http://www.hm.com/rest/storelocator/locations/1.0/locale/en_US/countries/',
                'store_url': 'http://www.hm.com/rest/storelocator/stores/1.0/locale/en_US/country',
                'url': 'http://www.gucci.com/us/storelocator',
                'brand_id': 10155, 'brandname_e': u'H&M', 'brandname_c': u'海恩斯莫里斯'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results