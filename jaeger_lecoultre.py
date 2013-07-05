# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'jaeger_lecoultre_log.txt'


def fetch_continents(data):
    vals = {2: 'Africa', 4: 'Asia', 6: 'Europe', 8: 'North America', 10: 'Oceania', 12: 'South America'}
    results = []
    for key in vals:
        d = data.copy()
        d['continent_id'] = key
        d['continent'] = vals[key]
        results.append(d)
    return results


def fetch_countries(data):
    url = data['host'] + data['continent_url'] % data['continent_id']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    body = body.decode('unicode_escape')
    results = []
    for m in re.findall(ur'<option value="(\d+)_([A-Z]{2})">([^<>]+)<', body):
        d = data.copy()
        d['country_id'] = string.atoi(m[0])
        d['country_code'] = m[1]
        d['country'] = m[2].strip()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['host'] + data['country_url'] % data['country_id']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    raw = json.loads(body)['rawPos']
    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        addr_list = []
        for tmp2 in [cm.html2plain(s[tmp1]).strip() for tmp1 in ['address%d' % v for v in xrange(1, 5)]]:
            if tmp2 != '':
                addr_list.append(tmp2)
        entry[cm.addr_e] = ', '.join(addr_list)
        entry[cm.city_e] = cm.extract_city(s['city']['name'])[0]
        entry[cm.country_e] = s['country']['countryCode']
        entry[cm.email] = s['email']
        entry[cm.fax] = s['fax']
        if s['latitude'] != '':
            entry[cm.lat] = string.atof(s['latitude'])
        if s['longitude'] != '':
            entry[cm.lng] = string.atof(s['longitude'])
        entry[cm.hours] = cm.reformat_addr(s['openingSchedule'])
        phone_list = []
        for key in ['phone1', 'phone2']:
            if s[key].strip() != '':
                phone_list.append(s[key].strip())
        entry[cm.tel] = ', '.join(phone_list)
        entry[cm.zip_code] = s['postalCode']
        entry[cm.name_e] = s['shopName']
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
            # if level == 2:
        #     # 州列表
        #     return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_states(data)]
        # if level == 3:
        #     # 城市列表
        #     return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店列表
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'host': 'http://collection.jaeger-lecoultre.com',
                'continent_url': '/WW/en/boutique/continent/%d/countries',
                'country_url': '/WW/en/boutique/country/%d/children-shops',
                'brand_id': 10178, 'brandname_e': u'Jaeger Le Coultre', 'brandname_c': u'积家'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results