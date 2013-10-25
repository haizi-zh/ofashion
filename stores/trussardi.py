# coding=utf-8
import json
import string
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'trussadi_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()

    # 国家
    m = re.search(ur'<div class="all-nations"[^<>]*>(.+?)</div>', body, re.S)
    if not m:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1)
    country_map = dict(
        (string.atoi(m[0]), cm.html2plain(m[1]).strip().upper()) for m in
        re.findall(ur'value="(\d+)"\s*>([^<>]+)', sub))

    # 城市
    m = re.search(ur'<div class="all-cities"[^<>]*>(.+?)</div>', body, re.S)
    if not m:
        cm.dump('Error in fetching cities: %s' % url, log_name)
        return ()
    sub = m.group(1)
    city_map = {}
    for m in re.findall(ur'data-country-id="(\d+)"[^<>]+value="(\d+)"\s*>([^<>]+)', sub):
        country_id = string.atoi(m[0])
        city_id = string.atoi(m[1])
        city = cm.html2plain(m[2]).strip().upper()
        city_map[city_id] = {'name': city, 'country': country_id}

    # 商店
    m = re.search(ur'var\s+storelocatorMarkers\s*=\s*', body)
    if not m:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return ()
    store_sub = cm.extract_closure(body[m.end():], ur'\[', ur'\]')[0]
    raw = json.loads(store_sub)[0]

    store_list = []
    for s in raw.values():
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])

        city = city_map[s['parent']]
        entry[cm.country_e] = country_map[city['country']]
        entry[cm.city_e] = city['name']
        entry[cm.name_e] = cm.html2plain(s['title']).strip()
        entry[cm.addr_e] = cm.reformat_addr(s['address'])
        entry[cm.tel] = s['store_phone'].strip()
        entry[cm.hours] = s['store_hours'].strip()

        latlng = s['latlong']
        try:
            entry[cm.lat] = string.atof(str(latlng['lat'])) if latlng['lat'] != '' else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        try:
            entry[cm.lng] = string.atof(str(latlng['lng'])) if latlng['lng'] != '' else ''
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
        data = {'url': 'http://experience.trussardi.com/en/pages/store-locator/',
                'brand_id': 10360, 'brandname_e': u'Trussardi', 'brandname_c': u'楚萨迪'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


