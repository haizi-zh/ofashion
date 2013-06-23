# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'versace_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'<a href="[^"]*" class="[^"]*">BOUTIQUE</a>', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []
    body = cm.extract_closure(body[m.end():], ur'<ul\b', ur'</ul>')[0]

    results = []
    for m in re.findall(ur'<a href="([^"]+)">([^<>]+)</a>', body):
        d = data.copy()
        d['region'] = m[1].strip()
        d['url'] = data['host'] + m[0].strip()
        results.append(d)
    return results


def fetch_stores(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []

    m = re.search(ur'var\s+geoShops\s*=', body)
    if m is None:
        cm.dump('Error in fetching stores: %s' % url, log_name)
        return []
    tmp = cm.extract_closure(body[m.end():], ur'\[', ur'\]')[0]
    raw = json.loads(re.sub(ur'(?<!")(city|address|lat|lng)(?!")', ur'"\1"', tmp))

    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.city_e] = s['city'].strip().upper()
        if s['lat'] is not None and s['lat'] != '':
            entry[cm.lat] = string.atof(s['lat'])
        if s['lng'] is not None and s['lng'] != '':
            entry[cm.lng] = string.atof(s['lng'])

        addr = cm.reformat_addr(s['address'])
        pat = re.compile(ur'ph[\.:](.*)$', re.I)
        m = re.search(pat, addr)
        if m is not None:
            entry[cm.tel] = m.group(1).strip()
        entry[cm.addr_e] = re.sub(pat, '', addr).strip()

        addr1 = re.sub(ur'[\u2e80-\u9fff]+', '', '%s, %s' % (addr, s['city'])).strip()
        ret = gs.geocode(addr1, '%f,%f' % (entry[cm.lat], entry[cm.lng]))
        if ret is None:
            ret = gs.geocode(addr1)
        if ret is None:
            ret = gs.geocode(latlng='%f,%f' % (entry[cm.lat], entry[cm.lng]))

        if ret is not None:
            city = ''
            province = ''
            country = ''
            zip_code = ''
            tmp = ret[0]['address_components']
            for v in tmp:
                if 'locality' in v['types']:
                    city = v['long_name'].strip().upper()
                elif 'administrative_area_level_1' in v['types']:
                    province = v['long_name'].strip().upper()
                elif 'country' in v['types']:
                    country = v['long_name'].strip().upper()
                elif 'postal_code' in v['types']:
                    zip_code = v['long_name'].strip()
            entry[cm.country_e] = country
            entry[cm.province_e] = province
            entry[cm.city_e] = city
            entry[cm.zip_code] = zip_code
        else:
            ret = gs.addr_sense(addr1)
            if ret[0] is not None:
                entry[cm.country_e] = ret[0]
            if ret[1] is not None:
                entry[cm.province_e] = ret[1]
            if ret[2] is not None:
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
            # 国家列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_countries(data)]
        if level == 1:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'url': 'http://www.versace.com/en/find-a-boutique',
                'host': 'http://www.versace.com',
                'brand_id': 10373, 'brandname_e': u'Versace', 'brandname_c': u'范思哲'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results
