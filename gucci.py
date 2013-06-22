# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'gucci_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    m = re.search(ur'storelocator_options\s*=\s*', body)
    if m is None:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return []

    sub = cm.extract_closure(body[m.end():], ur'\{', ur'\}')[0].replace("'", '"')
    countries = json.loads(sub)['countries']
    results = []
    for c in countries:
        d = data.copy()
        d['country_code'] = c['country_code']
        d['lat'] = string.atof(c['location']['lat'])
        d['lng'] = string.atof(c['location']['lng'])
        d['units'] = c['units']
        results.append(d)
    return results


def fetch_stores(data):
    url = data['data_url']
    param = {'lat': data['lat'], 'lng': data['lng'], 'country_code': data['country_code'],
             'units': data['units'], 'store_type': ''}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return []

    # 引号问题
    tmp = ''
    pat = re.compile(ur'(?<!\\)"')
    for line in body.split('\n'):
        m = re.search(ur'[\'"](\w+)[\'"]\s*:(.+?)(,?)\s*$', line)
        if m is None:
            tmp += line + '\n'
            continue
        else:
            key = m.group(1)
            m1 = re.search(ur'^\s*"(.+)"\s*$', m.group(2))
            if m1 is not None:
                val = re.sub(pat, ur'\"', m1.group(1))
                tmp += '"%s": "%s"' % (key, val) + m.group(3) + '\n'
            else:
                val = m.group(2)
                tmp += '"%s": %s' % (key, val) + m.group(3) + '\n'

    pat = re.compile(ur',\s*"terms":\s*\{.+?\}', re.S)
    tmp = re.sub(pat, '', tmp)

    try:
        raw = json.loads(tmp)['stores']
    except ValueError, e:
        print e
    store_list = []
    for s in raw:
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.name_e] = s['name']
        entry[cm.country_e] = data['country_code']
        addr = s['address']
        m = re.search(ur'<span class=\"locality\">([^<>]+?)</span>', addr)
        if m is not None:
            entry[cm.city_e] = m.group(1).strip().upper()
        m = re.search(ur'<span class=\"region\">([^<>]+?)</span>', addr)
        if m is not None:
            entry[cm.province_e] = m.group(1).strip().upper()
        m = re.search(ur'<span class=\"postal-code\">([^<>]+?)</span>', addr)
        if m is not None:
            entry[cm.zip_code] = m.group(1).strip()
        entry[cm.addr_e] = cm.reformat_addr(addr)
        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e], entry[cm.country_e])
        if ret[1] is not None and entry[cm.province_e] == '':
            entry[cm.province_e] = ret[1]
        if ret[2] is not None and entry[cm.city_e] == '':
            entry[cm.city_e] = ret[2]

        entry[cm.tel] = s['phone']
        if s['lat'] is not None and s['lat'] != '':
            entry[cm.lat] = string.atof(s['lat'])
        if s['lng'] is not None and s['lng'] != '':
            entry[cm.lng] = string.atof(s['lng'])
        entry[cm.store_type] = s['type']
        if s['event_link'] is not None:
            cm.dump('Event link: %s' % s['event_link'], log_name)
        if s['additional'] != '':
            entry[cm.comments]=s['additional'].strip()

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
        data = {'data_url': 'http://www.gucci.com/us/storelocator/stores.json',
                'url': 'http://www.gucci.com/us/storelocator',
                'brand_id': 10152, 'brandname_e': u'Gucci', 'brandname_c': u'古驰'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results

