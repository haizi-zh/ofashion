# coding=utf-8
import json
import re
from stores import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'mango_log.txt'


def fetch_countries(data):
    url = data['country_url']
    param = {'myid': '400-all', 'idioma': 'in'}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return []

    results = []
    for c in json.loads(body):
        if c['title'].strip() == '':
            continue
        d = data.copy()
        d['country'] = cm.html2plain(c['title']).strip().upper()
        d['key'] = c['key']
        results.append(d)
    return results


def fetch_stores(data):
    url = data['store_url']
    param = {'myid': data['key'], 'idioma': 'in'}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching countries: %s, %s' % (url, param), log_name)
        return []

    store_list = []
    for s in json.loads(body):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = cm.extract_city(data['city'])[0]
        entry[cm.name_e] = cm.reformat_addr(s['title'])

        m = re.search(ur'(.+?)-\s*<', s['key'])
        addr_list = [entry[cm.name_e]]
        if m is not None:
            m1 = re.search(ur'-+', m.group(1))
            if m1 is not None:
                tmp = [m.group(1)[:m1.start()], m.group(1)[m1.end():]]
            else:
                tmp = [m.group(1)]
            if len(tmp) > 1:
                entry[cm.tel] = cm.extract_tel(tmp[1])
            m1 = re.search(ur'\d{4,}', tmp[0])
            if m1 is not None:
                entry[cm.zip_code] = m1.group()
            addr_list.append(tmp[0].strip())
        entry[cm.addr_e] = ', '.join(addr_list)

        gs.field_sense(entry)
        ret = gs.addr_sense(entry[cm.addr_e])
        if ret[0] is not None and entry[cm.country_e] == '':
            entry[cm.country_e] = ret[0]
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


def fetch_cities(data):
    url = data['city_url']
    param = {'myid': data['key'], 'idioma': 'in'}
    try:
        body = cm.get_data(url, param)
    except Exception:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return []

    results = []
    try:
        cities = json.loads(body)
    except ValueError, e:
        cm.dump('Error in fetching cities: %s, %s, %s' % (url, param, body), log_name)
        return []

    for c in cities:
        if c['title'] == 'All':
            continue
        d = data.copy()
        d['city'] = c['title'].strip().upper()
        d['key'] = c['key']
        results.append(d)
    return results


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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return []

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'country_url': 'http://shop.mango.com/web/oi/servicios/tiendas/getpaises.php',
                'city_url': 'http://shop.mango.com/web/oi/servicios/tiendas/getciudades.php',
                'store_url': 'http://shop.mango.com/web/oi/servicios/tiendas/getdirecciones.php',
                'brand_id': 10235, 'brandname_e': u'Mango', 'brandname_c': u'芒果'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()

    return results