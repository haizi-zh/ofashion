# coding=utf-8
import json
import string
import re
import common as cm
import geosense as gs

__author__ = 'Zephyre'

db = None
log_name = 'sisley_log.txt'


def fetch_countries(data):
    url = data['url']
    try:
        body = cm.get_data(url)
    except Exception, e:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()

    m = re.search(ur'<select name="country_list"[^<>]*>(.+?)</select>', body, re.S)
    if not m:
        cm.dump('Error in fetching countries: %s' % url, log_name)
        return ()
    sub = m.group(1).strip()
    results = []
    for m in re.findall(ur'<option value="([^"]+)"', sub):
        country_tag = m.strip()
        country = m.strip().upper()
        if country_tag == 'Cote d Ivoire':
            country = u'CÔTE D’IVOIRE'
        d = data.copy()
        d['country_tag'] = country_tag
        d['country'] = country
        results.append(d)
    return tuple(results)


def fetch_stores(data):
    url = data['data_url']
    param = {'country': data['country_tag'], 'city': data['city_tag'],
             'adutl': ' 01', 'kids': ' 02', 'undercolor': ' 06', 'togetmap': 'mapdata'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching stores: %s, %s' % (url, param), log_name)
        return ()
    body = cm.extract_closure(body, ur'\(', ur'\)')[0][1:-1]
    sub = json.loads(body)['data']['xml_dt']
    store_list = []
    for m in re.findall(ur'<marker[^<>]+/\s*>', sub):
        entry = cm.init_store_entry(data['brand_id'], data['brandname_e'], data['brandname_c'])
        entry[cm.country_e] = data['country']
        entry[cm.city_e] = data['city']

        # m1 = re.search(ur'name\s*=\s*"([^"]+)"', m)
        # entry[cm.name_e] = m1.group(1) if m1 else ''

        m1 = re.search(ur'address\s*=\s*"([^"]+)"', m)
        if m1:
            addr = re.sub(ur'\.textmap\{.*\}', '', cm.reformat_addr(m1.group(1)))
            addr_list = [tmp.strip() for tmp in addr.split(',')]
            tel = cm.extract_tel(addr_list[-1])
            if tel != '':
                entry[cm.tel] = tel
                del addr_list[-1]
            entry[cm.addr_e] = ', '.join(addr_list)

        m1 = re.search(ur'lat\s*=\s*"([^"]+)"', m)
        val = m1.group(1)
        try:
            entry[cm.lat] = string.atof(val) if val != '' else ''
        except (ValueError, KeyError, TypeError) as e:
            cm.dump('Error in fetching lat: %s' % str(e), log_name)
        m1 = re.search(ur'lng\s*=\s*"([^"]+)"', m)
        val = m1.group(1)
        try:
            entry[cm.lng] = string.atof(val) if val != '' else ''
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


def fetch_cities(data):
    url = data['data_url']
    param = {'country_code': data['country_tag'], 'toget': 'citylist'}
    try:
        body = cm.get_data(url, param)
    except Exception, e:
        cm.dump('Error in fetching cities: %s, %s' % (url, param), log_name)
        return ()
    body = cm.extract_closure(body, ur'\(', ur'\)')[0][1:-1]
    raw = json.loads(body)['city']
    results = []
    for m in re.findall(ur'<option value="([^"]+)">', raw):
        d = data.copy()
        d['city_tag'] = cm.html2plain(m).strip()
        d['city'] = d['city_tag'].upper()
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
            # 城市列表
            return [{'func': lambda data: func(data, level + 1), 'data': s} for s in fetch_cities(data)]
        if level == 2:
            # 商店
            return [{'func': None, 'data': s} for s in fetch_stores(data)]
        else:
            return ()

    # Walk from the root node, where level == 1.
    if data is None:
        data = {'data_url': 'http://www.sisley.com/storelocator/map_ajax.php',
                'url': 'http://www.sisley.com/store-locator',
                'brand_id': 10322, 'brandname_e': u'Sisley', 'brandname_c': u'希思黎'}

    global db
    db = cm.StoresDb()
    db.connect_db(user=user, passwd=passwd)
    db.execute(u'DELETE FROM %s WHERE brand_id=%d' % ('stores', data['brand_id']))

    results = cm.walk_tree({'func': lambda data: func(data, 0), 'data': data})
    db.disconnect_db()
    cm.dump('Done!', log_name)

    return results


